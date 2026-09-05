package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
)

// Level 是刷盘级别（§6.2）。
type Level int

const (
	// L0 写穿：充值、开箱、交易。同步落 Redis 成功后才改内存回包。最大丢失 0。
	L0 Level = 0
	// L1 高频：货币、等级、背包。脏标记 + 3~5s。最大丢失 5s。
	L1 Level = 1
	// L2 低频：设置、社交。脏标记 + 60s。最大丢失 60s。
	L2 Level = 2
	// L3 临时：战斗中间态。不刷盘。全部丢失可接受。
	L3 Level = 3
)

func (l Level) String() string {
	switch l {
	case L0:
		return "L0"
	case L1:
		return "L1"
	case L2:
		return "L2"
	default:
		return "L3"
	}
}

// ModuleLevel 返回玩家模块的刷盘级别。
//
// 注意：货币虽在 base 模块，但充值、开箱这类「丢了会导致资产不一致」的操作
// 走的是 L0 写穿路径，不依赖这里的定时刷盘。
func ModuleLevel(m Module) Level {
	switch m {
	case ModBase, ModBag, ModQuest:
		return L1
	case ModSocial, ModMail:
		return L2
	default:
		return L1
	}
}

// HashWrite 描述一次 HASH 写入（profile 只读摘要）。
type HashWrite struct {
	Key    string
	TTLSec int64
	Fields []any // field/value 交替
}

// Entity 是一次刷盘的最小单位（一个玩家或一个房间）。
//
// Keys 中的值必须已经在 Actor goroutine 内序列化完成 ——
// 读内存必须如此，IO goroutine 不得触碰 Actor 的数据结构（§6.3）。
type Entity struct {
	ID      uint64
	Keys    map[string][]byte
	Hash    *HashWrite
	Modules []Module  // 本批覆盖的模块，失败时按此重新标脏
	DirtyAt time.Time // 最早标脏时刻，用于「脏数据滞留时长」指标
}

// Batch 是一次提交给 IO pool 的刷盘批次。
type Batch struct {
	Shard    uint32
	Epoch    int64
	Level    Level
	Entities []*Entity
}

// Result 是刷盘结果，回报给 Actor。
type Result struct {
	Shard  uint32
	Epoch  int64
	Level  Level
	Failed []*Entity // 需要重新标脏的实体，绝不丢弃
	Err    error
	Fenced bool // true = epoch 过期，必须丢弃内存并停止服务
}

// ErrBacklog 表示 flushCh 已满。
//
// 「flushCh 满时走 default 重新标脏并告警，绝不阻塞 Actor。
// 积压说明 Redis 已扛不住，阻塞会让故障扩散成全服卡死」（§6.3）。
var ErrBacklog = errors.New("store: flushCh 已满，重新标脏")

// Flusher 是刷盘 IO pool。
type Flusher struct {
	ch      chan *Batch
	fencer  *Fencer
	kind    string
	workers int
	batchSz int

	onResult func(*Result)

	wg       sync.WaitGroup
	stopOnce sync.Once
	quit     chan struct{}
}

// NewFlusher 构造刷盘器。onResult 会在 IO goroutine 中被调用，
// 实现方只应把结果投递回对应分片的 Actor，不得在其中做业务。
func NewFlusher(fencer *Fencer, kind string, chanSize, workers, batchSize int, onResult func(*Result)) *Flusher {
	if workers <= 0 {
		workers = 4
	}
	if chanSize <= 0 {
		chanSize = 512
	}
	if batchSize <= 0 {
		batchSize = 128
	}
	return &Flusher{
		ch:       make(chan *Batch, chanSize),
		fencer:   fencer,
		kind:     kind,
		workers:  workers,
		batchSz:  batchSize,
		onResult: onResult,
		quit:     make(chan struct{}),
	}
}

// Start 启动 IO 协程池。
func (f *Flusher) Start(ctx context.Context) error {
	if err := f.ensureScripts(ctx); err != nil {
		logx.Warn("预加载 Lua 脚本失败，将在首次使用时回退到 EVAL", "err", err)
	}
	for i := 0; i < f.workers; i++ {
		f.wg.Add(1)
		go f.worker(ctx, i)
	}
	logx.Info("刷盘 IO pool 已启动", "kind", f.kind, "workers", f.workers, "chan_size", cap(f.ch))
	return nil
}

// Submit 提交一个批次。永不阻塞。
//
// 返回 ErrBacklog 时调用方（Actor）必须把这批实体重新标脏。
func (f *Flusher) Submit(b *Batch) error {
	if b == nil || len(b.Entities) == 0 {
		return nil
	}
	select {
	case f.ch <- b:
		metrics.FlushChLen.WithLabelValues(f.kind).Set(float64(len(f.ch)))
		return nil
	default:
		metrics.FlushChBacklog.WithLabelValues(f.kind).Inc()
		logx.Error("flushCh 已满，重新标脏（告警：Redis 已扛不住，绝不阻塞 Actor）",
			"kind", f.kind, "shard", b.Shard, "level", b.Level, "entities", len(b.Entities),
			"chan_len", len(f.ch), "chan_cap", cap(f.ch))
		return ErrBacklog
	}
}

// SubmitSync 同步刷一批（优雅下线、handoff 的全量刷盘用）。
//
// 与 Submit 不同，它绕过队列直接写，因为此时必须确认刷盘成功才能继续（§10.3 步骤 4）。
func (f *Flusher) SubmitSync(ctx context.Context, b *Batch) *Result {
	if b == nil || len(b.Entities) == 0 {
		return &Result{Shard: b.GetShard(), Level: b.GetLevel()}
	}
	return f.write(ctx, b)
}

// GetShard/GetLevel 允许 nil 安全访问。
func (b *Batch) GetShard() uint32 {
	if b == nil {
		return 0
	}
	return b.Shard
}

func (b *Batch) GetLevel() Level {
	if b == nil {
		return L1
	}
	return b.Level
}

// Pending 返回队列中待处理批次数。
func (f *Flusher) Pending() int { return len(f.ch) }

// Stop 停止 IO pool，处理完队列中剩余批次。
func (f *Flusher) Stop() {
	f.stopOnce.Do(func() { close(f.quit) })
	f.wg.Wait()
	logx.Info("刷盘 IO pool 已停止", "kind", f.kind)
}

func (f *Flusher) worker(ctx context.Context, id int) {
	defer f.wg.Done()
	for {
		select {
		case b := <-f.ch:
			metrics.FlushChLen.WithLabelValues(f.kind).Set(float64(len(f.ch)))
			res := f.write(ctx, b)
			if f.onResult != nil {
				f.onResult(res)
			}
		case <-f.quit:
			// 排空剩余批次后退出，避免丢掉已经清了 dirty 标记的数据。
			for {
				select {
				case b := <-f.ch:
					res := f.write(context.Background(), b)
					if f.onResult != nil {
						f.onResult(res)
					}
				default:
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// write 用 pipeline + Lua 写一批实体。
func (f *Flusher) write(ctx context.Context, b *Batch) *Result {
	start := time.Now()
	res := &Result{Shard: b.Shard, Epoch: b.Epoch, Level: b.Level}

	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	failed, fenced, err := f.pipeline(wctx, b)
	if isNoScript(err) {
		// Redis 重启会丢失脚本缓存，重新加载后重试一次。
		if lerr := f.ensureScripts(wctx); lerr == nil {
			failed, fenced, err = f.pipeline(wctx, b)
		}
	}

	metrics.FlushDuration.WithLabelValues(f.kind, b.Level.String()).Observe(time.Since(start).Seconds())

	res.Failed = failed
	res.Fenced = fenced
	res.Err = err

	okCount := len(b.Entities) - len(failed)
	if okCount > 0 {
		metrics.FlushEntities.WithLabelValues(f.kind, b.Level.String()).Add(float64(okCount))
		now := time.Now()
		for _, e := range b.Entities {
			if !e.DirtyAt.IsZero() {
				metrics.DirtyAge.WithLabelValues(f.kind, b.Level.String()).Observe(now.Sub(e.DirtyAt).Seconds())
			}
		}
	}

	switch {
	case fenced:
		metrics.FlushFailures.WithLabelValues(f.kind, "epoch").Add(float64(len(failed)))
	case err != nil || len(failed) > 0:
		metrics.FlushFailures.WithLabelValues(f.kind, "redis").Add(float64(max(1, len(failed))))
		logx.Error("刷盘失败，将重新标脏（绝不丢弃）",
			"kind", f.kind, "shard", b.Shard, "level", b.Level,
			"failed", len(failed), "err", err)
	}
	return res
}

// pipeline 执行实际写入，返回失败实体、是否被 fencing 拒绝、整体错误。
func (f *Flusher) pipeline(ctx context.Context, b *Batch) ([]*Entity, bool, error) {
	epochKey := f.fencer.keys.Epoch(b.Shard)
	pipe := f.fencer.rdb.Pipeline()

	type slot struct {
		ent  *Entity
		mods *redis.Cmd
		hash *redis.Cmd
	}
	slots := make([]slot, 0, len(b.Entities))

	for _, e := range b.Entities {
		s := slot{ent: e}
		if len(e.Keys) > 0 {
			keys := make([]string, 0, len(e.Keys)+1)
			args := make([]any, 0, len(e.Keys)+1)
			keys = append(keys, epochKey)
			args = append(args, b.Epoch)
			for k, v := range e.Keys {
				keys = append(keys, k)
				args = append(args, v)
			}
			s.mods = scriptWriteModules.Run(ctx, pipe, keys, args...)
		}
		if e.Hash != nil && len(e.Hash.Fields) > 0 {
			args := make([]any, 0, len(e.Hash.Fields)+2)
			args = append(args, b.Epoch, e.Hash.TTLSec)
			args = append(args, e.Hash.Fields...)
			s.hash = scriptWriteHash.Run(ctx, pipe, []string{epochKey, e.Hash.Key}, args...)
		}
		slots = append(slots, s)
	}

	_, execErr := pipe.Exec(ctx)
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		// Exec 的错误是「其中至少一条失败」，仍需逐条检查。
		logx.Debug("刷盘 pipeline 返回错误，逐条核对", "err", execErr)
	}

	var failed []*Entity
	fenced := false
	var firstErr error

	for _, s := range slots {
		entOK := true
		for _, cmd := range []*redis.Cmd{s.mods, s.hash} {
			if cmd == nil {
				continue
			}
			v, err := cmd.Result()
			if err != nil {
				entOK = false
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			// 只有明确返回 1 才算成功。
			//
			// 这里必须「默认失败」：连接层面出错时 go-redis 只在 Exec 上返回错误，
			// 不会给每条已入队的命令挂上 err，此时 Result() 是 (nil, nil)。
			// 若把这种情况当成功，dirty 标记已被清掉而数据从未落盘 —— 静默丢档。
			n, ok := v.(int64)
			switch {
			case !ok:
				entOK = false
			case n == 0:
				// epoch 校验失败：调用方已过期。
				entOK = false
				fenced = true
			case n != 1:
				entOK = false
			}
		}
		if !entOK {
			failed = append(failed, s.ent)
		}
	}

	if fenced {
		f.fencer.reportFenced(b.Shard, b.Epoch)
	}
	if firstErr == nil && execErr != nil && !errors.Is(execErr, redis.Nil) {
		firstErr = execErr
	}
	return failed, fenced, firstErr
}

// ensureScripts 预加载全部 Lua 脚本，避免 pipeline 中 EVALSHA 命中 NOSCRIPT。
func (f *Flusher) ensureScripts(ctx context.Context) error {
	scripts := []*redis.Script{
		scriptRaiseEpoch, scriptWriteModules, scriptWriteHash,
		scriptWriteThrough, scriptDeleteKeys,
	}
	var firstErr error
	for _, s := range scripts {
		if err := s.Load(ctx, f.fencer.rdb).Err(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func isNoScript(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}

// EnsureScripts 供外部（非刷盘路径，如 L0 写穿）预加载脚本。
func EnsureScripts(ctx context.Context, rdb redis.UniversalClient) error {
	for _, s := range []*redis.Script{
		scriptRaiseEpoch, scriptWriteModules, scriptWriteHash,
		scriptWriteThrough, scriptDeleteKeys,
	} {
		if err := s.Load(ctx, rdb).Err(); err != nil {
			return fmt.Errorf("加载 Lua 脚本失败: %w", err)
		}
	}
	return nil
}
