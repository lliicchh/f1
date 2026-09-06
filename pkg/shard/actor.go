package shard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/protocol"
)

// State 一个分片的业务状态，所有方法都在该分片自己的 goroutine 里调用
//
// 所以裸 map、裸 slice 随便用，不用加锁。代价是回调里不能有阻塞操作：
// 序列化在这里做（读内存只能在这儿），真正的 IO 甩给别的 goroutine
type State interface {
	// Init 加载分片数据，返回错误会回滚认领
	Init(ctx context.Context, o Ownership) error
	// Handle 处理一条入站请求
	Handle(m *bus.Msg)
	// Tick 周期性回调，用于刷盘、卸载检查等
	Tick(now time.Time)
	// Close 在分片被释放时调用。graceful 要全量刷盘；
	// fenced 和 lease_lost 只能丢内存，一个字节都不许再写 Redis
	Close(reason ReleaseReason)
}

// StateFactory 给一个分片创建业务状态
type StateFactory func(o Ownership) State

type task struct {
	msg *bus.Msg
	fn  func()
	enq time.Time
}

// Actor 一个分片的执行单元，一条 goroutine 独占这个分片的全部数据
type Actor struct {
	o       Ownership
	mbox    chan task
	quit    chan struct{}
	done    chan struct{}
	state   State
	tick    time.Duration
	warnPct int

	closeOnce sync.Once
	reason    ReleaseReason

	lastWarn time.Time
}

func (a *Actor) Epoch() int64 { return a.o.Epoch }

func (a *Actor) Shard() uint32 { return a.o.Shard }

func (a *Actor) Len() int { return len(a.mbox) }

// ErrMailboxFull 表示 mailbox 已满
var ErrMailboxFull = errors.New("shard: mailbox 已满")

// ErrNotOwned 表示本实例未持有该分片
var ErrNotOwned = errors.New("shard: 本实例未持有该分片")

// post 投递任务，永不阻塞，调用方是 NATS 那条单 goroutine
func (a *Actor) post(t task) error {
	select {
	case a.mbox <- t:
		return nil
	default:
		return ErrMailboxFull
	}
}

func (a *Actor) run(kind Kind, initCtx context.Context, initErr chan<- error) {
	defer close(a.done)

	// Init 在这条 goroutine 里跑，加载出来的数据此后也只有它能碰
	if err := a.state.Init(initCtx, a.o); err != nil {
		initErr <- err
		return
	}
	initErr <- nil

	kindStr := string(kind)
	// 首次 tick 按分片号错开，免得 1024 个分片挤在同一秒
	offset := time.Duration(a.o.Shard%uint32(a.tick/time.Millisecond)) * time.Millisecond
	timer := time.NewTimer(offset)
	defer timer.Stop()
	ticked := false

	for {
		select {
		case <-a.quit:
			// 优雅停止时把 mailbox 里剩的处理完再收尾。
			// 失去所有权时不能处理，结果会写进一份已经过期的内存
			if a.reason == ReleaseGraceful {
				a.drain(kindStr)
			}
			a.state.Close(a.reason)
			return

		case t := <-a.mbox:
			a.exec(kindStr, t)

		case now := <-timer.C:
			if !ticked {
				ticked = true
				timer.Reset(a.tick)
			} else {
				timer.Reset(a.tick)
			}
			metrics.ShardMailboxLen.WithLabelValues(kindStr, fmt.Sprint(a.o.Shard)).Set(float64(len(a.mbox)))
			a.warnBacklog(kindStr)
			a.state.Tick(now)
		}
	}
}

// drain 把 mailbox 里剩的消息处理完
func (a *Actor) drain(kindStr string) {
	for {
		select {
		case t := <-a.mbox:
			a.exec(kindStr, t)
		default:
			return
		}
	}
}

func (a *Actor) exec(kindStr string, t task) {
	start := time.Now()
	cmd := "internal"

	defer func() {
		if r := recover(); r != nil {
			// 一条消息 panic 不该带走整个分片，那意味着一批玩家全不可用
			logx.Error("分片处理 panic，已隔离该消息", "kind", kindStr,
				"shard", a.o.Shard, "cmd", cmd, "panic", r)
			if t.msg != nil {
				_ = t.msg.RespondErr(protocol.ErrInternal, "内部错误")
			}
		}
		metrics.ShardHandleLatency.WithLabelValues(kindStr, cmd).Observe(time.Since(t.enq).Seconds())
		_ = start
	}()

	switch {
	case t.msg != nil:
		cmd = t.msg.Cmd().Name()
		a.state.Handle(t.msg)
	case t.fn != nil:
		t.fn()
	}
}

// warnBacklog 在 mailbox 积压超阈值时告警
func (a *Actor) warnBacklog(kindStr string) {
	n, capn := len(a.mbox), cap(a.mbox)
	if capn == 0 || a.warnPct <= 0 {
		return
	}
	if n*100/capn < a.warnPct {
		return
	}
	if time.Since(a.lastWarn) < 10*time.Second {
		return
	}
	a.lastWarn = time.Now()
	logx.Error("分片 mailbox 积压超阈值（告警，可能是单分片过热，需人工介入）",
		"kind", kindStr, "shard", a.o.Shard, "len", n, "cap", capn, "warn_pct", a.warnPct)
}

func (a *Actor) stop(reason ReleaseReason) {
	a.closeOnce.Do(func() {
		a.reason = reason
		close(a.quit)
	})
	<-a.done
}

// ---------------------------------------------------------------------------

// Runtime 管理本实例持有的所有分片 Actor
type Runtime struct {
	kind    Kind
	tick    time.Duration
	boxSize int
	warnPct int
	factory StateFactory

	mu     sync.RWMutex
	actors map[uint32]*Actor
}

// NewRuntime 构造 Actor 运行时
func NewRuntime(kind Kind, mailboxSize, warnPct int, tick time.Duration, factory StateFactory) *Runtime {
	if tick <= 0 {
		tick = time.Second
	}
	if mailboxSize <= 0 {
		mailboxSize = 4096
	}
	return &Runtime{
		kind:    kind,
		tick:    tick,
		boxSize: mailboxSize,
		warnPct: warnPct,
		factory: factory,
		actors:  make(map[uint32]*Actor),
	}
}

// Start 给已认领的分片起 Actor，同步等 Init 完成
func (r *Runtime) Start(ctx context.Context, o Ownership) error {
	r.mu.Lock()
	if _, ok := r.actors[o.Shard]; ok {
		r.mu.Unlock()
		return fmt.Errorf("分片 %d 的 Actor 已存在", o.Shard)
	}
	a := &Actor{
		o:       o,
		mbox:    make(chan task, r.boxSize),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
		state:   r.factory(o),
		tick:    r.tick,
		warnPct: r.warnPct,
	}
	r.actors[o.Shard] = a
	r.mu.Unlock()

	initErr := make(chan error, 1)
	go a.run(r.kind, ctx, initErr)

	if err := <-initErr; err != nil {
		r.mu.Lock()
		delete(r.actors, o.Shard)
		r.mu.Unlock()
		<-a.done
		return err
	}
	return nil
}

// Stop 停止某分片的 Actor
func (r *Runtime) Stop(shard uint32, reason ReleaseReason) {
	r.mu.Lock()
	a, ok := r.actors[shard]
	if ok {
		delete(r.actors, shard)
	}
	r.mu.Unlock()
	if !ok {
		return
	}
	a.stop(reason)
	metrics.ShardMailboxLen.DeleteLabelValues(string(r.kind), fmt.Sprint(shard))
}

// StopAll 停止全部 Actor
func (r *Runtime) StopAll(reason ReleaseReason) {
	for _, s := range r.Shards() {
		r.Stop(s, reason)
	}
}

// Shards 返回当前运行中的分片列表
func (r *Runtime) Shards() []uint32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]uint32, 0, len(r.actors))
	for s := range r.actors {
		out = append(out, s)
	}
	return out
}

func (r *Runtime) actor(shard uint32) (*Actor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.actors[shard]
	return a, ok
}

// Post 把一条请求投给分片 Actor
//
// 这是 NATS 回调里唯一该做的事。mailbox 满就直接回错让客户端重试，不阻塞回调
func (r *Runtime) Post(shard uint32, m *bus.Msg) {
	a, ok := r.actor(shard)
	if !ok {
		// 分片不在本实例，可能正在交接，让客户端重试
		_ = m.RespondErr(protocol.ErrNotOwner, "分片 %d 不由本实例持有，请重试", shard)
		return
	}
	if err := a.post(task{msg: m, enq: time.Now()}); err != nil {
		metrics.ShardMailboxDropped.WithLabelValues(string(r.kind), fmt.Sprint(shard)).Inc()
		_ = m.RespondErr(protocol.ErrUnavailable, "分片 %d 繁忙，请重试", shard)
	}
}

// Do 把闭包扔进分片 Actor 异步执行
func (r *Runtime) Do(shard uint32, fn func()) error {
	a, ok := r.actor(shard)
	if !ok {
		return ErrNotOwned
	}
	return a.post(task{fn: fn, enq: time.Now()})
}

// Call 把闭包投进 Actor 并等它跑完
//
// 别在 Actor goroutine 里调，会死锁。给控制面用：交接、下线、同步查询
func (r *Runtime) Call(ctx context.Context, shard uint32, fn func()) error {
	a, ok := r.actor(shard)
	if !ok {
		return ErrNotOwned
	}
	done := make(chan struct{})
	err := a.post(task{fn: func() {
		defer close(done)
		fn()
	}, enq: time.Now()})
	if err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-a.done:
		return ErrNotOwned
	}
}

// Epoch 返回某分片 Actor 的 epoch
func (r *Runtime) Epoch(shard uint32) (int64, bool) {
	a, ok := r.actor(shard)
	if !ok {
		return 0, false
	}
	return a.Epoch(), true
}

// Backlog 返回各分片 mailbox 的积压情况
func (r *Runtime) Backlog() map[uint32]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[uint32]int, len(r.actors))
	for s, a := range r.actors {
		out[s] = a.Len()
	}
	return out
}
