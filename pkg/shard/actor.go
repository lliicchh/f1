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

// State 是一个分片的业务状态，全部方法都在该分片专属的 goroutine 内被调用。
//
// 因此实现者可以放心地用裸 map、裸 slice，不需要任何锁：
// 「无锁、无竞态，业务代码是纯同步的」（§4.4）。
//
// 反过来，实现者必须遵守：回调内禁止任何阻塞操作。
// 刷盘的序列化在 Actor 内做（读内存必须如此），实际 IO 甩给独立 goroutine。
type State interface {
	// Init 加载分片数据。在 Actor goroutine 内执行，返回错误则认领回滚。
	Init(ctx context.Context, o Ownership) error
	// Handle 处理一条入站请求。
	Handle(m *bus.Msg)
	// Tick 周期性回调，用于刷盘、卸载检查等。
	Tick(now time.Time)
	// Close 在分片被释放时调用。graceful 时应全量刷盘；
	// fenced / lease_lost 时必须直接丢弃内存，绝不能再写 Redis（§7.2）。
	Close(reason ReleaseReason)
}

// StateFactory 为一个分片创建业务状态。
type StateFactory func(o Ownership) State

type task struct {
	msg *bus.Msg
	fn  func()
	enq time.Time
}

// Actor 是单个分片的执行单元：一条 goroutine 独占该分片全部内存数据。
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

// Epoch 返回该分片的 epoch。
func (a *Actor) Epoch() int64 { return a.o.Epoch }

// Shard 返回分片号。
func (a *Actor) Shard() uint32 { return a.o.Shard }

// Len 返回 mailbox 当前积压。
func (a *Actor) Len() int { return len(a.mbox) }

// ErrMailboxFull 表示 mailbox 已满。
var ErrMailboxFull = errors.New("shard: mailbox 已满")

// ErrNotOwned 表示本实例未持有该分片。
var ErrNotOwned = errors.New("shard: 本实例未持有该分片")

// post 投递任务。绝不阻塞 —— 调用方是 NATS 回调的单 goroutine（§5.4）。
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

	// Init 在本 goroutine 内执行：加载的数据从此刻起只被这条 goroutine 访问。
	if err := a.state.Init(initCtx, a.o); err != nil {
		initErr <- err
		return
	}
	initErr <- nil

	kindStr := string(kind)
	// 刷盘时刻打散：ticker 初始偏移，避免 1024 分片同秒触发造成 Redis 尖峰（§6.3）。
	offset := time.Duration(a.o.Shard%uint32(a.tick/time.Millisecond)) * time.Millisecond
	timer := time.NewTimer(offset)
	defer timer.Stop()
	ticked := false

	for {
		select {
		case <-a.quit:
			// 优雅停止时先把 mailbox 里剩下的消息处理完，
			// 对应 §10.3「处理完 pending 后」再收尾；
			// 失去所有权（lease_lost / fenced）时绝不能再处理 ——
			// 那些消息的处理结果会写进一份已经过期的内存。
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

// drain 处理 mailbox 中剩余的消息。
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
			// 单条消息 panic 不应带走整个分片：分片挂掉意味着一批玩家不可用。
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

// warnBacklog 在 mailbox 积压超阈值时告警（§4.4 / §13 单分片过热）。
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

// Runtime 管理本实例持有的全部分片 Actor。
type Runtime struct {
	kind    Kind
	tick    time.Duration
	boxSize int
	warnPct int
	factory StateFactory

	mu     sync.RWMutex
	actors map[uint32]*Actor
}

// NewRuntime 构造 Actor 运行时。
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

// Start 为一个已认领的分片启动 Actor，并同步等待 Init 完成。
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

// Stop 停止某分片的 Actor。
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

// StopAll 停止全部 Actor。
func (r *Runtime) StopAll(reason ReleaseReason) {
	for _, s := range r.Shards() {
		r.Stop(s, reason)
	}
}

// Shards 返回当前运行中的分片列表。
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

// Post 把一条 NATS 请求投递给分片 Actor。
//
// 这是 NATS 回调里唯一该做的事（§5.4）：投递入 mailbox，不做业务。
// mailbox 满时立刻回 ErrUnavailable 让客户端重试，绝不阻塞回调 goroutine。
func (r *Runtime) Post(shard uint32, m *bus.Msg) {
	a, ok := r.actor(shard)
	if !ok {
		// 分片不属于本实例：可能正处在交接窗口，客户端重试即可（§10.2）。
		_ = m.RespondErr(protocol.ErrNotOwner, "分片 %d 不由本实例持有，请重试", shard)
		return
	}
	if err := a.post(task{msg: m, enq: time.Now()}); err != nil {
		metrics.ShardMailboxDropped.WithLabelValues(string(r.kind), fmt.Sprint(shard)).Inc()
		_ = m.RespondErr(protocol.ErrUnavailable, "分片 %d 繁忙，请重试", shard)
	}
}

// Do 把一个闭包投递进分片 Actor 异步执行。
func (r *Runtime) Do(shard uint32, fn func()) error {
	a, ok := r.actor(shard)
	if !ok {
		return ErrNotOwned
	}
	return a.post(task{fn: fn, enq: time.Now()})
}

// Call 把闭包投递进 Actor 并等待其执行完成。
//
// 严禁在 Actor goroutine 内调用（会死锁）。用于控制面：handoff、优雅下线、同步查询。
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

// Epoch 返回某分片 Actor 的 epoch。
func (r *Runtime) Epoch(shard uint32) (int64, bool) {
	a, ok := r.actor(shard)
	if !ok {
		return 0, false
	}
	return a.Epoch(), true
}

// Backlog 返回各分片 mailbox 积压快照，用于监控与排查。
func (r *Runtime) Backlog() map[uint32]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[uint32]int, len(r.actors))
	for s, a := range r.actors {
		out[s] = a.Len()
	}
	return out
}
