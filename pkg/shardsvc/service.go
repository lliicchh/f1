// Package shardsvc 把分片认领、Actor 运行时、subject 订阅和交接拼在一起，
// 供 Lobby 和 Room 复用
//
// 两个服务的差别只在业务状态和分片键，所有权、fencing、刷盘、交接只实现一次
package shardsvc

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/nodeid"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/pkg/subject"
)

// State 在 shard.State 之上多了刷盘结果回报
type State interface {
	shard.State
	// OnFlushResult 处理刷盘结果，失败重新标脏，fenced 就丢内存。
	// 框架保证在该分片的 Actor goroutine 里调
	OnFlushResult(res *store.Result)
	// FlushAllSync 全量同步刷盘，交接和下线时用
	FlushAllSync(ctx context.Context) error
}

// Factory 为一个分片创建业务状态
type Factory func(o shard.Ownership, svc *Service) State

// Options 构造参数
type Options struct {
	Kind    shard.Kind
	Node    *node.Node
	Factory Factory
	// Wildcard 返回某分片的订阅 subject，不带 queue group
	Wildcard func(shard uint32) string
	// Tick Actor 的心跳间隔，默认 1s
	Tick time.Duration
	// Space 分片空间大小，0 表示用 cfg.ShardCount。Match 按模式 × 段位分桶，小得多
	Space uint32
	// SkipEpoch 跳过 epoch 抬高和 fencing。只有完全不落盘的分片空间才能设 true，
	// 比如匹配池。只要有数据落盘就不能省
	SkipEpoch bool
}

// Service 分片型服务的骨架
type Service struct {
	kind    shard.Kind
	node    *node.Node
	opts    Options
	runtime *shard.Runtime
	claimer *shard.Claimer
	fencer  *store.Fencer
	flusher *store.Flusher

	mu     sync.RWMutex
	states map[uint32]State
	subs   map[uint32]*nats.Subscription

	ctlSubs  []*nats.Subscription
	stopping bool
}

func New(opt Options) *Service {
	if opt.Tick <= 0 {
		opt.Tick = time.Second
	}
	s := &Service{
		kind:   opt.Kind,
		node:   opt.Node,
		opts:   opt,
		states: make(map[uint32]State),
		subs:   make(map[uint32]*nats.Subscription),
	}
	s.fencer = store.NewFencer(opt.Node.Redis, opt.Node.Keys, string(opt.Kind))
	return s
}

func (s *Service) Node() *node.Node { return s.node }

func (s *Service) Fencer() *store.Fencer { return s.fencer }

func (s *Service) Flusher() *store.Flusher { return s.flusher }

func (s *Service) Runtime() *shard.Runtime { return s.runtime }

func (s *Service) Claimer() *shard.Claimer { return s.claimer }

func (s *Service) Kind() shard.Kind { return s.kind }

// Start 启动刷盘池、Actor 运行时、控制面订阅与分片认领
func (s *Service) Start(ctx context.Context) error {
	cfg := s.node.Cfg

	s.flusher = store.NewFlusher(s.fencer, string(s.kind),
		cfg.FlushChanSize, cfg.FlushWorkers, cfg.FlushBatchSize, s.onFlushResult)
	if err := s.flusher.Start(ctx); err != nil {
		return err
	}

	s.runtime = shard.NewRuntime(s.kind, cfg.MailboxSize, cfg.MailboxWarnPct, s.opts.Tick,
		func(o shard.Ownership) shard.State {
			st := s.opts.Factory(o, s)
			s.mu.Lock()
			s.states[o.Shard] = st
			s.mu.Unlock()
			return st
		})

	if err := s.subscribeControl(); err != nil {
		return err
	}

	s.claimer = shard.NewClaimerWithSpace(s.node.Etcd, cfg, s.kind, s.node.NodeID(), s.opts.Space, shard.Hooks{
		OnAcquire:      s.onAcquire,
		OnRelease:      s.onRelease,
		OnFreed:        s.onFreed,
		LiveNodes:      s.liveNodes,
		RequestHandoff: s.requestHandoff,
	})
	return s.claimer.Start(ctx)
}

// ---------------------------------------------------------------------------
// 认领 / 释放
// ---------------------------------------------------------------------------

// onAcquire 初始化分片，顺序是抬 epoch、加载数据、订阅 subject
//
// 顺序不能换：epoch 必须在加载数据之前抬，否则旧 owner 可能在我们加载完之后
// 又把旧内存刷进来
func (s *Service) onAcquire(ctx context.Context, o shard.Ownership) error {
	actx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 抬 epoch，此后旧 owner 写什么都会被拒
	if !s.opts.SkipEpoch {
		if err := s.fencer.RaiseEpoch(actx, o.Shard, o.Epoch); err != nil {
			return fmt.Errorf("抬高 epoch 失败: %w", err)
		}
	}

	// 起 Actor，数据在它自己的 goroutine 里加载
	if err := s.runtime.Start(actx, o); err != nil {
		s.dropState(o.Shard)
		return fmt.Errorf("启动分片 Actor 失败: %w", err)
	}

	// 订阅 subject，到这一步才开始对外服务
	subj := s.opts.Wildcard(o.Shard)
	sub, err := s.node.Bus.Subscribe(subj, func(m *bus.Msg) {
		// NATS 回调是单 goroutine 的，这里只投递
		s.runtime.Post(o.Shard, m)
	})
	if err != nil {
		s.runtime.Stop(o.Shard, shard.ReleaseStartFailed)
		s.dropState(o.Shard)
		return fmt.Errorf("订阅 %s 失败: %w", subj, err)
	}

	s.mu.Lock()
	s.subs[o.Shard] = sub
	s.mu.Unlock()
	return nil
}

// onRelease 释放分片：先断流再停 Actor，刷不刷盘由 Actor 按 reason 决定
func (s *Service) onRelease(ctx context.Context, o shard.Ownership, reason shard.ReleaseReason) {
	// 先退订，不再收这个分片的新消息
	s.mu.Lock()
	sub := s.subs[o.Shard]
	delete(s.subs, o.Shard)
	s.mu.Unlock()
	if sub != nil {
		// 用 Unsubscribe 不用 Drain：这会儿要么已经不是 owner 不能再写，
		// 要么马上就要刷盘交出去
		_ = sub.Unsubscribe()
	}

	// 停 Actor。graceful 会全量刷盘，lease_lost 和 fenced 直接丢内存
	s.runtime.Stop(o.Shard, reason)
	s.dropState(o.Shard)
}

func (s *Service) onFreed(kind shard.Kind, sh uint32) {
	// 发个事件让别的实例马上重扫，省掉等下一轮 3s 的时间
	env, err := s.node.Bus.NewEnvelope(protocol.CmdHandoff, 0, "", &pb.HandoffReq{
		Kind:     string(kind),
		Shard:    sh,
		FromNode: s.node.NodeID(),
	})
	if err != nil {
		return
	}
	_ = s.node.Bus.PublishEnv(subject.ShardFreedEvt(string(kind)), env)
}

func (s *Service) liveNodes(ctx context.Context) int {
	return nodeid.CountLive(ctx, s.node.Etcd, s.node.Cfg, s.node.ID.SvcName(), s.node.Cfg.NodeStaleTimeout)
}

func (s *Service) dropState(sh uint32) {
	s.mu.Lock()
	delete(s.states, sh)
	s.mu.Unlock()
}

// State 返回某分片的业务状态，给框架内部和测试用
func (s *Service) State(sh uint32) (State, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.states[sh]
	return st, ok
}

// ---------------------------------------------------------------------------
// 刷盘结果回报
// ---------------------------------------------------------------------------

// onFlushResult 在 IO goroutine 里调，只负责把结果投回对应的 Actor
func (s *Service) onFlushResult(res *store.Result) {
	if res == nil {
		return
	}
	if res.Fenced {
		// epoch 过期，内存不可信了，丢掉停服务
		logx.Error("刷盘被 fencing 拒绝，丢弃该分片内存并停止服务（告警）",
			"kind", s.kind, "shard", res.Shard, "epoch", res.Epoch)
		if s.claimer != nil {
			s.claimer.Fence(res.Shard)
		}
		return
	}
	if len(res.Failed) == 0 && res.Err == nil {
		return
	}
	// 失败的重新标脏
	if err := s.runtime.Do(res.Shard, func() {
		if st, ok := s.State(res.Shard); ok {
			st.OnFlushResult(res)
		}
	}); err != nil {
		// 分片已经不在本节点，内存也丢了，没东西可标脏
		logx.Warn("刷盘结果无法回报（分片已释放）", "kind", s.kind, "shard", res.Shard, "err", err)
	}
}

// ---------------------------------------------------------------------------
// 控制面：handoff / shutdown / 分片释放事件
// ---------------------------------------------------------------------------

func (s *Service) subscribeControl() error {
	nodeID := s.node.NodeID()

	sub, err := s.node.Bus.Subscribe(subject.CtlHandoff(nodeID), s.handleHandoff)
	if err != nil {
		return fmt.Errorf("订阅 handoff 失败: %w", err)
	}
	s.ctlSubs = append(s.ctlSubs, sub)

	sub, err = s.node.Bus.Subscribe(subject.CtlShutdown(nodeID), s.handleShutdown)
	if err != nil {
		return fmt.Errorf("订阅 shutdown 失败: %w", err)
	}
	s.ctlSubs = append(s.ctlSubs, sub)

	// 别的实例一释放就重扫，把接管窗口压小
	sub, err = s.node.Bus.Subscribe(subject.ShardFreedEvt(string(s.kind)), func(m *bus.Msg) {
		if s.claimer != nil {
			s.claimer.TriggerRescan()
		}
	})
	if err != nil {
		return fmt.Errorf("订阅分片释放事件失败: %w", err)
	}
	s.ctlSubs = append(s.ctlSubs, sub)
	return nil
}

// handleHandoff 交接的旧节点这一侧：停手、全量刷盘、释放锁、回 READY
func (s *Service) handleHandoff(m *bus.Msg) {
	var req pb.HandoffReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析 handoff 请求失败")
		return
	}
	if req.GetKind() != string(s.kind) {
		_ = m.RespondErr(protocol.ErrBadRequest, "分片空间不匹配: %s", req.GetKind())
		return
	}

	sh := req.GetShard()
	log := logx.Trace(m.Env.GetTraceId()).With("kind", s.kind, "shard", sh, "to", req.GetToNode())

	if !s.claimer.Owns(sh) {
		// 已经不持有了，新节点可以直接抢，回 READY
		_ = m.Respond(&pb.HandoffResp{Ready: true, Reason: "not owned"})
		return
	}

	s.mu.RLock()
	stopping := s.stopping
	s.mu.RUnlock()
	if stopping {
		_ = m.RespondErr(protocol.ErrUnavailable, "本节点正在下线，请稍后重试")
		return
	}

	log.Info("收到分片交接请求，开始让出")
	start := time.Now()

	// Release 里面就是退订、停 Actor（会全量刷盘）、删 etcd 键
	if err := s.claimer.Release(context.Background(), sh, shard.ReleaseGraceful); err != nil {
		log.Error("分片交接失败", "err", err)
		metrics.ShardHandoff.WithLabelValues(string(s.kind), "source", "fail").Inc()
		_ = m.RespondErr(protocol.ErrInternal, "交接失败: %v", err)
		return
	}

	metrics.ShardHandoff.WithLabelValues(string(s.kind), "source", "ok").Inc()
	log.Info("分片交接完成，回复 READY", "elapsed", time.Since(start).Round(time.Millisecond))
	_ = m.Respond(&pb.HandoffResp{Ready: true})
}

// requestHandoff 交接的新节点这一侧
func (s *Service) requestHandoff(ctx context.Context, sh uint32, fromNode string) error {
	var resp pb.HandoffResp
	err := s.node.Bus.Call(ctx, subject.CtlHandoff(fromNode), protocol.CmdHandoff, 0, "",
		&pb.HandoffReq{
			Kind:     string(s.kind),
			Shard:    sh,
			FromNode: fromNode,
			ToNode:   s.node.NodeID(),
		}, &resp)
	if err != nil {
		return err
	}
	if !resp.GetReady() {
		return fmt.Errorf("对方拒绝交接: %s", resp.GetReason())
	}
	return nil
}

// handleShutdown 处理远程下线指令，走和信号一样的路径
func (s *Service) handleShutdown(m *bus.Msg) {
	var req pb.ShutdownReq
	_ = bus.Unpack(m.Env, &req)
	logx.Warn("收到远程优雅下线指令", "reason", req.GetReason(), "from", m.Env.GetFromNode())
	_ = m.Respond(&pb.Ack{Ok: true})

	go func() {
		time.Sleep(100 * time.Millisecond) // 等应答先发出去
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()
}

// ---------------------------------------------------------------------------
// 停机
// ---------------------------------------------------------------------------

// StopAccepting 退订控制面并释放全部分片，各分片会各自刷盘
func (s *Service) StopAccepting(ctx context.Context) {
	s.mu.Lock()
	s.stopping = true
	subs := s.ctlSubs
	s.ctlSubs = nil
	s.mu.Unlock()

	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}
	if s.claimer != nil {
		s.claimer.Stop(ctx)
	}
}

// FlushAll 确认刷盘队列排空了
//
// 分片在 StopAccepting 时已经各自刷过，这里只是最后确认一下。
// 队列没排空就是还有数据没落地，不能装作没事
func (s *Service) FlushAll(ctx context.Context) error {
	if s.flusher == nil {
		return nil
	}
	deadline := time.Now().Add(20 * time.Second)
	for s.flusher.Pending() > 0 {
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("刷盘队列未排空，仍有 %d 批待写", s.flusher.Pending())
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.flusher.Stop()
	return nil
}

func (s *Service) Close(ctx context.Context) {
	if s.runtime != nil {
		s.runtime.StopAll(shard.ReleaseGraceful)
	}
}

// ShardOf 算实体落在哪个分片
func (s *Service) ShardOf(id uint64) uint32 {
	if s.opts.Space > 0 {
		return shard.Of(id, s.opts.Space)
	}
	return shard.Of(id, s.node.Cfg.ShardCount)
}

// Owns 报告某实体所在分片是不是本实例持有
func (s *Service) Owns(id uint64) bool {
	return s.claimer != nil && s.claimer.Owns(s.ShardOf(id))
}
