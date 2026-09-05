// Package shardsvc 把「分片认领 + Actor 运行时 + subject 订阅 + 交接」拼成
// Lobby 与 Room 共用的骨架。
//
// 两个服务的差别只在业务状态（State）与分片键（uid vs roomID），
// 所有权、fencing、刷盘、交接这些最贵最难的部分只应实现一次。
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

// State 是分片业务状态，在 shard.State 之上多了刷盘结果回报。
type State interface {
	shard.State
	// OnFlushResult 处理刷盘结果：失败重新标脏，fenced 则丢弃内存。
	// 由框架保证在该分片的 Actor goroutine 内调用。
	OnFlushResult(res *store.Result)
	// FlushAllSync 全量同步刷盘，用于交接与优雅下线（§10.2 / §10.3）。
	FlushAllSync(ctx context.Context) error
}

// Factory 为一个分片创建业务状态。
type Factory func(o shard.Ownership, svc *Service) State

// Options 是构造参数。
type Options struct {
	Kind    shard.Kind
	Node    *node.Node
	Factory Factory
	// Wildcard 返回某分片的订阅 subject（不带 queue group）。
	Wildcard func(shard uint32) string
	// Tick 是 Actor 的心跳间隔，默认 1s。
	Tick time.Duration
	// Space 是分片空间大小，0 表示用 cfg.ShardCount。
	// Match 按「模式×段位」分桶，空间远小于 1024。
	Space uint32
	// SkipEpoch 跳过 Redis epoch 抬高与 fencing。
	// 只有完全不落盘的分片空间（如匹配池）才可以设为 true ——
	// 一旦有数据落盘，fencing 就是不可省略的（§7.2）。
	SkipEpoch bool
}

// Service 是分片型服务的骨架。
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

// New 构造骨架。
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

// Node 返回宿主 node。
func (s *Service) Node() *node.Node { return s.node }

// Fencer 返回带 epoch 校验的写入器。
func (s *Service) Fencer() *store.Fencer { return s.fencer }

// Flusher 返回刷盘 IO pool。
func (s *Service) Flusher() *store.Flusher { return s.flusher }

// Runtime 返回 Actor 运行时。
func (s *Service) Runtime() *shard.Runtime { return s.runtime }

// Claimer 返回分片认领器。
func (s *Service) Claimer() *shard.Claimer { return s.claimer }

// Kind 返回分片空间。
func (s *Service) Kind() shard.Kind { return s.kind }

// Start 启动刷盘池、Actor 运行时、控制面订阅与分片认领。
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

// onAcquire 按 §10.2 步骤 5 的顺序初始化分片：
//
//	抬高 Redis epoch → 加载数据 → 订阅 subject → 服务
//
// 顺序不能变：epoch 必须在「加载数据之前」抬高，
// 否则旧 owner 可能在我们加载完成之后又把旧内存刷进来（§7.2）。
func (s *Service) onAcquire(ctx context.Context, o shard.Ownership) error {
	actx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 1. 抬高 epoch —— 此后旧 owner 的任何写入都会被拒绝。
	if !s.opts.SkipEpoch {
		if err := s.fencer.RaiseEpoch(actx, o.Shard, o.Epoch); err != nil {
			return fmt.Errorf("抬高 epoch 失败: %w", err)
		}
	}

	// 2. 启动 Actor 并在其 goroutine 内加载数据。
	if err := s.runtime.Start(actx, o); err != nil {
		s.dropState(o.Shard)
		return fmt.Errorf("启动分片 Actor 失败: %w", err)
	}

	// 3. 订阅该分片的 subject —— 到这一步才开始对外服务。
	subj := s.opts.Wildcard(o.Shard)
	sub, err := s.node.Bus.Subscribe(subj, func(m *bus.Msg) {
		// NATS 回调是单 goroutine 串行的：这里只投递，不做业务（§5.4）。
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

// onRelease 释放分片：先断流，再停 Actor（Actor 内按 reason 决定是否刷盘）。
func (s *Service) onRelease(ctx context.Context, o shard.Ownership, reason shard.ReleaseReason) {
	// 1. 先退订，停止接收该分片的新消息。
	s.mu.Lock()
	sub := s.subs[o.Shard]
	delete(s.subs, o.Shard)
	s.mu.Unlock()
	if sub != nil {
		// Unsubscribe 而非 Drain：Drain 会继续处理 pending，
		// 而此刻我们要么已不是 owner（不能再写），要么正要刷盘后交出去。
		_ = sub.Unsubscribe()
	}

	// 2. 停 Actor。State.Close 内部按 reason 决定：
	//    graceful → 全量刷盘；lease_lost / fenced → 直接丢弃内存，绝不再写 Redis。
	s.runtime.Stop(o.Shard, reason)
	s.dropState(o.Shard)
}

func (s *Service) onFreed(kind shard.Kind, sh uint32) {
	// 通知其他实例尽快重扫，把「等下一轮 3s 扫描」压缩成一次事件。
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

// State 返回某分片的业务状态（仅供框架内部与测试使用）。
func (s *Service) State(sh uint32) (State, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.states[sh]
	return st, ok
}

// ---------------------------------------------------------------------------
// 刷盘结果回报
// ---------------------------------------------------------------------------

// onFlushResult 在 IO goroutine 中被调用，只做一件事：把结果投递回对应 Actor。
func (s *Service) onFlushResult(res *store.Result) {
	if res == nil {
		return
	}
	if res.Fenced {
		// epoch 过期：内存已不可信，立刻丢弃该分片并停止服务（§7.2）。
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
	// 失败重新标脏，绝不丢弃。
	if err := s.runtime.Do(res.Shard, func() {
		if st, ok := s.State(res.Shard); ok {
			st.OnFlushResult(res)
		}
	}); err != nil {
		// 分片已不在本节点：内存也已丢弃，没有可标脏的对象了。
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

	// 别的实例释放了分片就立刻重扫，把接管窗口压到最小。
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

// handleHandoff 是交接的「旧节点」侧（§10.2 步骤 2~4）。
//
//  2. 旧节点停止处理该分片新消息
//  3. 旧节点全量刷盘 → 释放 etcd 锁 → 回复 READY
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
		// 已经不持有了，对新节点来说这就是可以直接抢占，回 READY。
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

	// Release 内部：退订（停止处理新消息）→ 停 Actor（graceful 触发全量刷盘）→ 删 etcd 键。
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

// requestHandoff 是交接的「新节点」侧（§10.2 步骤 1、4）。
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

// handleShutdown 处理远程下线指令，走与信号完全相同的路径。
func (s *Service) handleShutdown(m *bus.Msg) {
	var req pb.ShutdownReq
	_ = bus.Unpack(m.Env, &req)
	logx.Warn("收到远程优雅下线指令", "reason", req.GetReason(), "from", m.Env.GetFromNode())
	_ = m.Respond(&pb.Ack{Ok: true})

	go func() {
		time.Sleep(100 * time.Millisecond) // 让应答先发出去
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()
}

// ---------------------------------------------------------------------------
// 停机
// ---------------------------------------------------------------------------

// StopAccepting 停止接新请求：退订控制面，释放全部分片（各自全量刷盘）。
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

// FlushAll 确认刷盘队列已排空（§10.3 步骤 4）。
//
// 分片在 StopAccepting 阶段已各自全量刷盘，这里只做最终确认：
// 队列没排空就说明还有数据没落地，必须等或告警，绝不能装作没事。
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

// Close 释放剩余资源。
func (s *Service) Close(ctx context.Context) {
	if s.runtime != nil {
		s.runtime.StopAll(shard.ReleaseGraceful)
	}
}

// ShardOf 计算实体所属分片。
func (s *Service) ShardOf(id uint64) uint32 {
	if s.opts.Space > 0 {
		return shard.Of(id, s.opts.Space)
	}
	return shard.Of(id, s.node.Cfg.ShardCount)
}

// Owns 报告本实例是否持有某实体所在分片。
func (s *Service) Owns(id uint64) bool {
	return s.claimer != nil && s.claimer.Owns(s.ShardOf(id))
}
