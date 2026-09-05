// Package match 实现匹配服：匹配池，按「模式×段位」分片（§2.1）。
//
// 匹配池是纯内存的临时数据（L3 级别，不刷盘）：实例挂了池子清空，
// 玩家重新排队即可，因此这里跳过 epoch fencing。
// 但「同一个 模式×段位 只能有一个实例在撮合」这条必须成立 ——
// 否则两个实例会把同一批玩家撮进两个房间，所以仍然走分片独占认领。
package match

import (
	"context"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/session"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/shardsvc"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/pkg/subject"
)

// 匹配空间：模式 × 段位。
const (
	ModeCount = 8
	TierCount = 16
	Buckets   = ModeCount * TierCount
)

// BucketOf 把 (mode, tier) 映射成桶号。
func BucketOf(mode, tier uint32) uint32 { return (mode%ModeCount)*TierCount + tier%TierCount }

// ModeTierOf 是 BucketOf 的逆映射。
func ModeTierOf(bucket uint32) (mode, tier uint32) {
	return bucket / TierCount, bucket % TierCount
}

// TeamSize 返回某模式的成队人数。真实项目应查配置表。
func TeamSize(mode uint32) int {
	switch mode {
	case 1:
		return 2
	case 2:
		return 4
	default:
		return 2
	}
}

// MaxWait 是放宽匹配条件前的等待上限。
const MaxWait = 30 * time.Second

type waiter struct {
	uid     uint64
	power   uint64
	since   time.Time
	traceID string
}

// Bucket 是一个 模式×段位 的匹配池，由独占该桶的实例持有。
type Bucket struct {
	o    shard.Ownership
	svc  *shardsvc.Service
	ms   *Service
	mode uint32
	tier uint32

	queue  []*waiter
	inPool map[uint64]bool
	closed bool
}

// NewBucket 构造匹配池。
func NewBucket(o shard.Ownership, svc *shardsvc.Service, ms *Service) *Bucket {
	mode, tier := ModeTierOf(o.Shard)
	return &Bucket{
		o: o, svc: svc, ms: ms, mode: mode, tier: tier,
		inPool: make(map[uint64]bool, 64),
	}
}

// Init 实现 shardsvc.State。匹配池无需从任何地方恢复。
func (b *Bucket) Init(ctx context.Context, o shard.Ownership) error {
	logx.Debug("匹配池就绪", "bucket", o.Shard, "mode", b.mode, "tier", b.tier)
	return nil
}

// Handle 处理入队 / 取消。
func (b *Bucket) Handle(m *bus.Msg) {
	if b.closed {
		_ = m.RespondErr(protocol.ErrUnavailable, "匹配池正在关闭，请重试")
		return
	}
	switch m.Cmd() {
	case protocol.CmdMatchEnqueue:
		b.handleEnqueue(m)
	case protocol.CmdMatchCancel:
		b.handleCancel(m)
	default:
		_ = m.RespondErr(protocol.ErrBadRequest, "未知命令 %d", m.Env.GetCmd())
	}
}

func (b *Bucket) handleEnqueue(m *bus.Msg) {
	var req pb.MatchEnqueueReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	uid := req.GetUid()
	if uid == 0 {
		uid = m.Env.GetUid()
	}
	if uid == 0 {
		_ = m.RespondErr(protocol.ErrBadRequest, "缺少 uid")
		return
	}
	if b.inPool[uid] {
		_ = m.Respond(&pb.MatchAck{Queued: true, Position: int32(b.position(uid))})
		return
	}

	b.queue = append(b.queue, &waiter{
		uid: uid, power: req.GetPower(), since: time.Now(), traceID: m.Env.GetTraceId(),
	})
	b.inPool[uid] = true
	metrics.MatchQueueLen.WithLabelValues(fmt.Sprint(b.mode), fmt.Sprint(b.tier)).Set(float64(len(b.queue)))

	_ = m.Respond(&pb.MatchAck{Queued: true, Position: int32(len(b.queue))})
	b.tryMatch()
}

func (b *Bucket) handleCancel(m *bus.Msg) {
	var req pb.MatchCancelReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	uid := req.GetUid()
	if uid == 0 {
		uid = m.Env.GetUid()
	}
	b.remove(uid)
	_ = m.Respond(&pb.Ack{Ok: true})
}

func (b *Bucket) position(uid uint64) int {
	for i, w := range b.queue {
		if w.uid == uid {
			return i + 1
		}
	}
	return 0
}

func (b *Bucket) remove(uid uint64) {
	if !b.inPool[uid] {
		return
	}
	delete(b.inPool, uid)
	for i, w := range b.queue {
		if w.uid == uid {
			b.queue = append(b.queue[:i], b.queue[i+1:]...)
			break
		}
	}
	metrics.MatchQueueLen.WithLabelValues(fmt.Sprint(b.mode), fmt.Sprint(b.tier)).Set(float64(len(b.queue)))
}

// Tick 周期撮合：等待越久，战力差容忍度越大。
func (b *Bucket) Tick(now time.Time) {
	if b.closed {
		return
	}
	b.tryMatch()
}

// tryMatch 尝试成队。
func (b *Bucket) tryMatch() {
	size := TeamSize(b.mode)
	for len(b.queue) >= size {
		// 按战力排序后取相邻的一组，战力差最小。
		sort.SliceStable(b.queue, func(i, j int) bool { return b.queue[i].power < b.queue[j].power })

		team := b.queue[:size]
		// 等待时间足够长就无条件放行，避免高战力玩家永远匹配不到。
		oldest := time.Since(team[0].since)
		spread := team[size-1].power - team[0].power
		if spread > b.tolerance(oldest) {
			// 找不到合适的一组，等下次。
			if oldest < MaxWait {
				return
			}
		}

		members := make([]uint64, 0, size)
		traceID := team[0].traceID
		for _, w := range team {
			members = append(members, w.uid)
			metrics.MatchWait.WithLabelValues(fmt.Sprint(b.mode)).Observe(time.Since(w.since).Seconds())
			delete(b.inPool, w.uid)
		}
		b.queue = b.queue[size:]
		metrics.MatchQueueLen.WithLabelValues(fmt.Sprint(b.mode), fmt.Sprint(b.tier)).Set(float64(len(b.queue)))

		b.ms.createRoom(b.mode, b.tier, members, traceID)
	}
}

// tolerance 返回可接受的战力差，随等待时长线性放宽。
func (b *Bucket) tolerance(waited time.Duration) uint64 {
	base := uint64(1000)
	return base + uint64(waited/time.Second)*500
}

// OnFlushResult 匹配池不落盘，无需处理。
func (b *Bucket) OnFlushResult(res *store.Result) {}

// FlushAllSync 匹配池不落盘。
func (b *Bucket) FlushAllSync(ctx context.Context) error { return nil }

// Close 释放匹配池。
//
// 池子里的玩家会被通知重新排队 —— 匹配池是 L3 临时数据，丢了不影响资产。
func (b *Bucket) Close(reason shard.ReleaseReason) {
	b.closed = true
	if len(b.queue) > 0 {
		logx.Info("匹配池释放，排队中的玩家需重新入队",
			"bucket", b.o.Shard, "mode", b.mode, "tier", b.tier,
			"queued", len(b.queue), "reason", reason)
		b.ms.notifyRequeue(b.queue)
	}
	metrics.MatchQueueLen.WithLabelValues(fmt.Sprint(b.mode), fmt.Sprint(b.tier)).Set(0)
	b.queue = nil
	b.inPool = nil
}

// ---------------------------------------------------------------------------

// Service 是匹配服务。
type Service struct {
	node     *node.Node
	shards   *shardsvc.Service
	sessions *session.Store
}

// New 构造匹配服务。
func New() *Service { return &Service{} }

// Name 实现 node.Service。
func (s *Service) Name() string { return "match" }

// OwnsBucket 报告某个 模式×段位 的匹配池是否由本实例持有。
func (s *Service) OwnsBucket(mode, tier uint32) bool {
	if s.shards == nil || s.shards.Claimer() == nil {
		return false
	}
	return s.shards.Claimer().Owns(BucketOf(mode, tier))
}

// Start 启动服务。
func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.sessions = session.NewStore(n.Redis, n.Keys, n.Cfg.SessionTTL)

	s.shards = shardsvc.New(shardsvc.Options{
		Kind:  shard.KindMatch,
		Node:  n,
		Space: Buckets,
		// 匹配池不落盘，因此不需要 epoch fencing。
		SkipEpoch: true,
		Tick:      500 * time.Millisecond,
		Wildcard: func(bucket uint32) string {
			mode, tier := ModeTierOf(bucket)
			return subject.MatchReq(mode, tier)
		},
		Factory: func(o shard.Ownership, svc *shardsvc.Service) shardsvc.State {
			return NewBucket(o, svc, s)
		},
	})
	return s.shards.Start(ctx)
}

// NotifyClients 实现 node.Service。
func (s *Service) NotifyClients(ctx context.Context) {}

// StopAccepting 停止接新请求。
func (s *Service) StopAccepting(ctx context.Context) { s.shards.StopAccepting(ctx) }

// FlushAll 匹配池无数据可刷。
func (s *Service) FlushAll(ctx context.Context) error { return s.shards.FlushAll(ctx) }

// Close 释放资源。
func (s *Service) Close(ctx context.Context) { s.shards.Close(ctx) }

// createRoom 撮合成功后建房并通知玩家。
//
// roomID 在这里生成：房间分片由 roomID % 1024 决定，
// 调用方必须先有 ID 才能选对 subject（§4.1）。
func (s *Service) createRoom(mode, tier uint32, members []uint64, traceID string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		roomID, err := s.node.NextID()
		if err != nil {
			logx.Error("生成 roomID 失败，撮合作废", "err", err, "members", members)
			return
		}
		sh := shard.Of(roomID, s.node.Cfg.ShardCount)

		var resp pb.CreateRoomResp
		err = s.node.Bus.Call(ctx, subject.RoomReq(sh, protocol.CmdCreateRoom.Name()),
			protocol.CmdCreateRoom, 0, traceID, &pb.CreateRoomReq{
				RoomId:    roomID,
				Mode:      mode,
				MaxPlayer: uint32(TeamSize(mode)),
				Members:   members,
			}, &resp)
		if err != nil {
			// 建房失败（Room 分片正在交接、或实例刚起还没认领完）不能把玩家丢掉：
			// 他们已经从池子里被取走了，不放回去就等于凭空消失在匹配队列里。
			logx.Trace(traceID).Warn("建房失败，把玩家放回匹配池",
				"room", roomID, "shard", sh, "members", members, "err", err)
			s.requeue(mode, tier, members, traceID)
			return
		}

		logx.Trace(traceID).Info("匹配成功", "room", roomID, "mode", mode, "members", members)
		s.pushMatchFound(members, &pb.MatchFound{RoomId: roomID, Mode: mode, Members: members}, traceID)

		// 开打。
		_ = s.node.Bus.Call(ctx, subject.RoomReq(sh, protocol.CmdStartBattle.Name()),
			protocol.CmdStartBattle, 0, traceID, &pb.RoomOpReq{RoomId: roomID}, nil)
	}()
}

// pushMatchFound 按 gateID 聚合后推送（§9.3）。
func (s *Service) pushMatchFound(members []uint64, found *pb.MatchFound, traceID string) {
	s.pushAggregated(members, protocol.PushMatchFound, found, traceID)
}

// requeue 把一组玩家放回其所属的匹配池。
func (s *Service) requeue(mode, tier uint32, members []uint64, traceID string) {
	for _, uid := range members {
		uid := uid
		bucket := BucketOf(mode, tier)
		if err := s.shards.Runtime().Do(bucket, func() {
			st, ok := s.shards.State(bucket)
			if !ok {
				return
			}
			b, ok := st.(*Bucket)
			if !ok || b.closed || b.inPool[uid] {
				return
			}
			b.queue = append(b.queue, &waiter{uid: uid, since: time.Now(), traceID: traceID})
			b.inPool[uid] = true
		}); err != nil {
			// 桶已不由本实例持有：告诉玩家重新入队，总好过静默丢弃。
			logx.Trace(traceID).Warn("无法放回匹配池，通知玩家重新入队", "uid", uid, "err", err)
			s.pushAggregated([]uint64{uid}, protocol.PushMatchFound, &pb.MatchFound{}, traceID)
		}
	}
}

func (s *Service) notifyRequeue(queue []*waiter) {
	members := make([]uint64, 0, len(queue))
	for _, w := range queue {
		members = append(members, w.uid)
	}
	s.pushAggregated(members, protocol.PushMatchFound, &pb.MatchFound{}, "")
}

func (s *Service) pushAggregated(members []uint64, push protocol.Push, body proto.Message, traceID string) {
	if len(members) == 0 {
		return
	}
	payload, err := proto.Marshal(body)
	if err != nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		byGate, err := s.sessions.GroupByGate(ctx, members)
		if err != nil {
			logx.Trace(traceID).Warn("查询路由表失败，跳过推送", "err", err)
			return
		}
		for gateID, uids := range byGate {
			if gateID == "" {
				continue
			}
			env, err := s.node.Bus.NewEnvelope(protocol.Cmd(push), 0, traceID, &pb.MultiPush{
				Uids:    uids,
				Cmd:     uint32(push),
				Payload: payload,
			})
			if err != nil {
				continue
			}
			_ = s.node.Bus.PublishEnv(subject.GatePush(gateID), env)
		}
	}()
}

var _ node.Service = (*Service)(nil)
