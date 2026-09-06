// Package match 匹配服，按模式 × 段位分桶
//
// 匹配池纯内存，不刷盘，实例挂了池子清空玩家重排就行，所以跳过 epoch fencing。
// 但「同一个桶只能有一个实例在撮合」这条必须成立，否则两个实例会把同一批人
// 撮进两个房间，所以还是走分片独占认领
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

// 匹配空间：模式 × 段位
const (
	ModeCount = 8
	TierCount = 16
	Buckets   = ModeCount * TierCount
)

func BucketOf(mode, tier uint32) uint32 { return (mode%ModeCount)*TierCount + tier%TierCount }

// ModeTierOf BucketOf 的逆映射
func ModeTierOf(bucket uint32) (mode, tier uint32) {
	return bucket / TierCount, bucket % TierCount
}

// TeamSize 返回该模式几人成队
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

// MaxWait 放宽条件前最多等多久
const MaxWait = 30 * time.Second

type waiter struct {
	uid     uint64
	power   uint64
	since   time.Time
	traceID string
}

// Bucket 一个模式 × 段位的匹配池，由独占这个桶的实例持有
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

func NewBucket(o shard.Ownership, svc *shardsvc.Service, ms *Service) *Bucket {
	mode, tier := ModeTierOf(o.Shard)
	return &Bucket{
		o: o, svc: svc, ms: ms, mode: mode, tier: tier,
		inPool: make(map[uint64]bool, 64),
	}
}

// Init 匹配池不用从哪儿恢复，空的就行
func (b *Bucket) Init(ctx context.Context, o shard.Ownership) error {
	logx.Debug("匹配池就绪", "bucket", o.Shard, "mode", b.mode, "tier", b.tier)
	return nil
}

// Handle 处理入队 / 取消
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

// Tick 定期撮合，等得越久战力差容得越宽
func (b *Bucket) Tick(now time.Time) {
	if b.closed {
		return
	}
	b.tryMatch()
}

func (b *Bucket) tryMatch() {
	size := TeamSize(b.mode)
	for len(b.queue) >= size {
		// 按战力排序取相邻的一组，差值最小
		sort.SliceStable(b.queue, func(i, j int) bool { return b.queue[i].power < b.queue[j].power })

		team := b.queue[:size]
		// 等够久就无条件放行，免得高战力的永远匹配不上
		oldest := time.Since(team[0].since)
		spread := team[size-1].power - team[0].power
		if spread > b.tolerance(oldest) {
			// 这组不合适，等下次
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

// tolerance 返回能接受的战力差，等得越久越宽
func (b *Bucket) tolerance(waited time.Duration) uint64 {
	base := uint64(1000)
	return base + uint64(waited/time.Second)*500
}

// OnFlushResult 匹配池不落盘，没事干
func (b *Bucket) OnFlushResult(res *store.Result) {}

// FlushAllSync 匹配池不落盘
func (b *Bucket) FlushAllSync(ctx context.Context) error { return nil }

// Close 释放匹配池，池子里的人会被通知重新排队
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

// Service 匹配服务
type Service struct {
	node     *node.Node
	shards   *shardsvc.Service
	sessions *session.Store
}

func New() *Service { return &Service{} }

func (s *Service) Name() string { return "match" }

// OwnsBucket 报告某个桶是不是本实例在管
func (s *Service) OwnsBucket(mode, tier uint32) bool {
	if s.shards == nil || s.shards.Claimer() == nil {
		return false
	}
	return s.shards.Claimer().Owns(BucketOf(mode, tier))
}

func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.sessions = session.NewStore(n.Redis, n.Keys, n.Cfg.SessionTTL)

	s.shards = shardsvc.New(shardsvc.Options{
		Kind:  shard.KindMatch,
		Node:  n,
		Space: Buckets,
		// 不落盘，用不上 fencing
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

func (s *Service) NotifyClients(ctx context.Context) {}

func (s *Service) StopAccepting(ctx context.Context) { s.shards.StopAccepting(ctx) }

func (s *Service) FlushAll(ctx context.Context) error { return s.shards.FlushAll(ctx) }

func (s *Service) Close(ctx context.Context) { s.shards.Close(ctx) }

// createRoom 撮合成功后建房并通知玩家
//
// roomID 在这里生成，房间分片由它决定，得先有 ID 才能选对 subject
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
			// 建房失败（分片在交接，或实例刚起还没认领完）不能把人丢了，
			// 他们已经从池子里取出来了，不放回去就凭空消失了
			logx.Trace(traceID).Warn("建房失败，把玩家放回匹配池",
				"room", roomID, "shard", sh, "members", members, "err", err)
			s.requeue(mode, tier, members, traceID)
			return
		}

		logx.Trace(traceID).Info("匹配成功", "room", roomID, "mode", mode, "members", members)
		s.pushMatchFound(members, &pb.MatchFound{RoomId: roomID, Mode: mode, Members: members}, traceID)

		// 开打
		_ = s.node.Bus.Call(ctx, subject.RoomReq(sh, protocol.CmdStartBattle.Name()),
			protocol.CmdStartBattle, 0, traceID, &pb.RoomOpReq{RoomId: roomID}, nil)
	}()
}

// pushMatchFound 按 gateID 聚合后推
func (s *Service) pushMatchFound(members []uint64, found *pb.MatchFound, traceID string) {
	s.pushAggregated(members, protocol.PushMatchFound, found, traceID)
}

// requeue 把一组玩家放回匹配池
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
			// 桶不在本实例了，让玩家重新入队，总比静默丢掉强
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
