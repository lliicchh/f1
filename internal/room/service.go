package room

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/session"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/shardsvc"
	"github.com/gamedev/f1/pkg/subject"
)

// Service 是 Room 服务：房间/战斗状态，分片独占。
type Service struct {
	node     *node.Node
	shards   *shardsvc.Service
	sessions *session.Store
	js       *bus.JS
}

// New 构造 Room 服务。
func New() *Service { return &Service{} }

// Name 实现 node.Service。
func (s *Service) Name() string { return "room" }

// ShardOf 返回 roomID 所属分片：roomShard(roomID) = roomID % 1024（§4.1）。
func (s *Service) ShardOf(roomID uint64) uint32 { return shard.Of(roomID, s.node.Cfg.ShardCount) }

// Owns 报告 roomID 所属分片是否由本实例持有。
func (s *Service) Owns(roomID uint64) bool {
	return s.shards != nil && s.shards.Owns(roomID)
}

// OwnedShards 返回本实例持有的分片列表。
func (s *Service) OwnedShards() []uint32 {
	if s.shards == nil || s.shards.Claimer() == nil {
		return nil
	}
	return s.shards.Claimer().Owned()
}

// Start 启动服务。
func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.sessions = session.NewStore(n.Redis, n.Keys, n.Cfg.SessionTTL)

	s.shards = shardsvc.New(shardsvc.Options{
		Kind: shard.KindRoom,
		Node: n,
		// Room 的 Tick 承担战斗帧推进，需要比 Lobby 更密。
		Tick:     200 * time.Millisecond,
		Wildcard: func(sh uint32) string { return subject.RoomShardWildcard(sh) },
		Factory: func(o shard.Ownership, svc *shardsvc.Service) shardsvc.State {
			return NewShard(o, svc, s)
		},
	})
	if err := s.shards.Start(ctx); err != nil {
		return err
	}

	js, err := n.Bus.NewJetStream(ctx)
	if err != nil {
		return fmt.Errorf("初始化 JetStream 失败: %w", err)
	}
	s.js = js
	return nil
}

// NotifyClients 实现 node.Service。
func (s *Service) NotifyClients(ctx context.Context) {}

// StopAccepting 停止接新请求。
func (s *Service) StopAccepting(ctx context.Context) { s.shards.StopAccepting(ctx) }

// FlushAll 确认刷盘完成。
func (s *Service) FlushAll(ctx context.Context) error { return s.shards.FlushAll(ctx) }

// Close 释放资源。
func (s *Service) Close(ctx context.Context) { s.shards.Close(ctx) }

// broadcastRoom 房间广播。
//
// 「房间/公会广播不走 NATS 广播。由 Room 分片批量查路由表，按 gateID 聚合，
// 每个网关发一条带多个 uid 的包」（§9.3）。
//
// 查路由表要读 Redis，因此在独立 goroutine 里做，绝不阻塞 Actor。
func (s *Service) broadcastRoom(r *Room, push protocol.Push, body proto.Message, traceID string) {
	if len(r.Members) == 0 {
		return
	}
	members := append([]uint64(nil), r.Members...)
	raw, err := proto.Marshal(body)
	if err != nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		byGate, err := s.sessions.GroupByGate(ctx, members)
		if err != nil {
			logx.Trace(traceID).Warn("查询路由表失败，跳过广播", "room", r.ID, "err", err)
			return
		}
		for gateID, uids := range byGate {
			if gateID == "" {
				continue
			}
			env, err := s.node.Bus.NewEnvelope(protocol.Cmd(push), 0, traceID, &pb.MultiPush{
				Uids:    uids,
				Cmd:     uint32(push),
				Payload: raw,
			})
			if err != nil {
				continue
			}
			if err := s.node.Bus.PublishEnv(subject.GatePush(gateID), env); err != nil {
				logx.Trace(traceID).Warn("房间广播失败", "gate", gateID, "uids", len(uids), "err", err)
			}
		}
	}()
}

// publishSettle 把战斗发奖投进 JetStream，每位成员一条。
func (s *Service) publishSettle(r *Room, res *pb.BattleResult) {
	members := append([]uint64(nil), r.Members...)
	roomID := r.ID

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		for _, uid := range members {
			one := proto.Clone(res).(*pb.BattleResult)
			one.TargetUid = uid
			// msgID 用 房间+玩家：同一场战斗对同一玩家只会发一次奖。
			// 接收侧还有订单键幂等兜底，双保险。
			msgID := fmt.Sprintf("battle-%d-%d", roomID, uid)
			if err := s.js.PublishJob(ctx, subject.JobBattleSettle,
				protocol.CmdBattleSettle, uid, "", msgID, one); err != nil {
				logx.Error("投递战斗发奖失败（告警：玩家可能收不到奖励）",
					"room", roomID, "uid", uid, "err", err)
			}
		}
	}()
}

var _ node.Service = (*Service)(nil)
