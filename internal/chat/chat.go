// Package chat 实现聊天服：消息中继，不存数据（§2.1）。
//
// 因为不持有任何数据，Chat 是全套设计里唯一可以放心用 queue group 的业务服务：
// 谁处理都一样，负载均衡即可。这与 §2.3 的约束并不矛盾 ——
// 那条约束针对的是「持有玩家数据的服务」。
package chat

import (
	"context"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/profile"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/session"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/subject"
)

// MaxTextLen 是单条消息的最大长度。
const MaxTextLen = 512

// Service 是聊天服务。
type Service struct {
	node     *node.Node
	sessions *session.Store
	profiles *profile.Reader
	subs     []*nats.Subscription
}

// New 构造聊天服务。
func New() *Service { return &Service{} }

// Name 实现 node.Service。
func (s *Service) Name() string { return "chat" }

// Start 启动服务。
func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.sessions = session.NewStore(n.Redis, n.Keys, n.Cfg.SessionTTL)
	s.profiles = profile.NewReader(n.Redis, n.Keys)

	// 用 queue group：Chat 不持有数据，多实例竞争消费就是我们要的负载均衡。
	sub, err := n.Bus.QueueSubscribe(subject.ChatWildcard(), "chat", s.onChat)
	if err != nil {
		return err
	}
	s.subs = append(s.subs, sub)
	logx.Info("聊天服已订阅", "subject", subject.ChatWildcard(), "queue", "chat")
	return nil
}

// NotifyClients 实现 node.Service。
func (s *Service) NotifyClients(ctx context.Context) {}

// StopAccepting 退订。
func (s *Service) StopAccepting(ctx context.Context) {
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
}

// FlushAll 聊天不存数据。
func (s *Service) FlushAll(ctx context.Context) error { return nil }

// Close 无资源可释放。
func (s *Service) Close(ctx context.Context) {}

func (s *Service) onChat(m *bus.Msg) {
	var req pb.ChatReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析聊天消息失败")
		return
	}
	from := req.GetFrom()
	if from == 0 {
		from = m.Env.GetUid()
	}
	text := req.GetText()
	if text == "" {
		_ = m.RespondErr(protocol.ErrBadRequest, "消息为空")
		return
	}
	if len(text) > MaxTextLen {
		text = text[:MaxTextLen]
	}

	msg := &pb.ChatMsg{
		Channel: req.GetChannel(),
		From:    from,
		Text:    text,
		TsMs:    time.Now().UnixMilli(),
	}
	_ = m.Respond(&pb.Ack{Ok: true})

	// 昵称从只读摘要拿：不唤醒发送者的玩家对象，也不经过其 owner（§6.5）。
	go s.relay(m, &req, msg, from)
}

func (s *Service) relay(m *bus.Msg, req *pb.ChatReq, msg *pb.ChatMsg, from uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if p, err := s.profiles.Get(ctx, from); err == nil && p != nil {
		msg.FromNick = p.GetNick()
	}
	traceID := m.Env.GetTraceId()

	switch req.GetChannel() {
	case protocol.ChanWorld:
		// 全服公告类走 push.broadcast（§9.3）。
		s.broadcast(msg, traceID)

	case protocol.ChanPrivate:
		s.toPlayers(ctx, []uint64{req.GetTo()}, msg, traceID)

	case protocol.ChanGuild:
		gid := req.GetGuildId()
		if gid == 0 {
			return
		}
		uids, err := s.guildMembers(ctx, gid)
		if err != nil || len(uids) == 0 {
			return
		}
		// 公会广播不走 NATS 广播：查路由表，按 gateID 聚合（§9.3）。
		s.toPlayers(ctx, uids, msg, traceID)

	case protocol.ChanRoom:
		// 房间广播交给房间分片：它才知道成员列表。
		sh := shard.Of(req.GetRoomId(), s.node.Cfg.ShardCount)
		payload, err := proto.Marshal(msg)
		if err != nil {
			return
		}
		if err := s.node.Bus.Publish(subject.RoomReq(sh, protocol.CmdRoomOp.Name()),
			protocol.CmdRoomOp, from, traceID, &pb.RoomOpReq{
				RoomId:  req.GetRoomId(),
				Uid:     from,
				Op:      3, // room.OpChat
				Payload: payload,
			}); err != nil {
			logx.Trace(traceID).Warn("转发房间聊天失败", "room", req.GetRoomId(), "err", err)
		}

	default:
		logx.Trace(traceID).Warn("未知聊天频道", "channel", req.GetChannel())
	}
}

// toPlayers 按 gateID 聚合后定向推送。
func (s *Service) toPlayers(ctx context.Context, uids []uint64, msg *pb.ChatMsg, traceID string) {
	byGate, err := s.sessions.GroupByGate(ctx, uids)
	if err != nil {
		logx.Trace(traceID).Warn("查询路由表失败", "err", err)
		return
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	for gateID, list := range byGate {
		if gateID == "" {
			continue
		}
		env, err := s.node.Bus.NewEnvelope(protocol.Cmd(protocol.PushChat), 0, traceID, &pb.MultiPush{
			Uids:    list,
			Cmd:     uint32(protocol.PushChat),
			Payload: payload,
		})
		if err != nil {
			continue
		}
		_ = s.node.Bus.PublishEnv(subject.GatePush(gateID), env)
	}
}

func (s *Service) broadcast(msg *pb.ChatMsg, traceID string) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	env, err := s.node.Bus.NewEnvelope(protocol.Cmd(protocol.PushChat), 0, traceID, &pb.MultiPush{
		Cmd:     uint32(protocol.PushChat),
		Payload: payload,
	})
	if err != nil {
		return
	}
	_ = s.node.Bus.PublishEnv(subject.Broadcast, env)
}

func (s *Service) guildMembers(ctx context.Context, guildID uint64) ([]uint64, error) {
	raw, err := s.node.Redis.SMembers(ctx, s.node.Keys.GuildMembers(guildID)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]uint64, 0, len(raw))
	for _, v := range raw {
		if uid, err := strconv.ParseUint(v, 10, 64); err == nil {
			out = append(out, uid)
		}
	}
	return out, nil
}

var _ node.Service = (*Service)(nil)
