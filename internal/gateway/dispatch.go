package gateway

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/authz"
	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/session"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/subject"
)

// dispatch 把客户端请求路由到后端。
//
// 每条请求起一个 goroutine：请求-应答是同步等待的，
// 不能让一条慢请求卡住该连接后续的读取。
func (s *Service) dispatch(c *Conn, env *pb.Envelope) {
	cmd := protocol.Cmd(env.GetCmd())

	// 登录必须先建会话，走单独路径。
	if cmd == protocol.CmdLogin {
		go s.handleLogin(c, env)
		return
	}

	// 第一道鉴权（评审 P0-1）：只放行客户端级命令。
	//
	// 以前这里是一个 switch 白名单，新增内部命令时忘了排除就默认放行 ——
	// add_currency 就是这么漏给客户端的，任何人都能给自己造币。
	// 现在级别标在命令定义处，未登记的命令默认是最严的 Internal 级。
	if err := authz.CheckClient(cmd); err != nil {
		metrics.AuthzRejected.WithLabelValues(cmd.Name(), "client_level").Inc()
		logx.Warn("客户端尝试调用非客户端级命令",
			"cmd", cmd, "level", cmd.Level(), "uid", c.UID.Load(), "conn", c.ID)
		c.replyErr(env, protocol.ErrPermission, "无权调用该命令")
		return
	}

	uid := c.UID.Load()
	if uid == 0 {
		c.replyErr(env, protocol.ErrPermission, "尚未登录")
		return
	}
	// 客户端不可信：uid 以服务端会话为准，且必须抹掉客户端自带的签名与操作者字段 ——
	// 否则客户端可以塞一段伪造的 auth 进来碰运气。
	env.Uid = uid
	env.Auth = nil
	env.Operator = ""
	env.FromNode = s.gateID
	if env.GetTraceId() == "" {
		env.TraceId = bus.NewTraceID()
	}

	subj, err := s.route(cmd, uid, env)
	if err != nil {
		c.replyErr(env, protocol.ErrBadRequest, "%v", err)
		return
	}
	go s.forward(c, env, subj)
}

// route 决定目标 subject（§5.1）。
//
// 进到这里的命令都已通过 authz.CheckClient，因此这里只关心「发去哪」，
// 不再兼任「能不能发」——把鉴权和路由分开，是为了避免以后改路由时顺手放开权限。
func (s *Service) route(cmd protocol.Cmd, uid uint64, env *pb.Envelope) (string, error) {
	count := s.node.Cfg.ShardCount

	switch cmd {
	case protocol.CmdCreateRoom, protocol.CmdJoinRoom, protocol.CmdLeaveRoom,
		protocol.CmdRoomOp, protocol.CmdRoomInfo, protocol.CmdStartBattle:
		// 房间逻辑：shard = roomID % 1024。
		// roomID 藏在 body 里，因此网关必须解一层 —— 绝不能让客户端自己指定分片。
		roomID, err := extractRoomID(cmd, env)
		if err != nil {
			return "", err
		}
		sh := shard.Of(roomID, count)
		env.Shard = sh
		return subject.RoomReq(sh, cmd.Name()), nil

	case protocol.CmdMatchEnqueue, protocol.CmdMatchCancel:
		mode, tier, err := extractMatchKey(cmd, env)
		if err != nil {
			return "", err
		}
		return subject.MatchReq(mode, tier), nil

	case protocol.CmdChatSend:
		return subject.ChatReq(cmd.Name()), nil

	case protocol.CmdWorldBossState, protocol.CmdWorldBossHit:
		return subject.WorldReq(cmd.Name()), nil

	case protocol.CmdLogin:
		// 登录走单独路径，不应到这里。
		return "", errUnknownCmd

	default:
		// 其余客户端级命令都是玩家逻辑：shard = uid % 1024。
		sh := shard.Of(uid, count)
		env.Shard = sh
		return subject.LobbyReq(sh, cmd.Name()), nil
	}
}

var errUnknownCmd = errors.New("未知命令")

func extractRoomID(cmd protocol.Cmd, env *pb.Envelope) (uint64, error) {
	switch cmd {
	case protocol.CmdCreateRoom:
		var req pb.CreateRoomReq
		if err := bus.Unpack(env, &req); err != nil {
			return 0, err
		}
		if req.GetRoomId() == 0 {
			return 0, errors.New("创建房间需带 room_id")
		}
		return req.GetRoomId(), nil
	case protocol.CmdJoinRoom:
		var req pb.JoinRoomReq
		if err := bus.Unpack(env, &req); err != nil {
			return 0, err
		}
		return nonZero(req.GetRoomId())
	case protocol.CmdLeaveRoom:
		var req pb.LeaveRoomReq
		if err := bus.Unpack(env, &req); err != nil {
			return 0, err
		}
		return nonZero(req.GetRoomId())
	default:
		var req pb.RoomOpReq
		if err := bus.Unpack(env, &req); err != nil {
			return 0, err
		}
		return nonZero(req.GetRoomId())
	}
}

func nonZero(id uint64) (uint64, error) {
	if id == 0 {
		return 0, errors.New("缺少 room_id")
	}
	return id, nil
}

func extractMatchKey(cmd protocol.Cmd, env *pb.Envelope) (uint32, uint32, error) {
	if cmd == protocol.CmdMatchCancel {
		var req pb.MatchCancelReq
		if err := bus.Unpack(env, &req); err != nil {
			return 0, 0, err
		}
		return req.GetMode(), req.GetTier(), nil
	}
	var req pb.MatchEnqueueReq
	if err := bus.Unpack(env, &req); err != nil {
		return 0, 0, err
	}
	return req.GetMode(), req.GetTier(), nil
}

// forward 转发请求并把应答回给客户端。
//
// 分片交接窗口内请求会被 Core NATS 丢弃（无订阅者），
// 这里做有限次重投 —— 即 §10.2 所说的「网关做 500ms 缓冲重投」，
// 让客户端即使没实现重试也不至于失败（§16.5）。
func (s *Service) forward(c *Conn, env *pb.Envelope, subj string) {
	var lastErr error

	for attempt := 0; attempt <= s.retryMax; attempt++ {
		if c.Closed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.reqTimeout)
		resp, err := s.node.Bus.RequestEnv(ctx, subj, env)
		cancel()

		if err == nil {
			if code := protocol.ErrCode(resp.GetErrCode()); isRetryable(code) && attempt < s.retryMax {
				lastErr = errors.New(resp.GetErrMsg())
				time.Sleep(s.retryWait)
				continue
			}
			c.SendEnv(resp)
			return
		}

		lastErr = err
		if !errors.Is(err, bus.ErrNoResponders) && !errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if attempt < s.retryMax {
			// 交接窗口通常在百毫秒级，等一会儿重投多半能成。
			time.Sleep(s.retryWait)
		}
	}

	logx.Trace(env.GetTraceId()).Warn("转发请求失败",
		"subject", subj, "uid", env.GetUid(), "cmd", protocol.Cmd(env.GetCmd()), "err", lastErr)
	c.replyErr(env, protocol.ErrUnavailable, "服务暂时不可用，请重试")
}

func isRetryable(code protocol.ErrCode) bool {
	return code == protocol.ErrUnavailable || code == protocol.ErrNotOwner
}

// ---------------------------------------------------------------------------
// 登录 / 顶号
// ---------------------------------------------------------------------------

// handleLogin 建立会话并处理顶号（§9.2）。
func (s *Service) handleLogin(c *Conn, env *pb.Envelope) {
	var req pb.LoginReq
	if err := bus.Unpack(env, &req); err != nil {
		c.replyErr(env, protocol.ErrBadRequest, "解析登录请求失败")
		return
	}
	uid := req.GetUid()
	if uid == 0 {
		uid = env.GetUid()
	}
	if uid == 0 {
		c.replyErr(env, protocol.ErrBadRequest, "缺少 uid")
		return
	}
	if err := s.auth.Verify(uid, req.GetToken()); err != nil {
		metrics.AuthnFailed.WithLabelValues("token").Inc()
		logx.Warn("登录认证失败", "uid", uid, "conn", c.ID, "err", err)
		c.replyErr(env, protocol.ErrPermission, "认证失败")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Lua 原子替换 session，同时取出被顶掉的旧 gateID/connID。
	old, err := s.sessions.Bind(ctx, uid, s.gateID, c.ID)
	if err != nil {
		c.replyErr(env, protocol.ErrInternal, "建立会话失败")
		return
	}

	if !old.Empty() {
		s.kick(old, uid, env.GetTraceId())
	}

	c.UID.Store(uid)
	s.mu.Lock()
	if prev, ok := s.byUID[uid]; ok && prev != c {
		// 同一网关上的顶号：本地直接断开旧连接。
		go func(p *Conn) {
			p.SendPush(protocol.PushKick, nil, env.GetTraceId())
			p.drainOut(300 * time.Millisecond)
			p.Close()
		}(prev)
	}
	s.byUID[uid] = c
	online := len(s.byUID)
	s.mu.Unlock()
	metrics.Online.WithLabelValues("player").Set(float64(online))

	s.publishSessionChanged(uid)

	// 会话就绪后再转给 Lobby：Lobby 需要 gateID/connID 才能定向推送。
	req.GateId = s.gateID
	req.ConnId = c.ID
	if err := bus.Pack(env, &req); err != nil {
		c.replyErr(env, protocol.ErrInternal, "打包失败")
		return
	}
	env.Uid = uid
	env.FromNode = s.gateID
	if env.GetTraceId() == "" {
		env.TraceId = bus.NewTraceID()
	}
	sh := shard.Of(uid, s.node.Cfg.ShardCount)
	env.Shard = sh

	s.forward(c, env, subject.LobbyReq(sh, protocol.CmdLogin.Name()))
}

// kick 通知旧连接所在的网关踢人。
func (s *Service) kick(old session.Info, uid uint64, traceID string) {
	metrics.SessionKick.Inc()
	logx.Trace(traceID).Info("顶号，通知旧网关踢下线",
		"uid", uid, "old_gate", old.GateID, "old_conn", old.ConnID)

	payload, _ := proto.Marshal(&pb.KickNotify{
		Uid:     uid,
		ConnId:  old.ConnID,
		Reason:  uint32(protocol.PushKick),
		Message: "账号在其他设备登录",
	})
	env, err := s.node.Bus.NewEnvelope(protocol.Cmd(protocol.PushKick), uid, traceID, &pb.MultiPush{
		Uids:    []uint64{uid},
		Cmd:     uint32(protocol.PushKick),
		Payload: payload,
	})
	if err != nil {
		return
	}
	if err := s.node.Bus.PublishEnv(subject.GatePush(old.GateID), env); err != nil {
		logx.Trace(traceID).Warn("发送 KICK 失败", "gate", old.GateID, "err", err)
	}
}
