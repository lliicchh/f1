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

// dispatch 把客户端请求路由到后端
//
// 每条请求起一个 goroutine，请求-应答是同步等的，别让一条慢请求卡住后面的读取
func (s *Service) dispatch(c *Conn, env *pb.Envelope) {
	cmd := protocol.Cmd(env.GetCmd())

	// 登录必须先建会话，走单独路径
	if cmd == protocol.CmdLogin {
		go s.handleLogin(c, env)
		return
	}

	// 只处理客户端的请求
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
	// uid 以服务端会话为准
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

// route 决定发去哪个 subject，路由与权限管理分开
func (s *Service) route(cmd protocol.Cmd, uid uint64, env *pb.Envelope) (string, error) {
	count := s.node.Cfg.ShardCount

	switch cmd {
	case protocol.CmdCreateRoom, protocol.CmdJoinRoom, protocol.CmdLeaveRoom,
		protocol.CmdRoomOp, protocol.CmdRoomInfo, protocol.CmdStartBattle:
		// 房间按 roomID 分片，roomID 在 body 里，网关得解一层
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

	case protocol.CmdAuthChannel, protocol.CmdBindChannel,
		protocol.CmdUnbindChannel, protocol.CmdListBindings:
		return subject.AccountReq(cmd.Name()), nil

	case protocol.CmdWorldBossState, protocol.CmdWorldBossHit:
		return subject.WorldReq(cmd.Name()), nil

	case protocol.CmdLogin:
		// 登录走单独路径
		return "", errUnknownCmd

	default:
		// 玩家逻辑，按 uid 分片
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

// forward 转发请求并把应答回给客户端
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

// handleLogin 建会话，处理顶号
func (s *Service) handleLogin(c *Conn, env *pb.Envelope) {
	var req pb.LoginReq
	if err := bus.Unpack(env, &req); err != nil {
		c.replyErr(env, protocol.ErrBadRequest, "解析登录请求失败")
		return
	}
	// trace_id 先定下来。后面有 goroutine 要用它，而主流程还会继续改 env，
	// 让它们各拿各的
	if env.GetTraceId() == "" {
		env.TraceId = bus.NewTraceID()
	}
	traceID := env.GetTraceId()

	uid, token := req.GetUid(), req.GetToken()
	if uid == 0 {
		uid = env.GetUid()
	}

	// 渠道登录：网关不打外部网络，转给账号服务换我们自己的票据。
	// 重连应当带上次的 token 走下面那条纯本地校验的路，不要再打一次渠道
	if req.GetChannel() != "" {
		u, t, err := s.authByChannel(c, env, &req, traceID)
		if err != nil {
			return // 错误已经回给客户端了
		}
		uid, token = u, t
	}

	if uid == 0 {
		c.replyErr(env, protocol.ErrBadRequest, "缺少 uid")
		return
	}
	// 渠道换来的票据也要在这里再验一遍。uid 只认票据这一个来源，
	// 多一条旁路就多一种伪造姿势
	if err := s.auth.Verify(uid, token); err != nil {
		metrics.AuthnFailed.WithLabelValues("token").Inc()
		logx.Warn("登录认证失败", "uid", uid, "conn", c.ID, "err", err)
		c.replyErr(env, protocol.ErrPermission, "认证失败")
		return
	}
	req.Uid = uid
	req.Token = token
	// 凭证不再往后传，Lobby 不需要也不该看到
	req.Channel = ""
	req.Credential = ""

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Lua 原子换 session，顺便拿到被顶掉的旧 gateID 和 connID
	old, err := s.sessions.Bind(ctx, uid, s.gateID, c.ID)
	if err != nil {
		c.replyErr(env, protocol.ErrInternal, "建立会话失败")
		return
	}

	// 旧连接就在本网关时不绕 NATS。下面那个本地分支按连接指针踢，比
	// 发给自己再按 uid 找回来准，也少一次往返
	if !old.Empty() && old.GateID != s.gateID {
		s.kick(old, uid, traceID)
	}

	c.UID.Store(uid)
	s.mu.Lock()
	if prev, ok := s.byUID[uid]; ok && prev != c {
		// 同一个网关上的顶号，本地直接断旧连接。
		// env 不能带进 goroutine，主流程等下还要改它
		go func(p *Conn, tid string) {
			p.SendPush(protocol.PushKick, nil, tid)
			p.drainOut(300 * time.Millisecond)
			p.Close()
		}(prev, traceID)
	}
	s.byUID[uid] = c
	online := len(s.byUID)
	s.mu.Unlock()
	metrics.Online.WithLabelValues("player").Set(float64(online))

	s.publishSessionChanged(uid)

	// 会话建好了再转给 Lobby，它要 gateID 和 connID 才能定向推送
	req.GateId = s.gateID
	req.ConnId = c.ID
	if err := bus.Pack(env, &req); err != nil {
		c.replyErr(env, protocol.ErrInternal, "打包失败")
		return
	}
	env.Uid = uid
	env.FromNode = s.gateID
	sh := shard.Of(uid, s.node.Cfg.ShardCount)
	env.Shard = sh

	s.forward(c, env, subject.LobbyReq(sh, protocol.CmdLogin.Name()))
}

// authByChannel 拿渠道凭证去账号服务换 uid 和票据
//
// 不重试。渠道故障时重试只是往一个已经扛不住的上游加压，账号服务那边有熔断，
// 这里把错误如实回给客户端，由它决定隔多久再来
func (s *Service) authByChannel(c *Conn, env *pb.Envelope, req *pb.LoginReq, traceID string) (uint64, string, error) {
	authEnv, err := s.node.Bus.NewEnvelope(protocol.CmdAuthChannel, 0, traceID,
		&pb.AuthChannelReq{
			Channel:    req.GetChannel(),
			Credential: req.GetCredential(),
			DeviceId:   req.GetDeviceId(),
		})
	if err != nil {
		c.replyErr(env, protocol.ErrInternal, "打包失败")
		return 0, "", err
	}
	authEnv.FromNode = s.gateID

	ctx, cancel := context.WithTimeout(context.Background(), s.authTimeout)
	defer cancel()

	resp, err := s.node.Bus.RequestEnv(ctx, subject.AccountReq(protocol.CmdAuthChannel.Name()), authEnv)
	if err != nil {
		metrics.AuthnFailed.WithLabelValues("account_unreachable").Inc()
		logx.Trace(traceID).Error("账号服不可达（告警）",
			"channel", req.GetChannel(), "conn", c.ID, "err", err)
		c.replyErr(env, protocol.ErrUpstreamFailed, "登录服务暂时不可用，请稍后重试")
		return 0, "", err
	}
	if code := protocol.ErrCode(resp.GetErrCode()); code != protocol.ErrOK {
		c.replyErr(env, code, "%s", resp.GetErrMsg())
		return 0, "", errors.New(resp.GetErrMsg())
	}

	var out pb.AuthChannelResp
	if err := bus.Unpack(resp, &out); err != nil {
		c.replyErr(env, protocol.ErrInternal, "解析账号应答失败")
		return 0, "", err
	}
	if out.GetUid() == 0 || out.GetToken() == "" {
		c.replyErr(env, protocol.ErrInternal, "账号服返回了空票据")
		return 0, "", errors.New("账号服返回了空票据")
	}
	logx.Trace(traceID).Info("渠道登录换票成功",
		"uid", out.GetUid(), "channel", req.GetChannel(), "first_login", out.GetFirstLogin())
	return out.GetUid(), out.GetToken(), nil
}

// kick 让旧连接所在的网关把人踢下去
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
