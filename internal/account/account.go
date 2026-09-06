// Package account 账号服务：渠道校验、开号、绑定
//
// 拆出来的理由只有一个，登录要打外部网络。渠道慢一秒，网关就得替它把连接
// 挂一秒，开服那会儿所有人同时进，网关的 goroutine 会先被这批请求吃光。
// 把这段挪走之后，网关的登录路径重新变成纯本地 HMAC 验签。
//
// 不持有玩家数据，可以用 queue group，实例数按渠道的响应时间加就行
package account

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/gamedev/f1/pkg/account"
	"github.com/gamedev/f1/pkg/authn"
	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/idp"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/subject"
)

// Service 账号服务
type Service struct {
	node   *node.Node
	store  *account.Store
	idp    *idp.Registry
	issuer *authn.Verifier
	ttl    time.Duration
	subs   []*nats.Subscription
}

func New() *Service { return &Service{} }

func (s *Service) Name() string { return "account" }

func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.store = account.NewStore(n.Redis, "acct")
	s.ttl = n.Cfg.TokenTTL

	s.issuer = authn.NewVerifier(n.Cfg.LoginSecret, false)
	if !s.issuer.Enabled() {
		// 签不出票据的账号服务没有存在意义，而且这种配置下网关也在拒绝一切登录，
		// 与其起来空转不如直接失败
		return errors.New("账号服务必须配置 LOGIN_SECRET，否则签不出登录票据")
	}

	var ps []idp.Provider
	if n.Cfg.IDPSandbox {
		ps = append(ps, idp.NewSandbox())
	}
	s.idp = idp.NewRegistry(idp.Options{
		Timeout: n.Cfg.IDPTimeout,
		Breaker: idp.BreakerOptions{
			Threshold: n.Cfg.IDPBreakerFails,
			Cooldown:  n.Cfg.IDPBreakerCooldown,
		},
	}, ps...)

	if !s.idp.Enabled() {
		logx.Error("未注册任何渠道 provider：所有渠道登录都会被拒绝")
	} else {
		logx.Info("渠道已注册", "channels", s.idp.Channels(),
			"timeout", n.Cfg.IDPTimeout, "breaker_fails", n.Cfg.IDPBreakerFails)
	}

	// 不持有玩家数据，谁处理都一样
	sub, err := n.Bus.QueueSubscribe(subject.AccountWildcard(), "account", s.onReq)
	if err != nil {
		return err
	}
	s.subs = append(s.subs, sub)
	logx.Info("账号服已订阅", "subject", subject.AccountWildcard(), "queue", "account")
	return nil
}

func (s *Service) NotifyClients(ctx context.Context) {}

func (s *Service) StopAccepting(ctx context.Context) {
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
}

func (s *Service) FlushAll(ctx context.Context) error { return nil }

func (s *Service) Close(ctx context.Context) {}

func (s *Service) onReq(m *bus.Msg) {
	switch m.Cmd() {
	case protocol.CmdAuthChannel:
		s.handleAuth(m)
	case protocol.CmdBindChannel:
		s.handleBind(m)
	case protocol.CmdUnbindChannel:
		s.handleUnbind(m)
	case protocol.CmdListBindings:
		s.handleList(m)
	default:
		_ = m.RespondErr(protocol.ErrBadRequest, "账号服不认识该命令")
	}
}

// handleAuth 渠道凭证换我们自己的 token，顺带首登开号
//
// 顺序不能改：先过渠道，再落绑定，最后才签票据。反过来先签票据的话，
// 绑定写失败时玩家手上会有一张指向不存在的号的合法票据
func (s *Service) handleAuth(m *bus.Msg) {
	var req pb.AuthChannelReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	log := logx.Trace(m.Env.GetTraceId())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id, err := s.idp.Verify(ctx, idp.Credential{
		Channel:  req.GetChannel(),
		Value:    req.GetCredential(),
		DeviceID: req.GetDeviceId(),
	})
	if err != nil {
		s.replyIDPErr(m, log, req.GetChannel(), err)
		return
	}

	// 候选 uid 先备好。绑过的话它不会被写进任何地方，丢掉就行
	candidate, err := s.node.NextID()
	if err != nil {
		log.Error("生成 uid 失败", "err", err)
		_ = m.RespondErr(protocol.ErrInternal, "生成账号失败")
		return
	}

	uid, created, err := s.store.Resolve(ctx, id.Channel, id.OpenID, candidate)
	if err != nil {
		log.Error("解析账号失败", "channel", id.Channel, "err", err)
		_ = m.RespondErr(protocol.ErrInternal, "账号查询失败")
		return
	}
	if created {
		metrics.AccountCreated.WithLabelValues(id.Channel).Inc()
		log.Info("首次登录，已开号", "uid", uid, "channel", id.Channel)
	}

	token, err := s.issuer.Issue(uid, s.ttl)
	if err != nil {
		log.Error("签发登录票据失败", "uid", uid, "err", err)
		_ = m.RespondErr(protocol.ErrInternal, "签发票据失败")
		return
	}

	_ = m.Respond(&pb.AuthChannelResp{
		Uid:        uid,
		Token:      token,
		ExpiresAt:  time.Now().Add(s.ttl).UnixMilli(),
		FirstLogin: created,
		OpenId:     id.OpenID,
	})
}

// handleBind 给已登录的 uid 再绑一个渠道
//
// uid 取 Envelope，那是网关按会话填的，不看客户端说自己是谁
func (s *Service) handleBind(m *bus.Msg) {
	var req pb.BindChannelReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	uid := m.Env.GetUid()
	if uid == 0 {
		_ = m.RespondErr(protocol.ErrPermission, "尚未登录")
		return
	}
	log := logx.Trace(m.Env.GetTraceId())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id, err := s.idp.Verify(ctx, idp.Credential{
		Channel: req.GetChannel(),
		Value:   req.GetCredential(),
	})
	if err != nil {
		s.replyIDPErr(m, log, req.GetChannel(), err)
		return
	}

	switch err := s.store.Bind(ctx, uid, id.Channel, id.OpenID); {
	case err == nil:
	case errors.Is(err, account.ErrBindConflict):
		metrics.AccountBindConflict.WithLabelValues(id.Channel, "taken").Inc()
		log.Warn("绑定冲突：渠道账号已属他人", "uid", uid, "channel", id.Channel)
		_ = m.RespondErr(protocol.ErrBindConflict, "该渠道账号已绑定其他玩家")
		return
	case errors.Is(err, account.ErrAlreadyBound):
		metrics.AccountBindConflict.WithLabelValues(id.Channel, "occupied").Inc()
		_ = m.RespondErr(protocol.ErrAlreadyBound, "你已在该渠道绑定过其他账号")
		return
	default:
		log.Error("写绑定失败", "uid", uid, "channel", id.Channel, "err", err)
		_ = m.RespondErr(protocol.ErrInternal, "绑定失败")
		return
	}

	list, err := s.store.List(ctx, uid)
	if err != nil {
		log.Warn("绑定已成功但回读失败", "uid", uid, "err", err)
	}
	log.Info("渠道绑定成功", "uid", uid, "channel", id.Channel)
	_ = m.Respond(&pb.BindChannelResp{Bindings: toPB(list)})
}

func (s *Service) handleUnbind(m *bus.Msg) {
	var req pb.UnbindChannelReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	uid := m.Env.GetUid()
	if uid == 0 {
		_ = m.RespondErr(protocol.ErrPermission, "尚未登录")
		return
	}
	log := logx.Trace(m.Env.GetTraceId())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch err := s.store.Unbind(ctx, uid, req.GetChannel()); {
	case err == nil:
	case errors.Is(err, account.ErrNotBound):
		_ = m.RespondErr(protocol.ErrNotBound, "该渠道未绑定")
		return
	case errors.Is(err, account.ErrLastBinding):
		// 放过去玩家就再也登不进这个号了，这是不可逆的
		_ = m.RespondErr(protocol.ErrLastBinding, "这是最后一个登录方式，不能解绑")
		return
	case errors.Is(err, account.ErrRaced):
		_ = m.RespondErr(protocol.ErrUnavailable, "绑定刚被改过，请重试")
		return
	default:
		log.Error("解绑失败", "uid", uid, "channel", req.GetChannel(), "err", err)
		_ = m.RespondErr(protocol.ErrInternal, "解绑失败")
		return
	}

	list, _ := s.store.List(ctx, uid)
	log.Info("渠道解绑成功", "uid", uid, "channel", req.GetChannel())
	_ = m.Respond(&pb.UnbindChannelResp{Bindings: toPB(list)})
}

func (s *Service) handleList(m *bus.Msg) {
	uid := m.Env.GetUid()
	if uid == 0 {
		_ = m.RespondErr(protocol.ErrPermission, "尚未登录")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	list, err := s.store.List(ctx, uid)
	if err != nil {
		_ = m.RespondErr(protocol.ErrInternal, "查询绑定失败")
		return
	}
	_ = m.Respond(&pb.ListBindingsResp{Bindings: toPB(list)})
}

// replyIDPErr 把渠道错误翻成错误码
//
// 「凭证不对」和「渠道挂了」必须分开：前者让客户端换个凭证重来，
// 后者让它稍后重试。合成一个码的话，渠道抖动期间玩家会以为自己号没了
func (s *Service) replyIDPErr(m *bus.Msg, log *logx.Logger, channel string, err error) {
	switch {
	case errors.Is(err, idp.ErrChannelUnknown):
		log.Warn("请求了未配置的渠道", "channel", channel)
		_ = m.RespondErr(protocol.ErrChannelUnknown, "不支持的登录渠道")
	case errors.Is(err, idp.ErrCredentialInvalid):
		metrics.AuthnFailed.WithLabelValues("channel_credential").Inc()
		_ = m.RespondErr(protocol.ErrCredentialBad, "渠道凭证无效")
	default:
		metrics.AuthnFailed.WithLabelValues("channel_upstream").Inc()
		log.Error("渠道校验失败（告警）", "channel", channel, "err", err)
		_ = m.RespondErr(protocol.ErrUpstreamFailed, "登录渠道暂时不可用，请稍后重试")
	}
}

func toPB(bs []account.Binding) []*pb.Binding {
	out := make([]*pb.Binding, 0, len(bs))
	for _, b := range bs {
		out = append(out, &pb.Binding{
			Channel: b.Channel,
			OpenId:  b.OpenID,
			BoundAt: b.BoundAt.UnixMilli(),
		})
	}
	return out
}
