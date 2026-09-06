package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/authn"
	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/session"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/subject"
)

// Service 网关
type Service struct {
	node     *node.Node
	gateID   string
	sessions *session.Store
	cache    *session.Cache
	auth     *authn.Verifier // auth 验登录token

	ln   net.Listener
	subs []*nats.Subscription
	wg   sync.WaitGroup
	quit chan struct{}
	once sync.Once

	mu     sync.RWMutex
	conns  map[uint64]*Conn // connID -> conn
	byUID  map[uint64]*Conn // uid    -> 当前连接
	closed bool

	rateQPS   int
	rateBurst int

	reqTimeout time.Duration // 转发到后端的超时
	// authTimeout 换票据的超时，比 reqTimeout 长
	//
	// 这条路径尽头是外部渠道，账号服自己还留了重试和熔断的余量，
	// 按 3 秒掐会把本该成功的登录切掉
	authTimeout time.Duration
	retryMax    int
	retryWait   time.Duration
}

func New() *Service {
	return &Service{
		quit:        make(chan struct{}),
		conns:       make(map[uint64]*Conn),
		byUID:       make(map[uint64]*Conn),
		rateQPS:     100,
		rateBurst:   200,
		reqTimeout:  3 * time.Second,
		authTimeout: 12 * time.Second,
		retryMax:    2,
		retryWait:   250 * time.Millisecond,
	}
}

func (s *Service) Name() string { return "gateway" }

func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.gateID = n.NodeID()
	s.sessions = session.NewStore(n.Redis, n.Keys, n.Cfg.SessionTTL)
	s.cache = session.NewCache(s.sessions, 2*time.Second)

	s.auth = authn.NewVerifier(n.Cfg.LoginSecret, n.Cfg.AllowDevAuth)
	switch {
	case s.auth.Enabled():
		logx.Info("登录票据校验已启用")
	case s.auth.DevMode():
		logx.Error("登录认证处于开发模式（告警）：任何 uid 都能直接登录，" +
			"仅限本地开发；生产必须配置 LOGIN_SECRET")
	default:
		// 既没有密钥又没显式开发模式：所有登录都会被拒绝。
		// 这是有意的默认拒绝，「忘了配密钥」不能变成「谁都能登录」
		logx.Error("未配置 LOGIN_SECRET 且未开启 ALLOW_DEV_AUTH：所有登录都会被拒绝")
	}

	// 清掉自己名下的残留会话。崩溃重启后 Redis 里会留着指向本网关的脏路由，
	// 不清理推送就发进黑洞了
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	if _, err := s.sessions.CleanGate(cctx, s.gateID); err != nil {
		logx.Warn("清理历史会话失败（不阻断启动）", "gate", s.gateID, "err", err)
	}
	cancel()

	// 只订两个 subject，订阅数不随在线人数变
	sub, err := n.Bus.Subscribe(subject.GatePush(s.gateID), s.onPush)
	if err != nil {
		return fmt.Errorf("订阅定向推送失败: %w", err)
	}
	s.subs = append(s.subs, sub)

	sub, err = n.Bus.Subscribe(subject.Broadcast, s.onBroadcast)
	if err != nil {
		return fmt.Errorf("订阅全服广播失败: %w", err)
	}
	s.subs = append(s.subs, sub)

	// 顶号之类的会话变更要让本地缓存失效
	sub, err = n.Bus.Subscribe(subject.SessionChangedEvt, func(m *bus.Msg) {
		s.cache.Invalidate(m.Env.GetUid())
	})
	if err != nil {
		return fmt.Errorf("订阅会话变更事件失败: %w", err)
	}
	s.subs = append(s.subs, sub)

	ln, err := net.Listen("tcp", n.Cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", n.Cfg.ListenAddr, err)
	}
	s.ln = ln
	logx.Info("网关开始监听", "addr", n.Cfg.ListenAddr, "gate_id", s.gateID)

	s.wg.Add(1)
	go s.acceptLoop()

	s.wg.Add(1)
	go s.heartbeatLoop()
	return nil
}

func (s *Service) acceptLoop() {
	defer s.wg.Done()
	for {
		raw, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logx.Warn("接受连接失败", "err", err)
			continue
		}

		if tcp, ok := raw.(*net.TCPConn); ok {
			_ = tcp.SetNoDelay(true)
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetKeepAlivePeriod(30 * time.Second)
		}

		// 不要用自增，防止重启后生成相同 ID
		id, err := s.node.NextID()
		if err != nil {
			logx.Error("生成 connID 失败，拒绝连接", "err", err)
			_ = raw.Close()
			continue
		}

		c := newConn(id, raw, s)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = raw.Close()
			continue
		}
		s.conns[id] = c
		n := len(s.conns)
		s.mu.Unlock()
		metrics.Online.WithLabelValues("conn").Set(float64(n))

		go c.readLoop()
		go c.writeLoop()
	}
}

// heartbeatLoop 定期给在线玩家的会话续期
func (s *Service) heartbeatLoop() {
	defer s.wg.Done()
	interval := s.node.Cfg.SessionTTL / 3
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-t.C:
			s.touchAll()
		}
	}
}

func (s *Service) touchAll() {
	s.mu.RLock()
	list := make([]*Conn, 0, len(s.byUID))
	for _, c := range s.byUID {
		list = append(list, c)
	}
	s.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, c := range list {
		uid := c.UID.Load()
		if uid == 0 || c.Closed() {
			continue
		}
		ok, err := s.sessions.Touch(ctx, uid, s.gateID, c.ID)
		if err != nil {
			logx.Debug("续期会话失败", "uid", uid, "err", err)
			continue
		}
		if !ok {
			// 会话被别人接管了（顶号），这条连接该断
			logx.Info("会话已不属于本连接，断开", "uid", uid, "conn", c.ID)
			c.SendPush(protocol.PushKick, nil, "")
			c.drainOut(500 * time.Millisecond)
			c.Close()
		}
	}
	metrics.Online.WithLabelValues("player").Set(float64(len(list)))
}

// ---------------------------------------------------------------------------
// 下行推送
// ---------------------------------------------------------------------------

// onPush 处理定向推送，一条消息可能带多个 uid
func (s *Service) onPush(m *bus.Msg) {
	var mp pb.MultiPush
	if err := bus.Unpack(m.Env, &mp); err != nil {
		logx.Warn("推送消息解析失败", "err", err)
		return
	}
	push := protocol.Push(mp.GetCmd())
	if push == 0 {
		push = protocol.Push(m.Env.GetCmd())
	}

	uids := mp.GetUids()
	if len(uids) == 0 && m.Env.GetUid() != 0 {
		uids = []uint64{m.Env.GetUid()}
	}

	// KICK 要认连接，不能只认 uid
	//
	// 玩家在网关之间来回跳时，KICK 绕一圈回来，byUID 里可能已经换成了
	// 更新的那条连接，照着 uid 踢会把刚登录成功的人踢掉
	var kickConn uint64
	if push == protocol.PushKick {
		var kn pb.KickNotify
		if err := proto.Unmarshal(mp.GetPayload(), &kn); err == nil {
			kickConn = kn.GetConnId()
		}
	}

	for _, uid := range uids {
		s.mu.RLock()
		c := s.byUID[uid]
		s.mu.RUnlock()
		if c == nil {
			continue // 玩家不在本网关了，路由表滞后，丢掉就行
		}
		if kickConn != 0 && c.ID != kickConn {
			logx.Trace(m.Env.GetTraceId()).Info("KICK 指向的连接已经不在了，忽略",
				"uid", uid, "kick_conn", kickConn, "current_conn", c.ID)
			continue
		}
		c.SendPush(push, mp.GetPayload(), m.Env.GetTraceId())

		if push == protocol.PushKick {
			// 发完 KICK 再断，让客户端有机会弹提示
			go func(c *Conn) {
				c.drainOut(500 * time.Millisecond)
				c.Close()
			}(c)
		}
	}
}

// onBroadcast 处理全服公告
func (s *Service) onBroadcast(m *bus.Msg) {
	var mp pb.MultiPush
	if err := bus.Unpack(m.Env, &mp); err != nil {
		return
	}
	push := protocol.Push(mp.GetCmd())
	if push == 0 {
		push = protocol.Push(m.Env.GetCmd())
	}

	s.mu.RLock()
	list := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		list = append(list, c)
	}
	s.mu.RUnlock()

	for _, c := range list {
		c.SendPush(push, mp.GetPayload(), m.Env.GetTraceId())
	}
}

// ---------------------------------------------------------------------------
// 连接生命周期
// ---------------------------------------------------------------------------

func (s *Service) onDisconnect(c *Conn) {
	uid := c.UID.Load()

	s.mu.Lock()
	delete(s.conns, c.ID)
	if uid != 0 {
		if cur, ok := s.byUID[uid]; ok && cur == c {
			delete(s.byUID, uid)
		}
	}
	n := len(s.conns)
	s.mu.Unlock()
	metrics.Online.WithLabelValues("conn").Set(float64(n))

	if uid == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 只有还指向本连接时才解绑，顶号后旧连接断开不能删掉新会话
	if _, err := s.sessions.Unbind(ctx, uid, s.gateID, c.ID); err != nil {
		logx.Warn("解绑会话失败", "uid", uid, "err", err)
	}
	s.publishSessionChanged(uid)

	// 告诉 Lobby 玩家下线了，对象还会留几分钟
	sh := shard.Of(uid, s.node.Cfg.ShardCount)
	fctx, fcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer fcancel()
	if err := s.node.Bus.Call(fctx, subject.LobbyReq(sh, protocol.CmdLogout.Name()),
		protocol.CmdLogout, uid, "", &pb.LogoutReq{
			Uid: uid, GateId: s.gateID, ConnId: c.ID, Reason: 1,
		}, nil); err != nil {
		logx.Debug("通知下线失败", "uid", uid, "err", err)
	}
}

func (s *Service) publishSessionChanged(uid uint64) {
	env, err := s.node.Bus.NewEnvelope(protocol.CmdLogout, uid, "", nil)
	if err != nil {
		return
	}
	_ = s.node.Bus.PublishEnv(subject.SessionChangedEvt, env)
}

// ---------------------------------------------------------------------------
// 停机
// ---------------------------------------------------------------------------

// NotifyClients 先告诉客户端去重连别的网关，比直接断开体验好
func (s *Service) NotifyClients(ctx context.Context) {
	s.mu.RLock()
	list := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		list = append(list, c)
	}
	s.mu.RUnlock()

	if len(list) == 0 {
		return
	}
	logx.Info("下线步骤 0/5：通知客户端重连", "conns", len(list))

	payload, _ := proto.Marshal(&pb.KickNotify{
		Reason:  uint32(protocol.PushMaintenance),
		Message: "服务器维护，请重连",
	})
	for _, c := range list {
		c.SendPush(protocol.PushMaintenance, payload, "")
	}
	// 给客户端一点时间收包再重连
	deadline := time.Now().Add(2 * time.Second)
	for _, c := range list {
		c.drainOut(time.Until(deadline))
	}
}

// StopAccepting 停止接受新连接
func (s *Service) StopAccepting(ctx context.Context) {
	s.once.Do(func() { close(s.quit) })

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	if s.ln != nil {
		_ = s.ln.Close()
	}
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
}

// FlushAll 网关没数据可刷，这里清掉自己名下的会话路由，
// 让玩家能马上连别的网关，不用等 TTL 到期
func (s *Service) FlushAll(ctx context.Context) error {
	s.mu.RLock()
	list := make([]*Conn, 0, len(s.byUID))
	for _, c := range s.byUID {
		list = append(list, c)
	}
	s.mu.RUnlock()

	for _, c := range list {
		uid := c.UID.Load()
		if uid == 0 {
			continue
		}
		if _, err := s.sessions.Unbind(ctx, uid, s.gateID, c.ID); err != nil {
			logx.Warn("下线时解绑会话失败", "uid", uid, "err", err)
		}
	}
	logx.Info("会话路由已清理", "count", len(list))
	return nil
}

func (s *Service) Close(ctx context.Context) {
	s.mu.Lock()
	list := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		list = append(list, c)
	}
	s.conns = make(map[uint64]*Conn)
	s.byUID = make(map[uint64]*Conn)
	s.mu.Unlock()

	for _, c := range list {
		c.Close()
	}
	s.wg.Wait()
}

var _ node.Service = (*Service)(nil)
