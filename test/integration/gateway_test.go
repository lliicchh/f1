package integration

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/internal/gateway"
	"github.com/gamedev/f1/internal/lobby"
	"github.com/gamedev/f1/pkg/authz"
	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/session"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/pkg/subject"
	"github.com/gamedev/f1/test/harness"
)

// client 是一个最小客户端，帧格式与 gateway 一致：[4 字节大端长度][Envelope]。
type client struct {
	conn net.Conn
	t    *testing.T
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("连接网关失败: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &client{conn: c, t: t}
}

func (c *client) send(cmd protocol.Cmd, uid uint64, body proto.Message) {
	c.t.Helper()
	raw, err := proto.Marshal(body)
	if err != nil {
		c.t.Fatal(err)
	}
	env := &pb.Envelope{Cmd: uint32(cmd), Uid: uid, Body: raw, TsMs: time.Now().UnixMilli(), Seq: 1}
	data, err := proto.Marshal(env)
	if err != nil {
		c.t.Fatal(err)
	}
	head := make([]byte, 4)
	binary.BigEndian.PutUint32(head, uint32(len(data)))
	if _, err := c.conn.Write(append(head, data...)); err != nil {
		c.t.Fatalf("发送失败: %v", err)
	}
}

func (c *client) recv(timeout time.Duration) (*pb.Envelope, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	var head [4]byte
	if _, err := io.ReadFull(c.conn, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n == 0 || n > 1<<20 {
		return nil, errors.New("非法帧长度")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return nil, err
	}
	env := &pb.Envelope{}
	if err := proto.Unmarshal(buf, env); err != nil {
		return nil, err
	}
	return env, nil
}

// recvCmd 一直读到指定 cmd 为止（跳过中途的推送）。
func (c *client) recvCmd(cmd uint32, timeout time.Duration) (*pb.Envelope, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		env, err := c.recv(time.Until(deadline))
		if err != nil {
			return nil, err
		}
		if env.GetCmd() == cmd {
			return env, nil
		}
	}
	return nil, errors.New("等待应答超时")
}

func startGateway(t *testing.T, env *harness.Env, seq int) (*node.Node, *gateway.Service, string) {
	t.Helper()
	addr := freeAddr(t)
	n := env.Node(t, "gateway", "lobby", seq, func(c *config.Config) {
		lobbyCfg(c)
		c.ListenAddr = addr
	})
	svc := gateway.New()
	if err := svc.Start(context.Background(), n); err != nil {
		t.Fatalf("启动网关失败: %v", err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		svc.StopAccepting(sctx)
		_ = svc.FlushAll(sctx)
		svc.Close(sctx)
	})
	return n, svc, addr
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// 端到端：客户端连网关 → 登录 → 会话落 Redis → 业务请求被路由到 Lobby 分片。
func TestGatewayLoginRoutesToLobby(t *testing.T) {
	env := harness.Start(t)
	_, lsvc := startLobby(t, env, 1)
	gn, _, addr := startGateway(t, env, 1)

	const uid = uint64(7007)
	harness.Eventually(t, 20*time.Second, "Lobby 认领分片", func() bool { return lsvc.Owns(uid) })

	c := dial(t, addr)
	c.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid, Token: harness.Token(t, uid)})

	resp, err := c.recvCmd(uint32(protocol.CmdLogin), 10*time.Second)
	if err != nil {
		t.Fatalf("等待登录应答失败: %v", err)
	}
	if resp.GetErrCode() != 0 {
		t.Fatalf("登录失败: %d %s", resp.GetErrCode(), resp.GetErrMsg())
	}
	var lr pb.LoginResp
	if err := proto.Unmarshal(resp.GetBody(), &lr); err != nil {
		t.Fatal(err)
	}
	if lr.GetBase().GetUid() != uid {
		t.Fatalf("登录返回的玩家不对: %+v", lr.GetBase())
	}

	// §9.1：会话路由表落在 Redis。
	sessions := session.NewStore(gn.Redis, gn.Keys, gn.Cfg.SessionTTL)
	ctx := context.Background()
	info, err := sessions.Get(ctx, uid)
	if err != nil || info.Empty() {
		t.Fatalf("会话未写入路由表: %+v err=%v", info, err)
	}
	if info.GateID != gn.NodeID() {
		t.Fatalf("会话指向的网关不对: %q", info.GateID)
	}

	// 后续业务请求应被网关按 uid 路由到正确的 Lobby 分片。
	c.send(protocol.CmdGetBag, uid, &pb.GetBagReq{})
	got, err := c.recvCmd(uint32(protocol.CmdGetBag), 10*time.Second)
	if err != nil {
		t.Fatalf("等待背包应答失败: %v", err)
	}
	if got.GetErrCode() != 0 {
		t.Fatalf("查询背包失败: %s", got.GetErrMsg())
	}
	var bag pb.GetBagResp
	if err := proto.Unmarshal(got.GetBody(), &bag); err != nil {
		t.Fatal(err)
	}
	if bag.GetBag().GetUid() != uid {
		t.Fatalf("背包属于 uid=%d，期望 %d", bag.GetBag().GetUid(), uid)
	}
}

// 评审 P0-1 的回归测试：客户端绝不能调用内部命令。
//
// 这曾经是一个真实存在的送钱漏洞 —— add_currency 在网关白名单里，
// 处理函数又不校验来源，任何人构造一个请求就能凭空造币。
func TestClientCannotCallInternalCommands(t *testing.T) {
	env := harness.Start(t)
	_, lsvc := startLobby(t, env, 1)
	_, _, addr := startGateway(t, env, 1)

	const uid = uint64(7100)
	harness.Eventually(t, 20*time.Second, "Lobby 认领分片", func() bool { return lsvc.Owns(uid) })

	c := dial(t, addr)
	c.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid, Token: harness.Token(t, uid)})
	if resp, err := c.recvCmd(uint32(protocol.CmdLogin), 10*time.Second); err != nil || resp.GetErrCode() != 0 {
		t.Fatalf("登录失败: %v", err)
	}

	internal := []struct {
		cmd  protocol.Cmd
		body proto.Message
	}{
		{protocol.CmdAddCurrency, &pb.AddCurrencyReq{Currency: 2, Delta: 999999999}},
		{protocol.CmdAddItem, &pb.AddItemReq{TplId: 4001, Count: 100}},
		{protocol.CmdApplyTransfer, &pb.TransferJob{ToUid: uid}},
		{protocol.CmdBattleSettle, &pb.BattleResult{RoomId: 1, TargetUid: uid}},
		{protocol.CmdApplyJackpot, &pb.JackpotClaimReq{PoolId: "grand", Uid: uid, Amount: 1e9, Txid: "x"}},
		{protocol.CmdGMGrant, &pb.GMGrantReq{Uid: uid, Currency: 2, Amount: 1e9, Ticket: "t"}},
	}

	for _, tc := range internal {
		c.send(tc.cmd, uid, tc.body)
		resp, err := c.recvCmd(uint32(tc.cmd), 10*time.Second)
		if err != nil {
			t.Fatalf("命令 %s 应收到拒绝应答: %v", tc.cmd, err)
		}
		if resp.GetErrCode() != uint32(protocol.ErrPermission) {
			t.Fatalf("命令 %s 必须被拒绝，实际 err_code=%d msg=%q",
				tc.cmd, resp.GetErrCode(), resp.GetErrMsg())
		}
	}

	// 确认真的没到账。
	if gold := goldOf(t, env.Node(t, "lobby", "lobby", 9, lobbyCfg), uid); gold != 0 {
		t.Fatalf("被拒绝的命令不应产生任何余额变动，实际金币 = %d", gold)
	}
}

// 伪造的内部签名必须被识破。
func TestForgedInternalSignatureRejected(t *testing.T) {
	env := harness.Start(t)
	n, lsvc := startLobby(t, env, 1)

	const uid = uint64(7200)
	harness.Eventually(t, 20*time.Second, "Lobby 认领分片", func() bool { return lsvc.Owns(uid) })

	sh := shard.Of(uid, n.Cfg.ShardCount)
	e, err := n.Bus.NewEnvelope(protocol.CmdAddCurrency, uid, "", &pb.AddCurrencyReq{
		Currency: 2, Delta: 1_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 用错误的密钥签名。
	bad := authz.NewSigner("wrong-secret")
	if err := bad.Sign(e); err != nil {
		t.Fatal(err)
	}

	// 分片刚认领时订阅可能还没起来，重试到拿得到应答为止 ——
	// 这里要断言的是「签名被拒」，不能被交接窗口的 no-responders 掩盖。
	var resp *pb.Envelope
	for attempt := 0; attempt < 20; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r, err := n.Bus.RequestEnv(ctx, subject.LobbyReq(sh, protocol.CmdAddCurrency.Name()), e)
		cancel()
		if err == nil {
			resp = r
			break
		}
		time.Sleep(200 * time.Millisecond)
		// 重发要刷新时间戳，否则会先撞上防重放的时效检查。
		e.TsMs = time.Now().UnixMilli()
		_ = bad.Sign(e)
	}
	if resp == nil {
		t.Fatal("始终没拿到应答")
	}
	if resp.GetErrCode() != uint32(protocol.ErrPermission) {
		t.Fatalf("伪造签名必须被拒绝，实际 err_code=%d msg=%q", resp.GetErrCode(), resp.GetErrMsg())
	}
}

// 无签名的内部命令同样必须被拒绝（即便直连内网 NATS）。
func TestUnsignedInternalCommandRejected(t *testing.T) {
	env := harness.Start(t)
	n, lsvc := startLobby(t, env, 1)

	const uid = uint64(7300)
	harness.Eventually(t, 20*time.Second, "Lobby 认领分片", func() bool { return lsvc.Owns(uid) })

	sh := shard.Of(uid, n.Cfg.ShardCount)

	// 必须明确断言是「无权限」，而不是「没人应答」——
	// 后者也会返回 error，但那不能证明鉴权起了作用。
	var remote *bus.RemoteError
	for attempt := 0; attempt < 20; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := n.Bus.Call(ctx, subject.LobbyReq(sh, protocol.CmdAddCurrency.Name()),
			protocol.CmdAddCurrency, uid, "", &pb.AddCurrencyReq{Currency: 2, Delta: 500}, nil)
		cancel()
		if err == nil {
			t.Fatal("未签名的内部命令必须被拒绝")
		}
		if errors.As(err, &remote) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if remote == nil {
		t.Fatal("始终没拿到业务错误应答")
	}
	if remote.Code != protocol.ErrPermission {
		t.Fatalf("应以无权限拒绝，实际 %d(%s)", remote.Code, remote.Msg)
	}
}

// 无效登录票据必须被拒绝（评审 P0-3）。
func TestInvalidLoginTokenRejected(t *testing.T) {
	env := harness.Start(t)
	_, _, addr := startGateway(t, env, 1)

	cases := []struct {
		name  string
		uid   uint64
		token string
	}{
		{"空票据", 7400, ""},
		{"乱填", 7400, "garbage"},
		{"别人的票据", 7400, harness.Token(t, 9999)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := dial(t, addr)
			c.send(protocol.CmdLogin, tc.uid, &pb.LoginReq{Uid: tc.uid, Token: tc.token})
			resp, err := c.recv(5 * time.Second)
			if err != nil {
				t.Fatalf("应收到拒绝应答: %v", err)
			}
			if resp.GetErrCode() != uint32(protocol.ErrPermission) {
				t.Fatalf("应拒绝登录，实际 err_code=%d", resp.GetErrCode())
			}
		})
	}
}

// §9.2 顶号：Lua 原子替换 session，取出旧 gateID 后发 KICK。
func TestGatewayKickOnDuplicateLogin(t *testing.T) {
	env := harness.Start(t)
	_, lsvc := startLobby(t, env, 1)
	gn1, _, addr1 := startGateway(t, env, 1)
	_, _, addr2 := startGateway(t, env, 2)

	const uid = uint64(8008)
	harness.Eventually(t, 20*time.Second, "Lobby 认领分片", func() bool { return lsvc.Owns(uid) })

	// 第一个设备登录到网关 1。
	c1 := dial(t, addr1)
	c1.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid, Token: harness.Token(t, uid)})
	if resp, err := c1.recvCmd(uint32(protocol.CmdLogin), 10*time.Second); err != nil || resp.GetErrCode() != 0 {
		t.Fatalf("首次登录失败: %v", err)
	}

	// 第二个设备登录到网关 2 —— 触发顶号。
	c2 := dial(t, addr2)
	c2.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid, Token: harness.Token(t, uid)})
	if resp, err := c2.recvCmd(uint32(protocol.CmdLogin), 10*time.Second); err != nil || resp.GetErrCode() != 0 {
		t.Fatalf("第二次登录失败: %v", err)
	}

	// 旧连接应收到 KICK 并被断开。
	deadline := time.Now().Add(10 * time.Second)
	kicked := false
	for time.Now().Before(deadline) {
		env, err := c1.recv(time.Until(deadline))
		if err != nil {
			// 连接被关闭同样说明顶号生效了。
			kicked = true
			break
		}
		if env.GetCmd() == uint32(protocol.PushKick) {
			kicked = true
			break
		}
	}
	if !kicked {
		t.Fatal("被顶号的连接必须收到 KICK 或被断开")
	}

	// 会话应指向新网关。
	sessions := session.NewStore(gn1.Redis, gn1.Keys, gn1.Cfg.SessionTTL)
	info, err := sessions.Get(context.Background(), uid)
	if err != nil {
		t.Fatal(err)
	}
	if info.GateID == gn1.NodeID() {
		t.Fatalf("会话仍指向旧网关: %+v", info)
	}
}

// §9.4：网关重启后必须清理自己名下的残留会话，否则推送会发进黑洞。
func TestGatewayCleansOwnSessionsOnStart(t *testing.T) {
	env := harness.Start(t)
	n := env.Node(t, "gateway", "lobby", 1, lobbyCfg)

	keys := store.NewKeys(store.TagUID, n.Cfg.ShardCount, "lobby")
	sessions := session.NewStore(n.Redis, keys, n.Cfg.SessionTTL)
	ctx := context.Background()

	// 伪造上一次崩溃留下的会话。
	if _, err := sessions.Bind(ctx, 9009, n.NodeID(), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.Bind(ctx, 9010, "s1-gateway-9", 2); err != nil {
		t.Fatal(err)
	}

	svc := gateway.New()
	n.Cfg.ListenAddr = freeAddr(t)
	if err := svc.Start(ctx, n); err != nil {
		t.Fatal(err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		svc.StopAccepting(sctx)
		svc.Close(sctx)
	}()

	if info, _ := sessions.Get(ctx, 9009); !info.Empty() {
		t.Fatalf("本网关名下的残留会话应被清理: %+v", info)
	}
	if info, _ := sessions.Get(ctx, 9010); info.GateID != "s1-gateway-9" {
		t.Fatalf("其他网关的会话不得被误清: %+v", info)
	}
}

// 未登录就发业务请求必须被拒绝：uid 一律以服务端会话为准。
func TestGatewayRejectsUnauthenticated(t *testing.T) {
	env := harness.Start(t)
	_, _, addr := startGateway(t, env, 1)

	c := dial(t, addr)
	c.send(protocol.CmdGetBag, 12345, &pb.GetBagReq{})
	resp, err := c.recv(5 * time.Second)
	if err != nil {
		t.Fatalf("应收到错误应答: %v", err)
	}
	if resp.GetErrCode() != uint32(protocol.ErrPermission) {
		t.Fatalf("应拒绝未登录请求，实际 err_code=%d", resp.GetErrCode())
	}
}

var _ = lobby.New
