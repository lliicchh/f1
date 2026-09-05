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
	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/session"
	"github.com/gamedev/f1/pkg/store"
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
	c.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid, Token: "t"})

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
	c.send(protocol.CmdAddCurrency, uid, &pb.AddCurrencyReq{
		Currency: uint32(protocol.CurrencyGold), Delta: 250,
	})
	got, err := c.recvCmd(uint32(protocol.CmdAddCurrency), 10*time.Second)
	if err != nil {
		t.Fatalf("等待加钱应答失败: %v", err)
	}
	if got.GetErrCode() != 0 {
		t.Fatalf("加钱失败: %s", got.GetErrMsg())
	}
	var cur pb.AddCurrencyResp
	_ = proto.Unmarshal(got.GetBody(), &cur)
	if cur.GetBalance() != 250 {
		t.Fatalf("余额 = %d，期望 250", cur.GetBalance())
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
	c1.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid})
	if resp, err := c1.recvCmd(uint32(protocol.CmdLogin), 10*time.Second); err != nil || resp.GetErrCode() != 0 {
		t.Fatalf("首次登录失败: %v", err)
	}

	// 第二个设备登录到网关 2 —— 触发顶号。
	c2 := dial(t, addr2)
	c2.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid})
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
