// Package harness 为集成测试提供进程内的 etcd / NATS / Redis。
//
// 设计文档把 P1（分片认领）与 P2（Actor + 刷盘 + epoch fencing）列为地基，
// 「出问题最贵、事后最难补，建议投入最多评审和测试」（§17）。
// 这些行为只有在真实的 etcd 事务与 Redis Lua 上才检验得出来，
// 因此这里把三个依赖都嵌进测试进程，而不是用假实现糊弄过去。
package harness

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/redis/go-redis/v9"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/authn"
	"github.com/gamedev/f1/pkg/authz"
	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/etcdx"
	"github.com/gamedev/f1/pkg/ident"
	"github.com/gamedev/f1/pkg/idgen"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/nodeid"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/store"
)

// Env 是一套进程内基础设施。
type Env struct {
	EtcdEndpoint string
	NatsURL      string
	RedisAddr    string

	etcd  *embed.Etcd
	nats  *natsserver.Server
	redis *miniredis.Miniredis
}

// Start 启动 etcd / NATS(JetStream) / Redis。
func Start(t *testing.T) *Env {
	t.Helper()

	e := &Env{}
	e.redis = miniredis.RunT(t)
	e.RedisAddr = e.redis.Addr()

	// —— NATS（开 JetStream，job.* 需要）——
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      freePort(t),
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("创建 NATS 失败: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS 启动超时")
	}
	e.nats = ns
	e.NatsURL = ns.ClientURL()

	// —— etcd ——
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{mustURL(t, fmt.Sprintf("http://127.0.0.1:%d", freePort(t)))}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	peer := mustURL(t, fmt.Sprintf("http://127.0.0.1:%d", freePort(t)))
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	ec, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("启动 etcd 失败: %v", err)
	}
	select {
	case <-ec.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		ec.Close()
		t.Fatal("etcd 启动超时")
	}
	e.etcd = ec
	e.EtcdEndpoint = cfg.ListenClientUrls[0].Host

	t.Cleanup(e.Stop)
	return e
}

// Stop 关闭全部依赖。
func (e *Env) Stop() {
	if e.etcd != nil {
		e.etcd.Close()
		e.etcd = nil
	}
	if e.nats != nil {
		e.nats.Shutdown()
		e.nats = nil
	}
}

// Config 生成一份指向本套依赖的配置。
func (e *Env) Config(t *testing.T, svcName string, nodeSeq int, tweak func(*config.Config)) (*config.Config, *ident.Identity) {
	t.Helper()

	t.Setenv("SERVER_ID", "1")
	t.Setenv("NODE_SEQ", fmt.Sprint(nodeSeq))
	t.Setenv("ETCD_ENDPOINTS", e.EtcdEndpoint)
	t.Setenv("ETCD_PREFIX", "/game/s1")
	t.Setenv("REDIS_ADDR", e.RedisAddr)
	t.Setenv("NATS_URL", e.NatsURL)
	t.Setenv("METRICS_ADDR", "")
	t.Setenv("REDIS_REQUIRE_NOEVICTION", "false") // miniredis 不支持 CONFIG GET

	// 安全相关的密钥。测试里显式配齐，正是为了让测试跑在与生产同一套鉴权规则下 ——
	// 如果测试靠「关掉鉴权」才能通过，那鉴权就等于没测。
	t.Setenv("INTERNAL_SECRET", TestInternalSecret)
	t.Setenv("LOGIN_SECRET", TestLoginSecret)
	t.Setenv("PAYMENT_SECRET", TestPaymentSecret)
	t.Setenv("ALLOW_DEV_AUTH", "false")
	t.Setenv("PAYMENT_SANDBOX", "false")

	cfg, id, err := config.Load(svcName)
	if err != nil {
		t.Fatalf("装载配置失败: %v", err)
	}
	if tweak != nil {
		tweak(cfg)
	}
	return cfg, id
}

// Node 构造一个可直接使用的 node.Node（跳过 metrics 服务，其余与生产一致）。
//
// 这里会真的做一次 §3.4 的 nodeID 抢占：分片认领算「公平份额」时要靠
// etcd 上的存活实例数（nodeid.CountLive），不注册就算不出来。
func (e *Env) Node(t *testing.T, svcName, kind string, nodeSeq int, tweak func(*config.Config)) *node.Node {
	t.Helper()

	cfg, id := e.Config(t, svcName, nodeSeq, tweak)

	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatalf("连接 etcd 失败: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := store.EnsureScripts(context.Background(), rdb); err != nil {
		t.Fatalf("加载 Lua 脚本失败: %v", err)
	}

	conn, err := bus.Connect(bus.Options{URL: cfg.NatsURL, NodeID: id.NodeID()})
	if err != nil {
		t.Fatalf("连接 NATS 失败: %v", err)
	}
	t.Cleanup(conn.Close)

	snow, err := idgen.NewFromIdentity(id)
	if err != nil {
		t.Fatal(err)
	}

	reg, err := nodeid.Claim(context.Background(), cli, cfg, id, nil)
	if err != nil {
		t.Fatalf("nodeID 自检失败: %v", err)
	}
	t.Cleanup(func() {
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = reg.Release(rctx)
	})

	return &node.Node{
		Cfg:   cfg,
		ID:    id,
		Etcd:  cli,
		Bus:   conn,
		Redis: rdb,
		Keys:  store.NewKeys(store.TagUID, cfg.ShardCount, kind),
		Snow:  snow,
	}
}

// 测试用密钥。生产由部署下发，且网关不应拿到 INTERNAL_SECRET。
const (
	TestInternalSecret = "test-internal-secret-do-not-use-in-prod"
	TestLoginSecret    = "test-login-secret-do-not-use-in-prod"
	TestPaymentSecret  = "test-payment-secret-do-not-use-in-prod"
)

// Redis 返回底层 miniredis，用于制造故障。
func (e *Env) Redis() *miniredis.Miniredis { return e.redis }

// Token 为某个 uid 签发一张登录票据。
func Token(t *testing.T, uid uint64) string {
	t.Helper()
	v := authn.NewVerifier(TestLoginSecret, false)
	tok, err := v.Issue(uid, time.Hour)
	if err != nil {
		t.Fatalf("签发登录票据失败: %v", err)
	}
	return tok
}

// InternalCall 以「服务端内部」的身份发起一条带签名的命令。
//
// 测试里凡是要发内部 / GM 命令，都必须走这里 —— 与生产路径完全一致。
func InternalCall(ctx context.Context, t *testing.T, n *node.Node, subj string,
	cmd protocol.Cmd, uid uint64, operator string, body, out proto.Message) error {
	t.Helper()

	env, err := n.Bus.NewEnvelope(cmd, uid, "", body)
	if err != nil {
		return err
	}
	env.Operator = operator
	signer := authz.NewSigner(TestInternalSecret)
	if err := signer.Sign(env); err != nil {
		return err
	}
	resp, err := n.Bus.RequestEnv(ctx, subj, env)
	if err != nil {
		return err
	}
	if resp.GetErrCode() != 0 {
		return &bus.RemoteError{
			Code: protocol.ErrCode(resp.GetErrCode()),
			Msg:  resp.GetErrMsg(),
		}
	}
	if out != nil {
		return bus.Unpack(resp, out)
	}
	return nil
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("分配端口失败: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func mustURL(t *testing.T, raw string) url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("解析 URL 失败: %v", err)
	}
	return *u
}

func init() {
	// 嵌入式 etcd 会往 stderr 打大量日志，测试里静音。
	_ = os.Setenv("ETCD_UNSUPPORTED_ARCH", "")
}

// Eventually 轮询等待条件成立。
func Eventually(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待超时（%v）：%s", timeout, desc)
}
