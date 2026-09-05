package integration

import (
	"context"
	"testing"
	"time"

	"github.com/gamedev/f1/internal/lobby"
	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/test/harness"
	"google.golang.org/protobuf/proto"
)

// §10.2：新实例上线后主动发起交接，把分片从老实例手里接过来。
//
// 靠 lease 自然过期有 8~10s 不可用窗口，主动交接可压到百毫秒 ——
// 这个测试验证的是「交接确实发生了」以及「交接后数据没丢」。
func TestHandoffRebalancesOnScaleOut(t *testing.T) {
	env := harness.Start(t)

	n1, svc1 := startLobby(t, env, 1)
	harness.Eventually(t, 20*time.Second, "首个实例认领全部分片", func() bool {
		return len(svc1.OwnedShards()) == testShards
	})

	// 在扩容前写入数据，交接后必须还在。
	const uid = uint64(5005)
	if err := lobbyCall(t, n1, uid, protocol.CmdLogin,
		&pb.LoginReq{Uid: uid, GateId: "g1", ConnId: 1}, &pb.LoginResp{}); err != nil {
		t.Fatal(err)
	}
	if err := lobbyCall(t, n1, uid, protocol.CmdAddCurrency,
		&pb.AddCurrencyReq{Currency: uint32(protocol.CurrencyGold), Delta: 777}, &pb.AddCurrencyResp{}); err != nil {
		t.Fatal(err)
	}

	// 第二个实例上线。
	n2 := env.Node(t, "lobby", "lobby", 2, lobbyCfg)
	svc2 := lobby.New()
	ctx := context.Background()
	if err := svc2.Start(ctx, n2); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		svc2.StopAccepting(sctx)
		_ = svc2.FlushAll(sctx)
		svc2.Close(sctx)
	})

	// 两边应收敛到接近公平份额（32 / 2 = 16）。
	harness.Eventually(t, 40*time.Second, "分片再平衡收敛", func() bool {
		a, b := len(svc1.OwnedShards()), len(svc2.OwnedShards())
		return a+b == testShards && a > 0 && b > 0 && abs(a-b) <= 2
	})

	// 交接期间没有分片被两边同时持有。
	owned := map[uint32]bool{}
	for _, s := range svc1.OwnedShards() {
		owned[s] = true
	}
	for _, s := range svc2.OwnedShards() {
		if owned[s] {
			t.Fatalf("分片 %d 被两个实例同时持有", s)
		}
	}

	// 数据必须完好：交接前旧 owner 做过全量刷盘，新 owner 从 Redis 加载。
	var bal pb.AddCurrencyResp
	if err := lobbyCall(t, n1, uid, protocol.CmdAddCurrency,
		&pb.AddCurrencyReq{Currency: uint32(protocol.CurrencyGold), Delta: 0}, &bal); err != nil {
		t.Fatalf("交接后读取失败: %v", err)
	}
	if bal.GetBalance() != 777 {
		t.Fatalf("交接后余额 = %d，期望 777 —— 数据在交接中丢了", bal.GetBalance())
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// §10.3：下线顺序不可颠倒 —— 停止接新请求 → Drain → 全量刷盘 → 注销。
// 这里验证最终结果：进程退出后，内存里的改动一条不少地留在 Redis。
func TestGracefulShutdownPersistsData(t *testing.T) {
	env := harness.Start(t)
	n := env.Node(t, "lobby", "lobby", 1, func(c *config.Config) {
		lobbyCfg(c)
		// 把 L1 间隔拉长，确保数据只能靠「下线时的全量刷盘」落地。
		c.FlushL1Interval = time.Hour
		c.FlushL2Interval = time.Hour
	})

	svc := lobby.New()
	ctx := context.Background()
	if err := svc.Start(ctx, n); err != nil {
		t.Fatal(err)
	}
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(6006) })

	const uid = uint64(6006)
	if err := lobbyCall(t, n, uid, protocol.CmdLogin,
		&pb.LoginReq{Uid: uid, GateId: "g1", ConnId: 1}, &pb.LoginResp{}); err != nil {
		t.Fatal(err)
	}
	if err := lobbyCall(t, n, uid, protocol.CmdAddCurrency,
		&pb.AddCurrencyReq{Currency: uint32(protocol.CurrencyGold), Delta: 1234}, &pb.AddCurrencyResp{}); err != nil {
		t.Fatal(err)
	}

	// 定时刷盘被关掉了，此刻 Redis 里应该还没有这笔钱。
	if blob, err := n.Redis.Get(ctx, n.Keys.Player(uid, store.ModBase)).Bytes(); err == nil {
		base := &pb.PlayerBase{}
		_ = proto.Unmarshal(blob, base)
		if base.GetCurrency()[uint32(protocol.CurrencyGold)] == 1234 {
			t.Fatal("前提失效：定时刷盘本应被关闭")
		}
	}

	// 优雅下线。
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	svc.StopAccepting(sctx)
	if err := svc.FlushAll(sctx); err != nil {
		t.Fatalf("下线刷盘失败: %v", err)
	}
	svc.Close(sctx)

	blob, err := n.Redis.Get(ctx, n.Keys.Player(uid, store.ModBase)).Bytes()
	if err != nil {
		t.Fatalf("下线后数据应已落盘: %v", err)
	}
	base := &pb.PlayerBase{}
	if err := proto.Unmarshal(blob, base); err != nil {
		t.Fatal(err)
	}
	if got := base.GetCurrency()[uint32(protocol.CurrencyGold)]; got != 1234 {
		t.Fatalf("下线刷盘丢数据：余额 = %d，期望 1234", got)
	}

	// 分片也应被释放，别人可以立刻接管。
	if len(svc.OwnedShards()) != 0 {
		t.Fatalf("下线后不应仍持有分片，实际 %d 个", len(svc.OwnedShards()))
	}
}
