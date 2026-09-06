package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/internal/lobby"
	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/payment"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/profile"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/pkg/subject"
	"github.com/gamedev/f1/test/harness"
)

func lobbyCfg(c *config.Config) {
	c.ShardCount = testShards
	c.ShardLeaseTTL = 2 * time.Second
	c.ShardKeepAlive = 500 * time.Millisecond
	c.FlushL1Interval = 300 * time.Millisecond
	c.FlushL2Interval = 500 * time.Millisecond
	c.UnloadIdle = time.Hour
	c.TxScanInterval = time.Hour // 测试里不让补偿扫描器插手
}

// startLobby 启动一个真实的 Lobby 服务并等它认领完分片
func startLobby(t *testing.T, env *harness.Env, seq int) (*node.Node, *lobby.Service) {
	t.Helper()
	n := env.Node(t, "lobby", "lobby", seq, lobbyCfg)
	svc := lobby.New()
	ctx := context.Background()
	if err := svc.Start(ctx, n); err != nil {
		t.Fatalf("启动 Lobby 失败: %v", err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		svc.StopAccepting(sctx)
		_ = svc.FlushAll(sctx)
		svc.Close(sctx)
	})
	return n, svc
}

// grant 走内部签名通道给玩家发钱
//
// 客户端已经调不了 add_currency 了，测试也得走和发奖服务一样的路径
func grant(t *testing.T, n *node.Node, uid uint64, currency uint32, amount int64) int64 {
	t.Helper()
	sh := shard.Of(uid, n.Cfg.ShardCount)
	var resp pb.AddCurrencyResp
	var lastErr error
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := harness.InternalCall(ctx, t, n,
			subject.LobbyReq(sh, protocol.CmdAddCurrency.Name()),
			protocol.CmdAddCurrency, uid, "", &pb.AddCurrencyReq{
				Currency: currency, Delta: amount, Reason: "test",
			}, &resp)
		cancel()
		if err == nil {
			return resp.GetBalance()
		}
		lastErr = err
		if !retryable(err) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("发放货币失败: %v", lastErr)
	return 0
}

// lobbyCall 带重试地调 Lobby
//
// 交接窗口内请求会被丢掉，客户端本来就该重试。这里顺带覆盖了刚启动时
// 分片还没订阅完的那一小段
func lobbyCall(t *testing.T, n *node.Node, uid uint64, cmd protocol.Cmd, body, out proto.Message) error {
	t.Helper()
	sh := shard.Of(uid, n.Cfg.ShardCount)
	subj := subject.LobbyReq(sh, cmd.Name())

	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := n.Bus.Call(ctx, subj, cmd, uid, "", body, out)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable(err) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return lastErr
}

func retryable(err error) bool {
	if errors.Is(err, bus.ErrNoResponders) {
		return true
	}
	var re *bus.RemoteError
	return errors.As(err, &re) && re.IsRetryable()
}

// 端到端：登录、改数据、等 L1 刷盘，然后 Redis 里能查到，摘要也跟着更新
func TestLobbyLoginAndFlush(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(1001)
	harness.Eventually(t, 20*time.Second, "认领 uid 所属分片", func() bool {
		return svc.Owns(uid)
	})

	var resp pb.LoginResp
	if err := lobbyCall(t, n, uid, protocol.CmdLogin,
		&pb.LoginReq{Uid: uid, GateId: "s1-gateway-1", ConnId: 7}, &resp); err != nil {
		t.Fatalf("登录失败: %v", err)
	}
	if resp.GetBase().GetUid() != uid {
		t.Fatalf("登录返回的玩家不对: %+v", resp.GetBase())
	}
	if resp.GetBase().GetLevel() != 1 {
		t.Fatalf("新玩家等级应为 1，实际 %d", resp.GetBase().GetLevel())
	}

	if bal := grant(t, n, uid, uint32(protocol.CurrencyGold), 500); bal != 500 {
		t.Fatalf("余额 = %d，期望 500", bal)
	}

	var add pb.AddItemResp
	sh := shard.Of(uid, n.Cfg.ShardCount)
	ictx, icancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := harness.InternalCall(ictx, t, n,
		subject.LobbyReq(sh, protocol.CmdAddItem.Name()),
		protocol.CmdAddItem, uid, "", &pb.AddItemReq{TplId: 100, Count: 3}, &add); err != nil {
		t.Fatalf("加道具失败: %v", err)
	}
	icancel()
	if add.GetItem().GetInstanceId() == 0 {
		t.Fatal("道具实例 ID 应由雪花生成")
	}

	// 等 L1 刷盘落地
	ctx := context.Background()
	harness.Eventually(t, 10*time.Second, "L1 刷盘落地", func() bool {
		blob, err := n.Redis.Get(ctx, n.Keys.Player(uid, store.ModBase)).Bytes()
		if err != nil {
			return false
		}
		base := &pb.PlayerBase{}
		return proto.Unmarshal(blob, base) == nil &&
			base.GetCurrency()[uint32(protocol.CurrencyGold)] == 500
	})

	// owner 分片改数据时会顺手写一份只读摘要
	reader := profile.NewReader(n.Redis, n.Keys)
	harness.Eventually(t, 10*time.Second, "profile 摘要写入", func() bool {
		p, err := reader.Get(ctx, uid)
		return err == nil && p != nil && p.GetUid() == uid
	})

	// 背包也应落盘
	blob, err := n.Redis.Get(ctx, n.Keys.Player(uid, store.ModBag)).Bytes()
	if err != nil {
		t.Fatalf("背包未落盘: %v", err)
	}
	bag := &pb.PlayerBag{}
	if err := proto.Unmarshal(blob, bag); err != nil {
		t.Fatal(err)
	}
	if len(bag.GetItems()) != 1 || bag.GetItems()[0].GetCount() != 3 {
		t.Fatalf("背包内容不对: %+v", bag.GetItems())
	}
}

// L0 得幂等：重复提交返回第一次的结果，不重复到账
func TestLobbyPurchaseIdempotent(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(2002)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })

	if err := lobbyCall(t, n, uid, protocol.CmdLogin,
		&pb.LoginReq{Uid: uid, GateId: "g1", ConnId: 1}, &pb.LoginResp{}); err != nil {
		t.Fatal(err)
	}

	first := purchase(t, n, uid, "order-42", 1)
	if first.GetDuplicate() {
		t.Fatal("首次充值不应判为重复")
	}
	if first.GetBalance() != 60 {
		t.Fatalf("首次到账余额 = %d，期望 60", first.GetBalance())
	}

	// 客户端重试同一订单
	second := purchase(t, n, uid, "order-42", 1)
	if !second.GetDuplicate() {
		t.Fatal("重复订单必须被识别")
	}
	if second.GetBalance() != first.GetBalance() {
		t.Fatalf("重复订单必须返回首次结果：first=%d second=%d",
			first.GetBalance(), second.GetBalance())
	}

	// 换个订单号才会真正再次到账
	third := purchase(t, n, uid, "order-43", 1)
	if third.GetBalance() != 120 {
		t.Fatalf("第二笔订单后余额 = %d，期望 120", third.GetBalance())
	}

	// L0 写穿：不等 ticker，Redis 里立刻就有
	ctx := context.Background()
	blob, err := n.Redis.Get(ctx, n.Keys.Player(uid, store.ModBase)).Bytes()
	if err != nil {
		t.Fatalf("L0 写穿应立即落盘: %v", err)
	}
	base := &pb.PlayerBase{}
	_ = proto.Unmarshal(blob, base)
	if got := base.GetCurrency()[uint32(protocol.CurrencyDiamond)]; got != 120 {
		t.Fatalf("落盘余额 = %d，期望 120", got)
	}
}

// 端到端：扣道具、写穿 tx、投 job、接收方幂等入账
func TestCrossShardTransferEndToEnd(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const from, to = uint64(3003), uint64(3004)
	harness.Eventually(t, 20*time.Second, "认领相关分片", func() bool {
		return svc.Owns(from) && svc.Owns(to)
	})

	for _, uid := range []uint64{from, to} {
		if err := lobbyCall(t, n, uid, protocol.CmdLogin,
			&pb.LoginReq{Uid: uid, GateId: "g1", ConnId: uid}, &pb.LoginResp{}); err != nil {
			t.Fatal(err)
		}
	}
	// 给发起方发点货（走内部通道）
	grantItem(t, n, from, 100, 10)

	if err := lobbyCall(t, n, from, protocol.CmdTransfer, &pb.TransferJob{
		ToUid:  to,
		Items:  []*pb.Attachment{{TplId: 100, Count: 4}},
		Reason: "trade",
	}, &pb.Ack{}); err != nil {
		t.Fatalf("发起转移失败: %v", err)
	}

	// 发起方立刻扣除（内存），并已写穿
	var bag pb.GetBagResp
	harness.Eventually(t, 10*time.Second, "发起方完成扣除", func() bool {
		if err := lobbyCall(t, n, from, protocol.CmdGetBag, &pb.GetBagReq{}, &bag); err != nil {
			return false
		}
		return countTpl(bag.GetBag(), 100) == 6
	})

	// 接收方经由 job 中转层入账
	harness.Eventually(t, 20*time.Second, "接收方入账", func() bool {
		var b pb.GetBagResp
		if err := lobbyCall(t, n, to, protocol.CmdGetBag, &pb.GetBagReq{}, &b); err != nil {
			return false
		}
		return countTpl(b.GetBag(), 100) == 4
	})

	// 资源既没蒸发也没复制
	var fromBag, toBag pb.GetBagResp
	_ = lobbyCall(t, n, from, protocol.CmdGetBag, &pb.GetBagReq{}, &fromBag)
	_ = lobbyCall(t, n, to, protocol.CmdGetBag, &pb.GetBagReq{}, &toBag)
	total := countTpl(fromBag.GetBag(), 100) + countTpl(toBag.GetBag(), 100)
	if total != 10 {
		t.Fatalf("转移前后总量应守恒，实际 %d（发起方 %d + 接收方 %d）",
			total, countTpl(fromBag.GetBag(), 100), countTpl(toBag.GetBag(), 100))
	}
}

// 不够的时候要整体失败，不能扣一半
func TestTransferInsufficientIsAtomic(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const from, to = uint64(4004), uint64(4005)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(from) })

	if err := lobbyCall(t, n, from, protocol.CmdLogin,
		&pb.LoginReq{Uid: from, GateId: "g1", ConnId: 1}, &pb.LoginResp{}); err != nil {
		t.Fatal(err)
	}
	grantItem(t, n, from, 100, 2)

	err := lobbyCall(t, n, from, protocol.CmdTransfer, &pb.TransferJob{
		ToUid: to,
		Items: []*pb.Attachment{{TplId: 100, Count: 99}},
	}, &pb.Ack{})
	if err == nil {
		t.Fatal("道具不足时必须失败")
	}

	var bag pb.GetBagResp
	if err := lobbyCall(t, n, from, protocol.CmdGetBag, &pb.GetBagReq{}, &bag); err != nil {
		t.Fatal(err)
	}
	if got := countTpl(bag.GetBag(), 100); got != 2 {
		t.Fatalf("失败的转移不得改动背包，实际剩余 %d", got)
	}
}

// 不归本实例的分片要明确回一个可重试的错，不能静默丢掉
func TestRequestToUnownedShardIsRejected(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(1) })

	// 直接发到一个不存在的分片号：没有订阅者，客户端应拿到可重试的错误
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := n.Bus.Call(ctx, subject.LobbyReq(testShards+5, protocol.CmdGetBag.Name()),
		protocol.CmdGetBag, 1, "", &pb.GetBagReq{}, &pb.GetBagResp{})
	if err == nil {
		t.Fatal("无人服务的分片必须报错，不能静默成功")
	}
}

func countTpl(bag *pb.PlayerBag, tpl uint32) int64 {
	var n int64
	for _, it := range bag.GetItems() {
		if it.GetTplId() == tpl {
			n += it.GetCount()
		}
	}
	return n
}

// grantItem 走内部通道发道具
func grantItem(t *testing.T, n *node.Node, uid uint64, tpl uint32, count int64) {
	t.Helper()
	sh := shard.Of(uid, n.Cfg.ShardCount)
	var resp pb.AddItemResp
	var lastErr error
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := harness.InternalCall(ctx, t, n,
			subject.LobbyReq(sh, protocol.CmdAddItem.Name()),
			protocol.CmdAddItem, uid, "", &pb.AddItemReq{TplId: tpl, Count: count}, &resp)
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		if !retryable(err) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("发放道具失败: %v", lastErr)
}

// purchase 走完整充值路径：配置里的商品加渠道回执验签
func purchase(t *testing.T, n *node.Node, uid uint64, orderID string, product uint32) *pb.PurchaseResp {
	t.Helper()
	conf := gameconfProduct(t, product)
	order := payment.Order{
		OrderID: orderID, UID: uid, ProductID: product,
		Channel: "sandbox", PriceCents: conf,
	}
	sig := payment.Sign(harness.TestPaymentSecret, order)

	var resp pb.PurchaseResp
	if err := lobbyCall(t, n, uid, protocol.CmdPurchase, &pb.PurchaseReq{
		OrderId: orderID,
		Product: product,
		Receipt: &pb.PurchaseReceipt{
			Channel: "sandbox", Receipt: "rcpt-" + orderID, Signature: sig,
		},
	}, &resp); err != nil {
		t.Fatalf("充值失败: %v", err)
	}
	return &resp
}

// gameconfProduct 返回商品定价，拿来构造回执签名
func gameconfProduct(t *testing.T, id uint32) int64 {
	t.Helper()
	c := gameconf.Default()
	p, ok := c.Product(id)
	if !ok {
		t.Fatalf("配置里没有商品 %d", id)
	}
	return p.PriceCents
}
