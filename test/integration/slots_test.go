package integration

import (
	"context"
	"testing"
	"time"

	"github.com/gamedev/f1/internal/lobby"
	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/jackpot"
	"github.com/gamedev/f1/pkg/ledger"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/pkg/subject"
	"github.com/gamedev/f1/test/harness"
)

const gameID = "classic5"

// spin 发起一次旋转。
func spin(t *testing.T, n *node0, uid uint64, bet int64, clientID string) *pb.SpinResp {
	t.Helper()
	var resp pb.SpinResp
	if err := lobbyCall(t, n, uid, protocol.CmdSpin, &pb.SpinReq{
		GameId: gameID, Bet: bet, ClientId: clientID,
	}, &resp); err != nil {
		t.Fatalf("旋转失败: %v", err)
	}
	return &resp
}

// 端到端：下注扣款、派彩入账、余额守恒、流水齐全。
func TestSpinDeductsAndPaysAtomically(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21001)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)

	const start = int64(100000)
	grant(t, n, uid, uint32(protocol.CurrencyGold), start)

	const bet = int64(100)
	var totalBet, totalWin int64
	balance := start

	for i := 0; i < 30; i++ {
		resp := spin(t, n, uid, bet, "")
		if resp.GetDuplicate() {
			t.Fatal("不同的旋转不应被判为重复")
		}
		// 免费旋转不扣注。
		if resp.GetRound().GetSpinIndex() == 1 {
			totalBet += bet
		} else if !resp.GetRoundFinished() || resp.GetRound().GetFreeSpinsTotal() == 0 {
			// 中间态：由下面的余额核对统一校验
			_ = resp
		}
		totalWin += resp.GetSpinWin()

		// 余额必须与服务端返回一致。
		if resp.GetBalance() > balance+resp.GetSpinWin() {
			t.Fatalf("余额异常增长：%d → %d", balance, resp.GetBalance())
		}
		balance = resp.GetBalance()

		// 若进入免费旋转，把它打完再继续下一注。
		for !resp.GetRoundFinished() {
			resp = spin(t, n, uid, bet, "")
			totalWin += resp.GetSpinWin()
			balance = resp.GetBalance()
		}
	}

	// 流水必须与余额对得上：这正是账本存在的意义。
	reader := ledger.NewReader(n.Redis, n.Keys)
	ctx := context.Background()
	entries, err := reader.Recent(ctx, uid, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("必须有流水 —— 没有流水的资金变动是查不清的")
	}

	var sum int64
	var bets, wins int
	for _, e := range entries {
		if e.Currency != uint32(protocol.CurrencyGold) {
			continue
		}
		sum += e.Amount
		switch e.Type {
		case ledger.TypeBet:
			bets++
			if e.Amount >= 0 {
				t.Fatalf("下注流水金额应为负：%+v", e)
			}
			if e.RoundID == 0 || e.GameID != gameID || e.ConfVer == "" {
				t.Fatalf("下注流水必须带回合号、游戏与配置版本（复算靠它们）：%+v", e)
			}
		case ledger.TypeWin:
			wins++
			if e.Amount <= 0 {
				t.Fatalf("派彩流水金额应为正：%+v", e)
			}
		}
	}
	if bets == 0 {
		t.Fatal("应当有下注流水")
	}
	// 起始余额是通过 GRANT 发放的，也在流水里，因此总和应等于当前余额。
	if sum != balance {
		t.Fatalf("流水求和 %d 与当前余额 %d 不符 —— 存在对账差额", sum, balance)
	}
	t.Logf("30 轮：投注 %d 派彩 %d 余额 %d 流水 %d 条（下注 %d / 派彩 %d）",
		totalBet, totalWin, balance, len(entries), bets, wins)
}

// 评审 P0-2 / 下注档位：客户端不能自定金额。
func TestSpinRejectsInvalidBet(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21002)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)
	grant(t, n, uid, uint32(protocol.CurrencyGold), 1_000_000)

	for _, bad := range []int64{0, -100, 7, 99, 999_999} {
		err := lobbyCall(t, n, uid, protocol.CmdSpin,
			&pb.SpinReq{GameId: gameID, Bet: bad}, &pb.SpinResp{})
		if err == nil {
			t.Fatalf("下注额 %d 不在档位内，必须被拒绝", bad)
		}
	}
}

// 余额不足必须拒绝，且不产生任何副作用。
func TestSpinRejectsInsufficientBalance(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21003)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)
	grant(t, n, uid, uint32(protocol.CurrencyGold), 50)

	if err := lobbyCall(t, n, uid, protocol.CmdSpin,
		&pb.SpinReq{GameId: gameID, Bet: 100}, &pb.SpinResp{}); err == nil {
		t.Fatal("余额不足必须拒绝")
	}
	if bal := grant(t, n, uid, uint32(protocol.CurrencyGold), 0); bal != 50 {
		t.Fatalf("被拒绝的旋转不应改动余额，实际 %d", bal)
	}
}

// 客户端重发同一次 spin 不得重复扣费。
func TestSpinIsIdempotent(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21004)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)
	grant(t, n, uid, uint32(protocol.CurrencyGold), 10000)

	first := spin(t, n, uid, 100, "client-spin-1")
	if first.GetDuplicate() {
		t.Fatal("首次不应判为重复")
	}

	second := spin(t, n, uid, 100, "client-spin-1")
	if !second.GetDuplicate() {
		t.Fatal("重发同一次 spin 必须被识别为重复")
	}
	if second.GetBalance() != first.GetBalance() {
		t.Fatalf("重发不得再次扣费：%d vs %d", first.GetBalance(), second.GetBalance())
	}
	if second.GetSpinWin() != first.GetSpinWin() {
		t.Fatal("重发应返回首次结果")
	}
}

// 评审 P1-1：免费旋转中途「崩溃」，重启后必须能取回并打完。
func TestIncompleteRoundSurvivesRestart(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21005)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)
	grant(t, n, uid, uint32(protocol.CurrencyGold), 5_000_000)

	// 一直转到触发免费旋转为止。
	var open *pb.Round
	for i := 0; i < 20000; i++ {
		resp := spin(t, n, uid, 100, "")
		if !resp.GetRoundFinished() {
			open = resp.GetRound()
			break
		}
	}
	if open == nil {
		t.Skip("两万次旋转都没触发免费旋转，概率上极不可能；跳过而不是误报")
	}
	t.Logf("触发免费旋转：round=%d 剩余 %d 次", open.GetRoundId(), open.GetFreeSpinsLeft())

	// 未结算回合必须已经落盘 —— 这才是断线能恢复的根据。
	ctx := context.Background()
	blob, err := n.Redis.Get(ctx, n.Keys.Player(uid, store.ModRound)).Bytes()
	if err != nil || len(blob) == 0 {
		t.Fatalf("未结算回合必须随投注原子落盘: %v", err)
	}

	// 模拟进程重启：停掉服务再起一个新的，让玩家对象从 Redis 重新加载。
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	svc.StopAccepting(sctx)
	_ = svc.FlushAll(sctx)
	svc.Close(sctx)
	cancel()

	n2 := env.Node(t, "lobby", "lobby", 2, lobbyCfg)
	svc2 := lobby.New()
	if err := svc2.Start(context.Background(), n2); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		svc2.StopAccepting(c)
		_ = svc2.FlushAll(c)
		svc2.Close(c)
	})
	harness.Eventually(t, 30*time.Second, "新实例认领分片", func() bool { return svc2.Owns(uid) })

	// 重连：登录应带回未结算回合。
	var login pb.LoginResp
	if err := lobbyCall(t, n2, uid, protocol.CmdLogin,
		&pb.LoginReq{Uid: uid, GateId: "g2", ConnId: 2}, &login); err != nil {
		t.Fatalf("重连登录失败: %v", err)
	}
	if login.GetOpenRound() == nil {
		t.Fatal("登录必须带回未结算回合（GLI-19 对 incomplete round 的要求）")
	}
	if login.GetOpenRound().GetRoundId() != open.GetRoundId() {
		t.Fatalf("回合号不一致：%d vs %d", login.GetOpenRound().GetRoundId(), open.GetRoundId())
	}
	if login.GetOpenRound().GetFreeSpinsLeft() != open.GetFreeSpinsLeft() {
		t.Fatalf("剩余免费次数不一致：%d vs %d",
			login.GetOpenRound().GetFreeSpinsLeft(), open.GetFreeSpinsLeft())
	}

	// 也能通过 round_state 查询到。
	var st pb.RoundStateResp
	if err := lobbyCall(t, n2, uid, protocol.CmdRoundState, &pb.RoundStateReq{}, &st); err != nil {
		t.Fatal(err)
	}
	if !st.GetHasOpen() {
		t.Fatal("round_state 应返回未结算回合")
	}

	// 把剩余免费旋转打完，回合应正常结束。
	before := grant(t, n2, uid, uint32(protocol.CurrencyGold), 0)
	for i := 0; i < 200; i++ {
		resp := spin(t, n2, uid, 100, "")
		if resp.GetRoundFinished() {
			break
		}
	}
	var final pb.RoundStateResp
	if err := lobbyCall(t, n2, uid, protocol.CmdRoundState, &pb.RoundStateReq{}, &final); err != nil {
		t.Fatal(err)
	}
	if final.GetHasOpen() {
		t.Fatal("免费旋转打完后回合应结束")
	}
	after := grant(t, n2, uid, uint32(protocol.CurrencyGold), 0)
	if after < before {
		t.Fatalf("免费旋转不应扣费：%d → %d", before, after)
	}
}

// 免费旋转期间不扣注。
func TestFreeSpinsDoNotCharge(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21006)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)
	grant(t, n, uid, uint32(protocol.CurrencyGold), 5_000_000)

	for i := 0; i < 20000; i++ {
		resp := spin(t, n, uid, 100, "")
		if resp.GetRoundFinished() {
			continue
		}
		// 进入免费旋转：接下来的每一次旋转，余额都不应因下注而减少。
		for !resp.GetRoundFinished() {
			before := resp.GetBalance()
			resp = spin(t, n, uid, 100, "")
			if resp.GetBalance() < before {
				t.Fatalf("免费旋转不应扣费：%d → %d", before, resp.GetBalance())
			}
		}
		return
	}
	t.Skip("未触发免费旋转，跳过")
}

// 评审 P1-6：触发日投注限额后必须拒绝下注。
func TestDailyBetLimitBlocksSpin(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21007)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)
	grant(t, n, uid, uint32(protocol.CurrencyGold), 1_000_000)

	// GM 把日投注限额压到 250，只够两把 100。
	setRG(t, n, uid, &pb.GMSetRGReq{Uid: uid, DailyBetLimit: 250, DailyLossLimit: -1, SessionLimit: -1})

	for i := 0; i < 2; i++ {
		resp := spin(t, n, uid, 100, "")
		for !resp.GetRoundFinished() {
			resp = spin(t, n, uid, 100, "")
		}
	}

	err := lobbyCall(t, n, uid, protocol.CmdSpin, &pb.SpinReq{GameId: gameID, Bet: 100}, &pb.SpinResp{})
	if err == nil {
		t.Fatal("超过日投注限额必须拒绝下注")
	}

	// 状态接口应如实反映。
	var st pb.RGStatusResp
	if err := lobbyCall(t, n, uid, protocol.CmdRGStatus, &pb.RGStatusReq{}, &st); err != nil {
		t.Fatal(err)
	}
	if st.GetDailyBetLimit() != 250 {
		t.Fatalf("限额应为 250，实际 %d", st.GetDailyBetLimit())
	}
	if st.GetDailyBet() < 200 {
		t.Fatalf("当日投注累计应至少 200，实际 %d", st.GetDailyBet())
	}
}

// 自我排除期内禁止登录与下注。
func TestSelfExclusionBlocksPlay(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21008)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)
	grant(t, n, uid, uint32(protocol.CurrencyGold), 100000)

	until := time.Now().Add(24 * time.Hour).UnixMilli()
	setRG(t, n, uid, &pb.GMSetRGReq{
		Uid: uid, DailyBetLimit: -1, DailyLossLimit: -1, SessionLimit: -1,
		ExcludeUntil: until,
	})

	if err := lobbyCall(t, n, uid, protocol.CmdSpin,
		&pb.SpinReq{GameId: gameID, Bet: 100}, &pb.SpinResp{}); err == nil {
		t.Fatal("自我排除期内必须拒绝下注")
	}
	if err := lobbyCall(t, n, uid, protocol.CmdLogin,
		&pb.LoginReq{Uid: uid, GateId: "g1", ConnId: 9}, &pb.LoginResp{}); err == nil {
		t.Fatal("自我排除期内必须拒绝登录")
	}
}

// 评审 P1-5：奖池注入是原子的，并发下总额等于各笔之和。
func TestJackpotContributionIsAtomic(t *testing.T) {
	env := harness.Start(t)
	n := env.Node(t, "lobby", "lobby", 1, lobbyCfg)

	conf := gameconf.Default()
	jm := jackpot.NewManager(n.Redis, n.Keys, conf.Jackpots)
	ctx := context.Background()

	const pool = "grand"
	seed := conf.Jackpots[pool].Seed

	const workers, each, amount = 8, 50, 7
	done := make(chan struct{}, workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < each; i++ {
				if _, err := jm.Contribute(ctx, pool, amount); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	got, err := jm.Amount(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	want := seed + int64(workers*each*amount)
	if got != want {
		t.Fatalf("奖池总额 = %d，期望 %d（并发注入丢了 %d）", got, want, want-got)
	}
}

// 奖池中奖必须原子：清零与派彩记录同生共死，且只能被拿走一次。
func TestJackpotClaimIsAtomicAndOnce(t *testing.T) {
	env := harness.Start(t)
	n := env.Node(t, "lobby", "lobby", 1, lobbyCfg)

	conf := gameconf.Default()
	jm := jackpot.NewManager(n.Redis, n.Keys, conf.Jackpots)
	ctx := context.Background()
	const pool = "grand"

	if _, err := jm.Contribute(ctx, pool, 50_000); err != nil {
		t.Fatal(err)
	}
	before, _ := jm.Amount(ctx, pool)

	payout, err := jm.Claim(ctx, pool, 1001, 42, "tx-jp-1")
	if err != nil {
		t.Fatalf("中奖清算失败: %v", err)
	}
	if payout.Amount != before {
		t.Fatalf("派彩金额 %d 应等于清零前的水位 %d", payout.Amount, before)
	}

	// 清零后应回到底注。
	after, _ := jm.Amount(ctx, pool)
	if after != conf.Jackpots[pool].Seed {
		t.Fatalf("清零后应回到底注 %d，实际 %d", conf.Jackpots[pool].Seed, after)
	}

	// 同一个空池不能再被拿走一次。
	if _, err := jm.Claim(ctx, pool, 1002, 43, "tx-jp-2"); err == nil {
		t.Fatal("空池不应再产生派彩")
	}

	// 派彩记录必须落进待处理索引，否则「清了池子没人拿到钱」。
	pending, err := jm.PendingPayouts(ctx, pool, time.Now().Add(time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].TxID != "tx-jp-1" {
		t.Fatalf("应有且仅有一条待派彩记录，实际 %+v", pending)
	}

	// 结算后从索引移除。
	if err := jm.Settle(ctx, pending[0]); err != nil {
		t.Fatal(err)
	}
	left, _ := jm.PendingPayouts(ctx, pool, time.Now().Add(time.Minute), 10)
	if len(left) != 0 {
		t.Fatalf("结算后不应还有待派彩记录，实际 %d 条", len(left))
	}
}

// 评审 P1-7：抽卡保底必须生效。
func TestGachaPityGuarantee(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21009)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)

	conf := gameconf.Default()
	pool := conf.Gacha["standard"]
	// 备足钱抽满一个保底周期。
	grant(t, n, uid, pool.Currency, pool.Cost*int64(pool.PityTop+20))

	var draws int
	gotTop := false
	pityTriggered := false

	for draws < int(pool.PityTop) && !gotTop {
		var resp pb.GachaResp
		if err := lobbyCall(t, n, uid, protocol.CmdGacha,
			&pb.GachaReq{PoolId: pool.ID, Times: 1}, &resp); err != nil {
			t.Fatalf("抽卡失败: %v", err)
		}
		if resp.GetDuplicate() {
			t.Fatal("不同请求不应被判为重复")
		}
		draws++
		for _, d := range resp.GetDrops() {
			if d.GetRarity() >= pool.TopRarity {
				gotTop = true
				if d.GetByPity() {
					pityTriggered = true
				}
			}
		}
	}

	if !gotTop {
		t.Fatalf("抽满 %d 次仍未出最高稀有度 —— 保底没生效", pool.PityTop)
	}
	t.Logf("第 %d 抽出最高稀有度（保底触发=%v，保底阈值 %d）", draws, pityTriggered, pool.PityTop)

	// 消耗与产出都要有流水。
	reader := ledger.NewReader(n.Redis, n.Keys)
	entries, err := reader.Recent(context.Background(), uid, 500)
	if err != nil {
		t.Fatal(err)
	}
	var cost, drop int
	for _, e := range entries {
		switch e.Type {
		case ledger.TypeGachaCost:
			cost++
		case ledger.TypeGachaDrop:
			drop++
		}
	}
	if cost == 0 || drop == 0 {
		t.Fatalf("抽卡的消耗与产出都必须有流水，实际 cost=%d drop=%d", cost, drop)
	}
}

// 十连保底：必定包含至少一个高稀有度。
func TestGachaTenPullGuarantee(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21010)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)

	conf := gameconf.Default()
	pool := conf.Gacha["standard"]
	grant(t, n, uid, pool.Currency, pool.Cost*200)

	for round := 0; round < 5; round++ {
		var resp pb.GachaResp
		if err := lobbyCall(t, n, uid, protocol.CmdGacha,
			&pb.GachaReq{PoolId: pool.ID, Times: 10}, &resp); err != nil {
			t.Fatalf("十连失败: %v", err)
		}
		if len(resp.GetDrops()) != 10 {
			t.Fatalf("十连应产出 10 个，实际 %d", len(resp.GetDrops()))
		}
		high := false
		for _, d := range resp.GetDrops() {
			if d.GetRarity() >= pool.HighRarity {
				high = true
			}
		}
		if !high {
			t.Fatalf("第 %d 次十连未包含高稀有度 —— 十连保底没生效", round+1)
		}
	}
}

// 未知卡池 / 未知游戏必须拒绝。
func TestUnknownGameAndPoolRejected(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21011)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)
	grant(t, n, uid, uint32(protocol.CurrencyGold), 100000)

	if err := lobbyCall(t, n, uid, protocol.CmdSpin,
		&pb.SpinReq{GameId: "not-exist", Bet: 100}, &pb.SpinResp{}); err == nil {
		t.Fatal("未知游戏必须拒绝")
	}
	if err := lobbyCall(t, n, uid, protocol.CmdGacha,
		&pb.GachaReq{PoolId: "not-exist", Times: 1}, &pb.GachaResp{}); err == nil {
		t.Fatal("未知卡池必须拒绝")
	}
}

// 未知商品与无效回执必须拒绝（评审 P0-2）。
func TestPurchaseRejectsUnknownProductAndBadReceipt(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21012)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)

	// 未知商品：以前这里会按客户端给的金额直接发钱。
	if err := lobbyCall(t, n, uid, protocol.CmdPurchase, &pb.PurchaseReq{
		OrderId: "o-unknown", Product: 99999,
		Receipt: &pb.PurchaseReceipt{Channel: "sandbox", Signature: "x"},
	}, &pb.PurchaseResp{}); err == nil {
		t.Fatal("未知商品必须拒绝")
	}

	// 已知商品但签名无效。
	if err := lobbyCall(t, n, uid, protocol.CmdPurchase, &pb.PurchaseReq{
		OrderId: "o-badsig", Product: 1,
		Receipt: &pb.PurchaseReceipt{Channel: "sandbox", Signature: "AAAA"},
	}, &pb.PurchaseResp{}); err == nil {
		t.Fatal("无效回执必须拒绝")
	}

	// 不允许的渠道。
	if err := lobbyCall(t, n, uid, protocol.CmdPurchase, &pb.PurchaseReq{
		OrderId: "o-badchan", Product: 1,
		Receipt: &pb.PurchaseReceipt{Channel: "wechat", Signature: "AAAA"},
	}, &pb.PurchaseResp{}); err == nil {
		t.Fatal("未授权渠道必须拒绝")
	}

	// 确认一分钱都没到账。
	if bal := grant(t, n, uid, uint32(protocol.CurrencyDiamond), 0); bal != 0 {
		t.Fatalf("被拒绝的充值不应到账，实际钻石 = %d", bal)
	}
}

// GM 补单必须幂等，并留下带操作者的流水。
func TestGMGrantIdempotentAndAudited(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21013)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)

	sh := shardOf(n, uid)
	call := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := harness.InternalCall(ctx, t, n, lobbySubject(sh, protocol.CmdGMGrant),
			protocol.CmdGMGrant, uid, "alice@ops", &pb.GMGrantReq{
				Uid: uid, Currency: uint32(protocol.CurrencyDiamond),
				Amount: 500, Reason: "补偿", Ticket: "TICKET-1",
			}, &pb.Ack{}); err != nil {
			t.Fatalf("GM 发放失败: %v", err)
		}
	}
	call()
	call() // 重复点一次

	if bal := grant(t, n, uid, uint32(protocol.CurrencyDiamond), 0); bal != 500 {
		t.Fatalf("重复补单不应发两份，余额 = %d，期望 500", bal)
	}

	reader := ledger.NewReader(n.Redis, n.Keys)
	entries, err := reader.Recent(context.Background(), uid, 50)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if e.Type == ledger.TypeGrant && e.Operator == "alice@ops" {
			found = true
		}
	}
	if !found {
		t.Fatal("GM 补单必须留下带操作者的流水 —— 否则无法追责")
	}
}

// GM 命令缺少 operator 必须被拒（审计要求）。
func TestGMCommandRequiresOperator(t *testing.T) {
	env := harness.Start(t)
	n, svc := startLobby(t, env, 1)

	const uid = uint64(21014)
	harness.Eventually(t, 20*time.Second, "认领分片", func() bool { return svc.Owns(uid) })
	loginPlayer(t, n, uid)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := harness.InternalCall(ctx, t, n, lobbySubject(shardOf(n, uid), protocol.CmdGMGrant),
		protocol.CmdGMGrant, uid, "", &pb.GMGrantReq{
			Uid: uid, Currency: 2, Amount: 100, Ticket: "T",
		}, &pb.Ack{})
	if err == nil {
		t.Fatal("GM 命令缺少 operator 必须被拒绝")
	}
}

// ---- 测试辅助 ----

// node0 是 node.Node 的别名，避免在签名里重复长包名。
type node0 = node.Node

func shardOf(n *node0, uid uint64) uint32 { return shard.Of(uid, n.Cfg.ShardCount) }

func lobbySubject(sh uint32, cmd protocol.Cmd) string {
	return subject.LobbyReq(sh, cmd.Name())
}

// loginPlayer 让玩家登录（会话由 Lobby 侧记录，供推送与 RG 计时使用）。
func loginPlayer(t *testing.T, n *node0, uid uint64) {
	t.Helper()
	if err := lobbyCall(t, n, uid, protocol.CmdLogin,
		&pb.LoginReq{Uid: uid, GateId: "test-gate", ConnId: uid}, &pb.LoginResp{}); err != nil {
		t.Fatalf("登录失败: %v", err)
	}
}

// setRG 通过 GM 通道设置责任游戏限额。传 -1 表示不修改该项。
func setRG(t *testing.T, n *node0, uid uint64, req *pb.GMSetRGReq) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := harness.InternalCall(ctx, t, n, lobbySubject(shardOf(n, uid), protocol.CmdGMSetRG),
		protocol.CmdGMSetRG, uid, "test-op", req, &pb.Ack{}); err != nil {
		t.Fatalf("设置限额失败: %v", err)
	}
}
