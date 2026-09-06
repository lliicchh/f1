package slots

import (
	"testing"

	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/rng"
)

func machine(t testing.TB) *gameconf.SlotMachine {
	t.Helper()
	c := gameconf.Default()
	m, ok := c.Machine("classic5")
	if !ok {
		t.Fatal("默认配置缺少 classic5")
	}
	return m
}

// 同种子加同配置必须重放出一模一样的结果，客服查单和审计都靠这个
func TestReplayIsDeterministic(t *testing.T) {
	m := machine(t)

	src, err := rng.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	seedHex := src.SeedHex()

	// 先正常打一局，10 次
	var original []*Result
	var freeLeft, mult uint32
	mult = 1
	for i := 0; i < 10; i++ {
		free := freeLeft > 0
		sm := uint32(1)
		if free {
			sm = mult
			freeLeft--
		}
		r, err := Spin(m, src, 100, sm, free)
		if err != nil {
			t.Fatal(err)
		}
		if r.FreeSpinsAwarded > 0 {
			freeLeft += r.FreeSpinsAwarded
			mult = m.FreeSpinMultiplier
		}
		original = append(original, r)
	}

	// 拿记下来的种子重放
	replayed, err := Replay(m, seedHex, 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != len(original) {
		t.Fatalf("重放次数 %d 与原始 %d 不符", len(replayed), len(original))
	}

	for i := range original {
		a, b := original[i], replayed[i]
		if len(a.Stops) != len(b.Stops) {
			t.Fatalf("第 %d 次旋转轴数不符", i)
		}
		for j := range a.Stops {
			if a.Stops[j] != b.Stops[j] {
				t.Fatalf("第 %d 次旋转第 %d 轴停位不同：%d vs %d", i, j, a.Stops[j], b.Stops[j])
			}
		}
		if a.TotalWin != b.TotalWin {
			t.Fatalf("第 %d 次旋转派彩不同：%d vs %d", i, a.TotalWin, b.TotalWin)
		}
		if a.Scatter != b.Scatter || a.FreeSpinsAwarded != b.FreeSpinsAwarded {
			t.Fatalf("第 %d 次旋转的散落/免费次数不同", i)
		}
	}
}

// 不同种子得出不同序列，不然说明种子压根没起作用
func TestDifferentSeedsDiffer(t *testing.T) {
	m := machine(t)
	s1, _ := rng.NewRandom()
	s2, _ := rng.NewRandom()

	same := 0
	const n = 50
	for i := 0; i < n; i++ {
		a, _ := Spin(m, s1, 100, 1, false)
		b, _ := Spin(m, s2, 100, 1, false)
		if a.Stops[0] == b.Stops[0] && a.Stops[1] == b.Stops[1] && a.Stops[2] == b.Stops[2] {
			same++
		}
	}
	if same > n/4 {
		t.Fatalf("两个不同种子产生了 %d/%d 次相同停位，随机源可能没起作用", same, n)
	}
}

// 用固定的 grid 验连线规则，不掺随机
func TestEvalLinesRules(t *testing.T) {
	m := machine(t)
	lineBet := int64(10)

	// 造一台只有中间一条线的简化机器，好断言
	simple := *m
	simple.Paylines = [][]int{{1, 1, 1, 1, 1}}

	mk := func(mid []gameconf.Symbol) Grid {
		g := make(Grid, 5)
		for i := range g {
			g[i] = []gameconf.Symbol{gameconf.SymTen, mid[i], gameconf.SymTen}
		}
		return g
	}

	t.Run("三连", func(t *testing.T) {
		g := mk([]gameconf.Symbol{
			gameconf.SymAce, gameconf.SymAce, gameconf.SymAce,
			gameconf.SymKing, gameconf.SymQueen,
		})
		wins, total := evalLines(&simple, g, lineBet)
		if len(wins) != 1 || wins[0].Count != 3 || wins[0].Symbol != gameconf.SymAce {
			t.Fatalf("应判定为 A 三连，实际 %+v", wins)
		}
		want := m.Paytable[gameconf.SymAce][2] * lineBet
		if total != want {
			t.Fatalf("派彩 = %d，期望 %d", total, want)
		}
	})

	t.Run("wild替代", func(t *testing.T) {
		g := mk([]gameconf.Symbol{
			gameconf.SymAce, gameconf.SymWild, gameconf.SymAce,
			gameconf.SymTen, gameconf.SymTen,
		})
		wins, _ := evalLines(&simple, g, lineBet)
		if len(wins) != 1 || wins[0].Count != 3 || wins[0].Symbol != gameconf.SymAce {
			t.Fatalf("wild 应替代成 A 三连，实际 %+v", wins)
		}
	})

	t.Run("必须左起连续", func(t *testing.T) {
		// 首轴不是 A，中间那三个不算
		g := mk([]gameconf.Symbol{
			gameconf.SymKing, gameconf.SymAce, gameconf.SymAce,
			gameconf.SymAce, gameconf.SymTen,
		})
		wins, _ := evalLines(&simple, g, lineBet)
		for _, w := range wins {
			if w.Symbol == gameconf.SymAce {
				t.Fatalf("非左起的 A 不应中奖：%+v", w)
			}
		}
	})

	t.Run("首轴wild取最优", func(t *testing.T) {
		// wild 加两个 MAJOR，应该按 MAJOR 三连赔
		g := mk([]gameconf.Symbol{
			gameconf.SymWild, gameconf.SymMajor, gameconf.SymMajor,
			gameconf.SymTen, gameconf.SymTen,
		})
		wins, _ := evalLines(&simple, g, lineBet)
		if len(wins) != 1 {
			t.Fatalf("应有一条中奖线，实际 %+v", wins)
		}
		want := m.Paytable[gameconf.SymMajor][2] * lineBet
		if wins[0].Payout != want {
			t.Fatalf("派彩 = %d，期望按 MAJOR 三连 %d", wins[0].Payout, want)
		}
	})

	t.Run("scatter不连线", func(t *testing.T) {
		g := mk([]gameconf.Symbol{
			gameconf.SymScatter, gameconf.SymScatter, gameconf.SymScatter,
			gameconf.SymTen, gameconf.SymTen,
		})
		wins, total := evalLines(&simple, g, lineBet)
		if total != 0 || len(wins) != 0 {
			t.Fatalf("scatter 不应参与连线，实际 %+v", wins)
		}
	})
}

// 散落符号按总注赔，跟位置无关
func TestEvalScatter(t *testing.T) {
	m := machine(t)
	g := make(Grid, 5)
	for i := range g {
		g[i] = []gameconf.Symbol{gameconf.SymTen, gameconf.SymTen, gameconf.SymTen}
	}
	// 在三个不同轴的不同行放 scatter
	g[0][0] = gameconf.SymScatter
	g[2][1] = gameconf.SymScatter
	g[4][2] = gameconf.SymScatter

	count, win := evalScatter(m, g, 100)
	if count != 3 {
		t.Fatalf("散落计数 = %d，期望 3", count)
	}
	if want := m.ScatterPays[2] * 100; win != want {
		t.Fatalf("散落派彩 = %d，期望 %d", win, want)
	}
}

// 免费旋转的触发次数要对，retrigger 开关也要生效
func TestFreeSpinAward(t *testing.T) {
	m := machine(t)

	if got := freeSpinsFor(m, 3, false); got != m.FreeSpins[2] {
		t.Fatalf("3 个散落应授予 %d 次，实际 %d", m.FreeSpins[2], got)
	}
	if got := freeSpinsFor(m, 2, false); got != 0 {
		t.Fatalf("2 个散落不应授予免费旋转，实际 %d", got)
	}
	// 开了 retrigger，免费旋转里再触发还应该给
	if got := freeSpinsFor(m, 3, true); got == 0 {
		t.Fatal("配置允许 retrigger，免费旋转中应能再次触发")
	}

	noRetrigger := *m
	noRetrigger.FreeSpinRetrigger = false
	if got := freeSpinsFor(&noRetrigger, 3, true); got != 0 {
		t.Fatalf("关闭 retrigger 后免费旋转中不应再触发，实际 %d", got)
	}
}

// 免费旋转没投注，不该参与奖池
func TestFreeSpinNeverHitsJackpot(t *testing.T) {
	m := *machine(t)
	m.JackpotChanceNum = 1
	m.JackpotChanceDen = 1 // 必中

	src, _ := rng.NewRandom()
	paid, _ := Spin(&m, src, 100, 1, false)
	if !paid.JackpotHit {
		t.Fatal("必中概率下付费旋转应命中奖池")
	}
	free, _ := Spin(&m, src, 100, 2, true)
	if free.JackpotHit {
		t.Fatal("免费旋转不应参与奖池判定")
	}
}

// 倍数只影响派彩，不改连线判定
func TestMultiplierAppliesToWin(t *testing.T) {
	m := machine(t)
	seed, _ := rng.NewSeed()

	a, _ := Spin(m, rng.New(seed), 100, 1, true)
	b, _ := Spin(m, rng.New(seed), 100, 3, true)

	if a.LineWin != b.LineWin || a.ScatterWin != b.ScatterWin {
		t.Fatal("倍数不应改变连线与散落的判定结果")
	}
	if b.TotalWin != a.TotalWin*3 {
		t.Fatalf("3 倍派彩 = %d，期望 %d", b.TotalWin, a.TotalWin*3)
	}
}

// 不在档位内的投注必须拒，否则等于让客户端自己定金额
func TestBetLevelValidation(t *testing.T) {
	m := machine(t)
	for _, b := range m.BetLevels {
		if !m.ValidBet(b) {
			t.Errorf("档位 %d 应合法", b)
		}
	}
	for _, b := range []int64{0, -100, 7, 99, 1_000_000} {
		if m.ValidBet(b) {
			t.Errorf("非档位金额 %d 不应通过校验", b)
		}
	}
}

// 线注必须整除，否则每把都在攒舍入误差
func TestBetDivisibleByLines(t *testing.T) {
	c := gameconf.Default()
	for id, m := range c.Slots {
		lines := int64(m.LineCount())
		for _, b := range m.BetLevels {
			if b%lines != 0 {
				t.Errorf("%s 的档位 %d 不能被 %d 条线整除", id, b, lines)
			}
		}
	}
}

func TestGridFlatOrder(t *testing.T) {
	g := Grid{
		{1, 2, 3},
		{4, 5, 6},
	}
	flat := g.Flat()
	want := []uint32{1, 2, 3, 4, 5, 6}
	if len(flat) != len(want) {
		t.Fatalf("长度 %d，期望 %d", len(flat), len(want))
	}
	for i := range want {
		if flat[i] != want[i] {
			t.Fatalf("第 %d 位 = %d，期望 %d", i, flat[i], want[i])
		}
	}
}
