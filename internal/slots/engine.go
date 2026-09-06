// Package slots 老虎机的数学引擎：旋转、连线、免费旋转状态机
//
// 这里全是纯函数，没有 IO、没有时间、没有全局状态。客服拿 round_id 找出当时的
// 配置版本和种子就能重放。
// 往这个包里塞时间戳或者玩家实时状态，复算就不成立了
package slots

import (
	"fmt"

	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/rng"
)

// Grid 一次旋转的可见符号，按 [轴][行] 排
type Grid [][]gameconf.Symbol

// Flat 按先轴后行摊成一维，走协议用
func (g Grid) Flat() []uint32 {
	out := make([]uint32, 0, len(g)*len(g[0]))
	for _, reel := range g {
		for _, s := range reel {
			out = append(out, uint32(s))
		}
	}
	return out
}

// LineWin 一条中奖线的结果
type LineWin struct {
	Line   int
	Symbol gameconf.Symbol
	Count  int
	Payout int64
}

// Result 一次旋转的完整结果
type Result struct {
	Grid  Grid
	Stops []int // 各轴停位，复算时对照用

	Lines      []LineWin
	LineWin    int64 // 连线派彩合计，没乘倍数
	Scatter    int
	ScatterWin int64
	// Multiplier 本次旋转的倍数，免费旋转期间大于 1
	Multiplier uint32
	// TotalWin 最终派彩，已经乘过倍数
	TotalWin int64

	FreeSpinsAwarded uint32
	JackpotHit       bool
}

// Spin 转一次
//
//	bet        总投注，免费旋转传触发时的额度，用来算赔付基数
//	multiplier 倍数，普通旋转传 1
//	free       是不是免费旋转，影响要不要判奖池
//
// 随机数的消费顺序固定：先每轴一个停位，付费时再来一个奖池判定。
// 这是复算契约，别动
func Spin(m *gameconf.SlotMachine, src *rng.Source, bet int64, multiplier uint32, free bool) (*Result, error) {
	if m == nil {
		return nil, fmt.Errorf("slots: 机器配置为空")
	}
	if bet <= 0 {
		return nil, fmt.Errorf("slots: 投注额必须为正")
	}
	if multiplier == 0 {
		multiplier = 1
	}

	reels := len(m.Reels)
	stops := make([]int, reels)
	grid := make(Grid, reels)

	for i := 0; i < reels; i++ {
		stripLen := len(m.Reels[i])
		stops[i] = src.IntN(stripLen)
		col := make([]gameconf.Symbol, m.Rows)
		for r := 0; r < m.Rows; r++ {
			col[r] = m.Reels[i][(stops[i]+r)%stripLen]
		}
		grid[i] = col
	}

	res := &Result{Grid: grid, Stops: stops, Multiplier: multiplier}

	lineBet := bet / int64(m.LineCount())
	res.Lines, res.LineWin = evalLines(m, grid, lineBet)
	res.Scatter, res.ScatterWin = evalScatter(m, grid, bet)
	res.FreeSpinsAwarded = freeSpinsFor(m, res.Scatter, free)

	res.TotalWin = (res.LineWin + res.ScatterWin) * int64(multiplier)

	// 免费旋转没投注，不参与奖池
	if !free && m.JackpotPool != "" && m.JackpotChanceDen > 0 {
		res.JackpotHit = src.Chance(m.JackpotChanceNum, m.JackpotChanceDen)
	}
	return res, nil
}

// evalLines 判所有中奖线，规则是标准的左起连线：
// 从最左轴往右连续出现同一符号才算，wild 能替代，一条线只取最高的那种赔付。
// scatter 不参与连线，单独按散落算
func evalLines(m *gameconf.SlotMachine, grid Grid, lineBet int64) ([]LineWin, int64) {
	var wins []LineWin
	var total int64

	for li, line := range m.Paylines {
		best := LineWin{Line: li}

		// 首轴是什么就试什么，是 wild 就所有符号试一遍取最高
		candidates := candidateSymbols(m, grid[0][line[0]])

		for _, sym := range candidates {
			count := 0
			for i := 0; i < len(line); i++ {
				s := grid[i][line[i]]
				if s == sym || (s == m.Wild && sym != m.Scatter) {
					count++
					continue
				}
				break
			}
			payout := payoutFor(m, sym, count, lineBet)
			if payout > best.Payout {
				best = LineWin{Line: li, Symbol: sym, Count: count, Payout: payout}
			}
		}

		if best.Payout > 0 {
			wins = append(wins, best)
			total += best.Payout
		}
	}
	return wins, total
}

// candidateSymbols 返回这条线要试哪些符号
func candidateSymbols(m *gameconf.SlotMachine, first gameconf.Symbol) []gameconf.Symbol {
	if first == m.Scatter {
		return nil // scatter 不连线
	}
	if first != m.Wild {
		return []gameconf.Symbol{first}
	}
	// 首轴是 wild，能当任何符号用，逐个试
	out := make([]gameconf.Symbol, 0, len(m.Paytable))
	for sym := range m.Paytable {
		if sym == m.Scatter {
			continue
		}
		out = append(out, sym)
	}
	return out
}

func payoutFor(m *gameconf.SlotMachine, sym gameconf.Symbol, count int, lineBet int64) int64 {
	if count <= 0 {
		return 0
	}
	table, ok := m.Paytable[sym]
	if !ok || count > len(table) {
		return 0
	}
	return table[count-1] * lineBet
}

// evalScatter 数散落符号并按总注算赔付
func evalScatter(m *gameconf.SlotMachine, grid Grid, bet int64) (int, int64) {
	count := 0
	for _, reel := range grid {
		for _, s := range reel {
			if s == m.Scatter {
				count++
			}
		}
	}
	if count == 0 || len(m.ScatterPays) == 0 {
		return count, 0
	}
	idx := count - 1
	if idx >= len(m.ScatterPays) {
		idx = len(m.ScatterPays) - 1
	}
	return count, m.ScatterPays[idx] * bet
}

// freeSpinsFor 返回这次转送几个免费旋转
func freeSpinsFor(m *gameconf.SlotMachine, scatter int, free bool) uint32 {
	if scatter <= 0 || len(m.FreeSpins) == 0 {
		return 0
	}
	if free && !m.FreeSpinRetrigger {
		return 0
	}
	idx := scatter - 1
	if idx >= len(m.FreeSpins) {
		idx = len(m.FreeSpins) - 1
	}
	return m.FreeSpins[idx]
}

// Replay 用记下来的种子重放一整局
//
// 客服和审计从这里进：有配置版本、种子和 spin_index，就能复现当时每一次旋转
func Replay(m *gameconf.SlotMachine, seedHex string, bet int64, spins int) ([]*Result, error) {
	seed, err := rng.ParseSeed(seedHex)
	if err != nil {
		return nil, err
	}
	src := rng.New(seed)

	out := make([]*Result, 0, spins)
	var freeLeft, multiplier uint32
	multiplier = 1

	for i := 0; i < spins; i++ {
		free := freeLeft > 0
		mult := uint32(1)
		if free {
			mult = multiplier
			freeLeft--
		}
		r, err := Spin(m, src, bet, mult, free)
		if err != nil {
			return nil, err
		}
		if r.FreeSpinsAwarded > 0 {
			freeLeft += r.FreeSpinsAwarded
			multiplier = m.FreeSpinMultiplier
			if multiplier == 0 {
				multiplier = 1
			}
		}
		out = append(out, r)
	}
	return out, nil
}
