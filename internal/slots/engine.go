// Package slots 是老虎机的数学引擎：旋转、连线判定、免费旋转状态机。
//
// 这个包刻意做成纯函数式的：输入（机器配置 + 随机源 + 回合状态）确定，
// 输出就完全确定。没有 IO、没有时间、没有全局状态。
//
// 这是「可复算」的前提 —— 客服拿到 round_id，取出当时的配置版本与种子，
// 在这里重放一遍，就能得到与当时位对位一致的结果。
// 任何把时间戳、随机全局量、玩家实时状态引入本包的改动都会破坏这个性质。
package slots

import (
	"fmt"

	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/rng"
)

// Grid 是一次旋转的可见符号矩阵，按 [轴][行] 组织。
type Grid [][]gameconf.Symbol

// Flat 按「先轴后行」展开成一维，用于协议传输。
func (g Grid) Flat() []uint32 {
	out := make([]uint32, 0, len(g)*len(g[0]))
	for _, reel := range g {
		for _, s := range reel {
			out = append(out, uint32(s))
		}
	}
	return out
}

// LineWin 是一条中奖线的结果。
type LineWin struct {
	Line   int
	Symbol gameconf.Symbol
	Count  int
	Payout int64
}

// Result 是一次旋转的完整结果。
type Result struct {
	Grid  Grid
	Stops []int // 各轴停止位置，复算校验用

	Lines      []LineWin
	LineWin    int64 // 连线派彩合计（未乘免费旋转倍数）
	Scatter    int
	ScatterWin int64
	// Multiplier 是本次旋转应用的倍数（免费旋转期间 > 1）。
	Multiplier uint32
	// TotalWin 是本次旋转的最终派彩，已乘倍数。
	TotalWin int64

	FreeSpinsAwarded uint32
	JackpotHit       bool
}

// Spin 执行一次旋转。
//
//	bet        本次旋转的总投注（免费旋转时传触发时的投注额，用于计算赔付基数）
//	multiplier 本次旋转的倍数（普通旋转传 1）
//	free       是否为免费旋转（影响是否判定奖池）
//
// 随机数消费顺序是固定的：先每轴一个停止位，再（非免费时）一个奖池判定。
// 这个顺序是复算契约的一部分，不能随意调整。
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

	// 奖池判定只在付费旋转时进行：免费旋转没有投注，不参与奖池。
	if !free && m.JackpotPool != "" && m.JackpotChanceDen > 0 {
		res.JackpotHit = src.Chance(m.JackpotChanceNum, m.JackpotChanceDen)
	}
	return res, nil
}

// evalLines 判定所有中奖线。
//
// 规则（业界标准的「左起连线」）：
//   - 从最左轴开始，向右连续出现同一符号（wild 可替代）才算连
//   - 一条线只取最高的一种赔付
//   - scatter 不参与连线，它按「散落」单独计算
func evalLines(m *gameconf.SlotMachine, grid Grid, lineBet int64) ([]LineWin, int64) {
	var wins []LineWin
	var total int64

	for li, line := range m.Paylines {
		best := LineWin{Line: li}

		// 候选符号：首轴的符号本身；若首轴是 wild，则所有可赔付符号都要试一遍，
		// 取赔付最高的那个（wild 自身也是候选）。
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

// candidateSymbols 返回该线需要尝试的符号集合。
func candidateSymbols(m *gameconf.SlotMachine, first gameconf.Symbol) []gameconf.Symbol {
	if first == m.Scatter {
		return nil // scatter 不连线
	}
	if first != m.Wild {
		return []gameconf.Symbol{first}
	}
	// 首轴是 wild：它可以充当任何可赔付符号，逐个试取最优。
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

// evalScatter 统计散落符号并计算赔付（按总注计）。
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

// freeSpinsFor 返回本次旋转授予的免费旋转次数。
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

// Replay 用记录下来的种子重放一个回合到指定的旋转次数，返回最后一次旋转的结果。
//
// 这是客服与审计的入口：给定 round_id 对应的 (配置版本, 种子, spin_index)，
// 就能复现出当时的每一次旋转。
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
