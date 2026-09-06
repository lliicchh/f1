package slots

import (
	"flag"
	"testing"

	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/rng"
)

var rtpSpins = flag.Int("rtp.spins", 2_000_000, "RTP 蒙特卡洛的旋转次数")

// Simulate 跑一轮蒙特卡洛，返回各部分 RTP。
//
// 这是 slots 的核心回归手段：轴带或赔付表改错一个数字，
// RTP 可能从 96% 跳到 130%（送钱）或 60%（玩家流失），
// 这种错误必须在 CI 拦住，不能等上线后看报表。
type rtpStat struct {
	Spins     int
	PaidSpins int64
	FreeSpins int64
	TotalBet  int64
	TotalWin  int64
	LineWin   int64
	ScatWin   int64
	FreeWin   int64
	Jackpots  int64
	MaxWin    int64
	HitCount  int64 // 有派彩的付费旋转次数
}

func (s rtpStat) RTP() float64     { return float64(s.TotalWin) / float64(s.TotalBet) }
func (s rtpStat) LineRTP() float64 { return float64(s.LineWin) / float64(s.TotalBet) }
func (s rtpStat) ScatRTP() float64 { return float64(s.ScatWin) / float64(s.TotalBet) }
func (s rtpStat) FreeRTP() float64 { return float64(s.FreeWin) / float64(s.TotalBet) }
func (s rtpStat) HitRate() float64 { return float64(s.HitCount) / float64(s.PaidSpins) }

func simulate(t testing.TB, m *gameconf.SlotMachine, spins int, bet int64) rtpStat {
	t.Helper()
	src, err := rng.NewRandom()
	if err != nil {
		t.Fatal(err)
	}

	st := rtpStat{Spins: spins}
	var freeLeft, mult uint32
	mult = 1

	for i := 0; i < spins; i++ {
		free := freeLeft > 0
		spinMult := uint32(1)
		if free {
			spinMult = mult
			freeLeft--
			st.FreeSpins++
		} else {
			st.TotalBet += bet
			st.PaidSpins++
		}

		r, err := Spin(m, src, bet, spinMult, free)
		if err != nil {
			t.Fatal(err)
		}

		st.TotalWin += r.TotalWin
		if r.TotalWin > st.MaxWin {
			st.MaxWin = r.TotalWin
		}
		if free {
			st.FreeWin += r.TotalWin
		} else {
			st.LineWin += r.LineWin
			st.ScatWin += r.ScatterWin
			if r.TotalWin > 0 {
				st.HitCount++
			}
		}
		if r.JackpotHit {
			st.Jackpots++
		}
		if r.FreeSpinsAwarded > 0 {
			freeLeft += r.FreeSpinsAwarded
			if m.FreeSpinMultiplier > 0 {
				mult = m.FreeSpinMultiplier
			}
		}
	}
	return st
}

// TestRTPMatchesConfig 是 RTP 回归：实测必须落在配置的理论值附近。
//
// 容差 ±0.5%（绝对）：200 万次旋转下，蒙特卡洛标准误远小于此，
// 超出容差基本可以断定是数学模型被改动了，而不是随机波动。
func TestRTPMatchesConfig(t *testing.T) {
	c := gameconf.Default()
	m, ok := c.Machine("classic5")
	if !ok {
		t.Fatal("默认配置里应有 classic5")
	}

	st := simulate(t, m, *rtpSpins, 100)

	t.Logf("旋转 %d 次（付费 %d / 免费 %d）", st.Spins, st.PaidSpins, st.FreeSpins)
	t.Logf("RTP 合计 = %.4f（配置理论值 %.4f）", st.RTP(), m.TheoreticalRTP)
	t.Logf("  连线   = %.4f", st.LineRTP())
	t.Logf("  散落   = %.4f", st.ScatRTP())
	t.Logf("  免费   = %.4f", st.FreeRTP())
	t.Logf("命中率   = %.4f  最大单次派彩 = %d（%.1fx 总注）",
		st.HitRate(), st.MaxWin, float64(st.MaxWin)/100)

	const tolerance = 0.005
	if diff := st.RTP() - m.TheoreticalRTP; diff > tolerance || diff < -tolerance {
		t.Fatalf("RTP 偏离配置：实测 %.4f，配置 %.4f，偏差 %+.4f（容差 ±%.3f）\n"+
			"轴带或赔付表被改动了；若是有意调整，请同步更新 TheoreticalRTP",
			st.RTP(), m.TheoreticalRTP, diff, tolerance)
	}
}

// TestRTPSanityBounds 是一道更粗的护栏：即便有人同时改了配置里的理论值，
// RTP 也不该跑到明显不合理的区间。
func TestRTPSanityBounds(t *testing.T) {
	c := gameconf.Default()
	for id, m := range c.Slots {
		if m.TheoreticalRTP < 0.80 || m.TheoreticalRTP > 0.99 {
			t.Errorf("%s 的理论 RTP %.4f 超出合理区间 [0.80, 0.99]", id, m.TheoreticalRTP)
		}
	}
}

func BenchmarkSpin(b *testing.B) {
	c := gameconf.Default()
	m, _ := c.Machine("classic5")
	src, _ := rng.NewRandom()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Spin(m, src, 100, 1, false); err != nil {
			b.Fatal(err)
		}
	}
}
