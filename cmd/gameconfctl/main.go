// Command gameconfctl 是游戏配置表的运维工具。
//
//	gameconfctl dump  -o deploy/game.json     导出内置默认配置
//	gameconfctl lint  -f deploy/game.json     校验配置并打印版本号
//	gameconfctl rtp   -f deploy/game.json     蒙特卡洛实测各机器 RTP
//
// 配置版本号是内容哈希：改一个数字它就变。发布前用 lint 确认版本、
// 用 rtp 确认数学模型没被改坏 —— 后者是 slots 上线前的必检项（评审 P1-4）。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/gamedev/f1/internal/slots"
	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/rng"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "dump":
		dump(os.Args[2:])
	case "lint":
		lint(os.Args[2:])
	case "rtp":
		rtp(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `用法:
  gameconfctl dump [-o 文件]        导出内置默认配置
  gameconfctl lint -f 文件          校验配置并打印版本号
  gameconfctl rtp  [-f 文件] [-n 次数]  蒙特卡洛实测 RTP`)
}

func dump(args []string) {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	out := fs.String("o", "", "输出文件，留空则打到标准输出")
	_ = fs.Parse(args)

	c := gameconf.Default()
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		fatal(err)
	}
	raw = append(raw, '\n')

	if *out == "" {
		_, _ = os.Stdout.Write(raw)
		return
	}
	if err := os.WriteFile(*out, raw, 0o644); err != nil {
		fatal(err)
	}
	fmt.Printf("已导出 %s（版本 %s）\n", *out, c.Version())
}

func lint(args []string) {
	fs := flag.NewFlagSet("lint", flag.ExitOnError)
	path := fs.String("f", "", "配置文件路径")
	_ = fs.Parse(args)

	c, err := gameconf.Load(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "配置校验失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("配置合法，版本 %s\n", c.Version())
	for id, m := range c.Slots {
		fmt.Printf("  老虎机 %-12s %d 轴 × %d 行，%d 条线，理论 RTP %.4f，奖池 %q\n",
			id, len(m.Reels), m.Rows, m.LineCount(), m.TheoreticalRTP, m.JackpotPool)
	}
	for id, p := range c.Gacha {
		fmt.Printf("  卡池   %-12s %d 个条目，保底 %d/%d\n", id, len(p.Entries), p.PityHigh, p.PityTop)
	}
	fmt.Printf("  商品 %d 个，道具 %d 个，奖池 %d 个\n", len(c.Products), len(c.Items), len(c.Jackpots))
}

func rtp(args []string) {
	fs := flag.NewFlagSet("rtp", flag.ExitOnError)
	path := fs.String("f", "", "配置文件路径，留空用内置默认")
	n := fs.Int("n", 2_000_000, "模拟旋转次数")
	_ = fs.Parse(args)

	c, err := gameconf.Load(*path)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("配置版本 %s，每台机器模拟 %d 次旋转\n\n", c.Version(), *n)

	failed := false
	for id, m := range c.Slots {
		src, err := rng.NewRandom()
		if err != nil {
			fatal(err)
		}
		bet := m.BetLevels[0]

		var totalBet, totalWin, maxWin int64
		var freeLeft, mult uint32
		var hits, paid int64
		mult = 1

		for i := 0; i < *n; i++ {
			free := freeLeft > 0
			sm := uint32(1)
			if free {
				sm = mult
				freeLeft--
			} else {
				totalBet += bet
				paid++
			}
			r, err := slots.Spin(m, src, bet, sm, free)
			if err != nil {
				fatal(err)
			}
			totalWin += r.TotalWin
			if r.TotalWin > maxWin {
				maxWin = r.TotalWin
			}
			if !free && r.TotalWin > 0 {
				hits++
			}
			if r.FreeSpinsAwarded > 0 {
				freeLeft += r.FreeSpinsAwarded
				if m.FreeSpinMultiplier > 0 {
					mult = m.FreeSpinMultiplier
				}
			}
		}

		actual := float64(totalWin) / float64(totalBet)
		diff := actual - m.TheoreticalRTP
		status := "OK"
		if diff > 0.005 || diff < -0.005 {
			status = "偏离"
			failed = true
		}
		fmt.Printf("%-12s 实测 RTP %.4f  配置 %.4f  偏差 %+.4f  [%s]\n",
			id, actual, m.TheoreticalRTP, diff, status)
		fmt.Printf("%-12s 命中率 %.4f  最大单次 %.1fx 总注\n\n",
			"", float64(hits)/float64(paid), float64(maxWin)/float64(bet))
	}

	if failed {
		fmt.Fprintln(os.Stderr, "有机器的实测 RTP 偏离配置值超过 0.5%，发布前必须查清")
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
