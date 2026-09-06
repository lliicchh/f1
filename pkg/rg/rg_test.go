package rg

import (
	"testing"
	"time"

	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/pb"
)

func limits() gameconf.RGLimits {
	return gameconf.RGLimits{
		DailyBetLimit:      1000,
		DailyLossLimit:     500,
		SessionLimit:       3600,
		ResetOffsetMinutes: 8 * 60,
		NoticeInterval:     1800,
	}
}

func TestDailyBetLimit(t *testing.T) {
	cfg := limits()
	now := time.Now()
	// 派彩填高一些，让亏损远离限额，从而单独检验投注限额。
	st := &pb.PlayerRG{
		DayStart: DayStart(now, cfg.ResetOffsetMinutes),
		DailyBet: 900, DailyWin: 700,
	}

	if d := CheckBet(st, 50, now, cfg); !d.Allowed {
		t.Fatalf("未超限应放行: %s", d.Reason)
	}
	if d := CheckBet(st, 200, now, cfg); d.Allowed {
		t.Fatal("超过当日投注限额应拒绝")
	} else if d.Code != ReasonDailyBet {
		t.Fatalf("拒绝原因应为日投注限额，实际 %v", d.Code)
	}
}

func TestDailyLossLimit(t *testing.T) {
	cfg := limits()
	now := time.Now()
	// 投注 800、派彩 400 → 亏损 400，限额 500。
	st := &pb.PlayerRG{
		DayStart: DayStart(now, cfg.ResetOffsetMinutes),
		DailyBet: 800, DailyWin: 400,
	}
	if d := CheckBet(st, 50, now, cfg); !d.Allowed {
		t.Fatalf("亏损未超限应放行: %s", d.Reason)
	}
	if d := CheckBet(st, 200, now, cfg); d.Allowed {
		t.Fatal("超过当日亏损限额应拒绝")
	} else if d.Code != ReasonDailyLoss {
		t.Fatalf("拒绝原因应为日亏损限额，实际 %v", d.Code)
	}
}

func TestSessionLimit(t *testing.T) {
	cfg := limits()
	now := time.Now()
	st := &pb.PlayerRG{
		DayStart:     DayStart(now, cfg.ResetOffsetMinutes),
		SessionStart: now.Add(-2 * time.Hour).UnixMilli(),
	}
	if d := CheckBet(st, 10, now, cfg); d.Allowed {
		t.Fatal("超过会话时长上限应拒绝")
	} else if d.Code != ReasonSession {
		t.Fatalf("拒绝原因应为会话时长，实际 %v", d.Code)
	}
}

// 自我排除优先级最高，且不能被其他条件绕过。
func TestSelfExclusionBlocksEverything(t *testing.T) {
	cfg := limits()
	now := time.Now()
	st := &pb.PlayerRG{
		DayStart:      DayStart(now, cfg.ResetOffsetMinutes),
		ExcludedUntil: now.Add(24 * time.Hour).UnixMilli(),
	}
	if d := CheckBet(st, 1, now, cfg); d.Allowed {
		t.Fatal("自我排除期内必须拒绝下注")
	} else if d.Code != ReasonSelfExcluded {
		t.Fatalf("拒绝原因应为自我排除，实际 %v", d.Code)
	}
	if blocked, _ := LoginBlocked(st, now); !blocked {
		t.Fatal("自我排除期内必须拒绝登录")
	}
	// 到期后自动解除。
	after := time.UnixMilli(st.ExcludedUntil).Add(time.Second)
	if blocked, _ := LoginBlocked(st, after); blocked {
		t.Fatal("排除期结束后应允许登录")
	}
}

// 跨日重置必须按配置时区发生，而不是服务器本地零点。
func TestRolloverUsesConfiguredTimezone(t *testing.T) {
	cfg := limits() // UTC+8
	// UTC+8 的 2026-01-02 00:30，属于「1 月 2 日」这个统计日。
	loc := time.FixedZone("t", 8*3600)
	now := time.Date(2026, 1, 2, 0, 30, 0, 0, loc)

	st := &pb.PlayerRG{
		DayStart: DayStart(now.Add(-2*time.Hour), cfg.ResetOffsetMinutes), // 1 月 1 日
		DailyBet: 900, DailyWin: 100,
	}
	if !Rollover(st, now, cfg) {
		t.Fatal("跨日应触发重置")
	}
	if st.DailyBet != 0 || st.DailyWin != 0 {
		t.Fatalf("重置后当日累计应清零，实际 bet=%d win=%d", st.DailyBet, st.DailyWin)
	}
	if st.DayStart != DayStart(now, cfg.ResetOffsetMinutes) {
		t.Fatal("重置后统计日起点不对")
	}
	// 同一天内再调用不应重置。
	st.DailyBet = 100
	if Rollover(st, now.Add(time.Hour), cfg) {
		t.Fatal("同一统计日内不应重复重置")
	}
	if st.DailyBet != 100 {
		t.Fatal("同日重置误清了累计")
	}
}

// 玩家自设限额只能更严，不能更松 —— 否则「设置限额」就成了解除限额的入口。
func TestPlayerLimitCanOnlyBeStricter(t *testing.T) {
	cfg := limits() // 全局 1000
	now := time.Now()

	stricter := &pb.PlayerRG{
		DayStart: DayStart(now, cfg.ResetOffsetMinutes),
		DailyBet: 400, DailyBetLimit: 500,
	}
	if d := CheckBet(stricter, 200, now, cfg); d.Allowed {
		t.Fatal("玩家自设更严的限额应生效")
	}

	looser := &pb.PlayerRG{
		DayStart: DayStart(now, cfg.ResetOffsetMinutes),
		DailyBet: 900, DailyBetLimit: 100000, // 想放宽到 10 万
	}
	if d := CheckBet(looser, 500, now, cfg); d.Allowed {
		t.Fatal("玩家不能把限额放得比全局更松")
	}
}

// 限额为 0 表示不限制（用于不受管辖的场景）。
func TestZeroLimitMeansUnlimited(t *testing.T) {
	cfg := gameconf.RGLimits{ResetOffsetMinutes: 0}
	now := time.Now()
	st := &pb.PlayerRG{DayStart: DayStart(now, 0), DailyBet: 1 << 40}
	if d := CheckBet(st, 1<<40, now, cfg); !d.Allowed {
		t.Fatalf("未配置限额时应放行: %s", d.Reason)
	}
}

func TestApplyBetAndWin(t *testing.T) {
	st := &pb.PlayerRG{}
	ApplyBet(st, 100)
	ApplyBet(st, 50)
	ApplyWin(st, 30)
	if st.DailyBet != 150 || st.DailyWin != 30 {
		t.Fatalf("累计错误: bet=%d win=%d", st.DailyBet, st.DailyWin)
	}
}

func TestStatusReportsLimits(t *testing.T) {
	cfg := limits()
	now := time.Now()
	st := &pb.PlayerRG{
		DayStart: DayStart(now, cfg.ResetOffsetMinutes),
		DailyBet: 300, DailyWin: 100,
		SessionStart: now.Add(-10 * time.Minute).UnixMilli(),
	}
	s := Status(st, now, cfg)
	if s.GetDailyBet() != 300 || s.GetDailyBetLimit() != 1000 {
		t.Fatalf("投注状态不对: %+v", s)
	}
	if s.GetDailyLoss() != 200 {
		t.Fatalf("亏损应为 200，实际 %d", s.GetDailyLoss())
	}
	if s.GetSessionSeconds() < 590 || s.GetSessionSeconds() > 610 {
		t.Fatalf("会话时长应约 600 秒，实际 %d", s.GetSessionSeconds())
	}
	if s.GetBlocked() {
		t.Fatal("未超限不应标记为阻断")
	}
}

func TestDayStartBoundary(t *testing.T) {
	// UTC+8 的 00:00 与 23:59 应属于不同统计日。
	loc := time.FixedZone("t", 8*3600)
	a := time.Date(2026, 3, 1, 23, 59, 59, 0, loc)
	b := time.Date(2026, 3, 2, 0, 0, 1, 0, loc)
	if DayStart(a, 8*60) == DayStart(b, 8*60) {
		t.Fatal("跨过配置时区的零点应属于不同统计日")
	}
	// 同一天内的两个时刻应属于同一统计日。
	c := time.Date(2026, 3, 2, 15, 0, 0, 0, loc)
	if DayStart(b, 8*60) != DayStart(c, 8*60) {
		t.Fatal("同一统计日内起点应相同")
	}
}
