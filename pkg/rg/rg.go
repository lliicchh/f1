// Package rg 实现责任游戏（Responsible Gaming）限额。
//
// 评审 P1-6 的修复。会话时长、单日投注/亏损限额、自我排除 —— 多数司法辖区
// 把这些列为发牌前置条件，不是「有空再做」的功能。
//
// 两个设计要点：
//
//	一、限额累计必须与扣款原子推进。分开写就会出现「扣了钱没记进当日累计」，
//	    限额随即形同虚设，而且是玩家侧不可见、监管侧可查的那种漏洞。
//	    因此这里只做纯计算，状态的持久化交给调用方塞进同一个 Lua。
//
//	二、跨日重置的时刻必须由配置显式决定。用「服务器本地时间的零点」很危险：
//	    换个部署地域，限额窗口就偏了，玩家可以在错位的窗口里绕过日限额。
package rg

import (
	"fmt"
	"time"

	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/pb"
)

// Decision 是一次限额检查的结果。
type Decision struct {
	Allowed bool
	// Code 用于映射到协议错误码。
	Code   Reason
	Reason string
	// Notice 非空时表示应给玩家一个提醒（未阻断）。
	Notice string
}

// Reason 是拒绝原因。
type Reason uint8

const (
	ReasonOK Reason = iota
	ReasonSelfExcluded
	ReasonDailyBet
	ReasonDailyLoss
	ReasonSession
)

// Allow 返回一个放行结果。
func Allow() Decision { return Decision{Allowed: true} }

// DayStart 返回给定时刻所属「统计日」的起点（毫秒）。
//
// offsetMinutes 是结算时区相对 UTC 的偏移。
func DayStart(now time.Time, offsetMinutes int) int64 {
	loc := time.FixedZone("rg", offsetMinutes*60)
	t := now.In(loc)
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, loc).UnixMilli()
}

// Rollover 在跨日时重置当日累计。返回是否发生了重置。
//
// 由调用方在每次下注前调用，重置结果随下注一起落盘。
func Rollover(state *pb.PlayerRG, now time.Time, cfg gameconf.RGLimits) bool {
	start := DayStart(now, cfg.ResetOffsetMinutes)
	if state.GetDayStart() == start {
		return false
	}
	state.DayStart = start
	state.DailyBet = 0
	state.DailyWin = 0
	return true
}

// effective 返回生效的限额：玩家自设的更严则用玩家的。
//
// 玩家可以把自己的限额调得更严，但不能调得比全局默认更松 ——
// 否则「设置限额」就成了解除限额的入口。
func effective(playerLimit, globalLimit int64) int64 {
	if playerLimit > 0 && (globalLimit <= 0 || playerLimit < globalLimit) {
		return playerLimit
	}
	return globalLimit
}

// CheckBet 在下注前做限额检查。
func CheckBet(state *pb.PlayerRG, bet int64, now time.Time, cfg gameconf.RGLimits) Decision {
	if state == nil {
		return Allow()
	}

	// 自我排除优先级最高：期间任何投注都必须拒绝。
	if until := state.GetExcludedUntil(); until > 0 && now.UnixMilli() < until {
		return Decision{
			Code: ReasonSelfExcluded,
			Reason: fmt.Sprintf("账号处于自我排除期，至 %s",
				time.UnixMilli(until).Format(time.RFC3339)),
		}
	}

	if limit := effective(state.GetDailyBetLimit(), cfg.DailyBetLimit); limit > 0 {
		if state.GetDailyBet()+bet > limit {
			return Decision{
				Code: ReasonDailyBet,
				Reason: fmt.Sprintf("已达当日投注限额（%d/%d）",
					state.GetDailyBet(), limit),
			}
		}
	}

	if limit := effective(state.GetDailyLossLimit(), cfg.DailyLossLimit); limit > 0 {
		loss := state.GetDailyBet() - state.GetDailyWin()
		if loss+bet > limit {
			return Decision{
				Code:   ReasonDailyLoss,
				Reason: fmt.Sprintf("已达当日亏损限额（%d/%d）", loss, limit),
			}
		}
	}

	if limit := effective(state.GetSessionLimit(), cfg.SessionLimit); limit > 0 && state.GetSessionStart() > 0 {
		elapsed := (now.UnixMilli() - state.GetSessionStart()) / 1000
		if elapsed > limit {
			return Decision{
				Code:   ReasonSession,
				Reason: fmt.Sprintf("本次会话已达时长上限（%d 秒）", limit),
			}
		}
	}

	d := Allow()
	if n := notice(state, now, cfg); n != "" {
		d.Notice = n
	}
	return d
}

// notice 生成非阻断的提醒文案（接近限额、会话时长）。
func notice(state *pb.PlayerRG, now time.Time, cfg gameconf.RGLimits) string {
	if cfg.NoticeInterval <= 0 {
		return ""
	}
	if now.UnixMilli()-state.GetLastNoticeAt() < cfg.NoticeInterval*1000 {
		return ""
	}
	if state.GetSessionStart() > 0 {
		elapsed := (now.UnixMilli() - state.GetSessionStart()) / 1000
		if elapsed >= cfg.NoticeInterval {
			return fmt.Sprintf("你已连续游戏 %d 分钟，注意休息", elapsed/60)
		}
	}
	if limit := effective(state.GetDailyBetLimit(), cfg.DailyBetLimit); limit > 0 {
		if state.GetDailyBet()*10 >= limit*8 {
			return "已接近当日投注限额"
		}
	}
	return ""
}

// ApplyBet 累计投注。必须与扣款在同一次提交中落盘。
func ApplyBet(state *pb.PlayerRG, bet int64) {
	if state == nil {
		return
	}
	state.DailyBet += bet
}

// ApplyWin 累计派彩。
func ApplyWin(state *pb.PlayerRG, win int64) {
	if state == nil {
		return
	}
	state.DailyWin += win
}

// MarkNotice 记录提醒时间，避免刷屏。
func MarkNotice(state *pb.PlayerRG, now time.Time) {
	if state != nil {
		state.LastNoticeAt = now.UnixMilli()
	}
}

// StartSession 在登录时记录会话起点。
func StartSession(state *pb.PlayerRG, now time.Time) {
	if state == nil {
		return
	}
	state.SessionStart = now.UnixMilli()
}

// LoginBlocked 报告是否处于自我排除期，登录时检查。
func LoginBlocked(state *pb.PlayerRG, now time.Time) (bool, int64) {
	if state == nil {
		return false, 0
	}
	until := state.GetExcludedUntil()
	if until > 0 && now.UnixMilli() < until {
		return true, until
	}
	return false, 0
}

// Status 生成给客户端看的限额状态。
func Status(state *pb.PlayerRG, now time.Time, cfg gameconf.RGLimits) *pb.RGStatusResp {
	if state == nil {
		state = &pb.PlayerRG{}
	}
	var session int64
	if state.GetSessionStart() > 0 {
		session = (now.UnixMilli() - state.GetSessionStart()) / 1000
	}
	d := CheckBet(state, 0, now, cfg)
	return &pb.RGStatusResp{
		DailyBet:       state.GetDailyBet(),
		DailyBetLimit:  effective(state.GetDailyBetLimit(), cfg.DailyBetLimit),
		DailyLoss:      state.GetDailyBet() - state.GetDailyWin(),
		DailyLossLimit: effective(state.GetDailyLossLimit(), cfg.DailyLossLimit),
		SessionSeconds: session,
		SessionLimit:   effective(state.GetSessionLimit(), cfg.SessionLimit),
		ExcludedUntil:  state.GetExcludedUntil(),
		Blocked:        !d.Allowed,
		Reason:         d.Reason,
	}
}
