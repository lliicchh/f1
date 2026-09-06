// Package rg 责任游戏限额：会话时长、日投注、日亏损、自我排除
//
// 这里只做纯计算，状态由调用方塞进扣款那一次提交。分开写会出现
// 「扣了钱没记进当日累计」，限额随即形同虚设。
//
// 跨日重置的时刻由配置的时区决定，不用服务器本地零点：换个部署地域窗口就偏了
package rg

import (
	"fmt"
	"time"

	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/pb"
)

// Decision 一次限额检查的结果
type Decision struct {
	Allowed bool
	// Code 用于映射到协议错误码
	Code   Reason
	Reason string
	// Notice 非空时表示应给玩家一个提醒（未阻断）
	Notice string
}

// Reason 拒绝原因
type Reason uint8

const (
	ReasonOK Reason = iota
	ReasonSelfExcluded
	ReasonDailyBet
	ReasonDailyLoss
	ReasonSession
)

func Allow() Decision { return Decision{Allowed: true} }

// DayStart 返回该时刻所属统计日的起点，offsetMinutes 是结算时区相对 UTC 的偏移
func DayStart(now time.Time, offsetMinutes int) int64 {
	loc := time.FixedZone("rg", offsetMinutes*60)
	t := now.In(loc)
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, loc).UnixMilli()
}

// Rollover 跨日时清掉当日累计，返回是否真的重置了。
// 在下注前调用，重置结果随下注一起落盘
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

// effective 取生效限额：玩家自设的更严就用玩家的。
// 只能更严，否则「设置限额」就成了解除限额的入口
func effective(playerLimit, globalLimit int64) int64 {
	if playerLimit > 0 && (globalLimit <= 0 || playerLimit < globalLimit) {
		return playerLimit
	}
	return globalLimit
}

// CheckBet 在下注前做限额检查
func CheckBet(state *pb.PlayerRG, bet int64, now time.Time, cfg gameconf.RGLimits) Decision {
	if state == nil {
		return Allow()
	}

	// 自我排除优先级最高
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

// notice 生成不阻断的提醒文案
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

// ApplyBet 累计投注，要和扣款在同一次提交里落盘
func ApplyBet(state *pb.PlayerRG, bet int64) {
	if state == nil {
		return
	}
	state.DailyBet += bet
}

func ApplyWin(state *pb.PlayerRG, win int64) {
	if state == nil {
		return
	}
	state.DailyWin += win
}

// MarkNotice 记下提醒时间，免得刷屏
func MarkNotice(state *pb.PlayerRG, now time.Time) {
	if state != nil {
		state.LastNoticeAt = now.UnixMilli()
	}
}

// StartSession 在登录时记录会话起点
func StartSession(state *pb.PlayerRG, now time.Time) {
	if state == nil {
		return
	}
	state.SessionStart = now.UnixMilli()
}

// LoginBlocked 报告是否在自我排除期内，登录时查
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

// Status 生成给客户端看的限额状态
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
