package idp

import (
	"sync"
	"time"

	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
)

// BreakerOptions 熔断参数
type BreakerOptions struct {
	// Threshold 连续失败多少次就断开，0 取 5
	Threshold int
	// Cooldown 断开后多久放一个探针进去，0 取 10s
	Cooldown time.Duration
	// Now 取时间，测试用
	Now func() time.Time
}

// Breaker 每渠道一个的熔断器
//
// 渠道挂了的时候，登录请求会全部堆在超时上。10 万人重连、每人卡 3 秒，
// 网关的 goroutine 和 NATS 的 pending 都会被这批请求吃光，最后是整个服
// 不可用，而不只是这个渠道登不进来。熔断把它压回快速失败。
//
// 只有渠道自己的故障计入失败，凭证不对不算，否则一批脏凭证就能把渠道打熔
type Breaker struct {
	channel   string
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu       sync.Mutex
	fails    int
	openTill time.Time
	probing  bool
}

func NewBreaker(channel string, o BreakerOptions) *Breaker {
	if o.Threshold <= 0 {
		o.Threshold = 5
	}
	if o.Cooldown <= 0 {
		o.Cooldown = 10 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Breaker{channel: channel, threshold: o.Threshold, cooldown: o.Cooldown, now: o.Now}
}

// Allow 报告这次请求放不放行
//
// 冷却期满后只放一个探针进去。不加这个限制，冷却一到就会有一大批请求
// 同时打过去，渠道刚喘口气又被压回去
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.openTill.IsZero() {
		return true
	}
	if b.now().Before(b.openTill) {
		return false
	}
	if b.probing {
		return false
	}
	b.probing = true
	return true
}

// Success 探针通过或者正常成功，闭合
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()

	wasOpen := !b.openTill.IsZero()
	b.fails = 0
	b.openTill = time.Time{}
	b.probing = false
	if wasOpen {
		metrics.IDPBreakerOpen.WithLabelValues(b.channel).Set(0)
		logx.Info("渠道恢复，熔断闭合", "channel", b.channel)
	}
}

// Fail 记一次渠道侧失败
func (b *Breaker) Fail() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.probing = false
	b.fails++
	if b.fails < b.threshold {
		return
	}
	first := b.openTill.IsZero()
	b.openTill = b.now().Add(b.cooldown)
	metrics.IDPBreakerOpen.WithLabelValues(b.channel).Set(1)
	if first {
		logx.Error("渠道连续失败，熔断打开（告警）",
			"channel", b.channel, "fails", b.fails, "cooldown", b.cooldown)
	}
}

// Open 报告当前是否处于断开状态，测试与运维接口用
func (b *Breaker) Open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.openTill.IsZero() && b.now().Before(b.openTill)
}
