// Package idp 渠道身份校验
//
// 每个渠道一个 Provider，输入客户端给的凭证，输出渠道内唯一的 openid。
// 这里是全仓库唯一会打外部网络的地方，所以超时、熔断、指标都在这一层做掉，
// 上层只看到「认了」或者「没认」。
//
// 没注册 provider 的渠道一律拒绝。沙箱要显式开，忘了配不能变成谁都能登
package idp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gamedev/f1/pkg/metrics"
)

var (
	// ErrChannelUnknown 表示这个渠道没配 provider
	ErrChannelUnknown = errors.New("idp: 渠道未配置")
	// ErrCredentialInvalid 表示渠道明确说这份凭证不认，重试没用
	ErrCredentialInvalid = errors.New("idp: 渠道凭证无效")
	// ErrUpstream 表示渠道自己出问题了，可以重试
	ErrUpstream = errors.New("idp: 渠道暂时不可用")
	// ErrBreakerOpen 表示熔断器打开，直接快速失败
	ErrBreakerOpen = errors.New("idp: 渠道已熔断")
)

// Credential 客户端交上来的凭证
//
// Value 各渠道自己解释：OAuth 是 code 或 id_token，小程序是 code，
// 移动端登录是渠道签发的 JWT
type Credential struct {
	Channel  string
	Value    string
	DeviceID string
}

// Identity 渠道校验通过后拿到的身份
//
// OpenID 在该渠道内唯一，是绑定表的键。UnionID 给同一渠道下多个应用打通用，
// 没有就留空
type Identity struct {
	Channel string
	OpenID  string
	UnionID string
}

// Provider 一个渠道的校验实现
//
// 要把「凭证不对」和「渠道挂了」区分开：前者返回 ErrCredentialInvalid 不该重试，
// 后者返回 ErrUpstream 可以重试。混在一起的话，渠道抖动会被当成盗号，
// 玩家的号会莫名其妙登不进来。
//
// 应当尊重 ctx，但不尊重也不会拖住调用方，Registry 那边另有一道硬超时
type Provider interface {
	Channel() string
	Verify(ctx context.Context, c Credential) (Identity, error)
}

// Registry 渠道名到 Provider 的表
type Registry struct {
	m        map[string]Provider
	breakers map[string]*Breaker
	timeout  time.Duration
}

// Options 注册表的公共参数
type Options struct {
	// Timeout 单次校验的上限，0 取 3s。给外部 IO 兜底，不能让登录挂死
	Timeout time.Duration
	// Breaker 熔断参数，零值取默认
	Breaker BreakerOptions
}

// NewRegistry 建注册表，渠道名取各 Provider 自报的名字
func NewRegistry(opt Options, ps ...Provider) *Registry {
	if opt.Timeout <= 0 {
		opt.Timeout = 3 * time.Second
	}
	r := &Registry{
		m:        make(map[string]Provider, len(ps)),
		breakers: make(map[string]*Breaker, len(ps)),
		timeout:  opt.Timeout,
	}
	for _, p := range ps {
		if p == nil {
			continue
		}
		name := strings.ToLower(p.Channel())
		r.m[name] = p
		r.breakers[name] = NewBreaker(name, opt.Breaker)
	}
	return r
}

// Channels 返回已注册的渠道名，启动日志里打出来，配错了一眼能看见
func (r *Registry) Channels() []string {
	out := make([]string, 0, len(r.m))
	for k := range r.m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Enabled 报告有没有可用渠道
func (r *Registry) Enabled() bool { return len(r.m) > 0 }

// Verify 找到对应渠道校验一次，带超时、熔断和指标
func (r *Registry) Verify(ctx context.Context, c Credential) (Identity, error) {
	ch := strings.ToLower(strings.TrimSpace(c.Channel))
	if ch == "" {
		return Identity{}, fmt.Errorf("%w: 未指定渠道", ErrChannelUnknown)
	}
	p, ok := r.m[ch]
	if !ok {
		metrics.IDPVerify.WithLabelValues(ch, "unknown_channel").Inc()
		return Identity{}, fmt.Errorf("%w: %s", ErrChannelUnknown, ch)
	}
	if c.Value == "" {
		metrics.IDPVerify.WithLabelValues(ch, "bad_credential").Inc()
		return Identity{}, fmt.Errorf("%w: 凭证为空", ErrCredentialInvalid)
	}

	br := r.breakers[ch]
	if !br.Allow() {
		metrics.IDPVerify.WithLabelValues(ch, "breaker_open").Inc()
		return Identity{}, fmt.Errorf("%w: %s", ErrBreakerOpen, ch)
	}

	c.Channel = ch
	start := time.Now()
	id, err := r.call(ctx, p, c)
	metrics.IDPLatency.WithLabelValues(ch).Observe(time.Since(start).Seconds())

	switch {
	case err == nil:
		// 校验通过但没给 openid，等于没通过。这种上游多半是配错了，
		// 放过去会让所有人共用一个空 openid 的号
		if id.OpenID == "" {
			br.Fail()
			metrics.IDPVerify.WithLabelValues(ch, "empty_openid").Inc()
			return Identity{}, fmt.Errorf("%w: %s 返回了空 openid", ErrUpstream, ch)
		}
		br.Success()
		metrics.IDPVerify.WithLabelValues(ch, "ok").Inc()
		id.Channel = ch
		return id, nil

	case errors.Is(err, ErrCredentialInvalid):
		// 凭证不对不是渠道的错，不能计入熔断，否则一批脏凭证就能把渠道打熔
		metrics.IDPVerify.WithLabelValues(ch, "bad_credential").Inc()
		return Identity{}, err

	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		br.Fail()
		metrics.IDPVerify.WithLabelValues(ch, "timeout").Inc()
		return Identity{}, fmt.Errorf("%w: %s 超时", ErrUpstream, ch)

	default:
		br.Fail()
		metrics.IDPVerify.WithLabelValues(ch, "upstream").Inc()
		return Identity{}, fmt.Errorf("%w: %s: %v", ErrUpstream, ch, err)
	}
}

// call 带硬超时地调一次 provider
//
// ctx 只是个请求，provider 不看它照样能卡住。这一层是全仓库唯一打外部网络的
// 地方，一个不看 ctx 的第三方 SDK 就能把账号服的 goroutine 全部占住，
// 所以超时由这里兜底：到点就不等了，让那条 goroutine 自己了结。
// 结果通道带缓冲，被放弃的那次返回时不会阻塞在发送上
func (r *Registry) call(ctx context.Context, p Provider, c Credential) (Identity, error) {
	cctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	type result struct {
		id  Identity
		err error
	}
	ch := make(chan result, 1)
	go func() {
		id, err := p.Verify(cctx, c)
		ch <- result{id, err}
	}()

	select {
	case res := <-ch:
		return res.id, res.err
	case <-cctx.Done():
		return Identity{}, cctx.Err()
	}
}

// Retryable 报告这个错误值不值得重试
func Retryable(err error) bool {
	return errors.Is(err, ErrUpstream) || errors.Is(err, ErrBreakerOpen)
}
