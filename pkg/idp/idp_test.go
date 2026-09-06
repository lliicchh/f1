package idp

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// stub 可编程的假渠道
type stub struct {
	name  string
	calls atomic.Int64
	fn    func(Credential) (Identity, error)
}

func (s *stub) Channel() string { return s.name }
func (s *stub) Verify(ctx context.Context, c Credential) (Identity, error) {
	s.calls.Add(1)
	return s.fn(c)
}

func reg(t *testing.T, ps ...Provider) *Registry {
	t.Helper()
	return NewRegistry(Options{Timeout: time.Second}, ps...)
}

// 没注册的渠道一律拒绝，不能有「默认放行」这种东西
func TestUnknownChannelRejected(t *testing.T) {
	r := reg(t)
	if _, err := r.Verify(context.Background(), Credential{Channel: "wechat", Value: "x"}); !errors.Is(err, ErrChannelUnknown) {
		t.Fatalf("未注册渠道应被拒绝，得到 %v", err)
	}
	if r.Enabled() {
		t.Error("没有 provider 时 Enabled 应为 false")
	}
}

// 「凭证不对」和「渠道挂了」必须分得开
//
// 合成一个错误码的话，渠道抖动期间玩家会看到「账号无效」，
// 客服收到的投诉是「我号没了」而不是「登不上」
func TestErrorClassification(t *testing.T) {
	cases := []struct {
		name      string
		fn        func(Credential) (Identity, error)
		wantIs    error
		retryable bool
	}{
		{"凭证不对", func(Credential) (Identity, error) {
			return Identity{}, fmt.Errorf("%w: 签名不符", ErrCredentialInvalid)
		}, ErrCredentialInvalid, false},
		{"上游故障", func(Credential) (Identity, error) {
			return Identity{}, errors.New("connection refused")
		}, ErrUpstream, true},
		{"上游超时", func(Credential) (Identity, error) {
			return Identity{}, context.DeadlineExceeded
		}, ErrUpstream, true},
		{"返回空 openid", func(Credential) (Identity, error) {
			return Identity{}, nil
		}, ErrUpstream, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := reg(t, &stub{name: "ch", fn: tc.fn})
			_, err := r.Verify(context.Background(), Credential{Channel: "ch", Value: "v"})
			if !errors.Is(err, tc.wantIs) {
				t.Fatalf("错误分类不对：want %v，got %v", tc.wantIs, err)
			}
			if Retryable(err) != tc.retryable {
				t.Errorf("Retryable = %v，期望 %v", Retryable(err), tc.retryable)
			}
		})
	}
}

// provider 卡住时必须被 Timeout 掐掉，不能把调用方一起挂死
func TestVerifyRespectsTimeout(t *testing.T) {
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	r := NewRegistry(Options{Timeout: 50 * time.Millisecond},
		&stub{name: "slow", fn: func(Credential) (Identity, error) {
			<-blocked
			return Identity{}, nil
		}})

	done := make(chan error, 1)
	go func() {
		_, err := r.Verify(context.Background(), Credential{Channel: "slow", Value: "v"})
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrUpstream) {
			t.Fatalf("超时应归为上游故障，得到 %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Verify 没有被超时掐断")
	}
}

// 渠道自己的故障计入熔断，凭证不对不计
//
// 不区分的话，一批脏凭证就能把这个渠道打熔，正常玩家跟着一起登不进来
func TestBadCredentialDoesNotTripBreaker(t *testing.T) {
	s := &stub{name: "ch", fn: func(Credential) (Identity, error) {
		return Identity{}, fmt.Errorf("%w: 假的", ErrCredentialInvalid)
	}}
	r := NewRegistry(Options{Timeout: time.Second,
		Breaker: BreakerOptions{Threshold: 3, Cooldown: time.Minute}}, s)

	for i := 0; i < 10; i++ {
		_, _ = r.Verify(context.Background(), Credential{Channel: "ch", Value: "v"})
	}
	if r.breakers["ch"].Open() {
		t.Fatal("凭证错误不该触发熔断")
	}
	if s.calls.Load() != 10 {
		t.Fatalf("provider 应被调用 10 次，实际 %d", s.calls.Load())
	}
}

// 连续故障要熔断，熔断期间不再打上游
func TestBreakerTripsAndFailsFast(t *testing.T) {
	s := &stub{name: "ch", fn: func(Credential) (Identity, error) {
		return Identity{}, errors.New("上游炸了")
	}}
	r := NewRegistry(Options{Timeout: time.Second,
		Breaker: BreakerOptions{Threshold: 3, Cooldown: time.Minute}}, s)

	for i := 0; i < 3; i++ {
		_, _ = r.Verify(context.Background(), Credential{Channel: "ch", Value: "v"})
	}
	if !r.breakers["ch"].Open() {
		t.Fatal("连续 3 次故障后应熔断")
	}

	before := s.calls.Load()
	_, err := r.Verify(context.Background(), Credential{Channel: "ch", Value: "v"})
	if !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("熔断期间应快速失败，得到 %v", err)
	}
	if s.calls.Load() != before {
		t.Fatal("熔断期间不该再打上游")
	}
}

// 冷却期满只放一个探针，不能一开闸就把上游再冲垮
func TestBreakerHalfOpenLetsOneProbe(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	clock := func() time.Time { return time.Unix(0, now.Load()) }

	b := NewBreaker("ch", BreakerOptions{Threshold: 2, Cooldown: 10 * time.Second, Now: clock})
	b.Fail()
	b.Fail()
	if b.Allow() {
		t.Fatal("熔断期间不该放行")
	}

	now.Add(int64(11 * time.Second))
	if !b.Allow() {
		t.Fatal("冷却期满应放一个探针")
	}
	if b.Allow() {
		t.Fatal("同一时刻只应放一个探针")
	}

	b.Success()
	if !b.Allow() || b.Open() {
		t.Fatal("探针成功后应闭合")
	}
}

// 沙箱要能造出「凭证不对」和「上游故障」两种失败，集成测试要用
func TestSandboxOutcomes(t *testing.T) {
	r := reg(t, NewSandbox())

	id, err := r.Verify(context.Background(), Credential{Channel: SandboxChannel, Value: "openid-1"})
	if err != nil {
		t.Fatalf("正常凭证应通过: %v", err)
	}
	if id.OpenID != "openid-1" || id.Channel != SandboxChannel {
		t.Fatalf("身份不对: %+v", id)
	}

	if _, err := r.Verify(context.Background(), Credential{Channel: SandboxChannel, Value: "bad"}); !errors.Is(err, ErrCredentialInvalid) {
		t.Errorf("bad 应为凭证错误，得到 %v", err)
	}
	if _, err := r.Verify(context.Background(), Credential{Channel: SandboxChannel, Value: "boom"}); !errors.Is(err, ErrUpstream) {
		t.Errorf("boom 应为上游故障，得到 %v", err)
	}
	if _, err := r.Verify(context.Background(), Credential{Channel: SandboxChannel, Value: ""}); !errors.Is(err, ErrCredentialInvalid) {
		t.Errorf("空凭证应被拒，得到 %v", err)
	}
}

// 渠道名大小写不敏感，配置里写 WeChat 和 wechat 应当是同一个
func TestChannelNameCaseInsensitive(t *testing.T) {
	r := reg(t, &stub{name: "WeChat", fn: func(Credential) (Identity, error) {
		return Identity{OpenID: "o1"}, nil
	}})
	id, err := r.Verify(context.Background(), Credential{Channel: "WECHAT", Value: "v"})
	if err != nil {
		t.Fatalf("大小写不该影响匹配: %v", err)
	}
	if id.Channel != "wechat" {
		t.Errorf("渠道名应归一化成小写，得到 %q", id.Channel)
	}
}
