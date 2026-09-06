package idp

import (
	"context"
	"fmt"
	"strings"

	"github.com/gamedev/f1/pkg/logx"
)

// SandboxChannel 沙箱渠道名
const SandboxChannel = "sandbox"

// Sandbox 本地开发用的假渠道，凭证原样当 openid
//
// 必须由 IDP_SANDBOX=true 显式打开。生产上开着它等于任何人报个字符串
// 就能登进任何号，所以启动时会持续打 Error 日志
type Sandbox struct{}

func NewSandbox() *Sandbox {
	logx.Error("渠道沙箱已开启（告警）：任何凭证都会被当作合法 openid，" +
		"仅限本地开发；生产必须关掉 IDP_SANDBOX")
	return &Sandbox{}
}

func (Sandbox) Channel() string { return SandboxChannel }

func (Sandbox) Verify(_ context.Context, c Credential) (Identity, error) {
	v := strings.TrimSpace(c.Value)
	// 留一个能造上游故障的口子，集成测试要验熔断和重试
	switch v {
	case "":
		return Identity{}, fmt.Errorf("%w: 凭证为空", ErrCredentialInvalid)
	case "bad":
		return Identity{}, fmt.Errorf("%w: 沙箱固定拒绝 %q", ErrCredentialInvalid, v)
	case "boom":
		return Identity{}, fmt.Errorf("%w: 沙箱模拟渠道故障", ErrUpstream)
	}
	return Identity{OpenID: v}, nil
}
