// Package payment 校验充值回执。
//
// 评审 P0-2 的修复：原来的充值接口只看客户端传来的 product 与 amount，
// 未知商品还会按客户端给的金额发钱 —— 构造一个请求就能到账 10 亿钻石。
// 订单幂等做得很扎实，但幂等保护的是「重复」，不是「伪造」。
//
// 真实项目里回执由 Apple / Google / 三方支付签发，服务端向渠道验签。
// 这里定义统一接口，并给出两种实现：
//
//	Signed  —— HMAC 验签，对接自建支付或已完成渠道回调的场景
//	Sandbox —— 只在显式开启时可用，供本地开发
//
// 默认行为是**拒绝**：没配密钥又没显式开沙箱，所有充值都不通过。
package payment

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrInvalidReceipt 表示回执缺失或签名不符。
	ErrInvalidReceipt = errors.New("payment: 支付回执无效")
	// ErrChannelNotAllowed 表示该商品不允许经由此渠道购买。
	ErrChannelNotAllowed = errors.New("payment: 商品不允许该支付渠道")
	// ErrAmountMismatch 表示回执金额与商品定价不符。
	ErrAmountMismatch = errors.New("payment: 回执金额与商品定价不符")
	// ErrDisabled 表示未配置校验方式，充值一律拒绝。
	ErrDisabled = errors.New("payment: 未配置 PAYMENT_SECRET 且未开启沙箱，充值被拒绝")
)

// Order 是一次充值请求中与钱有关的事实。
type Order struct {
	OrderID    string
	UID        uint64
	ProductID  uint32
	Channel    string
	Receipt    string
	Signature  string
	PriceCents int64 // 商品配置的价格，由服务端提供，不信客户端
}

// Verifier 校验充值回执。
type Verifier interface {
	// Verify 校验通过返回 nil。
	Verify(o Order) error
	// Strict 报告是否为严格模式（生产必须为 true）。
	Strict() bool
}

// New 按配置构造校验器。
//
// secret 非空 → 严格验签；否则若 sandbox 为真 → 沙箱放行；都不满足 → 一律拒绝。
func New(secret string, sandbox bool) Verifier {
	if secret != "" {
		return &signedVerifier{secret: []byte(secret)}
	}
	if sandbox {
		return sandboxVerifier{}
	}
	return disabledVerifier{}
}

// signedVerifier 用 HMAC 校验回执。
//
// 回执格式：base64(hmac(secret, orderID|uid|productID|channel|priceCents))
// 绑定这五项是有意的：
//   - 绑 orderID 防止同一张回执换个单号再用一次
//   - 绑 uid     防止把别人的回执拿来给自己充值
//   - 绑 product 与 price 防止用 6 元档的回执换 98 元档的商品
type signedVerifier struct{ secret []byte }

func (v *signedVerifier) Strict() bool { return true }

func (v *signedVerifier) Verify(o Order) error {
	if o.OrderID == "" || o.Channel == "" || o.Signature == "" {
		return fmt.Errorf("%w: 缺少订单号 / 渠道 / 签名", ErrInvalidReceipt)
	}
	sig, err := base64.RawURLEncoding.DecodeString(o.Signature)
	if err != nil {
		return fmt.Errorf("%w: 签名不是合法 base64", ErrInvalidReceipt)
	}
	if !hmac.Equal(sig, v.sign(o)) {
		return fmt.Errorf("%w: 签名不匹配 order=%s uid=%d", ErrInvalidReceipt, o.OrderID, o.UID)
	}
	return nil
}

func (v *signedVerifier) sign(o Order) []byte {
	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(strings.Join([]string{
		o.OrderID,
		strconv.FormatUint(o.UID, 10),
		strconv.FormatUint(uint64(o.ProductID), 10),
		o.Channel,
		strconv.FormatInt(o.PriceCents, 10),
	}, "|")))
	return mac.Sum(nil)[:16]
}

// Sign 生成回执签名。供自建支付回调与测试使用。
func Sign(secret string, o Order) string {
	v := &signedVerifier{secret: []byte(secret)}
	return base64.RawURLEncoding.EncodeToString(v.sign(o))
}

// sandboxVerifier 只做最基本的字段完整性检查。
//
// 它存在的唯一理由是让本地开发不必搭一套支付；
// 必须由 PAYMENT_SANDBOX=true 显式开启，且启动时会打 Error 级日志提醒。
type sandboxVerifier struct{}

func (sandboxVerifier) Strict() bool { return false }

func (sandboxVerifier) Verify(o Order) error {
	if o.OrderID == "" {
		return fmt.Errorf("%w: 缺少订单号", ErrInvalidReceipt)
	}
	if o.Channel != "sandbox" {
		return fmt.Errorf("%w: 沙箱模式只接受 channel=sandbox", ErrChannelNotAllowed)
	}
	return nil
}

// disabledVerifier 一律拒绝。这是没配置时的默认行为。
type disabledVerifier struct{}

func (disabledVerifier) Strict() bool { return true }

func (disabledVerifier) Verify(Order) error { return ErrDisabled }
