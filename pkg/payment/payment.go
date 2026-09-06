// Package payment 校验充值回执
//
// 订单号只能防重复，防不了伪造，所以到账前还得验签。
// 两种实现：signed 走 HMAC，sandbox 只给本地开发用。
// 都没配就一律拒绝
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
	// ErrInvalidReceipt 表示回执缺失或签名不符
	ErrInvalidReceipt = errors.New("payment: 支付回执无效")
	// ErrChannelNotAllowed 表示该商品不允许经由此渠道购买
	ErrChannelNotAllowed = errors.New("payment: 商品不允许该支付渠道")
	// ErrAmountMismatch 表示回执金额与商品定价不符
	ErrAmountMismatch = errors.New("payment: 回执金额与商品定价不符")
	// ErrDisabled 表示未配置校验方式，充值一律拒绝
	ErrDisabled = errors.New("payment: 未配置 PAYMENT_SECRET 且未开启沙箱，充值被拒绝")
)

// Order 一次充值里与钱相关的字段
type Order struct {
	OrderID    string
	UID        uint64
	ProductID  uint32
	Channel    string
	Receipt    string
	Signature  string
	PriceCents int64 // 商品配置的价格，由服务端提供，不信客户端
}

// Verifier 校验充值回执
type Verifier interface {
	// Verify 校验通过返回 nil
	Verify(o Order) error
	// Strict 报告是否为严格模式（生产必须为 true）
	Strict() bool
}

// New 按配置构造校验器：有密钥就严格验签，否则看是否开了沙箱，都没有就拒绝
func New(secret string, sandbox bool) Verifier {
	if secret != "" {
		return &signedVerifier{secret: []byte(secret)}
	}
	if sandbox {
		return sandboxVerifier{}
	}
	return disabledVerifier{}
}

// signedVerifier 用 HMAC 验回执
//
// 签的是 orderID|uid|productID|channel|priceCents，五项都绑上，
// 免得换个单号、换个人、换个档位就能重复用同一张回执
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

// Sign 生成回执签名，供自建支付回调与测试使用
func Sign(secret string, o Order) string {
	v := &signedVerifier{secret: []byte(secret)}
	return base64.RawURLEncoding.EncodeToString(v.sign(o))
}

// sandboxVerifier 只检查字段齐不齐，给本地开发用。
// 需要 PAYMENT_SANDBOX=true 显式打开，启动时会打 Error 日志提醒
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

// disabledVerifier 一律拒绝，没配置时用它
type disabledVerifier struct{}

func (disabledVerifier) Strict() bool { return true }

func (disabledVerifier) Verify(Order) error { return ErrDisabled }
