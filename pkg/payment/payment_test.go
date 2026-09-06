package payment

import (
	"errors"
	"testing"
)

func order() Order {
	return Order{
		OrderID: "order-1", UID: 1001, ProductID: 2,
		Channel: "apple", Receipt: "rcpt", PriceCents: 3000,
	}
}

// 没配校验方式时充值必须被拒。忘了配密钥如果默认放行，这道防线就等于没有
func TestDisabledByDefault(t *testing.T) {
	v := New("", false)
	if !v.Strict() {
		t.Fatal("未配置时应视为严格模式")
	}
	if err := v.Verify(order()); !errors.Is(err, ErrDisabled) {
		t.Fatalf("未配置校验方式时必须拒绝充值，实际 %v", err)
	}
}

func TestSignedVerifierAcceptsValidReceipt(t *testing.T) {
	const secret = "s3cr3t"
	v := New(secret, false)

	o := order()
	o.Signature = Sign(secret, o)
	if err := v.Verify(o); err != nil {
		t.Fatalf("合法回执应通过: %v", err)
	}
}

func TestSignedVerifierRejectsTampering(t *testing.T) {
	const secret = "s3cr3t"
	v := New(secret, false)

	base := order()
	base.Signature = Sign(secret, base)

	// 逐项篡改，每一项都必须被识破，这些是「伪造充值」的常见手法
	cases := []struct {
		name   string
		mutate func(*Order)
	}{
		{"换订单号复用回执", func(o *Order) { o.OrderID = "order-2" }},
		{"拿别人的回执", func(o *Order) { o.UID = 2002 }},
		{"低价档换高价商品", func(o *Order) { o.ProductID = 3 }},
		{"改价格", func(o *Order) { o.PriceCents = 100 }},
		{"换渠道", func(o *Order) { o.Channel = "google" }},
		{"空签名", func(o *Order) { o.Signature = "" }},
		{"乱填签名", func(o *Order) { o.Signature = "AAAAAAAAAAAAAAAAAAAAAA" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := base
			tc.mutate(&o)
			if err := v.Verify(o); err == nil {
				t.Fatalf("%s 必须被拒绝", tc.name)
			}
		})
	}
}

func TestSandboxOnlyAcceptsSandboxChannel(t *testing.T) {
	v := New("", true)
	if v.Strict() {
		t.Fatal("沙箱不是严格模式，应能被检测出来并告警")
	}

	o := order()
	o.Channel = "apple"
	if err := v.Verify(o); !errors.Is(err, ErrChannelNotAllowed) {
		t.Fatalf("沙箱只接受 sandbox 渠道，实际 %v", err)
	}

	o.Channel = "sandbox"
	if err := v.Verify(o); err != nil {
		t.Fatalf("沙箱渠道应放行: %v", err)
	}

	o.OrderID = ""
	if err := v.Verify(o); err == nil {
		t.Fatal("即便沙箱也必须要求订单号（幂等依赖它）")
	}
}

// 有密钥时不应退化成沙箱：生产配了密钥就必须严格
func TestSecretTakesPrecedenceOverSandbox(t *testing.T) {
	v := New("s3cr3t", true)
	if !v.Strict() {
		t.Fatal("配了密钥就应是严格模式")
	}
	o := order()
	o.Channel = "sandbox"
	if err := v.Verify(o); err == nil {
		t.Fatal("严格模式下没有合法签名必须拒绝")
	}
}
