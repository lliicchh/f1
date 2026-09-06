package authz

import (
	"errors"
	"testing"
	"time"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
)

// 造币类命令绝不能是客户端级。这条测试是为了拦住以后「顺手把新命令加进白名单」
func TestMoneyCommandsAreNotClientCallable(t *testing.T) {
	forbidden := []protocol.Cmd{
		protocol.CmdAddCurrency,
		protocol.CmdAddItem,
		protocol.CmdApplyTransfer,
		protocol.CmdApplyMail,
		protocol.CmdBattleSettle,
		protocol.CmdApplyJackpot,
		protocol.CmdGMGrant,
		protocol.CmdGMQuery,
		protocol.CmdGMKick,
		protocol.CmdGMSetRG,
	}
	for _, c := range forbidden {
		if c.ClientCallable() {
			t.Errorf("命令 %s 绝不能允许客户端调用，它能凭空造币或越权", c)
		}
		if err := CheckClient(c); err == nil {
			t.Errorf("CheckClient(%s) 应当拒绝", c)
		}
	}
}

// 未登记的命令必须落到最严级别：漏配的后果应该是「调不通」而不是「被刷钱」
func TestUnknownCommandDefaultsToInternal(t *testing.T) {
	unknown := protocol.Cmd(60001)
	if unknown.Level() != protocol.LevelInternal {
		t.Fatalf("未登记命令应默认为 internal，实际 %s", unknown.Level())
	}
	if unknown.ClientCallable() {
		t.Fatal("未登记命令不应允许客户端调用")
	}
}

// 玩家正常玩游戏要用的命令必须是客户端级，否则功能不可用
func TestPlayerCommandsAreClientCallable(t *testing.T) {
	allowed := []protocol.Cmd{
		protocol.CmdLogin, protocol.CmdHeartbeat, protocol.CmdGetBag,
		protocol.CmdSpin, protocol.CmdRoundState, protocol.CmdGacha,
		protocol.CmdPurchase, protocol.CmdJackpotInfo, protocol.CmdRGStatus,
	}
	for _, c := range allowed {
		if !c.ClientCallable() {
			t.Errorf("命令 %s 应允许客户端调用", c)
		}
	}
}

func newEnv(cmd protocol.Cmd, uid uint64) *pb.Envelope {
	return &pb.Envelope{
		Cmd: uint32(cmd), Uid: uid, TraceId: "trace-1",
		TsMs: time.Now().UnixMilli(), FromNode: "s1-lobby-1",
	}
}

func TestSignAndVerify(t *testing.T) {
	s := NewSigner("secret")
	env := newEnv(protocol.CmdAddCurrency, 1001)

	if err := s.Sign(env); err != nil {
		t.Fatal(err)
	}
	if len(env.GetAuth()) == 0 {
		t.Fatal("内部命令应被签名")
	}
	if err := s.Verify(env, false); err != nil {
		t.Fatalf("自己签的应当能验过: %v", err)
	}
}

// 客户端级命令不需要签名，也不应因为没签名被拒
func TestClientCommandNeedsNoSignature(t *testing.T) {
	s := NewSigner("secret")
	env := newEnv(protocol.CmdGetBag, 1001)
	if err := s.Sign(env); err != nil {
		t.Fatal(err)
	}
	if len(env.GetAuth()) != 0 {
		t.Fatal("客户端级命令不该被签名")
	}
	if err := s.Verify(env, true); err != nil {
		t.Fatalf("客户端级命令应放行: %v", err)
	}
}

// 网关带进来的内部命令一律拒，签名对也不行
func TestInternalCommandFromClientAlwaysRejected(t *testing.T) {
	s := NewSigner("secret")
	env := newEnv(protocol.CmdAddCurrency, 1001)
	_ = s.Sign(env)

	if err := s.Verify(env, true); !errors.Is(err, ErrNotClientCallable) {
		t.Fatalf("来自客户端的内部命令必须被拒，实际 %v", err)
	}
}

func TestMissingSignatureRejected(t *testing.T) {
	s := NewSigner("secret")
	env := newEnv(protocol.CmdAddCurrency, 1001)
	if err := s.Verify(env, false); !errors.Is(err, ErrMissingSignature) {
		t.Fatalf("缺签名应被拒，实际 %v", err)
	}
}

func TestWrongSecretRejected(t *testing.T) {
	a := NewSigner("secret-a")
	b := NewSigner("secret-b")
	env := newEnv(protocol.CmdAddCurrency, 1001)
	_ = a.Sign(env)

	if err := b.Verify(env, false); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("换密钥应验不过，实际 %v", err)
	}
}

// 签名绑定 cmd：不能把 add_item 的签名挪去当 add_currency 用
func TestSignatureBoundToCommand(t *testing.T) {
	s := NewSigner("secret")
	env := newEnv(protocol.CmdAddItem, 1001)
	_ = s.Sign(env)

	env.Cmd = uint32(protocol.CmdAddCurrency)
	if err := s.Verify(env, false); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("改命令号后签名应失效，实际 %v", err)
	}
}

// 签名绑定 uid：不能把给 A 发奖的签名挪去给 B 发奖
func TestSignatureBoundToUID(t *testing.T) {
	s := NewSigner("secret")
	env := newEnv(protocol.CmdAddCurrency, 1001)
	_ = s.Sign(env)

	env.Uid = 2002
	if err := s.Verify(env, false); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("改 uid 后签名应失效，实际 %v", err)
	}
}

// 签名有时效：旧签名不能被无限重放
func TestExpiredSignatureRejected(t *testing.T) {
	s := NewSigner("secret")
	env := newEnv(protocol.CmdAddCurrency, 1001)
	env.TsMs = time.Now().Add(-2 * MaxSkew).UnixMilli()
	_ = s.Sign(env)
	// Sign 会重置 ts_ms 只在为 0 时；这里手工构造一个过期签名
	env.TsMs = time.Now().Add(-2 * MaxSkew).UnixMilli()
	env.Auth = s.digest(env.GetCmd(), env.GetUid(), env.GetTsMs(), env.GetTraceId())

	if err := s.Verify(env, false); !errors.Is(err, ErrExpired) {
		t.Fatalf("过期签名应被拒，实际 %v", err)
	}
}

// GM 命令必须留下操作者，否则事后无法追责
func TestGMCommandRequiresOperator(t *testing.T) {
	s := NewSigner("secret")
	env := newEnv(protocol.CmdGMGrant, 1001)
	_ = s.Sign(env)

	if err := s.Verify(env, false); err == nil {
		t.Fatal("GM 命令缺少 operator 应被拒")
	}

	env.Operator = "alice@ops"
	env.Auth = s.digest(env.GetCmd(), env.GetUid(), env.GetTsMs(), env.GetTraceId())
	if err := s.Verify(env, false); err != nil {
		t.Fatalf("带 operator 的 GM 命令应通过: %v", err)
	}
}

// 没有密钥的进程（例如网关）签不出内部命令，这是物理隔离，不只是约定
func TestNoSecretCannotSign(t *testing.T) {
	s := NewSigner("")
	env := newEnv(protocol.CmdAddCurrency, 1001)
	if err := s.Sign(env); !errors.Is(err, ErrNoSecret) {
		t.Fatalf("无密钥不应签出内部命令，实际 %v", err)
	}
	// 也不能校验：无法校验就必须拒绝，而不是放行
	env.Auth = []byte("whatever")
	if err := s.Verify(env, false); err == nil {
		t.Fatal("无密钥时不应放行内部命令")
	}
}
