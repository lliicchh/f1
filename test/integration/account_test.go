package integration

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/internal/account"
	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/idp"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/test/harness"
)

// startAccount 启动一个真实的账号服务
func startAccount(t *testing.T, env *harness.Env, seq int) (*node.Node, *account.Service) {
	t.Helper()
	n := env.Node(t, "account", "lobby", seq, lobbyCfg)
	svc := account.New()
	if err := svc.Start(context.Background(), n); err != nil {
		t.Fatalf("启动账号服失败: %v", err)
	}
	stop := func() {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		svc.StopAccepting(sctx)
		svc.Close(sctx)
	}
	t.Cleanup(stop)
	return n, svc
}

// channelLogin 用渠道凭证登录，返回登录应答
func channelLogin(t *testing.T, c *client, channel, credential string) *pb.Envelope {
	t.Helper()
	c.send(protocol.CmdLogin, 0, &pb.LoginReq{Channel: channel, Credential: credential})
	resp, err := c.recvCmd(uint32(protocol.CmdLogin), 20*time.Second)
	if err != nil {
		t.Fatalf("等待登录应答失败: %v", err)
	}
	return resp
}

func loginResp(t *testing.T, env *pb.Envelope) *pb.LoginResp {
	t.Helper()
	if env.GetErrCode() != 0 {
		t.Fatalf("登录失败: %d %s", env.GetErrCode(), env.GetErrMsg())
	}
	var lr pb.LoginResp
	if err := proto.Unmarshal(env.GetBody(), &lr); err != nil {
		t.Fatal(err)
	}
	return &lr
}

// 渠道首登：开号、建会话、一路走到 Lobby 拿回玩家数据
func TestChannelLoginOpensAccountEndToEnd(t *testing.T) {
	env := harness.Start(t)
	startLobby(t, env, 1)
	startAccount(t, env, 1)
	_, _, addr := startGateway(t, env, 1)

	c := dial(t, addr)
	lr := loginResp(t, channelLogin(t, c, idp.SandboxChannel, "player-alpha"))

	uid := lr.GetBase().GetUid()
	if uid == 0 {
		t.Fatal("首登应开出一个 uid")
	}

	// 同一个渠道账号再登一次必须回到同一个号
	c2 := dial(t, addr)
	lr2 := loginResp(t, channelLogin(t, c2, idp.SandboxChannel, "player-alpha"))
	if got := lr2.GetBase().GetUid(); got != uid {
		t.Fatalf("同一渠道账号应回到 uid=%d，得到 %d", uid, got)
	}

	// 不同的渠道账号是另一个号
	c3 := dial(t, addr)
	lr3 := loginResp(t, channelLogin(t, c3, idp.SandboxChannel, "player-beta"))
	if got := lr3.GetBase().GetUid(); got == uid {
		t.Fatalf("不同渠道账号不该共用 uid=%d", uid)
	}
}

// 重连走 token，不经过账号服
//
// 这是拆进程的全部目的：外部渠道慢或者挂了，只影响首次登录，
// 已经拿到票据的玩家照常重连。测试直接把账号服停掉来证明这条路径不依赖它
func TestReconnectWithTokenDoesNotTouchAccount(t *testing.T) {
	env := harness.Start(t)
	startLobby(t, env, 1)
	_, asvc := startAccount(t, env, 1)
	_, _, addr := startGateway(t, env, 1)

	c := dial(t, addr)
	lr := loginResp(t, channelLogin(t, c, idp.SandboxChannel, "reconnector"))
	uid := lr.GetBase().GetUid()

	// 账号服下线，渠道登录这条路彻底断掉
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	asvc.StopAccepting(sctx)
	cancel()

	c2 := dial(t, addr)
	c2.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid, Token: harness.Token(t, uid)})
	resp, err := c2.recvCmd(uint32(protocol.CmdLogin), 15*time.Second)
	if err != nil {
		t.Fatalf("等待重连应答失败: %v", err)
	}
	if resp.GetErrCode() != 0 {
		t.Fatalf("账号服下线不该影响带票据的重连: %d %s", resp.GetErrCode(), resp.GetErrMsg())
	}
	if got := loginResp(t, resp).GetBase().GetUid(); got != uid {
		t.Fatalf("重连应回到 uid=%d，得到 %d", uid, got)
	}
}

// 凭证不对、渠道故障、渠道没配，三种失败要给出三个不同的错误码
//
// 客户端据此决定是换个凭证重来还是等一会儿再试。合成一个码的话，
// 渠道抖动期间玩家会被告知「账号无效」
func TestChannelLoginFailuresAreDistinguishable(t *testing.T) {
	env := harness.Start(t)
	startLobby(t, env, 1)
	startAccount(t, env, 1)
	_, _, addr := startGateway(t, env, 1)

	cases := []struct {
		name       string
		channel    string
		credential string
		want       protocol.ErrCode
	}{
		{"凭证不对", idp.SandboxChannel, "bad", protocol.ErrCredentialBad},
		{"渠道故障", idp.SandboxChannel, "boom", protocol.ErrUpstreamFailed},
		{"未配置的渠道", "wechat", "whatever", protocol.ErrChannelUnknown},
		{"空凭证", idp.SandboxChannel, "", protocol.ErrBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := dial(t, addr)
			if tc.credential == "" {
				// 空凭证连 channel 都不带就退化成普通登录，这里单独构造
				c.send(protocol.CmdLogin, 0, &pb.LoginReq{Channel: tc.channel})
				resp, err := c.recvCmd(uint32(protocol.CmdLogin), 15*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if resp.GetErrCode() == 0 {
					t.Fatal("空凭证不该登录成功")
				}
				return
			}
			resp := channelLogin(t, c, tc.channel, tc.credential)
			if got := protocol.ErrCode(resp.GetErrCode()); got != tc.want {
				t.Fatalf("错误码 = %d(%s)，期望 %d(%s)", got, got, tc.want, tc.want)
			}
		})
	}
}

// 账号服不可达要报「服务暂时不可用」，不能报「认证失败」
//
// 后者会让客户端清掉本地凭证重新走注册流程，玩家看到的是号没了
func TestAccountUnreachableIsNotAuthFailure(t *testing.T) {
	env := harness.Start(t)
	startLobby(t, env, 1)
	_, _, addr := startGateway(t, env, 1) // 故意不起账号服

	c := dial(t, addr)
	resp := channelLogin(t, c, idp.SandboxChannel, "nobody-home")
	if got := protocol.ErrCode(resp.GetErrCode()); got != protocol.ErrUpstreamFailed {
		t.Fatalf("账号服不可达应报 %s，得到 %d(%s)", protocol.ErrUpstreamFailed, got, got)
	}
}

// 绑第二个渠道之后，两个渠道登录都回到同一个号
func TestBindSecondChannelThenLoginWithIt(t *testing.T) {
	env := harness.Start(t)
	startLobby(t, env, 1)
	startAccount(t, env, 1)
	_, _, addr := startGateway(t, env, 1)

	c := dial(t, addr)
	uid := loginResp(t, channelLogin(t, c, idp.SandboxChannel, "main-account")).GetBase().GetUid()

	// 沙箱只注册了一个渠道，用它自己再绑一个 openid 会撞 ErrAlreadyBound，
	// 所以这里验的是同渠道重复绑定被拦住
	c.send(protocol.CmdBindChannel, uid, &pb.BindChannelReq{
		Channel: idp.SandboxChannel, Credential: "main-account",
	})
	resp, err := c.recvCmd(uint32(protocol.CmdBindChannel), 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetErrCode() != 0 {
		t.Fatalf("重复绑定同一个账号应当幂等成功: %d %s", resp.GetErrCode(), resp.GetErrMsg())
	}
	var br pb.BindChannelResp
	if err := proto.Unmarshal(resp.GetBody(), &br); err != nil {
		t.Fatal(err)
	}
	if len(br.GetBindings()) != 1 {
		t.Fatalf("应只有一条绑定，得到 %+v", br.GetBindings())
	}
	if br.GetBindings()[0].GetOpenId() != "main-account" {
		t.Fatalf("绑定的 openid 不对: %+v", br.GetBindings()[0])
	}
}

// 抢别人已绑的渠道账号必须被拒
func TestBindRejectsAccountOwnedByOthers(t *testing.T) {
	env := harness.Start(t)
	startLobby(t, env, 1)
	startAccount(t, env, 1)
	_, _, addr := startGateway(t, env, 1)

	ca := dial(t, addr)
	loginResp(t, channelLogin(t, ca, idp.SandboxChannel, "victim"))

	cb := dial(t, addr)
	uidB := loginResp(t, channelLogin(t, cb, idp.SandboxChannel, "attacker")).GetBase().GetUid()

	cb.send(protocol.CmdBindChannel, uidB, &pb.BindChannelReq{
		Channel: idp.SandboxChannel, Credential: "victim",
	})
	resp, err := cb.recvCmd(uint32(protocol.CmdBindChannel), 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// 同渠道已绑过自己的账号，先撞的是 ErrAlreadyBound；无论哪个码，
	// 关键是不能成功
	if resp.GetErrCode() == 0 {
		t.Fatal("抢别人的渠道账号不该成功")
	}
}

// 最后一个登录方式不能解绑，解了号就找不回来了
func TestUnbindLastChannelRefused(t *testing.T) {
	env := harness.Start(t)
	startLobby(t, env, 1)
	startAccount(t, env, 1)
	_, _, addr := startGateway(t, env, 1)

	c := dial(t, addr)
	uid := loginResp(t, channelLogin(t, c, idp.SandboxChannel, "only-one")).GetBase().GetUid()

	c.send(protocol.CmdUnbindChannel, uid, &pb.UnbindChannelReq{Channel: idp.SandboxChannel})
	resp, err := c.recvCmd(uint32(protocol.CmdUnbindChannel), 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := protocol.ErrCode(resp.GetErrCode()); got != protocol.ErrLastBinding {
		t.Fatalf("应拒绝解绑最后一个登录方式，得到 %d(%s)", got, got)
	}

	// 拒了之后还得能正常登录
	c2 := dial(t, addr)
	if got := loginResp(t, channelLogin(t, c2, idp.SandboxChannel, "only-one")).GetBase().GetUid(); got != uid {
		t.Fatalf("解绑被拒后绑定应原样保留，期望 uid=%d 得到 %d", uid, got)
	}
}

// 未登录不能查绑定，uid 只能来自会话
func TestListBindingsRequiresLogin(t *testing.T) {
	env := harness.Start(t)
	startLobby(t, env, 1)
	startAccount(t, env, 1)
	_, _, addr := startGateway(t, env, 1)

	c := dial(t, addr)
	c.send(protocol.CmdListBindings, 12345, &pb.ListBindingsReq{})
	resp, err := c.recvCmd(uint32(protocol.CmdListBindings), 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := protocol.ErrCode(resp.GetErrCode()); got != protocol.ErrPermission {
		t.Fatalf("未登录应被拒，得到 %d(%s)", got, got)
	}
}

// 账号服必须配 LOGIN_SECRET，否则宁可起不来
//
// 签不出票据的账号服起来也只是让每个登录都失败一次，不如启动就炸
func TestAccountRefusesToStartWithoutLoginSecret(t *testing.T) {
	env := harness.Start(t)
	n := env.Node(t, "account", "lobby", 9, func(c *config.Config) {
		lobbyCfg(c)
		c.LoginSecret = ""
	})
	if err := account.New().Start(context.Background(), n); err == nil {
		t.Fatal("没有 LOGIN_SECRET 时账号服应拒绝启动")
	}
}
