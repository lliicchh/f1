package integration

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/internal/chat"
	"github.com/gamedev/f1/internal/match"
	"github.com/gamedev/f1/internal/room"
	"github.com/gamedev/f1/internal/world"
	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/subject"
	"github.com/gamedev/f1/test/harness"
)

func startService(t *testing.T, env *harness.Env, svcName, kind string, seq int, svc node.Service, tweak func(*config.Config)) *node.Node {
	t.Helper()
	n := env.Node(t, svcName, kind, seq, tweak)
	if err := svc.Start(context.Background(), n); err != nil {
		t.Fatalf("启动 %s 失败: %v", svcName, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		svc.StopAccepting(ctx)
		_ = svc.FlushAll(ctx)
		svc.Close(ctx)
	})
	return n
}

// 全链路：匹配 → 建房 → 开打 → 结算 → 发奖回到 Lobby。
//
// 这条路径同时覆盖了：
//   - 两套独立分片空间（Lobby 按 uid、Room 按 roomID）
//   - 按 gateID 聚合的定向推送（§9.3）
//   - 发奖走 JetStream 必达任务（§5.2）
func TestFullStackMatchBattleSettle(t *testing.T) {
	env := harness.Start(t)

	lobbyNode, lsvc := startLobby(t, env, 1)
	rsvc := room.New()
	startService(t, env, "room", "room", 1, rsvc, lobbyCfg)
	msvc := match.New()
	startService(t, env, "match", "match", 1, msvc, lobbyCfg)
	_, _, addr := startGateway(t, env, 1)

	// Room 必须先认领完分片，否则匹配成功后建房会打空；
	// 匹配桶同理，入队请求发过去得有人接。
	harness.Eventually(t, 30*time.Second, "Room 与 Match 就绪", func() bool {
		return len(rsvc.OwnedShards()) == testShards && msvc.OwnsBucket(1, 0)
	})

	const uidA, uidB = uint64(11001), uint64(11002)
	harness.Eventually(t, 20*time.Second, "Lobby 认领两名玩家的分片", func() bool {
		return lsvc.Owns(uidA) && lsvc.Owns(uidB)
	})

	ca, cb := dial(t, addr), dial(t, addr)
	loginVia(t, ca, uidA)
	loginVia(t, cb, uidB)

	// 记录战前金币。
	beforeA := goldOf(t, lobbyNode, uidA)

	// 两人入队同一模式与段位（mode=1 需要 2 人成队）。
	ca.send(protocol.CmdMatchEnqueue, uidA, &pb.MatchEnqueueReq{Uid: uidA, Mode: 1, Tier: 0, Power: 1000})
	if _, err := ca.recvCmd(uint32(protocol.CmdMatchEnqueue), 10*time.Second); err != nil {
		t.Fatalf("A 入队失败: %v", err)
	}
	cb.send(protocol.CmdMatchEnqueue, uidB, &pb.MatchEnqueueReq{Uid: uidB, Mode: 1, Tier: 0, Power: 1100})
	if _, err := cb.recvCmd(uint32(protocol.CmdMatchEnqueue), 10*time.Second); err != nil {
		t.Fatalf("B 入队失败: %v", err)
	}

	// 匹配成功推送。
	found := waitMatchFound(t, ca, 20*time.Second)
	if found.GetRoomId() == 0 {
		t.Fatal("匹配成功推送里应带 room_id")
	}
	if len(found.GetMembers()) != 2 {
		t.Fatalf("成队人数 = %d，期望 2", len(found.GetMembers()))
	}

	// 打一下再结束。
	ca.send(protocol.CmdRoomOp, uidA, &pb.RoomOpReq{RoomId: found.GetRoomId(), Uid: uidA, Op: 1, Payload: []byte("hit")})
	_, _ = ca.recvCmd(uint32(protocol.CmdRoomOp), 10*time.Second)
	ca.send(protocol.CmdRoomOp, uidA, &pb.RoomOpReq{RoomId: found.GetRoomId(), Uid: uidA, Op: 2})
	_, _ = ca.recvCmd(uint32(protocol.CmdRoomOp), 10*time.Second)

	// 发奖经 job.battle.settle → Lobby 中转层 → 各自 owner 分片入账。
	harness.Eventually(t, 30*time.Second, "战斗奖励到账", func() bool {
		return goldOf(t, lobbyNode, uidA) > beforeA
	})
}

// 世界服选主：只有一个实例会当选并对外服务。
func TestWorldLeaderElection(t *testing.T) {
	env := harness.Start(t)

	w1 := world.New()
	n1 := startService(t, env, "world", "world", 1, w1, lobbyCfg)
	w2 := world.New()
	startService(t, env, "world", "world", 2, w2, lobbyCfg)

	// 无论谁当选，req.world.* 都应该恰好被服务一次。
	harness.Eventually(t, 20*time.Second, "世界服就绪", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var st pb.WorldBossState
		err := n1.Bus.Call(ctx, "req.world."+protocol.CmdWorldBossState.Name(),
			protocol.CmdWorldBossState, 0, "", &pb.Empty{}, &st)
		return err == nil && st.GetMaxHp() > 0
	})

	// 打一刀，血量应减少 —— 说明确实有唯一一个实例在维护状态。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var hit pb.WorldBossHitResp
	if err := n1.Bus.Call(ctx, "req.world."+protocol.CmdWorldBossHit.Name(),
		protocol.CmdWorldBossHit, 1, "", &pb.WorldBossHitReq{Uid: 1, Damage: 1000}, &hit); err != nil {
		t.Fatalf("攻击世界 BOSS 失败: %v", err)
	}
	if hit.GetHpLeft() <= 0 || hit.GetKilled() {
		t.Fatalf("一刀不该秒杀: %+v", &hit)
	}
}

// 聊天中继：世界频道走全服广播，客户端能收到。
func TestChatWorldChannel(t *testing.T) {
	env := harness.Start(t)
	_, lsvc := startLobby(t, env, 1)
	startService(t, env, "chat", "lobby", 1, chat.New(), lobbyCfg)
	_, _, addr := startGateway(t, env, 1)

	const uid = uint64(12001)
	harness.Eventually(t, 20*time.Second, "Lobby 认领分片", func() bool { return lsvc.Owns(uid) })

	c := dial(t, addr)
	loginVia(t, c, uid)

	c.send(protocol.CmdChatSend, uid, &pb.ChatReq{
		Channel: protocol.ChanWorld, From: uid, Text: "大家好",
	})
	if _, err := c.recvCmd(uint32(protocol.CmdChatSend), 10*time.Second); err != nil {
		t.Fatalf("聊天应答失败: %v", err)
	}

	// 世界频道走 push.broadcast，发送者自己也会收到。
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		env, err := c.recv(time.Until(deadline))
		if err != nil {
			t.Fatalf("等待聊天推送失败: %v", err)
		}
		if env.GetCmd() != uint32(protocol.PushChat) {
			continue
		}
		// 网关下行时只带 payload 本身，MultiPush 那层聚合是服务端内部用的。
		var msg pb.ChatMsg
		if proto.Unmarshal(env.GetBody(), &msg) != nil {
			continue
		}
		if msg.GetText() != "大家好" {
			t.Fatalf("聊天内容不对: %q", msg.GetText())
		}
		return
	}
	t.Fatal("未收到世界频道聊天推送")
}

func loginVia(t *testing.T, c *client, uid uint64) {
	t.Helper()
	c.send(protocol.CmdLogin, uid, &pb.LoginReq{Uid: uid, Token: harness.Token(t, uid)})
	resp, err := c.recvCmd(uint32(protocol.CmdLogin), 15*time.Second)
	if err != nil {
		t.Fatalf("uid=%d 登录失败: %v", uid, err)
	}
	if resp.GetErrCode() != 0 {
		t.Fatalf("uid=%d 登录被拒: %s", uid, resp.GetErrMsg())
	}
}

// goldOf 通过 GM 查询读余额。
//
// 客户端已经没有「查余额」的通用接口了 —— 因为原来那个接口是 add_currency(delta=0)，
// 而它同时也能加钱。把读和写分开、并把写收回内部，正是评审 P0-1 的修复内容。
func goldOf(t *testing.T, n *node.Node, uid uint64) int64 {
	t.Helper()
	sh := shard.Of(uid, n.Cfg.ShardCount)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp pb.GMQueryResp
	if err := harness.InternalCall(ctx, t, n,
		subject.LobbyReq(sh, protocol.CmdGMQuery.Name()),
		protocol.CmdGMQuery, uid, "test-op", &pb.GMQueryReq{Uid: uid, Limit: 1}, &resp); err != nil {
		t.Fatalf("GM 查询失败: %v", err)
	}
	return resp.GetBase().GetCurrency()[uint32(protocol.CurrencyGold)]
}

func waitMatchFound(t *testing.T, c *client, timeout time.Duration) *pb.MatchFound {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		env, err := c.recv(time.Until(deadline))
		if err != nil {
			t.Fatalf("等待匹配推送失败: %v", err)
		}
		if env.GetCmd() != uint32(protocol.PushMatchFound) {
			continue
		}
		var found pb.MatchFound
		if proto.Unmarshal(env.GetBody(), &found) != nil {
			continue
		}
		if found.GetRoomId() != 0 {
			return &found
		}
	}
	t.Fatal("未收到匹配成功推送")
	return nil
}
