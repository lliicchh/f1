package session

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/store"
)

func newStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStore(rdb, store.NewKeys(store.TagUID, 1024, "lobby"), time.Minute), mr
}

// §9.2：Lua 脚本原子替换 session，取出旧 gateID 后发 KICK。
func TestBindReturnsKickedSession(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	old, err := s.Bind(ctx, 1001, "s1-gateway-1", 111)
	if err != nil {
		t.Fatal(err)
	}
	if !old.Empty() {
		t.Fatalf("首次登录不应有旧会话: %+v", old)
	}

	old, err = s.Bind(ctx, 1001, "s1-gateway-2", 222)
	if err != nil {
		t.Fatal(err)
	}
	if old.GateID != "s1-gateway-1" || old.ConnID != 111 {
		t.Fatalf("顶号应返回旧会话，实际 %+v", old)
	}

	cur, _ := s.Get(ctx, 1001)
	if cur.GateID != "s1-gateway-2" || cur.ConnID != 222 {
		t.Fatalf("会话未被替换: %+v", cur)
	}
}

// 顶号后旧连接的清理会迟到，绝不能把新会话删掉。
func TestUnbindOnlyMatchingConn(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	_, _ = s.Bind(ctx, 1001, "gw1", 111)
	_, _ = s.Bind(ctx, 1001, "gw2", 222)

	// 旧连接迟到的解绑。
	removed, err := s.Unbind(ctx, 1001, "gw1", 111)
	if err != nil {
		t.Fatal(err)
	}
	if removed {
		t.Fatal("旧连接的解绑不应删除新会话")
	}
	cur, _ := s.Get(ctx, 1001)
	if cur.GateID != "gw2" {
		t.Fatalf("新会话被误删: %+v", cur)
	}

	// 当前连接的解绑正常生效。
	removed, _ = s.Unbind(ctx, 1001, "gw2", 222)
	if !removed {
		t.Fatal("当前连接的解绑应生效")
	}
	if cur, _ := s.Get(ctx, 1001); !cur.Empty() {
		t.Fatalf("会话应已删除: %+v", cur)
	}
}

// 心跳续期同样要求 gate/conn 匹配：被顶号的连接续不上，据此断开自己。
func TestTouchDetectsTakeover(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	_, _ = s.Bind(ctx, 1001, "gw1", 111)
	if ok, _ := s.Touch(ctx, 1001, "gw1", 111); !ok {
		t.Fatal("当前连接续期应成功")
	}

	_, _ = s.Bind(ctx, 1001, "gw2", 222)
	if ok, _ := s.Touch(ctx, 1001, "gw1", 111); ok {
		t.Fatal("被顶掉的连接不应续期成功")
	}
}

// §9.3：房间/公会广播由分片查路由表后按 gateID 聚合。
func TestGroupByGate(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	_, _ = s.Bind(ctx, 1, "gw1", 1)
	_, _ = s.Bind(ctx, 2, "gw1", 2)
	_, _ = s.Bind(ctx, 3, "gw2", 3)
	// uid=4 不在线。

	byGate, err := s.GroupByGate(ctx, []uint64{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(byGate) != 2 {
		t.Fatalf("应聚合成 2 个网关，实际 %d: %+v", len(byGate), byGate)
	}
	if len(byGate["gw1"]) != 2 {
		t.Fatalf("gw1 应有 2 人，实际 %v", byGate["gw1"])
	}
	if len(byGate["gw2"]) != 1 {
		t.Fatalf("gw2 应有 1 人，实际 %v", byGate["gw2"])
	}
}

// §9.4：网关启动时清理自己名下的会话；已重连到别处的玩家不能被误清。
func TestCleanGateSkipsMigratedPlayers(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	_, _ = s.Bind(ctx, 1, "gw1", 1)
	_, _ = s.Bind(ctx, 2, "gw1", 2)
	// 玩家 2 已经重连到 gw2。
	_, _ = s.Bind(ctx, 2, "gw2", 22)

	n, err := s.CleanGate(ctx, "gw1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("应只清理 1 个会话，实际 %d", n)
	}
	if cur, _ := s.Get(ctx, 1); !cur.Empty() {
		t.Fatal("玩家 1 的会话应被清理")
	}
	if cur, _ := s.Get(ctx, 2); cur.GateID != "gw2" {
		t.Fatalf("玩家 2 已迁移到 gw2，不应被清理: %+v", cur)
	}
}

// session 必须有 TTL：网关崩溃后靠它自然过期（§9.4 的第二条清理路径）。
func TestSessionHasTTL(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	_, _ = s.Bind(ctx, 1001, "gw1", 1)

	ttl := mr.TTL(s.keys.Session(1001))
	if ttl <= 0 {
		t.Fatal("session 必须设置 TTL，否则网关崩溃后会留下永久脏路由")
	}

	mr.FastForward(2 * time.Minute)
	if cur, _ := s.Get(ctx, 1001); !cur.Empty() {
		t.Fatalf("TTL 过后会话应消失: %+v", cur)
	}
}

func TestCacheInvalidation(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	c := NewCache(s, time.Minute)

	_, _ = s.Bind(ctx, 1001, "gw1", 1)
	if got, _ := c.Get(ctx, 1001); got.GateID != "gw1" {
		t.Fatal("缓存首次读取失败")
	}

	_, _ = s.Bind(ctx, 1001, "gw2", 2)
	// 未失效前仍读到旧值 —— 这正是需要事件驱动失效的原因。
	if got, _ := c.Get(ctx, 1001); got.GateID != "gw1" {
		t.Fatal("缓存应命中旧值")
	}
	c.Invalidate(1001)
	if got, _ := c.Get(ctx, 1001); got.GateID != "gw2" {
		t.Fatal("失效后应读到新值")
	}
}
