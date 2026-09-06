package store

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestFencer(t *testing.T) (*Fencer, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewFencer(rdb, NewKeys(TagUID, 1024, "lobby"), "lobby"), mr
}

// 核心场景：
//
//	T4 节点 B 接管，抬高 epoch
//	T5 节点 A 恢复，把旧内存刷入 Redis → 必须被拒绝
func TestEpochFencingRejectsStaleOwner(t *testing.T) {
	f, _ := newTestFencer(t)
	ctx := context.Background()

	const shard = 7
	// A 先接管，epoch=100
	if err := f.RaiseEpoch(ctx, shard, 100); err != nil {
		t.Fatal(err)
	}
	// A 正常写入
	key := f.keys.Player(1001, ModBase)
	if err := f.WriteModules(ctx, shard, 100, map[string][]byte{key: []byte("A-data")}); err != nil {
		t.Fatalf("A 在持有期内写入应成功: %v", err)
	}

	// B 接管，抬高 epoch
	if err := f.RaiseEpoch(ctx, shard, 200); err != nil {
		t.Fatal(err)
	}
	if err := f.WriteModules(ctx, shard, 200, map[string][]byte{key: []byte("B-data")}); err != nil {
		t.Fatalf("B 写入应成功: %v", err)
	}

	// A 恢复后带着过期 epoch 写入，必须被拒绝，且不能覆盖 B 的数据
	err := f.WriteModules(ctx, shard, 100, map[string][]byte{key: []byte("A-stale")})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("陈旧 owner 的写入必须被 fencing 拒绝，实际: %v", err)
	}
	got, _ := f.rdb.Get(ctx, key).Result()
	if got != "B-data" {
		t.Fatalf("B 的数据被覆盖了：%q", got)
	}
}

// 接管方抬高 epoch 时，不得被更旧的 epoch 拉低
func TestRaiseEpochRejectsRegression(t *testing.T) {
	f, _ := newTestFencer(t)
	ctx := context.Background()

	if err := f.RaiseEpoch(ctx, 7, 500); err != nil {
		t.Fatal(err)
	}
	if err := f.RaiseEpoch(ctx, 7, 400); !errors.Is(err, ErrFenced) {
		t.Fatalf("更低的 epoch 不得覆盖，实际: %v", err)
	}
	cur, err := f.CurrentEpoch(ctx, 7)
	if err != nil || cur != 500 {
		t.Fatalf("epoch = %d (err=%v)，期望 500", cur, err)
	}
}

// epoch 相同（未发生接管）时写入应放行，fencing 只拒绝「更旧」的调用方
func TestEqualEpochAllowed(t *testing.T) {
	f, _ := newTestFencer(t)
	ctx := context.Background()
	_ = f.RaiseEpoch(ctx, 3, 42)
	if err := f.WriteModules(ctx, 3, 42, map[string][]byte{"k": []byte("v")}); err != nil {
		t.Fatalf("同 epoch 写入应成功: %v", err)
	}
}

// L0 必须幂等，客户端带唯一订单号，重复提交返回首次结果
func TestWriteThroughIdempotent(t *testing.T) {
	f, _ := newTestFencer(t)
	ctx := context.Background()
	const shard, epoch = 5, 10
	_ = f.RaiseEpoch(ctx, shard, epoch)

	key := f.keys.Player(2002, ModBase)
	order := f.keys.Order(2002, "order-abc")

	res, err := f.WriteThrough(ctx, shard, epoch, order, 3600, []byte("balance=100"),
		map[string][]byte{key: []byte("v1")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Duplicate {
		t.Fatal("首次提交不应判为重复")
	}

	// 重复提交：返回首次结果，且不得再次写入数据
	res2, err := f.WriteThrough(ctx, shard, epoch, order, 3600, []byte("balance=200"),
		map[string][]byte{key: []byte("v2")})
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Duplicate {
		t.Fatal("重复提交必须被识别")
	}
	if string(res2.Payload) != "balance=100" {
		t.Fatalf("重复提交应返回首次结果，实际 %q", res2.Payload)
	}
	if got, _ := f.rdb.Get(ctx, key).Result(); got != "v1" {
		t.Fatalf("重复提交不得覆盖数据，实际 %q", got)
	}
}

// 写穿同样受 fencing 保护
func TestWriteThroughFenced(t *testing.T) {
	f, _ := newTestFencer(t)
	ctx := context.Background()
	_ = f.RaiseEpoch(ctx, 5, 100)

	_, err := f.WriteThrough(ctx, 5, 50, f.keys.Order(1, "o1"), 60, []byte("x"),
		map[string][]byte{"k": []byte("v")})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("过期 epoch 的写穿必须被拒绝，实际: %v", err)
	}
}

func TestLoadPlayerMissingModules(t *testing.T) {
	f, _ := newTestFencer(t)
	ctx := context.Background()
	_ = f.RaiseEpoch(ctx, f.keys.Shard(1001), 1)

	// 只写 base，其他模块缺着，模拟新玩家或加了新模块的老玩家
	f.rdb.Set(ctx, f.keys.Player(1001, ModBase), "base-blob", 0)

	blobs, err := f.LoadPlayer(ctx, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if string(blobs[ModBase]) != "base-blob" {
		t.Fatalf("base 未读到: %q", blobs[ModBase])
	}
	if _, ok := blobs[ModBag]; ok {
		t.Fatal("缺失的模块不应出现在结果里")
	}
}

func TestDeleteFenced(t *testing.T) {
	f, _ := newTestFencer(t)
	ctx := context.Background()
	_ = f.RaiseEpoch(ctx, 1, 100)
	f.rdb.Set(ctx, "victim", "x", 0)

	if err := f.Delete(ctx, 1, 50, "victim"); !errors.Is(err, ErrFenced) {
		t.Fatalf("过期 epoch 的删除必须被拒绝，实际: %v", err)
	}
	if n, _ := f.rdb.Exists(ctx, "victim").Result(); n != 1 {
		t.Fatal("被拒绝的删除不应真的删掉键")
	}
	if err := f.Delete(ctx, 1, 100, "victim"); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.rdb.Exists(ctx, "victim").Result(); n != 0 {
		t.Fatal("合法删除未生效")
	}
}
