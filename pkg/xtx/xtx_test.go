package xtx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/store"
)

func newEnv(t *testing.T) (*Manager, *store.Fencer, *store.Keys) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	keys := store.NewKeys(store.TagUID, 1024, "lobby")
	return NewManager(rdb, keys, time.Hour, time.Hour), store.NewFencer(rdb, keys, "lobby"), keys
}

func sampleRec(txid string) *Record {
	return &Record{
		TxID:      txid,
		FromUID:   1001,
		ToUID:     2002,
		FromShard: 1001 % 1024,
		ToShard:   2002 % 1024,
		Items:     []*pb.Attachment{{TplId: 5, Count: 3}},
		Reason:    "trade",
		CreatedAt: time.Now().UnixMilli(),
	}
}

// 发起方先扣除并写穿，tx 记录与扣减后的玩家数据必须在同一个 Lua 里原子落盘
func TestBeginWritesRecordAndPlayerAtomically(t *testing.T) {
	m, f, keys := newEnv(t)
	ctx := context.Background()
	rec := sampleRec("tx1")
	_ = f.RaiseEpoch(ctx, rec.FromShard, 10)

	playerKey := keys.Player(rec.FromUID, store.ModBag)
	if err := m.Begin(ctx, 10, rec, map[string][]byte{playerKey: []byte("bag-after-deduct")}); err != nil {
		t.Fatal(err)
	}

	got, err := m.Get(ctx, rec.FromShard, "tx1")
	if err != nil || got == nil {
		t.Fatalf("tx 记录未写入: %v", err)
	}
	if got.State != StatePending {
		t.Fatalf("初始状态应为 PENDING，实际 %s", got.State)
	}
	if v, _ := m.rdb.Get(ctx, playerKey).Result(); v != "bag-after-deduct" {
		t.Fatalf("扣减后的玩家数据未随 tx 一起落盘: %q", v)
	}
	if n, _ := m.PendingCount(ctx, rec.FromShard); n != 1 {
		t.Fatalf("PENDING 索引应有 1 条，实际 %d", n)
	}
}

// 重复发起同一 txid 不得重复扣减
func TestBeginIsIdempotent(t *testing.T) {
	m, f, keys := newEnv(t)
	ctx := context.Background()
	rec := sampleRec("tx1")
	_ = f.RaiseEpoch(ctx, rec.FromShard, 10)

	key := keys.Player(rec.FromUID, store.ModBag)
	if err := m.Begin(ctx, 10, rec, map[string][]byte{key: []byte("v1")}); err != nil {
		t.Fatal(err)
	}
	err := m.Begin(ctx, 10, sampleRec("tx1"), map[string][]byte{key: []byte("v2")})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("重复发起应被识别，实际 %v", err)
	}
	if v, _ := m.rdb.Get(ctx, key).Result(); v != "v1" {
		t.Fatalf("重复发起不得再次写入，实际 %q", v)
	}
}

// 接收方用 txid 幂等去重
func TestClaimIsIdempotent(t *testing.T) {
	m, f, keys := newEnv(t)
	ctx := context.Background()
	rec := sampleRec("tx1")
	_ = f.RaiseEpoch(ctx, rec.ToShard, 20)

	key := keys.Player(rec.ToUID, store.ModBag)

	applied, err := m.Claim(ctx, 20, rec, map[string][]byte{key: []byte("credited")})
	if err != nil || !applied {
		t.Fatalf("首次入账应成功: applied=%v err=%v", applied, err)
	}

	// 重投：必须识别为重复，且不再写入
	applied, err = m.Claim(ctx, 20, rec, map[string][]byte{key: []byte("credited-again")})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("重复投递必须被幂等拦下")
	}
	if v, _ := m.rdb.Get(ctx, key).Result(); v != "credited" {
		t.Fatalf("重复投递不得重复入账，实际 %q", v)
	}
}

// 入账被 fencing 拒时 done 标记要回滚，否则这笔会卡在「已处理但没入账」
func TestClaimRollsBackDoneMarkerWhenFenced(t *testing.T) {
	m, f, keys := newEnv(t)
	ctx := context.Background()
	rec := sampleRec("tx1")
	_ = f.RaiseEpoch(ctx, rec.ToShard, 100)

	key := keys.Player(rec.ToUID, store.ModBag)
	_, err := m.Claim(ctx, 50, rec, map[string][]byte{key: []byte("x")}) // 过期 epoch
	if !errors.Is(err, store.ErrFenced) {
		t.Fatalf("过期 epoch 的入账必须被拒绝，实际 %v", err)
	}
	if n, _ := m.rdb.Exists(ctx, keys.TxDone(rec.ToShard, rec.TxID)).Result(); n != 0 {
		t.Fatal("被拒绝时 done 标记必须回滚，否则新 owner 重投会被误判为重复而永久丢失这笔资产")
	}

	// 新 owner 重试应能成功入账
	applied, err := m.Claim(ctx, 100, rec, map[string][]byte{key: []byte("ok")})
	if err != nil || !applied {
		t.Fatalf("新 owner 应能入账: applied=%v err=%v", applied, err)
	}
}

func TestCompleteRemovesFromPendingIndex(t *testing.T) {
	m, f, keys := newEnv(t)
	ctx := context.Background()
	rec := sampleRec("tx1")
	_ = f.RaiseEpoch(ctx, rec.FromShard, 10)
	_ = m.Begin(ctx, 10, rec, map[string][]byte{keys.Player(rec.FromUID, store.ModBag): []byte("v")})

	if err := m.Complete(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if n, _ := m.PendingCount(ctx, rec.FromShard); n != 0 {
		t.Fatalf("完成后 PENDING 索引应为空，实际 %d", n)
	}
	got, _ := m.Get(ctx, rec.FromShard, "tx1")
	if got == nil || got.State != StateCompleted {
		t.Fatalf("状态应为 COMPLETED，实际 %+v", got)
	}
}

// 后台扫描器兜底，定期重投超时 PENDING
func TestScanPendingFindsOnlyTimedOut(t *testing.T) {
	m, f, keys := newEnv(t)
	ctx := context.Background()
	_ = f.RaiseEpoch(ctx, 1001%1024, 10)

	oldRec := sampleRec("old")
	oldRec.CreatedAt = time.Now().Add(-5 * time.Minute).UnixMilli()
	_ = m.Begin(ctx, 10, oldRec, map[string][]byte{keys.Player(1001, store.ModBag): []byte("v")})

	newRec := sampleRec("new")
	newRec.CreatedAt = time.Now().UnixMilli()
	_ = m.Begin(ctx, 10, newRec, map[string][]byte{keys.Player(1001, store.ModBag): []byte("v")})

	got, err := m.ScanPending(ctx, oldRec.FromShard, time.Now().Add(-time.Minute), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].TxID != "old" {
		t.Fatalf("只应扫出超时的那笔，实际 %+v", got)
	}
}

func TestRequeueBumpsRetries(t *testing.T) {
	m, f, keys := newEnv(t)
	ctx := context.Background()
	rec := sampleRec("tx1")
	rec.CreatedAt = time.Now().Add(-5 * time.Minute).UnixMilli()
	_ = f.RaiseEpoch(ctx, rec.FromShard, 10)
	_ = m.Begin(ctx, 10, rec, map[string][]byte{keys.Player(1001, store.ModBag): []byte("v")})

	if err := m.Requeue(ctx, rec); err != nil {
		t.Fatal(err)
	}
	// 重投后 score 被推到当前时刻，不会在同一轮被重复扫到
	got, _ := m.ScanPending(ctx, rec.FromShard, time.Now().Add(-time.Minute), 100)
	if len(got) != 0 {
		t.Fatalf("重投后不应立刻再次被扫到，实际 %+v", got)
	}
	all, _ := m.ScanPending(ctx, rec.FromShard, time.Now().Add(time.Minute), 100)
	if len(all) != 1 || all[0].Retries != 1 {
		t.Fatalf("重试计数应为 1，实际 %+v", all)
	}
}
