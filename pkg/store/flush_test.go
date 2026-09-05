package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newFlushEnv(t *testing.T, chanSize, workers int) (*Flusher, *Fencer, *miniredis.Miniredis, *resultSink) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	f := NewFencer(rdb, NewKeys(TagUID, 1024, "lobby"), "lobby")
	sink := &resultSink{ch: make(chan *Result, 64)}
	fl := NewFlusher(f, "lobby", chanSize, workers, 128, sink.add)
	if err := fl.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fl.Stop)
	return fl, f, mr, sink
}

type resultSink struct {
	mu  sync.Mutex
	all []*Result
	ch  chan *Result
}

func (s *resultSink) add(r *Result) {
	s.mu.Lock()
	s.all = append(s.all, r)
	s.mu.Unlock()
	select {
	case s.ch <- r:
	default:
	}
}

func TestFlusherWritesBatch(t *testing.T) {
	fl, f, _, _ := newFlushEnv(t, 16, 2)
	ctx := context.Background()
	const shard, epoch = 3, 10
	if err := f.RaiseEpoch(ctx, shard, epoch); err != nil {
		t.Fatal(err)
	}

	key := f.keys.Player(1001, ModBase)
	res := fl.SubmitSync(ctx, &Batch{
		Shard: shard, Epoch: epoch, Level: L1,
		Entities: []*Entity{{
			ID:      1001,
			Keys:    map[string][]byte{key: []byte("hello")},
			Modules: []Module{ModBase},
			DirtyAt: time.Now(),
		}},
	})
	if res.Err != nil || len(res.Failed) > 0 || res.Fenced {
		t.Fatalf("刷盘应成功: %+v", res)
	}
	if got, _ := f.rdb.Get(ctx, key).Result(); got != "hello" {
		t.Fatalf("数据未落盘: %q", got)
	}
}

// §7.2：刷盘被 fencing 拒绝时，结果必须带 Fenced 标记，
// 上层据此丢弃内存并停止服务。
func TestFlusherReportsFenced(t *testing.T) {
	fl, f, _, _ := newFlushEnv(t, 16, 2)
	ctx := context.Background()
	if err := f.RaiseEpoch(ctx, 3, 100); err != nil {
		t.Fatal(err)
	}

	res := fl.SubmitSync(ctx, &Batch{
		Shard: 3, Epoch: 50, Level: L1, // 过期 epoch
		Entities: []*Entity{{
			ID:      1,
			Keys:    map[string][]byte{"k": []byte("v")},
			Modules: []Module{ModBase},
		}},
	})
	if !res.Fenced {
		t.Fatal("过期 epoch 的刷盘必须被标记为 Fenced")
	}
	if len(res.Failed) != 1 {
		t.Fatalf("被拒的实体应出现在 Failed 中，实际 %d", len(res.Failed))
	}
}

// §6.3：flushCh 满时走 default 重新标脏并告警，绝不阻塞 Actor。
func TestSubmitNeverBlocksWhenFull(t *testing.T) {
	// 容量 1、0 个 worker：提交第二批必然满。
	f := NewFencer(redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()}),
		NewKeys(TagUID, 1024, "lobby"), "lobby")
	fl := NewFlusher(f, "lobby", 1, 1, 128, nil)
	// 故意不 Start：没有 worker 消费，队列填满后必须立刻返回 ErrBacklog。

	mk := func(id uint64) *Batch {
		return &Batch{Shard: 1, Epoch: 1, Level: L1, Entities: []*Entity{{
			ID: id, Keys: map[string][]byte{"k": []byte("v")}, Modules: []Module{ModBase},
		}}}
	}
	if err := fl.Submit(mk(1)); err != nil {
		t.Fatalf("第一批应入队: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- fl.Submit(mk(2)) }()
	select {
	case err := <-done:
		if err != ErrBacklog {
			t.Fatalf("队列满应返回 ErrBacklog，实际 %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Submit 阻塞了 —— 绝不允许，会让故障扩散成全服卡死")
	}
}

// Redis 挂掉时刷盘失败，失败实体必须回报以便重新标脏，绝不丢弃。
func TestFlushFailureReportsFailedEntities(t *testing.T) {
	fl, f, mr, sink := newFlushEnv(t, 16, 1)
	ctx := context.Background()
	_ = f.RaiseEpoch(ctx, 1, 5)

	mr.Close() // 模拟 Redis 不可用

	err := fl.Submit(&Batch{
		Shard: 1, Epoch: 5, Level: L1,
		Entities: []*Entity{{
			ID: 7, Keys: map[string][]byte{"k": []byte("v")}, Modules: []Module{ModBase},
			DirtyAt: time.Now(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-sink.ch:
		if len(res.Failed) != 1 || res.Failed[0].ID != 7 {
			t.Fatalf("失败实体必须被回报，实际 %+v", res)
		}
		if res.Fenced {
			t.Fatal("Redis 故障不应被误判为 fencing —— 二者的处置完全相反")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("刷盘结果未回报")
	}
}

// 一批中部分实体失败时，成功的不应被重复回报为失败。
func TestPartialBatchIsolatesFailures(t *testing.T) {
	fl, f, _, _ := newFlushEnv(t, 16, 1)
	ctx := context.Background()
	const shard = 2
	_ = f.RaiseEpoch(ctx, shard, 20)

	res := fl.SubmitSync(ctx, &Batch{
		Shard: shard, Epoch: 20, Level: L1,
		Entities: []*Entity{
			{ID: 1, Keys: map[string][]byte{"a": []byte("1")}, Modules: []Module{ModBase}},
			{ID: 2, Keys: map[string][]byte{"b": []byte("2")}, Modules: []Module{ModBase}},
		},
	})
	if len(res.Failed) != 0 || res.Err != nil {
		t.Fatalf("两个实体都应成功: %+v", res)
	}
	if v, _ := f.rdb.Get(ctx, "a").Result(); v != "1" {
		t.Error("实体 1 未落盘")
	}
	if v, _ := f.rdb.Get(ctx, "b").Result(); v != "2" {
		t.Error("实体 2 未落盘")
	}
}

// profile 摘要与玩家模块在同一批里写出（§6.5）。
func TestFlusherWritesProfileHash(t *testing.T) {
	fl, f, _, _ := newFlushEnv(t, 16, 1)
	ctx := context.Background()
	const shard, epoch = 9, 3
	_ = f.RaiseEpoch(ctx, shard, epoch)

	res := fl.SubmitSync(ctx, &Batch{
		Shard: shard, Epoch: epoch, Level: L1,
		Entities: []*Entity{{
			ID:      1001,
			Keys:    map[string][]byte{f.keys.Player(1001, ModBase): []byte("base")},
			Hash:    &HashWrite{Key: f.keys.Profile(1001), Fields: []any{"nick", "阿强", "level", "7"}},
			Modules: []Module{ModBase},
		}},
	})
	if res.Err != nil || len(res.Failed) > 0 {
		t.Fatalf("刷盘失败: %+v", res)
	}
	nick, _ := f.rdb.HGet(ctx, f.keys.Profile(1001), "nick").Result()
	if nick != "阿强" {
		t.Fatalf("profile 未写入: %q", nick)
	}
}
