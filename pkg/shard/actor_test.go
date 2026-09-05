package shard

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamedev/f1/pkg/bus"
)

type fakeState struct {
	mu       sync.Mutex
	handled  int
	ticks    int
	closed   ReleaseReason
	initErr  error
	initDone bool
	// goroutines 记录处理消息的 goroutine 身份，用于验证串行性。
	concurrent atomic.Int32
	maxConc    atomic.Int32
	onHandle   func()
}

func (f *fakeState) Init(ctx context.Context, o Ownership) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initDone = true
	return f.initErr
}

func (f *fakeState) Handle(m *bus.Msg) {
	n := f.concurrent.Add(1)
	if n > f.maxConc.Load() {
		f.maxConc.Store(n)
	}
	f.mu.Lock()
	f.handled++
	f.mu.Unlock()
	if f.onHandle != nil {
		f.onHandle()
	}
	f.concurrent.Add(-1)
}

func (f *fakeState) Tick(now time.Time) {
	f.mu.Lock()
	f.ticks++
	f.mu.Unlock()
}

func (f *fakeState) Close(reason ReleaseReason) {
	f.mu.Lock()
	f.closed = reason
	f.mu.Unlock()
}

func (f *fakeState) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handled, f.ticks
}

func TestShardOf(t *testing.T) {
	// §4.1：lobbyShard(uid) = uid % 1024
	for _, c := range []struct {
		id   uint64
		want uint32
	}{
		{0, 0}, {7, 7}, {1023, 1023}, {1024, 0}, {2049, 1},
	} {
		if got := Of(c.id, 1024); got != c.want {
			t.Errorf("Of(%d) = %d，期望 %d", c.id, got, c.want)
		}
	}
}

// §4.4：同一分片的处理必须严格串行 —— 这是「不存在并发扣道具类问题」的全部依据。
func TestActorSerializesHandling(t *testing.T) {
	st := &fakeState{onHandle: func() { time.Sleep(time.Millisecond) }}
	rt := NewRuntime(KindLobby, 128, 70, 50*time.Millisecond, func(o Ownership) State { return st })

	o := Ownership{Kind: KindLobby, Shard: 3, Epoch: 1}
	if err := rt.Start(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	defer rt.Stop(3, ReleaseGraceful)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rt.Do(3, func() { st.Handle(nil) })
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h, _ := st.counts(); h >= 50 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h, _ := st.counts(); h != 50 {
		t.Fatalf("处理数 = %d，期望 50", h)
	}
	if st.maxConc.Load() > 1 {
		t.Fatalf("检测到并发处理（峰值 %d）：Actor 必须单 goroutine 串行", st.maxConc.Load())
	}
}

// Init 失败必须让认领回滚。
func TestStartFailsWhenInitFails(t *testing.T) {
	st := &fakeState{initErr: errors.New("加载失败")}
	rt := NewRuntime(KindLobby, 16, 70, time.Second, func(o Ownership) State { return st })

	err := rt.Start(context.Background(), Ownership{Shard: 1, Epoch: 1})
	if err == nil {
		t.Fatal("Init 失败时 Start 必须返回错误")
	}
	if len(rt.Shards()) != 0 {
		t.Fatal("失败的分片不应留在运行时里")
	}
}

// mailbox 满时必须立即返回错误，绝不阻塞调用方（NATS 回调是单 goroutine）。
func TestMailboxFullNeverBlocks(t *testing.T) {
	block := make(chan struct{})
	st := &fakeState{onHandle: func() { <-block }}
	rt := NewRuntime(KindLobby, 2, 70, time.Hour, func(o Ownership) State { return st })

	if err := rt.Start(context.Background(), Ownership{Shard: 1, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	defer func() { close(block); rt.Stop(1, ReleaseGraceful) }()

	// 第一条被取走并卡住，随后 2 条填满 mailbox。
	for i := 0; i < 3; i++ {
		_ = rt.Do(1, func() { st.Handle(nil) })
	}

	done := make(chan error, 1)
	go func() { done <- rt.Do(1, func() {}) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrMailboxFull) {
			t.Fatalf("mailbox 满应返回 ErrMailboxFull，实际 %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("投递阻塞了 —— 会把 NATS 回调 goroutine 拖死")
	}
}

// 单条消息 panic 不应带走整个分片：那意味着一批玩家全部不可用。
func TestPanicIsIsolated(t *testing.T) {
	st := &fakeState{}
	rt := NewRuntime(KindLobby, 16, 70, time.Hour, func(o Ownership) State { return st })
	if err := rt.Start(context.Background(), Ownership{Shard: 1, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	defer rt.Stop(1, ReleaseGraceful)

	_ = rt.Do(1, func() { panic("boom") })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Call(ctx, 1, func() {}); err != nil {
		t.Fatalf("panic 之后 Actor 应继续工作: %v", err)
	}
}

func TestCallRunsInsideActor(t *testing.T) {
	st := &fakeState{}
	rt := NewRuntime(KindLobby, 16, 70, time.Hour, func(o Ownership) State { return st })
	if err := rt.Start(context.Background(), Ownership{Shard: 1, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	defer rt.Stop(1, ReleaseGraceful)

	ran := false
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.Call(ctx, 1, func() { ran = true }); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("Call 应同步等待闭包执行完成")
	}
	if err := rt.Call(ctx, 99, func() {}); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("未持有的分片应返回 ErrNotOwned，实际 %v", err)
	}
}

func TestStopPassesReason(t *testing.T) {
	st := &fakeState{}
	rt := NewRuntime(KindLobby, 16, 70, time.Hour, func(o Ownership) State { return st })
	if err := rt.Start(context.Background(), Ownership{Shard: 1, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	rt.Stop(1, ReleaseFenced)

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed != ReleaseFenced {
		t.Fatalf("Close 应收到 fenced 原因，实际 %q", st.closed)
	}
}

func TestTickFires(t *testing.T) {
	st := &fakeState{}
	rt := NewRuntime(KindLobby, 16, 70, 20*time.Millisecond, func(o Ownership) State { return st })
	if err := rt.Start(context.Background(), Ownership{Shard: 5, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	defer rt.Stop(5, ReleaseGraceful)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ticks := st.counts(); ticks >= 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, ticks := st.counts()
	t.Fatalf("Tick 未按期触发，实际 %d 次", ticks)
}
