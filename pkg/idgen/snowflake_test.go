package idgen

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamedev/f1/pkg/ident"
)

func TestUniqueAndMonotonic(t *testing.T) {
	g, err := New(257) // s1-lobby-1
	if err != nil {
		t.Fatal(err)
	}
	const n = 200000
	seen := make(map[uint64]struct{}, n)
	var last uint64
	for i := 0; i < n; i++ {
		id, err := g.Next()
		if err != nil {
			t.Fatalf("第 %d 个 ID 生成失败: %v", i, err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("ID 重复: %d", id)
		}
		seen[id] = struct{}{}
		if id <= last && last != 0 {
			t.Fatalf("ID 非单调: %d <= %d", id, last)
		}
		last = id
	}
}

func TestConcurrentUnique(t *testing.T) {
	g, _ := New(1)
	const workers, each = 16, 5000

	var mu sync.Mutex
	seen := make(map[uint64]struct{}, workers*each)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]uint64, 0, each)
			for i := 0; i < each; i++ {
				id, err := g.Next()
				if err != nil {
					t.Error(err)
					return
				}
				local = append(local, id)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range local {
				if _, dup := seen[id]; dup {
					t.Errorf("并发下 ID 重复: %d", id)
					return
				}
				seen[id] = struct{}{}
			}
		}()
	}
	wg.Wait()
	if len(seen) != workers*each {
		t.Fatalf("期望 %d 个唯一 ID，实际 %d", workers*each, len(seen))
	}
}

func TestParseRoundTrip(t *testing.T) {
	id, _ := ident.New(1, ident.SvcRoom, 5)
	g, _ := NewFromIdentity(id)
	raw, err := g.Next()
	if err != nil {
		t.Fatal(err)
	}
	p := Parse(raw)
	if p.WorkerID != id.WorkerID() {
		t.Errorf("workerID = %d, 期望 %d", p.WorkerID, id.WorkerID())
	}
	if p.SvcType != ident.SvcRoom {
		t.Errorf("svcType = %v, 期望 room", p.SvcType)
	}
	if p.NodeSeq != 5 {
		t.Errorf("nodeSeq = %d, 期望 5", p.NodeSeq)
	}
	if d := time.Since(p.Timestamp); d > time.Minute || d < -time.Minute {
		t.Errorf("时间戳偏差过大: %v", d)
	}
}

// §3.6：回拨 <= 10ms 自旋等待追平。
func TestSmallClockRollbackSpins(t *testing.T) {
	g, _ := New(1)
	base := time.Now().UnixMilli()
	var cur atomic.Int64
	cur.Store(base)
	g.now = cur.Load

	if _, err := g.Next(); err != nil {
		t.Fatal(err)
	}
	// 回拨 5ms，随后由「真实时间」慢慢追平。
	cur.Store(base - 5)
	go func() {
		for i := 0; i <= 5; i++ {
			time.Sleep(2 * time.Millisecond)
			cur.Store(base - 5 + int64(i)*2)
		}
		cur.Store(base + 1)
	}()

	done := make(chan error, 1)
	go func() {
		_, err := g.Next()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("小回拨应自旋等待而非报错: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("小回拨自旋超时")
	}
}

// §3.6：回拨 > 10ms 必须停止发号，绝不能继续。
func TestLargeClockRollbackHalts(t *testing.T) {
	g, _ := New(1)
	base := time.Now().UnixMilli()
	cur := base
	g.now = func() int64 { return cur }

	if _, err := g.Next(); err != nil {
		t.Fatal(err)
	}

	cur = base - 5000 // 回拨 5 秒
	if _, err := g.Next(); err == nil {
		t.Fatal("大幅回拨必须停止发号")
	}
	if !g.Halted() {
		t.Fatal("大幅回拨后应处于停止状态")
	}
	// 停止期间持续拒绝。
	if _, err := g.Next(); err == nil {
		t.Fatal("停止期间不得继续发号")
	}
	// 时钟修正后自动恢复。
	cur = base + 1
	if _, err := g.Next(); err != nil {
		t.Fatalf("时钟追平后应恢复发号: %v", err)
	}
	if g.Halted() {
		t.Fatal("恢复后不应仍处于停止状态")
	}
}

// 同一毫秒内耗尽 4096 个序列号后必须等到下一毫秒，而不是回绕产生重复 ID。
func TestSequenceOverflowWaitsNextMilli(t *testing.T) {
	g, _ := New(1)
	base := time.Now().UnixMilli()
	cur := base
	calls := 0
	g.now = func() int64 {
		calls++
		// 前 5000 次调用停留在同一毫秒，之后前进。
		if calls > 5000 {
			cur = base + 1
		}
		return cur
	}

	seen := make(map[uint64]struct{}, MaxSeq+2)
	for i := 0; i <= MaxSeq+1; i++ {
		id, err := g.Next()
		if err != nil {
			t.Fatalf("第 %d 个失败: %v", i, err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("序列号回绕产生了重复 ID: %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestWorkerIDBounds(t *testing.T) {
	if _, err := New(MaxWorkerID + 1); err == nil {
		t.Error("workerID 超过 1023 应被拒绝")
	}
	if _, err := New(MaxWorkerID); err != nil {
		t.Errorf("workerID = 1023 应合法: %v", err)
	}
}

func BenchmarkNext(b *testing.B) {
	g, _ := New(257)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.Next(); err != nil {
			b.Fatal(err)
		}
	}
}
