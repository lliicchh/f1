package store

import (
	"testing"
	"time"
)

func TestDirtyTakeByLevel(t *testing.T) {
	d := NewDirtySet()
	d.Mark(1, ModBase)   // L1
	d.Mark(1, ModSocial) // L2
	d.Mark(2, ModBag)    // L1

	l1 := d.Take(L1, 0)
	if len(l1) != 2 {
		t.Fatalf("L1 应取出 2 个实体，实际 %d", len(l1))
	}
	// L2 的脏标记不应被 L1 顺手清掉。
	if !d.IsDirty(1) {
		t.Fatal("uid=1 的 L2 标记被误清")
	}
	l2 := d.Take(L2, 0)
	if len(l2) != 1 || l2[0].ID != 1 {
		t.Fatalf("L2 应只取出 uid=1，实际 %+v", l2)
	}
	if d.Len() != 0 {
		t.Fatalf("取完后应为空，实际 %d", d.Len())
	}
}

// §6.3：刷盘失败重新标脏，绝不丢弃。
func TestReMarkPreservesOriginalDirtyTime(t *testing.T) {
	d := NewDirtySet()
	d.Mark(1, ModBase)
	items := d.Take(L1, 0)
	if len(items) != 1 {
		t.Fatal("取出失败")
	}
	original := items[0].DirtyAt

	time.Sleep(5 * time.Millisecond)
	d.ReMark([]*Entity{{ID: 1, Modules: items[0].Modules, DirtyAt: original}})

	again := d.Take(L1, 0)
	if len(again) != 1 {
		t.Fatal("重新标脏后应能再取出")
	}
	if !again[0].DirtyAt.Equal(original) {
		t.Fatal("重试不得把脏数据滞留时长洗白 —— 那会让真实丢失窗口指标失真")
	}
}

func TestMarkKeepsEarliestTime(t *testing.T) {
	d := NewDirtySet()
	d.Mark(1, ModBase)
	first := d.entries[1][ModBase]
	time.Sleep(3 * time.Millisecond)
	d.Mark(1, ModBase)
	if !d.entries[1][ModBase].Equal(first) {
		t.Fatal("重复标脏应保留最早时刻")
	}
}

func TestTakeLimit(t *testing.T) {
	d := NewDirtySet()
	for i := uint64(0); i < 10; i++ {
		d.Mark(i, ModBase)
	}
	got := d.Take(L1, 3)
	if len(got) != 3 {
		t.Fatalf("limit=3 应取出 3 个，实际 %d", len(got))
	}
	if d.Len() != 7 {
		t.Fatalf("剩余应为 7，实际 %d", d.Len())
	}
}

func TestTakeOneAndClear(t *testing.T) {
	d := NewDirtySet()
	d.Mark(1, ModBase)
	d.Mark(1, ModBag)
	d.Mark(2, ModBase)

	item, ok := d.TakeOne(1)
	if !ok || len(item.Modules) != 2 {
		t.Fatalf("TakeOne 应取出 uid=1 的 2 个模块，实际 %+v ok=%v", item, ok)
	}
	if d.IsDirty(1) {
		t.Fatal("TakeOne 后不应再脏")
	}
	d.Clear(2)
	if d.Len() != 0 {
		t.Fatal("Clear 未生效")
	}
}

// §6.3：刷盘时刻打散，避免 1024 分片同秒刷造成 Redis 尖峰。
func TestDeadlineSpreadsAcrossShards(t *testing.T) {
	now := time.Now()
	interval := 5 * time.Second
	offsets := make(map[time.Duration]int)
	for sh := uint32(0); sh < 1024; sh++ {
		offsets[Deadline(now, sh, interval).Sub(now)]++
	}
	if len(offsets) != 5 {
		t.Fatalf("5s 间隔应打散到 5 个秒级偏移，实际 %d 个", len(offsets))
	}
	for off, n := range offsets {
		if off < 0 || off >= interval {
			t.Errorf("偏移 %v 超出间隔范围", off)
		}
		if n == 0 {
			t.Errorf("偏移 %v 没有分片落入", off)
		}
	}
}

func TestModuleLevels(t *testing.T) {
	// §6.2：货币/等级/背包属 L1；设置/社交属 L2。
	for _, m := range []Module{ModBase, ModBag, ModQuest} {
		if ModuleLevel(m) != L1 {
			t.Errorf("%s 应为 L1", m)
		}
	}
	for _, m := range []Module{ModSocial, ModMail} {
		if ModuleLevel(m) != L2 {
			t.Errorf("%s 应为 L2", m)
		}
	}
}
