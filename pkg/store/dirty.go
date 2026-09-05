package store

import "time"

// DirtySet 是分片内的脏标记表。
//
// 非并发安全 —— 它只被所属分片的 Actor goroutine 访问，这正是内存态设计的收益：
// 无锁、无竞态（§4.4）。
type DirtySet struct {
	// id -> module -> 最早标脏时刻
	entries map[uint64]map[Module]time.Time
}

// NewDirtySet 构造脏标记表。
func NewDirtySet() *DirtySet {
	return &DirtySet{entries: make(map[uint64]map[Module]time.Time)}
}

// Mark 标记某实体的某个模块为脏。重复标记保留最早时刻，
// 这样「脏数据滞留时长」指标反映的才是真实丢失窗口。
func (d *DirtySet) Mark(id uint64, m Module) {
	mods, ok := d.entries[id]
	if !ok {
		mods = make(map[Module]time.Time, 2)
		d.entries[id] = mods
	}
	if _, exists := mods[m]; !exists {
		mods[m] = time.Now()
	}
}

// MarkAll 标记实体的全部模块。
func (d *DirtySet) MarkAll(id uint64) {
	for _, m := range AllModules {
		d.Mark(id, m)
	}
}

// Clear 清除某实体的全部脏标记（卸载时用）。
func (d *DirtySet) Clear(id uint64) { delete(d.entries, id) }

// IsDirty 报告某实体是否有未落盘的修改。
func (d *DirtySet) IsDirty(id uint64) bool {
	mods, ok := d.entries[id]
	return ok && len(mods) > 0
}

// Len 返回有脏数据的实体数。
func (d *DirtySet) Len() int { return len(d.entries) }

// Item 是一次取出的脏项。
type Item struct {
	ID      uint64
	Modules []Module
	DirtyAt time.Time // 该实体最早的标脏时刻
}

// Take 取出并清除指定级别的脏项，最多 limit 个实体（limit<=0 表示不限）。
//
// 「遍历 dirty 序列化 → 清 dirty → batch 提交 → 立即返回」（§6.3）。
// 清了 dirty 之后如果刷盘失败，必须通过 ReMark 重新标脏。
func (d *DirtySet) Take(level Level, limit int) []Item {
	var out []Item
	for id, mods := range d.entries {
		if limit > 0 && len(out) >= limit {
			break
		}
		var picked []Module
		earliest := time.Time{}
		for m, at := range mods {
			if ModuleLevel(m) != level {
				continue
			}
			picked = append(picked, m)
			if earliest.IsZero() || at.Before(earliest) {
				earliest = at
			}
		}
		if len(picked) == 0 {
			continue
		}
		for _, m := range picked {
			delete(mods, m)
		}
		if len(mods) == 0 {
			delete(d.entries, id)
		}
		out = append(out, Item{ID: id, Modules: picked, DirtyAt: earliest})
	}
	return out
}

// TakeAll 取出全部脏项，不分级别（全量刷盘：handoff / 优雅下线 / 卸载）。
func (d *DirtySet) TakeAll() []Item {
	out := make([]Item, 0, len(d.entries))
	for id, mods := range d.entries {
		if len(mods) == 0 {
			continue
		}
		picked := make([]Module, 0, len(mods))
		earliest := time.Time{}
		for m, at := range mods {
			picked = append(picked, m)
			if earliest.IsZero() || at.Before(earliest) {
				earliest = at
			}
		}
		out = append(out, Item{ID: id, Modules: picked, DirtyAt: earliest})
	}
	d.entries = make(map[uint64]map[Module]time.Time)
	return out
}

// TakeOne 取出单个实体的全部脏项（玩家卸载时的最终刷盘）。
func (d *DirtySet) TakeOne(id uint64) (Item, bool) {
	mods, ok := d.entries[id]
	if !ok || len(mods) == 0 {
		return Item{}, false
	}
	picked := make([]Module, 0, len(mods))
	earliest := time.Time{}
	for m, at := range mods {
		picked = append(picked, m)
		if earliest.IsZero() || at.Before(earliest) {
			earliest = at
		}
	}
	delete(d.entries, id)
	return Item{ID: id, Modules: picked, DirtyAt: earliest}, true
}

// ReMark 把刷盘失败的实体重新标脏。
//
// 「刷盘失败重新标脏，绝不丢弃。内存仍是权威副本，Redis 不可用期间服务可继续」（§6.3）。
func (d *DirtySet) ReMark(ents []*Entity) {
	for _, e := range ents {
		if e == nil {
			continue
		}
		mods, ok := d.entries[e.ID]
		if !ok {
			mods = make(map[Module]time.Time, len(e.Modules))
			d.entries[e.ID] = mods
		}
		for _, m := range e.Modules {
			// 保留原始标脏时刻，滞留时长指标才不会被重试洗白。
			at := e.DirtyAt
			if at.IsZero() {
				at = time.Now()
			}
			if old, exists := mods[m]; !exists || at.Before(old) {
				mods[m] = at
			}
		}
	}
}

// Deadline 计算带分片偏移的下一次刷盘时刻。
//
// 「刷盘时刻打散：ticker 初始偏移 shardID % interval，
// 避免 1024 分片同秒刷造成 Redis 尖峰」（§6.3）。
func Deadline(now time.Time, shard uint32, interval time.Duration) time.Time {
	if interval <= 0 {
		return now
	}
	sec := int64(interval / time.Second)
	if sec <= 0 {
		return now.Add(interval)
	}
	offset := time.Duration(int64(shard)%sec) * time.Second
	return now.Add(offset)
}
