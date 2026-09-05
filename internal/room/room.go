// Package room 实现 Room 服务：房间/战斗状态，分片独占（§2.1）。
//
// 房间按 roomShard(roomID) = roomID % 1024 分片，与 Lobby 使用独立分片空间（§4.1）。
package room

import (
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
)

// Room 是一个房间的内存状态。
type Room struct {
	ID      uint64
	Mode    uint32
	State   uint32
	Members []uint64
	MaxN    uint32

	CreatedAt time.Time
	StartedAt time.Time
	Frame     uint32

	// —— L3 临时数据：战斗中间态，不刷盘，全部丢失可接受（§6.2）——
	scores  map[uint64]int64
	inputs  []*pb.RoomOpReq
	settled bool
}

// NewRoom 创建房间。
func NewRoom(id uint64, mode, maxN uint32, members []uint64, now time.Time) *Room {
	if maxN == 0 {
		maxN = 8
	}
	r := &Room{
		ID:        id,
		Mode:      mode,
		MaxN:      maxN,
		State:     protocol.RoomStateWaiting,
		CreatedAt: now,
		scores:    make(map[uint64]int64, maxN),
	}
	for _, uid := range members {
		r.Join(uid)
	}
	return r
}

// Join 加入房间。
func (r *Room) Join(uid uint64) bool {
	if uid == 0 || r.Has(uid) {
		return false
	}
	if uint32(len(r.Members)) >= r.MaxN {
		return false
	}
	r.Members = append(r.Members, uid)
	if r.scores == nil {
		r.scores = make(map[uint64]int64, r.MaxN)
	}
	r.scores[uid] = 0
	return true
}

// Leave 离开房间。
func (r *Room) Leave(uid uint64) bool {
	for i, m := range r.Members {
		if m == uid {
			r.Members = append(r.Members[:i], r.Members[i+1:]...)
			delete(r.scores, uid)
			return true
		}
	}
	return false
}

// Has 报告玩家是否在房间中。
func (r *Room) Has(uid uint64) bool {
	for _, m := range r.Members {
		if m == uid {
			return true
		}
	}
	return false
}

// Full 报告房间是否已满。
func (r *Room) Full() bool { return uint32(len(r.Members)) >= r.MaxN }

// Empty 报告房间是否已空。
func (r *Room) Empty() bool { return len(r.Members) == 0 }

// Start 开始战斗。
func (r *Room) Start(now time.Time) bool {
	if r.State != protocol.RoomStateWaiting || len(r.Members) == 0 {
		return false
	}
	r.State = protocol.RoomStateBattling
	r.StartedAt = now
	r.Frame = 0
	return true
}

// AddScore 累加战斗分数（L3 临时数据）。
func (r *Room) AddScore(uid uint64, delta int64) int64 {
	if r.scores == nil {
		r.scores = make(map[uint64]int64, r.MaxN)
	}
	r.scores[uid] += delta
	return r.scores[uid]
}

// Score 返回分数。
func (r *Room) Score(uid uint64) int64 { return r.scores[uid] }

// Result 生成战斗结果：分数最高者为胜者。
func (r *Room) Result() *pb.BattleResult {
	res := &pb.BattleResult{RoomId: r.ID, Score: make(map[uint64]int64, len(r.Members))}
	var best int64
	first := true
	for _, uid := range r.Members {
		sc := r.scores[uid]
		res.Score[uid] = sc
		if first || sc > best {
			best, first = sc, false
		}
	}
	for _, uid := range r.Members {
		if r.scores[uid] == best {
			res.Winners = append(res.Winners, uid)
		} else {
			res.Losers = append(res.Losers, uid)
		}
	}
	return res
}

// Snapshot 生成可落盘的房间快照。
func (r *Room) Snapshot() *pb.RoomSnapshot {
	snap := &pb.RoomSnapshot{
		RoomId:  r.ID,
		Mode:    r.Mode,
		State:   r.State,
		Members: append([]uint64(nil), r.Members...),
		Frame:   r.Frame,
	}
	if !r.CreatedAt.IsZero() {
		snap.CreatedAt = r.CreatedAt.UnixMilli()
	}
	if !r.StartedAt.IsZero() {
		snap.StartedAt = r.StartedAt.UnixMilli()
	}
	return snap
}

// Marshal 序列化快照。
func (r *Room) Marshal() ([]byte, error) { return proto.Marshal(r.Snapshot()) }

// FromSnapshot 由快照还原房间。
//
// 战斗中间态（L3）不落盘，因此接管后一律回退到「等待」状态重新开局 ——
// 这正是 L3 的语义：全部丢失可接受。
func FromSnapshot(snap *pb.RoomSnapshot) *Room {
	r := &Room{
		ID:      snap.GetRoomId(),
		Mode:    snap.GetMode(),
		State:   snap.GetState(),
		Members: snap.GetMembers(),
		MaxN:    8,
		Frame:   snap.GetFrame(),
		scores:  make(map[uint64]int64, 8),
	}
	if uint32(len(r.Members)) > r.MaxN {
		r.MaxN = uint32(len(r.Members))
	}
	if snap.GetCreatedAt() > 0 {
		r.CreatedAt = time.UnixMilli(snap.GetCreatedAt())
	}
	if snap.GetStartedAt() > 0 {
		r.StartedAt = time.UnixMilli(snap.GetStartedAt())
	}
	if r.State == protocol.RoomStateBattling {
		r.State = protocol.RoomStateWaiting
		r.Frame = 0
	}
	return r
}

// BattleTimeout 是单场战斗的最长时长，超时强制结算。
const BattleTimeout = 5 * time.Minute

// IdleTimeout 是空房间的回收时限。
const IdleTimeout = 2 * time.Minute
