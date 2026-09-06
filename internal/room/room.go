// Package room 房间和战斗状态，分片独占
//
// 房间按 roomID 取模分片，和 Lobby 用的是两套独立的分片空间
package room

import (
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
)

// Room 一个房间的内存状态
type Room struct {
	ID      uint64
	Mode    uint32
	State   uint32
	Members []uint64
	MaxN    uint32

	CreatedAt time.Time
	StartedAt time.Time
	Frame     uint32

	// 战斗中间态，不刷盘，丢了就丢了
	scores  map[uint64]int64
	inputs  []*pb.RoomOpReq
	settled bool
}

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

func (r *Room) Has(uid uint64) bool {
	for _, m := range r.Members {
		if m == uid {
			return true
		}
	}
	return false
}

func (r *Room) Full() bool { return uint32(len(r.Members)) >= r.MaxN }

func (r *Room) Empty() bool { return len(r.Members) == 0 }

func (r *Room) Start(now time.Time) bool {
	if r.State != protocol.RoomStateWaiting || len(r.Members) == 0 {
		return false
	}
	r.State = protocol.RoomStateBattling
	r.StartedAt = now
	r.Frame = 0
	return true
}

// AddScore 加分，属于临时数据
func (r *Room) AddScore(uid uint64, delta int64) int64 {
	if r.scores == nil {
		r.scores = make(map[uint64]int64, r.MaxN)
	}
	r.scores[uid] += delta
	return r.scores[uid]
}

func (r *Room) Score(uid uint64) int64 { return r.scores[uid] }

// Result 出战斗结果，分最高的赢
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

// Snapshot 生成可落盘的房间快照
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

func (r *Room) Marshal() ([]byte, error) { return proto.Marshal(r.Snapshot()) }

// FromSnapshot 从快照还原房间
//
// 战斗中间态不落盘，所以接管后一律回到等待状态重新开局
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

// BattleTimeout 单场战斗的上限，超时强制结算
const BattleTimeout = 5 * time.Minute

// IdleTimeout 空房间多久回收
const IdleTimeout = 2 * time.Minute
