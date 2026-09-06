package room

import (
	"time"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
)

func (s *Shard) handleCreate(m *bus.Msg) {
	var req pb.CreateRoomReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	id := req.GetRoomId()
	if id == 0 {
		_ = m.RespondErr(protocol.ErrBadRequest, "缺少 room_id：房间 ID 必须由调用方生成，分片由它决定")
		return
	}
	if s.rs.ShardOf(id) != s.o.Shard {
		_ = m.RespondErr(protocol.ErrBadRequest, "room_id %d 不属于分片 %d", id, s.o.Shard)
		return
	}
	if _, ok := s.rooms[id]; ok {
		// 重复创建同一个房间直接返回成功
		_ = m.Respond(&pb.CreateRoomResp{RoomId: id})
		return
	}

	r := NewRoom(id, req.GetMode(), req.GetMaxPlayer(), req.GetMembers(), time.Now())
	s.rooms[id] = r
	s.mark(id)
	s.indexRoom(id)
	metrics.RoomsResident.Inc()

	logx.Trace(m.Env.GetTraceId()).Info("房间创建",
		"room", id, "shard", s.o.Shard, "mode", r.Mode, "members", len(r.Members))

	_ = m.Respond(&pb.CreateRoomResp{RoomId: id})

	if len(r.Members) > 0 {
		s.rs.broadcastRoom(r, protocol.PushRoomEvent, r.Snapshot(), m.Env.GetTraceId())
	}
}

func (s *Shard) handleJoin(m *bus.Msg) {
	var req pb.JoinRoomReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	r, ok := s.rooms[req.GetRoomId()]
	if !ok {
		_ = m.RespondErr(protocol.ErrRoomNotFound, "房间不存在")
		return
	}
	uid := req.GetUid()
	if uid == 0 {
		uid = m.Env.GetUid()
	}
	if r.Has(uid) {
		_ = m.Respond(&pb.RoomSnapshot{RoomId: r.ID, Mode: r.Mode, State: r.State, Members: r.Members})
		return
	}
	if !r.Join(uid) {
		_ = m.RespondErr(protocol.ErrRoomFull, "房间已满（%d/%d）", len(r.Members), r.MaxN)
		return
	}
	s.mark(r.ID)
	_ = m.Respond(r.Snapshot())
	s.rs.broadcastRoom(r, protocol.PushRoomEvent, r.Snapshot(), m.Env.GetTraceId())
}

func (s *Shard) handleLeave(m *bus.Msg) {
	var req pb.LeaveRoomReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	r, ok := s.rooms[req.GetRoomId()]
	if !ok {
		_ = m.RespondErr(protocol.ErrRoomNotFound, "房间不存在")
		return
	}
	uid := req.GetUid()
	if uid == 0 {
		uid = m.Env.GetUid()
	}
	if !r.Leave(uid) {
		_ = m.RespondErr(protocol.ErrNotFound, "玩家不在房间中")
		return
	}
	s.mark(r.ID)
	_ = m.Respond(&pb.Ack{Ok: true})

	if r.Empty() {
		s.closeRoom(r.ID)
		return
	}
	s.rs.broadcastRoom(r, protocol.PushRoomEvent, r.Snapshot(), m.Env.GetTraceId())
}

func (s *Shard) handleStart(m *bus.Msg) {
	var req pb.RoomOpReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	r, ok := s.rooms[req.GetRoomId()]
	if !ok {
		_ = m.RespondErr(protocol.ErrRoomNotFound, "房间不存在")
		return
	}
	if !r.Start(time.Now()) {
		_ = m.RespondErr(protocol.ErrBadRequest, "房间状态不允许开始（state=%d, members=%d）", r.State, len(r.Members))
		return
	}
	s.mark(r.ID)
	_ = m.Respond(&pb.Ack{Ok: true})
	s.rs.broadcastRoom(r, protocol.PushRoomEvent, r.Snapshot(), m.Env.GetTraceId())
}

// handleOp 处理战斗内操作。这些是临时数据，丢了下一帧就覆盖，走 Core NATS 就行
func (s *Shard) handleOp(m *bus.Msg) {
	var req pb.RoomOpReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	r, ok := s.rooms[req.GetRoomId()]
	if !ok {
		_ = m.RespondErr(protocol.ErrRoomNotFound, "房间不存在")
		return
	}
	uid := req.GetUid()
	if uid == 0 {
		uid = m.Env.GetUid()
	}
	if !r.Has(uid) {
		_ = m.RespondErr(protocol.ErrPermission, "不在房间中")
		return
	}
	if r.State != protocol.RoomStateBattling {
		_ = m.RespondErr(protocol.ErrBadRequest, "战斗未开始")
		return
	}

	switch req.GetOp() {
	case OpScore:
		r.AddScore(uid, int64(len(req.GetPayload()))+1)
	case OpFinish:
		s.settle(r)
	}

	_ = m.Respond(&pb.Ack{Ok: true})

	if req.GetOp() != OpFinish {
		s.rs.broadcastRoom(r, protocol.PushRoomEvent, &pb.RoomBroadcast{
			RoomId:  r.ID,
			Cmd:     req.GetOp(),
			Payload: req.GetPayload(),
		}, m.Env.GetTraceId())
	}
}

// 战斗操作码
const (
	OpScore  uint32 = 1
	OpFinish uint32 = 2
	// OpChat Chat 转进来的，由房间分片聚合后广播
	OpChat uint32 = 3
)

func (s *Shard) handleInfo(m *bus.Msg) {
	var req pb.RoomOpReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	r, ok := s.rooms[req.GetRoomId()]
	if !ok {
		_ = m.RespondErr(protocol.ErrRoomNotFound, "房间不存在")
		return
	}
	_ = m.Respond(r.Snapshot())
}

// settle 结算战斗：广播结果，再把发奖投给每位成员的 Lobby 分片
func (s *Shard) settle(r *Room) {
	if r.settled || r.State == protocol.RoomStateSettling {
		return
	}
	r.settled = true
	r.State = protocol.RoomStateSettling
	s.mark(r.ID)

	res := r.Result()
	logx.Info("战斗结算", "room", r.ID, "shard", s.o.Shard,
		"members", len(r.Members), "winners", len(res.GetWinners()))

	s.rs.broadcastRoom(r, protocol.PushBattleResult, res, "")

	// 发奖是资产操作，得必达，走 job.battle.settle。每位成员一条，
	// 由 Lobby 的中转层转给各自的 owner
	s.rs.publishSettle(r, res)
}
