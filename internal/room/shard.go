package room

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/shardsvc"
	"github.com/gamedev/f1/pkg/store"
)

// Shard 一个 Room 分片的内存状态
type Shard struct {
	o   shard.Ownership
	svc *shardsvc.Service
	rs  *Service

	rooms map[uint64]*Room
	dirty *store.DirtySet

	nextFlush time.Time
	nextGC    time.Time
	closed    bool
}

// modRoom 借用玩家那套脏标记和刷盘设施，给房间快照当模块名
const modRoom store.Module = "snapshot"

func NewShard(o shard.Ownership, svc *shardsvc.Service, rs *Service) *Shard {
	return &Shard{
		o:     o,
		svc:   svc,
		rs:    rs,
		rooms: make(map[uint64]*Room, 128),
		dirty: store.NewDirtySet(),
	}
}

// Init 从 Redis 恢复本分片的房间
//
// 房间不能像玩家那样懒加载，它没有登录这个触发点，玩家下一条操作就要用了
func (s *Shard) Init(ctx context.Context, o shard.Ownership) error {
	now := time.Now()
	s.nextFlush = store.Deadline(now, o.Shard, s.rs.node.Cfg.FlushL1Interval)
	s.nextGC = now.Add(30 * time.Second)

	keys := s.rs.node.Keys
	rdb := s.rs.node.Redis

	ids, err := rdb.SMembers(ctx, keys.RoomIndex(o.Shard)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("读取房间索引失败: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}

	pipe := rdb.Pipeline()
	cmds := make(map[string]*redis.StringCmd, len(ids))
	for _, raw := range ids {
		var id uint64
		if _, perr := fmt.Sscanf(raw, "%d", &id); perr != nil {
			continue
		}
		cmds[raw] = pipe.Get(ctx, keys.Room(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("批量读取房间快照失败: %w", err)
	}

	var stale []any
	for raw, cmd := range cmds {
		blob, err := cmd.Bytes()
		if errors.Is(err, redis.Nil) {
			stale = append(stale, raw) // 索引里有但快照没了，清掉
			continue
		}
		if err != nil {
			continue
		}
		snap := &pb.RoomSnapshot{}
		if proto.Unmarshal(blob, snap) != nil {
			continue
		}
		s.rooms[snap.GetRoomId()] = FromSnapshot(snap)
	}
	if len(stale) > 0 {
		_ = rdb.SRem(ctx, keys.RoomIndex(o.Shard), stale...).Err()
	}

	metrics.RoomsResident.Add(float64(len(s.rooms)))
	logx.Info("Room 分片恢复完成", "shard", o.Shard, "epoch", o.Epoch, "rooms", len(s.rooms))
	return nil
}

func (s *Shard) Handle(m *bus.Msg) {
	if s.closed {
		_ = m.RespondErr(protocol.ErrUnavailable, "分片正在关闭，请重试")
		return
	}
	switch m.Cmd() {
	case protocol.CmdCreateRoom:
		s.handleCreate(m)
	case protocol.CmdJoinRoom:
		s.handleJoin(m)
	case protocol.CmdLeaveRoom:
		s.handleLeave(m)
	case protocol.CmdStartBattle:
		s.handleStart(m)
	case protocol.CmdRoomOp:
		s.handleOp(m)
	case protocol.CmdRoomInfo:
		s.handleInfo(m)
	default:
		_ = m.RespondErr(protocol.ErrBadRequest, "未知命令 %d", m.Env.GetCmd())
	}
}

// Tick 推进战斗帧，顺带刷盘和回收空房间
func (s *Shard) Tick(now time.Time) {
	if s.closed {
		return
	}

	for _, r := range s.rooms {
		if r.State != protocol.RoomStateBattling {
			continue
		}
		r.Frame++
		// 战斗中间态不刷盘，只有状态跃迁才落快照
		if now.Sub(r.StartedAt) >= BattleTimeout {
			s.settle(r)
		}
	}

	if now.After(s.nextFlush) {
		s.nextFlush = now.Add(s.rs.node.Cfg.FlushL1Interval)
		s.flush()
	}
	if now.After(s.nextGC) {
		s.nextGC = now.Add(30 * time.Second)
		s.gc(now)
	}
}

func (s *Shard) mark(roomID uint64) { s.dirty.Mark(roomID, modRoom) }

// flush 把脏的房间快照甩给 IO pool
func (s *Shard) flush() {
	items := s.dirty.Take(store.ModuleLevel(modRoom), s.rs.node.Cfg.FlushBatchSize)
	if len(items) == 0 {
		return
	}
	batch := s.buildBatch(items)
	if batch == nil {
		return
	}
	if err := s.svc.Flusher().Submit(batch); err != nil {
		s.dirty.ReMark(batch.Entities)
	}
}

func (s *Shard) buildBatch(items []store.Item) *store.Batch {
	keys := s.rs.node.Keys
	ents := make([]*store.Entity, 0, len(items))
	for _, it := range items {
		r, ok := s.rooms[it.ID]
		if !ok {
			continue // 房间关了，快照那会儿就删了
		}
		blob, err := r.Marshal()
		if err != nil {
			s.mark(it.ID)
			continue
		}
		ents = append(ents, &store.Entity{
			ID:      it.ID,
			Keys:    map[string][]byte{keys.Room(it.ID): blob},
			Modules: it.Modules,
			DirtyAt: it.DirtyAt,
		})
	}
	if len(ents) == 0 {
		return nil
	}
	return &store.Batch{Shard: s.o.Shard, Epoch: s.o.Epoch, Level: store.L1, Entities: ents}
}

// OnFlushResult 刷盘失败重新标脏
func (s *Shard) OnFlushResult(res *store.Result) {
	if res == nil || s.closed {
		return
	}
	if len(res.Failed) > 0 {
		s.dirty.ReMark(res.Failed)
	}
}

// FlushAllSync 全量同步刷盘
func (s *Shard) FlushAllSync(ctx context.Context) error {
	items := s.dirty.TakeAll()
	if len(items) == 0 {
		return nil
	}
	batch := s.buildBatch(items)
	if batch == nil {
		return nil
	}
	res := s.svc.Flusher().SubmitSync(ctx, batch)
	if res == nil {
		return nil
	}
	if len(res.Failed) > 0 {
		s.dirty.ReMark(res.Failed)
	}
	if res.Fenced {
		return fmt.Errorf("%w: shard=%d", store.ErrFenced, s.o.Shard)
	}
	if res.Err != nil {
		return res.Err
	}
	if len(res.Failed) > 0 {
		return fmt.Errorf("全量刷盘部分失败: %d 个房间未落盘", len(res.Failed))
	}
	return nil
}

func (s *Shard) Close(reason shard.ReleaseReason) {
	s.closed = true

	switch reason {
	case shard.ReleaseGraceful:
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := s.FlushAllSync(ctx); err != nil {
			logx.Error("释放 Room 分片时刷盘失败（告警）", "shard", s.o.Shard, "err", err)
		}
		cancel()
	case shard.ReleaseFenced, shard.ReleaseLeaseLost:
		logx.Error("Room 分片失去所有权，丢弃内存，不做任何写入（告警）",
			"shard", s.o.Shard, "epoch", s.o.Epoch, "reason", reason, "rooms", len(s.rooms))
	}

	metrics.RoomsResident.Sub(float64(len(s.rooms)))
	s.rooms = nil
	s.dirty = store.NewDirtySet()
}

// gc 回收空房间和已结算的房间
func (s *Shard) gc(now time.Time) {
	var dead []uint64
	for id, r := range s.rooms {
		if r.Empty() && now.Sub(r.CreatedAt) > IdleTimeout {
			dead = append(dead, id)
			continue
		}
		if r.State == protocol.RoomStateSettling && now.Sub(r.StartedAt) > IdleTimeout {
			dead = append(dead, id)
		}
	}
	for _, id := range dead {
		s.closeRoom(id)
	}
}

// closeRoom 关房间并删快照
func (s *Shard) closeRoom(id uint64) {
	if _, ok := s.rooms[id]; !ok {
		return
	}
	delete(s.rooms, id)
	s.dirty.Clear(id)
	metrics.RoomsResident.Dec()

	keys := s.rs.node.Keys
	rdb := s.rs.node.Redis
	sh, epoch := s.o.Shard, s.o.Epoch
	fencer := s.svc.Fencer()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// 删也要过 epoch 校验，陈旧 owner 没资格删新 owner 的房间
		if err := fencer.Delete(ctx, sh, epoch, keys.Room(id)); err != nil {
			logx.Warn("删除房间快照失败", "room", id, "err", err)
			return
		}
		_ = rdb.SRem(ctx, keys.RoomIndex(sh), id).Err()
	}()
}

// indexRoom 把新房间登记进分片索引
func (s *Shard) indexRoom(id uint64) {
	rdb := s.rs.node.Redis
	key := s.rs.node.Keys.RoomIndex(s.o.Shard)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rdb.SAdd(ctx, key, id).Err(); err != nil {
			logx.Warn("写入房间索引失败", "room", id, "err", err)
		}
	}()
}
