// Package world 实现世界服：全局唯一逻辑（世界 BOSS、活动调度），选主主备（§2.1）。
//
// 「全局唯一」这件事本身就是一致性约束：同一时刻只能有一个实例在改 BOSS 血量，
// 否则两个实例各扣各的，最后写谁的都不对。因此这里同时用两道保护：
//   - 选主（软保护）：只有 leader 订阅 req.world.>
//   - epoch fencing（硬保护）：leader 的 etcd revision 作为 epoch，落盘时校验
//
// 与分片那套完全同构，只是分片空间只有一个格子。
package world

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/leader"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/pkg/subject"
)

const (
	// worldShard 是世界服在 epoch 命名空间里占用的固定格位。
	worldShard = 0
	// bossMaxHP 是 BOSS 满血值。
	bossMaxHP = 1_000_000
	// bossRespawn 是击杀后的重生间隔。
	bossRespawn = 10 * time.Minute
	// flushInterval 是 BOSS 血量的落盘间隔（L1 级别）。
	flushInterval = 3 * time.Second
	// mailboxSize 是世界服的消息队列容量。
	mailboxSize = 4096
)

type task struct {
	msg *bus.Msg
	fn  func()
}

// Service 是世界服务。
type Service struct {
	node    *node.Node
	elector *leader.Elector
	fencer  *store.Fencer

	// 世界服本身就是一个 Actor：单 goroutine 串行处理，无锁。
	mbox chan task
	quit chan struct{}
	done chan struct{}

	subs  []*nats.Subscription
	epoch int64

	boss      *pb.WorldBossState
	dirty     bool
	dirtyAt   time.Time
	nextFlush time.Time
	nextEvent time.Time
	running   bool
}

// New 构造世界服务。
func New() *Service {
	return &Service{
		mbox: make(chan task, mailboxSize),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Name 实现 node.Service。
func (s *Service) Name() string { return "world" }

// Start 启动服务：先起 Actor，再参选。
func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.fencer = store.NewFencer(n.Redis, n.Keys, "world")

	go s.loop()

	s.elector = leader.New(n.Etcd, n.Cfg, "world", n.NodeID(), leader.Hooks{
		OnElected:  s.onElected,
		OnResigned: s.onResigned,
	})
	if err := s.elector.Start(ctx); err != nil {
		return err
	}
	logx.Info("世界服已参选，等待当选后开始服务", "node", n.NodeID())
	return nil
}

// loop 是世界服的 Actor 主循环。
func (s *Service) loop() {
	defer close(s.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-s.quit:
			return
		case tk := <-s.mbox:
			s.exec(tk)
		case now := <-t.C:
			s.tick(now)
		}
	}
}

func (s *Service) exec(tk task) {
	defer func() {
		if r := recover(); r != nil {
			logx.Error("世界服处理 panic", "panic", r)
			if tk.msg != nil {
				_ = tk.msg.RespondErr(protocol.ErrInternal, "内部错误")
			}
		}
	}()
	switch {
	case tk.msg != nil:
		s.handle(tk.msg)
	case tk.fn != nil:
		tk.fn()
	}
}

func (s *Service) post(tk task) bool {
	select {
	case s.mbox <- tk:
		return true
	default:
		return false
	}
}

// onElected 当选后：抬高 epoch → 加载状态 → 订阅 subject → 服务。
//
// 顺序与分片接管完全一致（§10.2 步骤 5）。
func (s *Service) onElected(ctx context.Context, epoch int64) error {
	// 1. 抬高 epoch —— 此后旧 leader 的落盘会被拒绝。
	if err := s.fencer.RaiseEpoch(ctx, worldShard, epoch); err != nil {
		return err
	}

	// 2. 加载状态。
	boss, err := s.loadBoss(ctx)
	if err != nil {
		return err
	}

	// 3. 装载进 Actor。
	done := make(chan struct{})
	if !s.post(task{fn: func() {
		defer close(done)
		s.epoch = epoch
		s.boss = boss
		s.running = true
		s.nextFlush = time.Now().Add(flushInterval)
		s.nextEvent = time.Now().Add(time.Minute)
	}}) {
		return errors.New("世界服 mailbox 已满，放弃当选")
	}
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	// 4. 订阅：只有 leader 订阅 req.world.>，备机完全不接消息。
	sub, err := s.node.Bus.Subscribe(subject.WorldWildcard(), func(m *bus.Msg) {
		if !s.post(task{msg: m}) {
			_ = m.RespondErr(protocol.ErrUnavailable, "世界服繁忙，请重试")
		}
	})
	if err != nil {
		return err
	}
	s.subs = append(s.subs, sub)

	logx.Info("世界服开始服务", "epoch", epoch, "boss_hp", boss.GetHp())
	return nil
}

// onResigned 失去领导权：立即退订并停止处理。
//
// 内存中未落盘的 BOSS 血量直接丢弃 —— 此刻我们已不是权威副本，
// 再写只会覆盖新 leader 的数据（§7.2）。
func (s *Service) onResigned(reason string) {
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
	s.subs = nil

	s.post(task{fn: func() {
		s.running = false
		if s.dirty {
			logx.Error("失去领导权时仍有未落盘的 BOSS 血量，直接丢弃（告警）",
				"reason", reason, "hp", s.boss.GetHp())
		}
		s.dirty = false
		s.boss = nil
	}})
}

func (s *Service) tick(now time.Time) {
	if !s.running {
		return
	}
	if s.dirty && now.After(s.nextFlush) {
		s.nextFlush = now.Add(flushInterval)
		s.flushBoss()
	}
	// 活动调度：BOSS 死亡后定时重生。
	if s.boss != nil && !s.boss.GetAlive() && now.After(s.nextEvent) {
		s.respawn(now)
	}
}

func (s *Service) handle(m *bus.Msg) {
	if !s.running {
		_ = m.RespondErr(protocol.ErrUnavailable, "世界服尚未就绪")
		return
	}
	switch m.Cmd() {
	case protocol.CmdWorldBossState:
		_ = m.Respond(s.boss)
	case protocol.CmdWorldBossHit:
		s.handleHit(m)
	default:
		_ = m.RespondErr(protocol.ErrBadRequest, "未知命令 %d", m.Env.GetCmd())
	}
}

func (s *Service) handleHit(m *bus.Msg) {
	var req pb.WorldBossHitReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	if s.boss == nil || !s.boss.GetAlive() {
		_ = m.RespondErr(protocol.ErrNotFound, "BOSS 不在场")
		return
	}
	dmg := req.GetDamage()
	if dmg <= 0 {
		_ = m.RespondErr(protocol.ErrBadRequest, "伤害非法")
		return
	}

	s.boss.Hp -= dmg
	killed := false
	if s.boss.Hp <= 0 {
		s.boss.Hp = 0
		s.boss.Alive = false
		killed = true
		s.nextEvent = time.Now().Add(bossRespawn)
	}
	s.markDirty()

	_ = m.Respond(&pb.WorldBossHitResp{HpLeft: s.boss.GetHp(), Killed: killed})

	if killed {
		logx.Info("世界 BOSS 被击杀", "boss", s.boss.GetBossId(), "killer", req.GetUid())
		// 击杀是关键状态跃迁，立即落盘，不等下一个 ticker。
		s.flushBoss()
		s.announce("世界 BOSS 已被击败！")
	}
}

func (s *Service) markDirty() {
	if !s.dirty {
		s.dirty = true
		s.dirtyAt = time.Now()
	}
}

func (s *Service) respawn(now time.Time) {
	id, err := s.node.NextID()
	if err != nil {
		logx.Error("生成 BOSS ID 失败，推迟重生", "err", err)
		s.nextEvent = now.Add(time.Minute)
		return
	}
	s.boss = &pb.WorldBossState{
		BossId:    id,
		Hp:        bossMaxHP,
		MaxHp:     bossMaxHP,
		StartedAt: now.UnixMilli(),
		Alive:     true,
	}
	s.markDirty()
	s.flushBoss()
	logx.Info("世界 BOSS 重生", "boss", id, "hp", bossMaxHP)
	s.announce("世界 BOSS 已刷新！")
}

// flushBoss 把 BOSS 状态落盘（带 epoch 校验）。
func (s *Service) flushBoss() {
	if s.boss == nil {
		return
	}
	blob, err := proto.Marshal(s.boss)
	if err != nil {
		return
	}
	epoch := s.epoch
	key := s.node.Keys.WorldBoss()
	dirtyAt := s.dirtyAt
	s.dirty = false

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := s.fencer.WriteModules(ctx, worldShard, epoch, map[string][]byte{key: blob})
		if err == nil {
			return
		}
		if errors.Is(err, store.ErrFenced) {
			// 已经不是 leader 了：让位，绝不重试。
			logx.Error("BOSS 落盘被 fencing 拒绝，主动让位（告警）", "epoch", epoch, "err", err)
			s.elector.Stop(context.Background())
			return
		}
		// 其他失败重新标脏，绝不丢弃。
		logx.Error("BOSS 落盘失败，重新标脏", "err", err)
		s.post(task{fn: func() {
			if !s.dirty {
				s.dirty = true
				s.dirtyAt = dirtyAt
			}
		}})
	}()
}

func (s *Service) loadBoss(ctx context.Context) (*pb.WorldBossState, error) {
	blob, err := s.node.Redis.Get(ctx, s.node.Keys.WorldBoss()).Bytes()
	if errors.Is(err, redis.Nil) {
		id, ierr := s.node.NextID()
		if ierr != nil {
			return nil, ierr
		}
		return &pb.WorldBossState{
			BossId: id, Hp: bossMaxHP, MaxHp: bossMaxHP,
			StartedAt: time.Now().UnixMilli(), Alive: true,
		}, nil
	}
	if err != nil {
		return nil, err
	}
	boss := &pb.WorldBossState{}
	if err := proto.Unmarshal(blob, boss); err != nil {
		return nil, err
	}
	return boss, nil
}

// announce 全服公告走 push.broadcast（§9.3）。
func (s *Service) announce(text string) {
	payload, err := proto.Marshal(&pb.ChatMsg{
		Channel: protocol.ChanWorld, FromNick: "系统", Text: text, TsMs: time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	env, err := s.node.Bus.NewEnvelope(protocol.Cmd(protocol.PushAnnounce), 0, "", &pb.MultiPush{
		Cmd: uint32(protocol.PushAnnounce), Payload: payload,
	})
	if err != nil {
		return
	}
	_ = s.node.Bus.PublishEnv(subject.Broadcast, env)
}

// NotifyClients 实现 node.Service。
func (s *Service) NotifyClients(ctx context.Context) {}

// StopAccepting 让位并退订。
func (s *Service) StopAccepting(ctx context.Context) {
	if s.elector != nil {
		s.elector.Stop(ctx)
	}
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
	s.subs = nil
}

// FlushAll 落盘 BOSS 状态。
func (s *Service) FlushAll(ctx context.Context) error {
	// 让位时内存已被丢弃，这里只处理「还是 leader 就下线」的场景。
	done := make(chan struct{})
	var blob []byte
	var epoch int64
	s.post(task{fn: func() {
		defer close(done)
		if s.boss == nil || !s.dirty {
			return
		}
		blob, _ = proto.Marshal(s.boss)
		epoch = s.epoch
		s.dirty = false
	}})
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if blob == nil {
		return nil
	}
	return s.fencer.WriteModules(ctx, worldShard, epoch,
		map[string][]byte{s.node.Keys.WorldBoss(): blob})
}

// Close 停止 Actor。
func (s *Service) Close(ctx context.Context) {
	close(s.quit)
	<-s.done
}

var _ node.Service = (*Service)(nil)
