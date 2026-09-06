// Package world 世界服：世界 BOSS、活动调度这类全局唯一的逻辑，选主主备
//
// 同一时刻只能有一个实例改 BOSS 血量，不然两边各扣各的，写谁的都不对。
// 两道保护：选主决定谁订阅 req.world.>，epoch fencing 在落盘时做最终裁决。
// 跟分片那套相同，只是空间只有一格
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
	// worldShard 世界服在 epoch 命名空间里占的那一格
	worldShard = 0
	// bossMaxHP BOSS 满血值
	bossMaxHP = 1_000_000
	// bossRespawn 击杀后的重生间隔
	bossRespawn = 10 * time.Minute
	// flushInterval BOSS 血量的落盘间隔
	flushInterval = 3 * time.Second
	// mailboxSize 世界服的消息队列容量
	mailboxSize = 4096
)

type task struct {
	msg *bus.Msg
	fn  func()
}

// Service 世界服务
type Service struct {
	node    *node.Node
	elector *leader.Elector
	fencer  *store.Fencer

	// 世界服自己就是一个 Actor，单 goroutine 串行，不用锁
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

func New() *Service {
	return &Service{
		mbox: make(chan task, mailboxSize),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (s *Service) Name() string { return "world" }

// Start 先起 Actor 再参选
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

// onElected 当选后依次抬 epoch、加载状态、订阅 subject，跟分片接管一个顺序
func (s *Service) onElected(ctx context.Context, epoch int64) error {
	// 抬 epoch，旧 leader 之后再落盘就会被拒
	if err := s.fencer.RaiseEpoch(ctx, worldShard, epoch); err != nil {
		return err
	}

	// 加载状态
	boss, err := s.loadBoss(ctx)
	if err != nil {
		return err
	}

	// 装进 Actor
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

	// 订阅。只有 leader 订，备机压根收不到消息
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

// onResigned 失去领导权就退订停手
//
// 内存里没落盘的血量直接丢，这会儿已经不是权威副本了，再写会盖掉新 leader 的
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
	// BOSS 死了定时重生
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
		// 击杀是关键跃迁，立刻落盘，不等下一个 ticker
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

// flushBoss 把 BOSS 状态落盘，带 epoch 校验
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
			// 已经不是 leader 了，让位，别重试
			logx.Error("BOSS 落盘被 fencing 拒绝，主动让位（告警）", "epoch", epoch, "err", err)
			s.elector.Stop(context.Background())
			return
		}
		// 其他失败重新标脏
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

func (s *Service) NotifyClients(ctx context.Context) {}

// StopAccepting 让位并退订
func (s *Service) StopAccepting(ctx context.Context) {
	if s.elector != nil {
		s.elector.Stop(ctx)
	}
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
	s.subs = nil
}

// FlushAll 落盘 BOSS 状态
func (s *Service) FlushAll(ctx context.Context) error {
	// 让位时内存已经丢了，这里只管「还是 leader 就下线」这种情况
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

func (s *Service) Close(ctx context.Context) {
	close(s.quit)
	<-s.done
}

var _ node.Service = (*Service)(nil)
