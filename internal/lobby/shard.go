package lobby

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/ledger"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/profile"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/shardsvc"
	"github.com/gamedev/f1/pkg/store"
)

// maxWaitersPerPlayer 限制单个玩家的排队请求数。
//
// 超过说明该玩家的加载或提交卡住了，继续堆积只会放大故障。
// slots 的 autoplay 会连续发 spin，每次 spin 都是一次 L0 提交，
// 因此这个值不能太小 —— 太小会在网络抖动时误伤正常玩家（评审 P2-2）。
const maxWaitersPerPlayer = 256

// isGatewayNode 判断消息是否来自网关。
//
// nodeID 形如 s1-gateway-1，服务名是第二段。这只是快速路径判断；
// 真正的安全边界是签名校验 —— 网关没有内部密钥，谎报身份也签不出合法签名。
func isGatewayNode(nodeID string) bool {
	parts := strings.Split(nodeID, "-")
	return len(parts) >= 2 && parts[1] == "gateway"
}

// Shard 是一个 Lobby 分片的全部内存状态。
//
// 对应设计文档 §4.4 的结构：
//
//	Shard {
//	    id      uint32
//	    epoch   int64
//	    mbox    chan *Msg           // 由框架持有
//	    players map[uid]*Player     // 仅本 goroutine 访问
//	    dirty   map[uid]DirtyFlag
//	}
type Shard struct {
	o   shard.Ownership
	svc *shardsvc.Service
	lob *Service

	players map[uint64]*Player
	dirty   *store.DirtySet

	// waiters 保存「玩家尚未加载完成」或「有 L0 写穿在途」期间到达的请求。
	waiters map[uint64][]*bus.Msg
	loading map[uint64]bool

	nextL1     time.Time
	nextL2     time.Time
	nextUnload time.Time
	nextTxScan time.Time

	closed bool
}

// NewShard 构造分片状态。
func NewShard(o shard.Ownership, svc *shardsvc.Service, lob *Service) *Shard {
	return &Shard{
		o:       o,
		svc:     svc,
		lob:     lob,
		players: make(map[uint64]*Player, 512),
		dirty:   store.NewDirtySet(),
		waiters: make(map[uint64][]*bus.Msg),
		loading: make(map[uint64]bool),
	}
}

// Init 初始化分片。
//
// 玩家数据是懒加载的（§10.1），这里不预热任何玩家，
// 只把各级刷盘的首次触发时刻按分片号打散（§6.3）。
func (s *Shard) Init(ctx context.Context, o shard.Ownership) error {
	now := time.Now()
	cfg := s.lob.node.Cfg
	s.nextL1 = store.Deadline(now, o.Shard, cfg.FlushL1Interval)
	s.nextL2 = store.Deadline(now, o.Shard, cfg.FlushL2Interval)
	s.nextUnload = now.Add(30 * time.Second)
	s.nextTxScan = now.Add(store.Deadline(now, o.Shard, cfg.TxScanInterval).Sub(now))

	logx.Debug("Lobby 分片就绪", "shard", o.Shard, "epoch", o.Epoch)
	return nil
}

// Epoch 返回分片 epoch。
func (s *Shard) Epoch() int64 { return s.o.Epoch }

// ID 返回分片号。
func (s *Shard) ID() uint32 { return s.o.Shard }

// Handle 处理一条请求。所有业务逻辑都在这条 goroutine 上串行执行。
func (s *Shard) Handle(m *bus.Msg) {
	if s.closed {
		_ = m.RespondErr(protocol.ErrUnavailable, "分片正在关闭，请重试")
		return
	}

	cmd := m.Cmd()
	uid := m.Env.GetUid()

	// 第二道鉴权（评审 P0-1）。
	//
	// 网关已经按 Cmd.Level() 挡过一次，这里再验一次签名，理由是「不信任网关」：
	// 网关配置写错、或有人直接连上内网 NATS，这一关仍然拦得住。
	// fromClient 判定依据是发送方 nodeID —— 网关签不出内部签名，
	// 所以即便它谎称自己不是网关，签名校验一样过不了。
	fromClient := isGatewayNode(m.Env.GetFromNode())
	if err := s.lob.signer.Verify(m.Env, fromClient); err != nil {
		metrics.AuthzRejected.WithLabelValues(cmd.Name(), "verify").Inc()
		logx.Trace(m.Env.GetTraceId()).Error("命令鉴权失败（告警：可能有人在探测内部接口）",
			"cmd", cmd, "level", cmd.Level(), "from", m.Env.GetFromNode(), "uid", uid, "err", err)
		_ = m.RespondErr(protocol.ErrPermission, "无权调用该命令")
		return
	}

	// 不需要玩家对象的命令先处理掉。
	switch cmd {
	case protocol.CmdGetProfile:
		s.handleGetProfile(m)
		return
	case protocol.CmdJackpotInfo:
		s.handleJackpotInfo(m)
		return
	}

	if uid == 0 {
		_ = m.RespondErr(protocol.ErrBadRequest, "缺少 uid")
		return
	}
	if got := s.lob.ShardOf(uid); got != s.o.Shard {
		// 路由错了。正常情况下 subject 已经保证不会发生，出现即是 bug。
		logx.Error("请求路由到了错误的分片", "uid", uid, "want", got, "got", s.o.Shard, "cmd", cmd)
		_ = m.RespondErr(protocol.ErrBadRequest, "分片路由错误")
		return
	}

	p := s.acquire(uid, m)
	if p == nil {
		return // 已排队等待加载 / 写穿完成
	}
	s.dispatch(p, m)
}

// dispatch 把请求分发到具体处理函数。
func (s *Shard) dispatch(p *Player, m *bus.Msg) {
	p.LastActive = time.Now()

	switch m.Cmd() {
	case protocol.CmdLogin:
		s.handleLogin(p, m)
	case protocol.CmdLogout:
		s.handleLogout(p, m)
	case protocol.CmdHeartbeat:
		s.handleHeartbeat(p, m)
	case protocol.CmdAddCurrency:
		s.handleAddCurrency(p, m)
	case protocol.CmdAddItem:
		s.handleAddItem(p, m)
	case protocol.CmdUseItem:
		s.handleUseItem(p, m)
	case protocol.CmdGetBag:
		s.handleGetBag(p, m)
	case protocol.CmdAcceptQuest:
		s.handleAcceptQuest(p, m)
	case protocol.CmdQuestProgress:
		s.handleQuestProgress(p, m)
	case protocol.CmdGetSocial:
		s.handleGetSocial(p, m)
	case protocol.CmdAddFriend:
		s.handleAddFriend(p, m)
	case protocol.CmdGetMail:
		s.handleGetMail(p, m)
	case protocol.CmdClaimMail:
		s.handleClaimMail(p, m)
	case protocol.CmdPurchase:
		s.handlePurchase(p, m)
	case protocol.CmdTransfer:
		s.handleTransfer(p, m)
	case protocol.CmdApplyTransfer:
		s.handleApplyTransfer(p, m)
	case protocol.CmdApplyMail:
		s.handleApplyMail(p, m)
	case protocol.CmdBattleSettle:
		s.handleBattleSettle(p, m)

	// --- slots ---
	case protocol.CmdSpin:
		s.handleSpin(p, m)
	case protocol.CmdRoundState:
		s.handleRoundState(p, m)
	case protocol.CmdGacha:
		s.handleGacha(p, m)
	case protocol.CmdRGStatus:
		s.handleRGStatus(p, m)
	case protocol.CmdApplyJackpot:
		s.handleApplyJackpot(p, m)

	// --- GM ---
	case protocol.CmdGMGrant:
		s.handleGMGrant(p, m)
	case protocol.CmdGMQuery:
		s.handleGMQuery(p, m)
	case protocol.CmdGMSetRG:
		s.handleGMSetRG(p, m)
	case protocol.CmdGMKick:
		s.handleGMKick(p, m)

	default:
		_ = m.RespondErr(protocol.ErrBadRequest, "未知命令 %d", m.Env.GetCmd())
	}
}

// ---------------------------------------------------------------------------
// 玩家加载 / 排队
// ---------------------------------------------------------------------------

// acquire 取得可用的玩家对象。
//
// 返回 nil 表示请求已被排队：玩家正在加载，或有 L0 写穿在途。
// 这样既不阻塞 Actor goroutine（§4.4），又能兑现 L0 的「落盘成功才改内存」语义。
func (s *Shard) acquire(uid uint64, m *bus.Msg) *Player {
	p, ok := s.players[uid]
	if ok && !p.busy && len(s.waiters[uid]) == 0 {
		return p
	}
	if ok {
		// 已经有排队的请求就必须跟着排：
		// 同一玩家的请求顺序是业务语义的一部分（先用道具再看背包，不能反过来）。
		s.enqueue(uid, m)
		return nil
	}

	s.enqueue(uid, m)
	if !s.loading[uid] {
		s.loading[uid] = true
		s.startLoad(uid)
	}
	return nil
}

func (s *Shard) enqueue(uid uint64, m *bus.Msg) {
	q := s.waiters[uid]
	if len(q) >= maxWaitersPerPlayer {
		logx.Warn("玩家排队请求过多，拒绝新请求", "uid", uid, "queued", len(q))
		_ = m.RespondErr(protocol.ErrUnavailable, "玩家状态加载中，请重试")
		return
	}
	s.waiters[uid] = append(q, m)
}

// startLoad 异步加载玩家数据。
//
// 「登录时由所属 Lobby 分片 pipeline 读回全部模块 key」（§10.1）。
// IO 在独立 goroutine 上做，完成后把结果投递回 Actor —— 绝不在 Actor 内阻塞。
func (s *Shard) startLoad(uid uint64) {
	epoch := s.o.Epoch
	sh := s.o.Shard
	fencer := s.svc.Fencer()
	rt := s.svc.Runtime()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		blobs, err := fencer.LoadPlayer(ctx, uid)
		_ = rt.Do(sh, func() {
			s.onLoaded(uid, epoch, blobs, err)
		})
	}()
}

func (s *Shard) onLoaded(uid uint64, epoch int64, blobs map[store.Module][]byte, err error) {
	delete(s.loading, uid)

	if epoch != s.o.Epoch {
		// 加载期间分片被接管又拿回来了（epoch 变了），这批数据不可信，丢弃重来。
		s.failWaiters(uid, protocol.ErrUnavailable, "分片已变更，请重试")
		return
	}
	if err != nil {
		logx.Error("加载玩家失败", "uid", uid, "shard", s.o.Shard, "err", err)
		s.failWaiters(uid, protocol.ErrInternal, "加载玩家数据失败")
		return
	}

	now := time.Now()
	p, perr := FromBlobs(uid, blobs, now)
	if perr != nil {
		// 反序列化失败绝不能用空对象顶上 —— 那等于清档。
		logx.Error("玩家数据反序列化失败，拒绝服务该玩家（告警）",
			"uid", uid, "shard", s.o.Shard, "err", perr)
		s.failWaiters(uid, protocol.ErrInternal, "玩家数据损坏")
		return
	}

	isNew := len(blobs) == 0
	s.players[uid] = p
	metrics.PlayersResident.Inc()

	if isNew {
		// 新玩家：首次落盘要把全部模块写下去。
		s.dirty.MarkAll(uid)
		logx.Info("创建新玩家", "uid", uid, "shard", s.o.Shard)
	}

	s.drainWaiters(uid)
}

// drainWaiters 把排队的请求依次执行。
func (s *Shard) drainWaiters(uid uint64) {
	for {
		q := s.waiters[uid]
		if len(q) == 0 {
			delete(s.waiters, uid)
			return
		}
		p, ok := s.players[uid]
		if !ok || p.busy {
			return // 又进入忙碌状态，剩下的继续等
		}
		m := q[0]
		s.waiters[uid] = q[1:]
		s.dispatch(p, m)
	}
}

func (s *Shard) failWaiters(uid uint64, code protocol.ErrCode, format string, args ...any) {
	for _, m := range s.waiters[uid] {
		_ = m.RespondErr(code, format, args...)
	}
	delete(s.waiters, uid)
}

// ---------------------------------------------------------------------------
// 脏标记 / 刷盘
// ---------------------------------------------------------------------------

// mark 标记玩家某模块为脏。
func (s *Shard) mark(uid uint64, mods ...store.Module) {
	for _, m := range mods {
		s.dirty.Mark(uid, m)
	}
}

// Tick 周期回调：分级刷盘、卸载检查、PENDING 转移补偿。
func (s *Shard) Tick(now time.Time) {
	if s.closed {
		return
	}
	cfg := s.lob.node.Cfg

	if now.After(s.nextL1) {
		s.nextL1 = now.Add(cfg.FlushL1Interval)
		s.flushLevel(store.L1)
	}
	if now.After(s.nextL2) {
		s.nextL2 = now.Add(cfg.FlushL2Interval)
		s.flushLevel(store.L2)
	}
	if now.After(s.nextUnload) {
		s.nextUnload = now.Add(30 * time.Second)
		s.unloadIdle(now)
	}
	if now.After(s.nextTxScan) {
		s.nextTxScan = now.Add(cfg.TxScanInterval)
		s.lob.scanPendingTx(s.o.Shard)
		// 奖池派彩的补偿扫描：只让 0 号分片的 owner 做，避免每个分片都扫一遍。
		if s.o.Shard == 0 {
			s.lob.scanJackpotPayouts()
		}
	}
}

// flushLevel 取出该级别的脏数据，在 Actor 内序列化，然后甩给 IO pool（§6.3）。
func (s *Shard) flushLevel(level store.Level) {
	items := s.dirty.Take(level, s.lob.node.Cfg.FlushBatchSize)
	if len(items) == 0 {
		return
	}
	batch := s.buildBatch(level, items)
	if batch == nil {
		return
	}
	if err := s.svc.Flusher().Submit(batch); err != nil {
		// flushCh 满：重新标脏，绝不阻塞 Actor（§6.3）。
		s.dirty.ReMark(batch.Entities)
	}
}

// buildBatch 在 Actor goroutine 内完成序列化 —— 读内存必须如此。
func (s *Shard) buildBatch(level store.Level, items []store.Item) *store.Batch {
	keys := s.lob.node.Keys
	ents := make([]*store.Entity, 0, len(items))

	for _, it := range items {
		p, ok := s.players[it.ID]
		if !ok {
			continue // 玩家已卸载，卸载时已做过最终刷盘
		}
		kv, err := p.MarshalKeys(keys, it.Modules)
		if err != nil {
			logx.Error("序列化玩家失败，保留脏标记", "uid", it.ID, "err", err)
			s.mark(it.ID, it.Modules...)
			continue
		}
		ent := &store.Entity{
			ID:      it.ID,
			Keys:    kv,
			Modules: it.Modules,
			DirtyAt: it.DirtyAt,
		}
		// base 变了就顺带刷新只读摘要（§6.5）。
		if containsModule(it.Modules, store.ModBase) {
			ent.Hash = profile.HashWrite(keys, p.Profile())
		}
		ents = append(ents, ent)
	}

	if len(ents) == 0 {
		return nil
	}
	return &store.Batch{Shard: s.o.Shard, Epoch: s.o.Epoch, Level: level, Entities: ents}
}

func containsModule(mods []store.Module, want store.Module) bool {
	for _, m := range mods {
		if m == want {
			return true
		}
	}
	return false
}

// OnFlushResult 处理刷盘结果。
func (s *Shard) OnFlushResult(res *store.Result) {
	if res == nil || s.closed {
		return
	}
	if len(res.Failed) > 0 {
		// 「刷盘失败重新标脏，绝不丢弃。内存仍是权威副本，
		// Redis 不可用期间服务可继续」（§6.3）。
		s.dirty.ReMark(res.Failed)
	}
}

// unloadIdle 卸载下线超时的玩家（§10.1）。
//
// 下线后保留 5~10 分钟（断线重连、离线结算、好友查看），超时后最终刷盘并删除。
func (s *Shard) unloadIdle(now time.Time) {
	idle := s.lob.node.Cfg.UnloadIdle
	var victims []uint64
	for uid, p := range s.players {
		if p.busy || s.loading[uid] || len(s.waiters[uid]) > 0 {
			continue
		}
		if p.Idle(now) >= idle {
			victims = append(victims, uid)
		}
	}
	if len(victims) == 0 {
		return
	}

	var ents []*store.Entity
	keys := s.lob.node.Keys
	unloaded := make([]uint64, 0, len(victims))

	for _, uid := range victims {
		p := s.players[uid]
		item, dirty := s.dirty.TakeOne(uid)
		if dirty {
			kv, err := p.MarshalKeys(keys, item.Modules)
			if err != nil {
				s.dirty.ReMark([]*store.Entity{{ID: uid, Modules: item.Modules, DirtyAt: item.DirtyAt}})
				continue
			}
			ent := &store.Entity{ID: uid, Keys: kv, Modules: item.Modules, DirtyAt: item.DirtyAt}
			if containsModule(item.Modules, store.ModBase) {
				ent.Hash = profile.HashWrite(keys, p.Profile())
			}
			ents = append(ents, ent)
		}
		unloaded = append(unloaded, uid)
	}

	if len(ents) > 0 {
		batch := &store.Batch{Shard: s.o.Shard, Epoch: s.o.Epoch, Level: store.L1, Entities: ents}
		if err := s.svc.Flusher().Submit(batch); err != nil {
			// 提交不进去就别卸载：内存是权威副本，丢了就真丢了。
			s.dirty.ReMark(batch.Entities)
			return
		}
	}

	for _, uid := range unloaded {
		delete(s.players, uid)
		delete(s.waiters, uid)
		metrics.PlayersResident.Dec()
	}
	logx.Debug("卸载空闲玩家", "shard", s.o.Shard, "count", len(unloaded), "resident", len(s.players))
}

// FlushAllSync 全量同步刷盘，用于交接与优雅下线。
func (s *Shard) FlushAllSync(ctx context.Context) error {
	items := s.dirty.TakeAll()
	if len(items) == 0 {
		return nil
	}
	batch := s.buildBatch(store.L1, items)
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
		return fmt.Errorf("全量刷盘部分失败: %d 个实体未落盘", len(res.Failed))
	}
	return nil
}

// Close 分片被释放。
func (s *Shard) Close(reason shard.ReleaseReason) {
	s.closed = true

	switch reason {
	case shard.ReleaseGraceful:
		// 主动交接 / 优雅下线：必须全量刷盘。
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		if err := s.FlushAllSync(ctx); err != nil {
			logx.Error("释放分片时全量刷盘失败（告警：可能丢数据）",
				"shard", s.o.Shard, "epoch", s.o.Epoch, "err", err)
		}
		cancel()

	case shard.ReleaseFenced, shard.ReleaseLeaseLost:
		// 「旧 owner 收到拒绝 → 丢弃该分片内存、停止服务、告警。绝不重试」（§7.2）。
		// 此刻内存已过期，任何写入都只会加重损坏。
		logx.Error("分片失去所有权，直接丢弃内存，不做任何写入（告警）",
			"shard", s.o.Shard, "epoch", s.o.Epoch, "reason", reason,
			"players", len(s.players), "dirty", s.dirty.Len())

	case shard.ReleaseStartFailed:
		logx.Warn("分片初始化失败，清理内存", "shard", s.o.Shard)
	}

	// 排队中的请求要回错，别让客户端干等到超时。
	for uid, q := range s.waiters {
		for _, m := range q {
			_ = m.RespondErr(protocol.ErrUnavailable, "分片正在交接，请重试")
		}
		delete(s.waiters, uid)
	}

	metrics.PlayersResident.Sub(float64(len(s.players)))
	s.players = nil
	s.dirty = store.NewDirtySet()
}

// ---------------------------------------------------------------------------
// L0 写穿
// ---------------------------------------------------------------------------

// CommitSpec 描述一次资金类提交。
//
// 调用约定（很重要）：改动先算在**副本**上，序列化进 KV，提交成功后才换进内存。
// 这样兑现了 §6.2 的「同步落 Redis 成功后才改内存回包」，
// 同时保证「余额 + 流水 + 回合 + 限额」要么全成，要么全不成。
type CommitSpec struct {
	// IdemKey 为空表示不做幂等。资金操作原则上都该有幂等键。
	IdemKey string
	Payload []byte
	Entries []*ledger.Entry
	KV      map[string][]byte
	// Op 用于指标打标。
	Op string
}

// commitDone 是提交完成后的回调，在 Actor goroutine 内执行。
type commitDone func(res *store.CommitResult, err error)

// commit 执行一次带幂等与流水的原子提交（L0）。
//
// 期间玩家被标记 busy，其余请求排队 —— 这是「Redis 未确认前内存不变」的实现方式。
func (s *Shard) commit(p *Player, spec CommitSpec, done commitDone) {
	entries, err := ledger.Encode(spec.Entries)
	if err != nil {
		done(nil, err)
		return
	}

	req := &store.CommitReq{
		Shard:        s.o.Shard,
		Epoch:        s.o.Epoch,
		IdemKey:      spec.IdemKey,
		IdemTTLSec:   int64(s.lob.orderTTL.Seconds()),
		IdemPayload:  spec.Payload,
		LedgerKey:    s.lob.node.Keys.Ledger(p.UID),
		LedgerMaxLen: ledger.DefaultMaxLen,
		Entries:      entries,
		KV:           spec.KV,
	}

	p.busy = true
	uid := p.UID
	sh := s.o.Shard
	fencer := s.svc.Fencer()
	rt := s.svc.Runtime()
	op := spec.Op
	if op == "" {
		op = "commit"
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		start := time.Now()
		res, cerr := fencer.Commit(ctx, req)
		metrics.WriteThrough.WithLabelValues(op).Observe(time.Since(start).Seconds())
		if cerr == nil {
			metrics.LedgerEntries.Add(float64(len(entries)))
		}

		_ = rt.Do(sh, func() {
			if pl, ok := s.players[uid]; ok {
				pl.busy = false
			}
			done(res, cerr)
			if errors.Is(cerr, store.ErrFenced) {
				// 提交被 fencing 拒绝：整个分片都已过期，丢弃内存并停止服务。
				s.svc.Claimer().Fence(sh)
				return
			}
			s.drainWaiters(uid)
		})
	}()
}

// writeThroughDone 是写穿完成后的回调，在 Actor goroutine 内执行。
type writeThroughDone func(res *store.WriteThroughResult, err error)

// writeThrough 是不带流水的 L0 写穿，仅用于非资金类的幂等操作。
//
// 资金类一律走 commit：没有流水的资金变动是查不清的（评审 P1-2）。
func (s *Shard) writeThrough(p *Player, orderKey string, payload []byte,
	kv map[string][]byte, done writeThroughDone) {

	p.busy = true
	uid := p.UID
	epoch := s.o.Epoch
	sh := s.o.Shard
	fencer := s.svc.Fencer()
	rt := s.svc.Runtime()
	ttl := int64(s.lob.orderTTL.Seconds())

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		start := time.Now()
		res, err := fencer.WriteThrough(ctx, sh, epoch, orderKey, ttl, payload, kv)
		metrics.WriteThrough.WithLabelValues("write_through").Observe(time.Since(start).Seconds())

		_ = rt.Do(sh, func() {
			if pl, ok := s.players[uid]; ok {
				pl.busy = false
			}
			done(res, err)
			if errors.Is(err, store.ErrFenced) {
				s.svc.Claimer().Fence(sh)
				return
			}
			s.drainWaiters(uid)
		})
	}()
}

// kvOf 把玩家的若干模块序列化成提交用的 key→value。
//
// 必须在 Actor goroutine 内调用（读内存），这是 §6.3 的硬要求。
func (s *Shard) kvOf(p *Player, mods ...store.Module) (map[string][]byte, error) {
	return p.MarshalKeys(s.lob.node.Keys, mods)
}

// appendLedger 追加一条流水到异步刷盘路径。
//
// 只用于「非资金关键路径」的补记（例如内部发放的货币变动）：
// 资金关键路径（下注、充值、派彩）必须走 commit，与余额同一个 Lua 原子写入。
// 这里的写入是 best-effort 的，失败会记日志。
func (s *Shard) appendLedger(p *Player, e *ledger.Entry) {
	blob, err := e.JSON()
	if err != nil {
		return
	}
	key := s.lob.node.Keys.Ledger(p.UID)
	rdb := s.lob.node.Redis
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			MaxLen: ledger.DefaultMaxLen,
			Approx: true,
			Values: map[string]any{ledger.StreamField: blob},
		}).Err(); err != nil {
			logx.Error("追加流水失败（告警：该笔资金变动将无法追溯）",
				"uid", p.UID, "type", e.Type, "err", err)
		}
	}()
}

// appendItemLedger 记录道具类资产变动。
//
// 道具没有「余额」概念，因此 amount/balance 留 0，数量记在 ref 里。
func (s *Shard) appendItemLedger(p *Player, t ledger.Type, tpl uint32, count int64, reason string) {
	s.appendLedger(p, s.entry(p.UID, t, 0, 0, 0).
		WithRef(fmt.Sprintf("tpl=%d;count=%d;reason=%s", tpl, count, reason)))
}
