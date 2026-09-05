package lobby

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/pkg/subject"
	"github.com/gamedev/f1/pkg/xtx"
)

// ---------------------------------------------------------------------------
// 会话
// ---------------------------------------------------------------------------

func (s *Shard) handleLogin(p *Player, m *bus.Msg) {
	var req pb.LoginReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析登录请求失败")
		return
	}

	now := time.Now()
	reconnect := p.Online || !p.OfflineAt.IsZero()

	if p.Online && p.ConnID != req.GetConnId() {
		// 顶号：旧连接所在的 Lobby 分片（就是这里）保存并卸载旧上下文（§9.2）。
		// KICK 由网关侧发出，它在替换 session 时拿到了旧 gateID。
		logx.Info("玩家被顶号，卸载旧连接上下文",
			"uid", p.UID, "old_gate", p.GateID, "old_conn", p.ConnID,
			"new_gate", req.GetGateId(), "new_conn", req.GetConnId())
		s.mark(p.UID, store.ModBase, store.ModBag, store.ModQuest)
	}

	p.Online = true
	p.GateID = req.GetGateId()
	p.ConnID = req.GetConnId()
	p.OfflineAt = time.Time{}
	p.Base.LastLogin = now.UnixMilli()
	s.mark(p.UID, store.ModBase)

	// 离线期间投递到「待领取队列」的邮件在这里消费（§6.5）。
	s.drainPendingMail(p.UID)

	// 维护公会成员索引，供 Chat 做公会广播时查路由表（§9.3）。
	if gid := p.Base.GetGuildId(); gid != 0 {
		s.lob.indexGuildMember(gid, p.UID)
	}

	s.lob.PublishPlayerEvent(p.UID, "login", p.Profile(), m.Env.GetTraceId())

	_ = m.Respond(&pb.LoginResp{
		Base:      p.Base,
		Bag:       p.Bag,
		Quest:     p.Quest,
		Reconnect: reconnect,
	})
}

func (s *Shard) handleLogout(p *Player, m *bus.Msg) {
	var req pb.LogoutReq
	_ = bus.Unpack(m.Env, &req)

	// 顶号场景下旧连接的 logout 会迟到，不能把新连接踢下线。
	if req.GetConnId() != 0 && p.ConnID != req.GetConnId() {
		_ = m.Respond(&pb.Ack{Ok: false})
		return
	}

	p.Online = false
	p.OfflineAt = time.Now()
	p.Base.LastLogout = p.OfflineAt.UnixMilli()
	s.mark(p.UID, store.ModBase)

	s.lob.PublishPlayerEvent(p.UID, "logout", p.Profile(), m.Env.GetTraceId())

	// 下线不立刻卸载：保留 5~10 分钟供断线重连、离线结算、好友查看（§10.1）。
	_ = m.Respond(&pb.Ack{Ok: true})
}

func (s *Shard) handleHeartbeat(p *Player, m *bus.Msg) {
	_ = m.Respond(&pb.HeartbeatResp{ServerTs: time.Now().UnixMilli()})
}

// ---------------------------------------------------------------------------
// 货币 / 背包（L1）
// ---------------------------------------------------------------------------

func (s *Shard) handleAddCurrency(p *Player, m *bus.Msg) {
	var req pb.AddCurrencyReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	balance, ok := p.AddCurrency(req.GetCurrency(), req.GetDelta())
	if !ok {
		_ = m.RespondErr(protocol.ErrNotEnough, "货币不足：当前 %d，需要 %d", balance, -req.GetDelta())
		return
	}
	s.mark(p.UID, store.ModBase)

	logx.Trace(m.Env.GetTraceId()).Debug("货币变更",
		"uid", p.UID, "currency", req.GetCurrency(), "delta", req.GetDelta(),
		"balance", balance, "reason", req.GetReason())

	_ = m.Respond(&pb.AddCurrencyResp{Balance: balance})
}

func (s *Shard) handleAddItem(p *Player, m *bus.Msg) {
	var req pb.AddItemReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	id, err := s.lob.node.NextID()
	if err != nil {
		// 时钟回拨等导致发不出号：绝不能用随机数兜底，直接失败（§3.6）。
		_ = m.RespondErr(protocol.ErrInternal, "ID 生成失败: %v", err)
		return
	}

	it, ok := p.AddItem(id, req.GetTplId(), req.GetCount(), time.Now())
	if !ok {
		_ = m.RespondErr(protocol.ErrBagFull, "背包已满（容量 %d）", p.Bag.GetCapacity())
		return
	}
	s.mark(p.UID, store.ModBag)
	_ = m.Respond(&pb.AddItemResp{Item: it})
}

func (s *Shard) handleUseItem(p *Player, m *bus.Msg) {
	var req pb.UseItemReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	remain, ok := p.UseItem(int64(req.GetInstanceId()), req.GetCount())
	if !ok {
		_ = m.RespondErr(protocol.ErrItemNotFound, "道具不存在或数量不足")
		return
	}
	s.mark(p.UID, store.ModBag)
	_ = m.Respond(&pb.UseItemResp{Remain: remain})
}

func (s *Shard) handleGetBag(p *Player, m *bus.Msg) {
	_ = m.Respond(&pb.GetBagResp{Bag: p.Bag})
}

// ---------------------------------------------------------------------------
// 任务（L1）
// ---------------------------------------------------------------------------

func (s *Shard) handleAcceptQuest(p *Player, m *bus.Msg) {
	var req pb.AcceptQuestReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	if q := p.FindQuest(req.GetQuestId()); q != nil {
		_ = m.RespondErr(protocol.ErrDuplicate, "任务已接取")
		return
	}
	q := &pb.Quest{
		QuestId:  req.GetQuestId(),
		State:    1,
		Target:   QuestTarget(req.GetQuestId()),
		AcceptAt: time.Now().UnixMilli(),
	}
	p.Quest.Quests = append(p.Quest.Quests, q)
	s.mark(p.UID, store.ModQuest)
	_ = m.Respond(&pb.QuestResp{Quest: q})
}

func (s *Shard) handleQuestProgress(p *Player, m *bus.Msg) {
	var req pb.QuestProgressReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	q := p.FindQuest(req.GetQuestId())
	if q == nil {
		_ = m.RespondErr(protocol.ErrNotFound, "任务未接取")
		return
	}
	if q.GetState() == 1 {
		q.Progress += req.GetDelta()
		if q.Target > 0 && q.Progress >= q.Target {
			q.Progress = q.Target
			q.State = 2 // 可领奖
		}
		s.mark(p.UID, store.ModQuest)
	}
	_ = m.Respond(&pb.QuestResp{Quest: q})
}

// QuestTarget 返回任务目标值。真实项目应查配置表。
func QuestTarget(id uint32) int64 { return int64(10 + id%10) }

// ---------------------------------------------------------------------------
// 社交（L2）
// ---------------------------------------------------------------------------

func (s *Shard) handleGetSocial(p *Player, m *bus.Msg) {
	_ = m.Respond(&pb.GetSocialResp{Social: p.Social})
}

func (s *Shard) handleAddFriend(p *Player, m *bus.Msg) {
	var req pb.AddFriendReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	target := req.GetTarget()
	if target == 0 || target == p.UID {
		_ = m.RespondErr(protocol.ErrBadRequest, "好友 uid 非法")
		return
	}
	if p.HasFriend(target) {
		_ = m.RespondErr(protocol.ErrDuplicate, "已经是好友")
		return
	}

	// 对方可能在别的分片、甚至不在线：这里只加自己这一侧，
	// 对方那一侧走 job 中转层由其 owner 分片处理，绝不直接操作对方内存（§8）。
	p.Social.Friends = append(p.Social.Friends, target)
	s.mark(p.UID, store.ModSocial)

	s.lob.notifyFriendAdded(p.UID, target, m.Env.GetTraceId())
	_ = m.Respond(&pb.Ack{Ok: true})
}

// handleGetProfile 读只读摘要。
//
// 不唤醒目标玩家对象、不经过其 owner，直接读 Redis（§6.5）。
// 这是纯读操作，因此可以在独立 goroutine 完成并直接回包，不占用 Actor。
func (s *Shard) handleGetProfile(m *bus.Msg) {
	var req pb.GetProfileReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	reader := s.lob.profiles
	uids := req.GetUids()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		list, err := reader.GetMany(ctx, uids)
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "读取摘要失败")
			return
		}
		_ = m.Respond(&pb.GetProfileResp{Profiles: list})
	}()
}

// ---------------------------------------------------------------------------
// 邮件
// ---------------------------------------------------------------------------

func (s *Shard) handleGetMail(p *Player, m *bus.Msg) {
	_ = m.Respond(&pb.GetMailResp{Mail: p.Mail})
}

// handleClaimMail 领取附件。附件是资产，走 L0 写穿 + 订单幂等（§6.2）。
func (s *Shard) handleClaimMail(p *Player, m *bus.Msg) {
	var req pb.ClaimMailReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	mail := p.FindMail(req.GetMailId())
	if mail == nil {
		_ = m.RespondErr(protocol.ErrNotFound, "邮件不存在")
		return
	}
	if mail.GetClaimed() {
		_ = m.RespondErr(protocol.ErrDuplicate, "附件已领取")
		return
	}

	// 在副本上结算，落盘成功后才替换内存。
	bagCopy := proto.Clone(p.Bag).(*pb.PlayerBag)
	mailCopy := proto.Clone(p.Mail).(*pb.PlayerMail)
	tmp := &Player{UID: p.UID, Bag: bagCopy, Base: p.Base}

	now := time.Now()
	granted := make([]*pb.Item, 0, len(mail.GetAttachments()))
	for _, att := range mail.GetAttachments() {
		id, err := s.lob.node.NextID()
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "ID 生成失败: %v", err)
			return
		}
		it, ok := tmp.AddItem(id, att.GetTplId(), att.GetCount(), now)
		if !ok {
			_ = m.RespondErr(protocol.ErrBagFull, "背包空间不足，无法领取附件")
			return
		}
		granted = append(granted, it)
	}
	for _, mm := range mailCopy.GetMails() {
		if mm.GetMailId() == req.GetMailId() {
			mm.Claimed = true
			mm.Read = true
		}
	}

	keys := s.lob.node.Keys
	bagBlob, err1 := proto.Marshal(bagCopy)
	mailBlob, err2 := proto.Marshal(mailCopy)
	if err1 != nil || err2 != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	kv := map[string][]byte{
		keys.Player(p.UID, store.ModBag):  bagBlob,
		keys.Player(p.UID, store.ModMail): mailBlob,
	}

	resp := &pb.ClaimMailResp{Items: granted}
	payload, _ := proto.Marshal(resp)
	orderKey := keys.Order(p.UID, fmt.Sprintf("mail%d", req.GetMailId()))

	s.writeThrough(p, orderKey, payload, kv, func(res *store.WriteThroughResult, err error) {
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "领取失败: %v", err)
			return
		}
		if res.Duplicate {
			out := &pb.ClaimMailResp{}
			_ = proto.Unmarshal(res.Payload, out)
			_ = m.Respond(out)
			return
		}
		// 落盘成功，替换内存。
		p.Bag = bagCopy
		p.Mail = mailCopy
		_ = m.Respond(resp)
	})
}

// handleApplyMail 应用 job 投递来的邮件（可能来自其他分片或系统）。
func (s *Shard) handleApplyMail(p *Player, m *bus.Msg) {
	var job pb.MailSendJob
	if err := bus.Unpack(m.Env, &job); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析发信任务失败")
		return
	}
	mail := job.GetMail()
	if mail == nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "邮件为空")
		return
	}

	rec := &xtx.Record{
		TxID:      job.GetTxid(),
		ToUID:     p.UID,
		FromShard: s.o.Shard,
		ToShard:   s.o.Shard,
		Reason:    "mail",
	}

	mailCopy := proto.Clone(p.Mail).(*pb.PlayerMail)
	if mail.GetMailId() == 0 {
		id, err := s.lob.node.NextID()
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "ID 生成失败")
			return
		}
		mail.MailId = id
	}
	mailCopy.Mails = append(mailCopy.Mails, mail)

	blob, err := proto.Marshal(mailCopy)
	if err != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	kv := map[string][]byte{s.lob.node.Keys.Player(p.UID, store.ModMail): blob}

	s.claimAsync(p, rec, kv, m, func(applied bool) {
		if applied {
			p.Mail = mailCopy
			s.lob.pushToPlayer(p, protocol.PushMailNew, mail, m.Env.GetTraceId())
		}
	})
}

// drainPendingMail 消费离线期间写入待领取队列的邮件（§6.5）。
func (s *Shard) drainPendingMail(uid uint64) {
	key := s.lob.node.Keys.MailPending(uid)
	rdb := s.lob.node.Redis
	rt := s.svc.Runtime()
	sh := s.o.Shard

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		raws, err := rdb.LRange(ctx, key, 0, 199).Result()
		if err != nil || len(raws) == 0 {
			return
		}
		mails := make([]*pb.Mail, 0, len(raws))
		for _, raw := range raws {
			mail := &pb.Mail{}
			if proto.Unmarshal([]byte(raw), mail) == nil {
				mails = append(mails, mail)
			}
		}
		if err := rdb.LTrim(ctx, key, int64(len(raws)), -1).Err(); err != nil {
			logx.Warn("裁剪待领取邮件队列失败", "uid", uid, "err", err)
			return
		}

		_ = rt.Do(sh, func() {
			p, ok := s.players[uid]
			if !ok {
				return
			}
			p.Mail.Mails = append(p.Mail.Mails, mails...)
			s.mark(uid, store.ModMail)
			logx.Info("消费离线待领取邮件", "uid", uid, "count", len(mails))
		})
	}()
}

// ---------------------------------------------------------------------------
// L0 写穿：充值
// ---------------------------------------------------------------------------

// handlePurchase 是 L0 写穿的样板：同步落 Redis 成功后才改内存回包，
// 幂等由客户端唯一订单号保证，重复提交返回首次结果（§6.2）。
func (s *Shard) handlePurchase(p *Player, m *bus.Msg) {
	var req pb.PurchaseReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	if req.GetOrderId() == "" {
		_ = m.RespondErr(protocol.ErrBadRequest, "缺少订单号：L0 操作必须幂等")
		return
	}

	currency, amount := ProductReward(req.GetProduct(), req.GetAmount())
	if amount <= 0 {
		_ = m.RespondErr(protocol.ErrBadRequest, "商品配置非法")
		return
	}

	// 在副本上算出结果，Redis 确认后才替换内存。
	baseCopy := proto.Clone(p.Base).(*pb.PlayerBase)
	if baseCopy.Currency == nil {
		baseCopy.Currency = map[uint32]int64{}
	}
	baseCopy.Currency[currency] += amount
	balance := baseCopy.Currency[currency]

	blob, err := proto.Marshal(baseCopy)
	if err != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	keys := s.lob.node.Keys
	kv := map[string][]byte{keys.Player(p.UID, store.ModBase): blob}

	resp := &pb.PurchaseResp{Balance: balance}
	payload, _ := proto.Marshal(resp)

	s.writeThrough(p, keys.Order(p.UID, req.GetOrderId()), payload, kv,
		func(res *store.WriteThroughResult, err error) {
			if err != nil {
				_ = m.RespondErr(protocol.ErrInternal, "充值失败: %v", err)
				return
			}
			if res.Duplicate {
				out := &pb.PurchaseResp{}
				_ = proto.Unmarshal(res.Payload, out)
				out.Duplicate = true
				logx.Trace(m.Env.GetTraceId()).Info("重复订单，返回首次结果",
					"uid", p.UID, "order", req.GetOrderId(), "balance", out.GetBalance())
				_ = m.Respond(out)
				return
			}
			p.Base = baseCopy
			logx.Trace(m.Env.GetTraceId()).Info("充值到账",
				"uid", p.UID, "order", req.GetOrderId(), "currency", currency,
				"amount", amount, "balance", balance)
			_ = m.Respond(resp)
		})
}

// ProductReward 返回商品对应的货币与数量。真实项目应查配置表。
func ProductReward(product uint32, amount int64) (uint32, int64) {
	switch product {
	case 1:
		return uint32(protocol.CurrencyDiamond), 60
	case 2:
		return uint32(protocol.CurrencyDiamond), 300
	case 3:
		return uint32(protocol.CurrencyGold), 10000
	default:
		if amount > 0 {
			return uint32(protocol.CurrencyDiamond), amount
		}
		return 0, 0
	}
}

// ---------------------------------------------------------------------------
// 战斗结算
// ---------------------------------------------------------------------------

// handleBattleSettle 写回战斗奖励。以 roomID 作为幂等键，
// 房间重发结算不会重复发奖。
func (s *Shard) handleBattleSettle(p *Player, m *bus.Msg) {
	var res pb.BattleResult
	if err := bus.Unpack(m.Env, &res); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析结算失败")
		return
	}

	score := res.GetScore()[p.UID]
	win := false
	for _, w := range res.GetWinners() {
		if w == p.UID {
			win = true
			break
		}
	}
	gold := int64(100) + score
	exp := int64(50) + score/2
	if win {
		gold *= 2
		exp *= 2
	}

	baseCopy := proto.Clone(p.Base).(*pb.PlayerBase)
	if baseCopy.Currency == nil {
		baseCopy.Currency = map[uint32]int64{}
	}
	baseCopy.Currency[uint32(protocol.CurrencyGold)] += gold
	baseCopy.Exp += uint64(exp)
	leveledUp := false
	for baseCopy.Exp >= LevelUpExp(baseCopy.Level) {
		baseCopy.Exp -= LevelUpExp(baseCopy.Level)
		baseCopy.Level++
		leveledUp = true
	}

	blob, err := proto.Marshal(baseCopy)
	if err != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	keys := s.lob.node.Keys
	kv := map[string][]byte{keys.Player(p.UID, store.ModBase): blob}

	resp := &pb.AddCurrencyResp{Balance: baseCopy.Currency[uint32(protocol.CurrencyGold)]}
	payload, _ := proto.Marshal(resp)
	orderKey := keys.Order(p.UID, fmt.Sprintf("battle%d", res.GetRoomId()))

	s.writeThrough(p, orderKey, payload, kv, func(wt *store.WriteThroughResult, err error) {
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "结算失败: %v", err)
			return
		}
		if wt.Duplicate {
			out := &pb.AddCurrencyResp{}
			_ = proto.Unmarshal(wt.Payload, out)
			_ = m.Respond(out)
			return
		}
		p.Base = baseCopy
		if leveledUp {
			s.lob.PublishPlayerEvent(p.UID, "levelup", p.Profile(), m.Env.GetTraceId())
		}
		_ = m.Respond(resp)
	})
}

// LevelUpExp 返回升到下一级所需经验。
func LevelUpExp(level uint32) uint64 { return uint64(100 * (level + 1)) }

// ---------------------------------------------------------------------------
// 跨分片转移（§8）
// ---------------------------------------------------------------------------

// handleTransfer 是发起方：扣道具（内存）→ 写穿 tx 记录 → 投递 job。
func (s *Shard) handleTransfer(p *Player, m *bus.Msg) {
	var req pb.TransferJob
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	if req.GetToUid() == 0 || req.GetToUid() == p.UID {
		_ = m.RespondErr(protocol.ErrBadRequest, "目标 uid 非法")
		return
	}

	// —— 扣除（内存）——
	// 「发起方先扣除并写穿，确保资源不会凭空增加」。
	bagCopy := proto.Clone(p.Bag).(*pb.PlayerBag)
	baseCopy := proto.Clone(p.Base).(*pb.PlayerBase)
	tmp := &Player{UID: p.UID, Bag: bagCopy, Base: baseCopy}

	for _, att := range req.GetItems() {
		if !tmp.RemoveItemByTpl(att.GetTplId(), att.GetCount()) {
			_ = m.RespondErr(protocol.ErrNotEnough, "道具 %d 不足", att.GetTplId())
			return
		}
	}
	if req.GetCurrencyDelta() > 0 {
		if _, ok := tmp.AddCurrency(req.GetCurrencyType(), -req.GetCurrencyDelta()); !ok {
			_ = m.RespondErr(protocol.ErrNotEnough, "货币不足")
			return
		}
	}

	sf, err := s.lob.node.NextID()
	if err != nil {
		_ = m.RespondErr(protocol.ErrInternal, "ID 生成失败: %v", err)
		return
	}
	txid := xtx.NewTxID(sf)

	rec := &xtx.Record{
		TxID:      txid,
		FromUID:   p.UID,
		ToUID:     req.GetToUid(),
		FromShard: s.o.Shard,
		ToShard:   s.lob.ShardOf(req.GetToUid()),
		Items:     req.GetItems(),
		CurrType:  req.GetCurrencyType(),
		CurrDelta: req.GetCurrencyDelta(),
		Reason:    req.GetReason(),
		State:     xtx.StatePending,
		CreatedAt: time.Now().UnixMilli(),
	}

	keys := s.lob.node.Keys
	bagBlob, e1 := proto.Marshal(bagCopy)
	baseBlob, e2 := proto.Marshal(baseCopy)
	if e1 != nil || e2 != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	kv := map[string][]byte{
		keys.Player(p.UID, store.ModBag):  bagBlob,
		keys.Player(p.UID, store.ModBase): baseBlob,
	}

	p.busy = true
	uid := p.UID
	sh := s.o.Shard
	epoch := s.o.Epoch
	rt := s.svc.Runtime()
	traceID := m.Env.GetTraceId()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		beginErr := s.lob.txm.Begin(ctx, epoch, rec, kv)
		var pubErr error
		if beginErr == nil {
			// 写穿成功后才投递 job。反过来会出现「job 已到、扣减未落盘」，
			// 崩溃后资源凭空增加。
			pubErr = s.lob.publishTransfer(ctx, rec, traceID)
		}

		_ = rt.Do(sh, func() {
			pl, ok := s.players[uid]
			if ok {
				pl.busy = false
			}
			defer func() {
				if ok {
					s.drainWaiters(uid)
				}
			}()

			if beginErr != nil {
				if errors.Is(beginErr, store.ErrFenced) {
					s.svc.Claimer().Fence(sh)
					return
				}
				// 写穿失败：内存未改动过（我们改的是副本），天然回滚。
				_ = m.RespondErr(protocol.ErrInternal, "发起转移失败: %v", beginErr)
				return
			}

			// 扣减已落盘，替换内存。
			if ok {
				pl.Bag = bagCopy
				pl.Base = baseCopy
			}

			if pubErr != nil {
				// 记录已是 PENDING，补偿扫描器会重投，因此这里回成功是安全的。
				logx.Trace(traceID).Warn("投递转移任务失败，交由补偿扫描器重投",
					"txid", txid, "err", pubErr)
			}
			_ = m.Respond(&pb.Ack{Ok: true})
		})
	}()
}

// handleApplyTransfer 是接收方：SET tx:{txid}:done NX（幂等）→ 加道具（内存）→ 标记 COMPLETED。
func (s *Shard) handleApplyTransfer(p *Player, m *bus.Msg) {
	var job pb.TransferJob
	if err := bus.Unpack(m.Env, &job); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析转移任务失败")
		return
	}

	rec := &xtx.Record{
		TxID:      job.GetTxid(),
		FromUID:   job.GetFromUid(),
		ToUID:     p.UID,
		FromShard: s.lob.ShardOf(job.GetFromUid()),
		ToShard:   s.o.Shard,
		Items:     job.GetItems(),
		CurrType:  job.GetCurrencyType(),
		CurrDelta: job.GetCurrencyDelta(),
		Reason:    job.GetReason(),
		CreatedAt: job.GetCreatedAt(),
	}
	if job.GetFromUid() == 0 {
		rec.FromShard = s.o.Shard // 系统发放
	}

	bagCopy := proto.Clone(p.Bag).(*pb.PlayerBag)
	baseCopy := proto.Clone(p.Base).(*pb.PlayerBase)
	tmp := &Player{UID: p.UID, Bag: bagCopy, Base: baseCopy}

	now := time.Now()
	for _, att := range job.GetItems() {
		id, err := s.lob.node.NextID()
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "ID 生成失败")
			return
		}
		if _, ok := tmp.AddItem(id, att.GetTplId(), att.GetCount(), now); !ok {
			// 背包满：不能默默丢弃资产，转投待领取邮件队列。
			s.lob.fallbackToMailbox(p.UID, &job, m.Env.GetTraceId())
			_ = m.Respond(&pb.Ack{Ok: true})
			return
		}
	}
	if job.GetCurrencyDelta() > 0 {
		tmp.AddCurrency(job.GetCurrencyType(), job.GetCurrencyDelta())
	}

	keys := s.lob.node.Keys
	bagBlob, e1 := proto.Marshal(bagCopy)
	baseBlob, e2 := proto.Marshal(baseCopy)
	if e1 != nil || e2 != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	kv := map[string][]byte{
		keys.Player(p.UID, store.ModBag):  bagBlob,
		keys.Player(p.UID, store.ModBase): baseBlob,
	}

	s.claimAsync(p, rec, kv, m, func(applied bool) {
		if applied {
			p.Bag = bagCopy
			p.Base = baseCopy
			s.lob.pushToPlayer(p, protocol.PushItemChanged, &pb.GetBagResp{Bag: p.Bag}, m.Env.GetTraceId())
		}
	})
}

// claimAsync 执行接收方的幂等入账，完成后在 Actor 内回调。
func (s *Shard) claimAsync(p *Player, rec *xtx.Record, kv map[string][]byte, m *bus.Msg, apply func(applied bool)) {
	p.busy = true
	uid := p.UID
	sh := s.o.Shard
	epoch := s.o.Epoch
	rt := s.svc.Runtime()
	traceID := m.Env.GetTraceId()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		applied, err := s.lob.txm.Claim(ctx, epoch, rec, kv)
		if err == nil {
			// 标记 COMPLETED 并移出 PENDING 索引。失败也无妨：
			// done 标记已在，重投会被幂等拦下，扫描器最多多扫一次。
			if cerr := s.lob.txm.Complete(ctx, rec); cerr != nil {
				logx.Trace(traceID).Warn("标记转移完成失败", "txid", rec.TxID, "err", cerr)
			}
		}

		_ = rt.Do(sh, func() {
			pl, ok := s.players[uid]
			if ok {
				pl.busy = false
			}
			defer func() {
				if ok {
					s.drainWaiters(uid)
				}
			}()

			if err != nil {
				if errors.Is(err, store.ErrFenced) {
					s.svc.Claimer().Fence(sh)
					return
				}
				// 回错误让 job 重投。接收方幂等，重复投递安全。
				_ = m.RespondErr(protocol.ErrInternal, "入账失败: %v", err)
				return
			}
			if applied && ok {
				apply(true)
			}
			_ = m.Respond(&pb.Ack{Ok: true})
		})
	}()
}

// pushOfflineOrOnline 决定推送方式。
func (s *Shard) online(p *Player) bool { return p.Online && p.GateID != "" }

var _ = subject.Broadcast
