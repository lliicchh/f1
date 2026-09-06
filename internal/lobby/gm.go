package lobby

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/ledger"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/store"
)

// handleGMGrant 补单发放。工单号兼作幂等键，运营手抖点两下不会发两份
func (s *Shard) handleGMGrant(p *Player, m *bus.Msg) {
	var req pb.GMGrantReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}
	if req.GetTicket() == "" {
		// 没工单号就既没幂等也没审计线索
		_ = m.RespondErr(protocol.ErrBadRequest, "GM 发放必须带工单号")
		return
	}
	operator := m.Env.GetOperator()

	conf := s.lob.conf.Get()
	now := time.Now()

	baseCopy := proto.Clone(p.Base).(*pb.PlayerBase)
	bagCopy := proto.Clone(p.Bag).(*pb.PlayerBag)
	if baseCopy.Currency == nil {
		baseCopy.Currency = map[uint32]int64{}
	}

	entries := make([]*ledger.Entry, 0, len(req.GetItems())+1)

	if req.GetAmount() != 0 && req.GetCurrency() != 0 {
		cur := req.GetCurrency()
		if baseCopy.Currency[cur]+req.GetAmount() < 0 {
			_ = m.RespondErr(protocol.ErrNotEnough, "扣减后余额为负，拒绝执行")
			return
		}
		baseCopy.Currency[cur] += req.GetAmount()
		entries = append(entries, s.entry(p.UID, ledger.TypeGrant, cur,
			req.GetAmount(), baseCopy.Currency[cur]).
			WithRef(fmt.Sprintf("ticket=%s;reason=%s", req.GetTicket(), req.GetReason())).
			WithOperator(operator))
	}

	tmp := &Player{UID: p.UID, Base: baseCopy, Bag: bagCopy}
	for _, att := range req.GetItems() {
		id, err := s.lob.node.NextID()
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "生成道具 ID 失败")
			return
		}
		if _, ok := tmp.AddItem(id, att.GetTplId(), att.GetCount(), now, conf.Stackable(att.GetTplId())); !ok {
			_ = m.RespondErr(protocol.ErrBagFull, "背包空间不足")
			return
		}
		entries = append(entries, s.entry(p.UID, ledger.TypeGrant, 0, 0, 0).
			WithRef(fmt.Sprintf("ticket=%s;tpl=%d;count=%d", req.GetTicket(), att.GetTplId(), att.GetCount())).
			WithOperator(operator))
	}

	keys := s.lob.node.Keys
	baseBlob, e1 := proto.Marshal(baseCopy)
	bagBlob, e2 := proto.Marshal(bagCopy)
	if e1 != nil || e2 != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	kv := map[string][]byte{
		keys.Player(p.UID, store.ModBase): baseBlob,
		keys.Player(p.UID, store.ModBag):  bagBlob,
	}

	resp := &pb.Ack{Ok: true}
	payload, _ := proto.Marshal(resp)

	s.commit(p, CommitSpec{
		Op:      "gm_grant",
		IdemKey: keys.Order(p.UID, "gm-"+req.GetTicket()),
		Payload: payload,
		Entries: entries,
		KV:      kv,
	}, func(res *store.CommitResult, cerr error) {
		if cerr != nil {
			_ = m.RespondErr(protocol.ErrInternal, "发放失败: %v", cerr)
			return
		}
		if !res.Duplicate {
			p.Base = baseCopy
			p.Bag = bagCopy
			logx.Trace(m.Env.GetTraceId()).Warn("GM 发放已执行",
				"operator", operator, "uid", p.UID, "ticket", req.GetTicket(),
				"currency", req.GetCurrency(), "amount", req.GetAmount(),
				"items", len(req.GetItems()), "reason", req.GetReason())
		}
		_ = m.Respond(resp)
	})
}

// handleGMQuery 查玩家状态和最近流水，给客服用
func (s *Shard) handleGMQuery(p *Player, m *bus.Msg) {
	var req pb.GMQueryReq
	_ = bus.Unpack(m.Env, &req)

	limit := int64(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	base := proto.Clone(p.Base).(*pb.PlayerBase)
	var round *pb.Round
	if p.Round != nil {
		round = proto.Clone(p.Round).(*pb.Round)
	}
	reader := s.lob.ledger
	uid := p.UID
	operator := m.Env.GetOperator()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		entries, err := reader.Recent(ctx, uid, limit)
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "读取流水失败")
			return
		}
		out := &pb.GMQueryResp{Base: base, OpenRound: round}
		for _, e := range entries {
			out.Entries = append(out.Entries, e.Proto())
		}
		logx.Info("GM 查询", "operator", operator, "uid", uid, "entries", len(out.Entries))
		_ = m.Respond(out)
	}()
}

// handleGMSetRG 设限额或自我排除
//
// 自我排除设了之后连登录都拦，玩家自己解不掉
func (s *Shard) handleGMSetRG(p *Player, m *bus.Msg) {
	var req pb.GMSetRGReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	if p.RG == nil {
		p.RG = &pb.PlayerRG{Uid: p.UID}
	}
	if req.GetDailyBetLimit() >= 0 {
		p.RG.DailyBetLimit = req.GetDailyBetLimit()
	}
	if req.GetDailyLossLimit() >= 0 {
		p.RG.DailyLossLimit = req.GetDailyLossLimit()
	}
	if req.GetSessionLimit() >= 0 {
		p.RG.SessionLimit = req.GetSessionLimit()
	}
	if req.GetExcludeUntil() > 0 {
		p.RG.ExcludedUntil = req.GetExcludeUntil()
	}
	s.mark(p.UID, store.ModRG)

	logx.Trace(m.Env.GetTraceId()).Warn("GM 设置责任游戏限额",
		"operator", m.Env.GetOperator(), "uid", p.UID,
		"daily_bet", p.RG.DailyBetLimit, "daily_loss", p.RG.DailyLossLimit,
		"session", p.RG.SessionLimit, "excluded_until", p.RG.ExcludedUntil)

	_ = m.Respond(&pb.Ack{Ok: true})
}

// handleGMKick 把玩家踢下线
func (s *Shard) handleGMKick(p *Player, m *bus.Msg) {
	if !p.Online {
		_ = m.Respond(&pb.Ack{Ok: false})
		return
	}
	logx.Trace(m.Env.GetTraceId()).Warn("GM 强制下线",
		"operator", m.Env.GetOperator(), "uid", p.UID, "gate", p.GateID)

	s.lob.pushToPlayer(p, protocol.PushKick, &pb.KickNotify{
		Uid: p.UID, ConnId: p.ConnID, Message: "账号已被管理员下线",
	}, m.Env.GetTraceId())

	p.Online = false
	p.OfflineAt = time.Now()
	s.mark(p.UID, store.ModBase)
	_ = m.Respond(&pb.Ack{Ok: true})
}
