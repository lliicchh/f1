package lobby

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/internal/slots"
	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/jackpot"
	"github.com/gamedev/f1/pkg/ledger"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/rg"
	"github.com/gamedev/f1/pkg/rng"
	"github.com/gamedev/f1/pkg/store"
)

// handleRoundState 返回未结算的回合，供断线重连恢复（评审 P1-1）。
func (s *Shard) handleRoundState(p *Player, m *bus.Msg) {
	resp := &pb.RoundStateResp{}
	if p.Round != nil {
		resp.Round = p.Round
		resp.HasOpen = true
	}
	_ = m.Respond(resp)
}

// handleSpin 是 slots 的核心：一次旋转。
//
// 这是整个服务里最需要小心的一段代码，因为它同时是：
//   - 一笔金融事务（扣注、派彩）
//   - 一个可中断的状态机（免费旋转）
//   - 一条必须能事后复算的审计记录
//
// 因此它的每一步都遵循同一个模式：
//
//  1. 先做所有拒绝性检查（限额、档位、回合冲突）—— 拒绝不产生任何副作用
//  2. 在**副本**上算出结果
//  3. 一次原子提交：余额 + 回合 + 限额 + 流水
//  4. 提交成功后才改内存、才回包
//
// 任何把顺序打乱的改动都会引入资损或对账差异。
func (s *Shard) handleSpin(p *Player, m *bus.Msg) {
	var req pb.SpinReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	conf := s.lob.conf.Get()
	now := time.Now()

	// ---- 1. 拒绝性检查 ----

	gameID := req.GetGameId()
	machine, ok := conf.Machine(gameID)
	if !ok {
		_ = m.RespondErr(protocol.ErrGameNotFound, "未知游戏 %q", gameID)
		return
	}

	free := p.Round != nil && p.Round.GetFreeSpinsLeft() > 0
	if p.Round != nil && !free {
		// 有回合但没有免费旋转次数：说明上一局没走完结算流程，先收尾。
		// 直接开新局会把上一局的派彩吞掉。
		_ = m.RespondErr(protocol.ErrRoundOpen, "存在未结算的回合 %d", p.Round.GetRoundId())
		return
	}
	if free && p.Round.GetGameId() != gameID {
		_ = m.RespondErr(protocol.ErrRoundOpen, "免费旋转必须在同一台机器上完成")
		return
	}

	bet := req.GetBet()
	currency := machine.Currency
	if free {
		// 免费旋转沿用触发时的投注额与币种，客户端说了不算。
		bet = p.Round.GetBet()
		currency = p.Round.GetCurrency()
	} else {
		if !machine.ValidBet(bet) {
			_ = m.RespondErr(protocol.ErrBetInvalid, "下注额 %d 不在允许档位内", bet)
			return
		}
		if p.Currency(currency) < bet {
			_ = m.RespondErr(protocol.ErrNotEnough, "余额不足：当前 %d，需要 %d", p.Currency(currency), bet)
			return
		}

		// 责任游戏限额：只对付费旋转生效（免费旋转没有投注）。
		rgCfg := conf.RG
		rg.Rollover(p.RG, now, rgCfg)
		if d := rg.CheckBet(p.RG, bet, now, rgCfg); !d.Allowed {
			metrics.RGBlocked.WithLabelValues(fmt.Sprint(d.Code)).Inc()
			code := protocol.ErrRGLimit
			if d.Code == rg.ReasonSelfExcluded {
				code = protocol.ErrSelfExcluded
			}
			_ = m.RespondErr(code, "%s", d.Reason)
			return
		}
	}

	// ---- 2. 在副本上算结果 ----

	round := p.Round
	var src *rng.Source
	if free {
		// 免费旋转沿用回合的种子链：整局只有一颗种子，复算时一次重放到底。
		seed, err := rng.ParseSeed(round.GetSeedHex())
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "回合种子损坏")
			return
		}
		src = rng.New(seed)
		// 重放到当前进度，让随机流对齐。
		if err := s.fastForward(machine, src, round); err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "回合重放失败: %v", err)
			return
		}
	} else {
		newSrc, err := rng.NewRandom()
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "生成随机种子失败")
			return
		}
		src = newSrc

		roundID, err := s.lob.node.NextID()
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "生成回合号失败: %v", err)
			return
		}
		round = &pb.Round{
			RoundId:       roundID,
			GameId:        gameID,
			ConfigVersion: conf.Version(),
			SeedHex:       src.SeedHex(),
			Currency:      currency,
			Bet:           bet,
			Multiplier:    1,
			State:         protocol.RoundOpen,
			CreatedAt:     now.UnixMilli(),
		}
	}

	mult := uint32(1)
	if free {
		mult = round.GetMultiplier()
		if mult == 0 {
			mult = 1
		}
	}

	result, err := slots.Spin(machine, src, bet, mult, free)
	if err != nil {
		_ = m.RespondErr(protocol.ErrInternal, "旋转失败: %v", err)
		return
	}

	// 副本上推进回合状态。
	next := proto.Clone(round).(*pb.Round)
	next.SpinIndex++
	next.TotalWin += result.TotalWin
	if free && next.FreeSpinsLeft > 0 {
		next.FreeSpinsLeft--
	}
	if result.FreeSpinsAwarded > 0 {
		next.FreeSpinsLeft += result.FreeSpinsAwarded
		next.FreeSpinsTotal += result.FreeSpinsAwarded
		next.Multiplier = machine.FreeSpinMultiplier
		if next.Multiplier == 0 {
			next.Multiplier = 1
		}
	}

	// 副本上算余额。
	baseCopy := proto.Clone(p.Base).(*pb.PlayerBase)
	if baseCopy.Currency == nil {
		baseCopy.Currency = map[uint32]int64{}
	}
	entries := make([]*ledger.Entry, 0, 4)
	confVer := conf.Version()

	if !free {
		baseCopy.Currency[currency] -= bet
		if baseCopy.Currency[currency] < 0 {
			_ = m.RespondErr(protocol.ErrNotEnough, "余额不足")
			return
		}
		entries = append(entries, s.entry(p.UID, ledger.TypeBet, currency, -bet,
			baseCopy.Currency[currency]).WithRound(next.RoundId, gameID, confVer))
	}

	if result.TotalWin > 0 {
		baseCopy.Currency[currency] += result.TotalWin
		entries = append(entries, s.entry(p.UID, ledger.TypeWin, currency, result.TotalWin,
			baseCopy.Currency[currency]).WithRound(next.RoundId, gameID, confVer))
	}

	// 奖池注入：从投注里切一小块。它也要进流水，
	// 否则「投注额」与「玩家实际支出」对不上，且奖池差额无从追溯。
	var contrib int64
	if !free && machine.JackpotPool != "" {
		contrib = jackpot.ContributionOf(bet, machine.JackpotContribBP)
		if contrib > 0 {
			entries = append(entries, s.entry(p.UID, ledger.TypeJackpotContrib, currency, 0,
				baseCopy.Currency[currency]).
				WithRound(next.RoundId, gameID, confVer).
				WithRef(fmt.Sprintf("pool=%s;amount=%d", machine.JackpotPool, contrib)))
		}
	}

	// 限额累计（副本）。
	rgCopy := proto.Clone(p.RG).(*pb.PlayerRG)
	if !free {
		rg.ApplyBet(rgCopy, bet)
	}
	rg.ApplyWin(rgCopy, result.TotalWin)

	// 回合是否结束。
	finished := next.FreeSpinsLeft == 0
	if finished {
		next.State = protocol.RoundSettled
		next.SettledAt = now.UnixMilli()
	}

	// ---- 3. 原子提交 ----

	keys := s.lob.node.Keys
	baseBlob, e1 := proto.Marshal(baseCopy)
	rgBlob, e2 := proto.Marshal(rgCopy)
	if e1 != nil || e2 != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	var roundBlob []byte
	if finished {
		// 结算后清掉回合，避免下次加载把它当成未完成的局恢复出来。
		roundBlob = []byte{}
	} else {
		roundBlob, err = proto.Marshal(next)
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "序列化回合失败")
			return
		}
	}

	kv := map[string][]byte{
		keys.Player(p.UID, store.ModBase):  baseBlob,
		keys.Player(p.UID, store.ModRG):    rgBlob,
		keys.Player(p.UID, store.ModRound): roundBlob,
	}

	resp := &pb.SpinResp{
		Round:            next,
		Grid:             result.Grid.Flat(),
		Stops:            toUint32(result.Stops),
		Lines:            toLineWins(result.Lines),
		ScatterCount:     uint32(result.Scatter),
		SpinWin:          result.TotalWin,
		Balance:          baseCopy.Currency[currency],
		FreeSpinsAwarded: result.FreeSpinsAwarded,
		RoundFinished:    finished,
	}
	payload, _ := proto.Marshal(resp)

	// 幂等键：客户端重发同一次 spin 不得重复扣费。
	// 免费旋转用回合号 + 旋转序号，天然唯一；付费旋转用客户端提供的 client_id。
	idem := req.GetClientId()
	if idem == "" || free {
		idem = fmt.Sprintf("spin%d-%d", next.RoundId, next.SpinIndex)
	}

	s.commit(p, CommitSpec{
		Op:      "spin",
		IdemKey: keys.Order(p.UID, idem),
		Payload: payload,
		Entries: entries,
		KV:      kv,
	}, func(res *store.CommitResult, cerr error) {
		if cerr != nil {
			_ = m.RespondErr(protocol.ErrInternal, "旋转提交失败: %v", cerr)
			return
		}
		if res.Duplicate {
			out := &pb.SpinResp{}
			_ = proto.Unmarshal(res.Payload, out)
			out.Duplicate = true
			_ = m.Respond(out)
			return
		}

		// ---- 4. 落盘成功，换进内存 ----
		hadRound := p.Round != nil
		p.Base = baseCopy
		p.RG = rgCopy
		if finished {
			p.Round = nil
			if hadRound || !free {
				metrics.RoundsOpen.Dec()
			}
		} else {
			if p.Round == nil {
				metrics.RoundsOpen.Inc()
			}
			p.Round = next
		}

		kind := "paid"
		if free {
			kind = "free"
		}
		metrics.Spins.WithLabelValues(gameID, kind).Inc()
		if !free {
			metrics.BetAmount.WithLabelValues(gameID, confVer).Add(float64(bet))
		}
		metrics.WinAmount.WithLabelValues(gameID, confVer).Add(float64(result.TotalWin))
		if bet > 0 {
			metrics.SpinWinRatio.WithLabelValues(gameID).Observe(float64(result.TotalWin) / float64(bet))
		}

		_ = m.Respond(resp)

		// 提交之后的收尾动作都是「best effort」：它们失败不影响玩家的钱，
		// 且都有流水或待处理索引可以追溯补偿。
		if contrib > 0 {
			s.lob.contributeJackpot(machine.JackpotPool, contrib)
		}
		if result.JackpotHit {
			s.lob.claimJackpot(machine.JackpotPool, p.UID, next.RoundId, m.Env.GetTraceId())
		}
		if d := rg.CheckBet(p.RG, 0, now, conf.RG); d.Notice != "" {
			rg.MarkNotice(p.RG, now)
			s.mark(p.UID, store.ModRG)
			s.lob.pushToPlayer(p, protocol.PushRGNotice,
				&pb.RGStatusResp{Reason: d.Notice}, m.Env.GetTraceId())
		}
	})
}

// fastForward 把随机流重放到回合当前进度。
//
// 免费旋转必须与主旋转共用一条随机流，否则「同一颗种子重放整局」就不成立，
// 复算能力也就没了。代价是每次免费旋转要重放前面若干次 ——
// 一局最多几十次旋转，纯内存计算，开销可以忽略。
func (s *Shard) fastForward(m *gameconf.SlotMachine, src *rng.Source, round *pb.Round) error {
	var freeLeft, mult uint32
	mult = 1
	for i := uint32(0); i < round.GetSpinIndex(); i++ {
		free := freeLeft > 0
		sm := uint32(1)
		if free {
			sm = mult
			freeLeft--
		}
		r, err := slots.Spin(m, src, round.GetBet(), sm, free)
		if err != nil {
			return err
		}
		if r.FreeSpinsAwarded > 0 {
			freeLeft += r.FreeSpinsAwarded
			if m.FreeSpinMultiplier > 0 {
				mult = m.FreeSpinMultiplier
			}
		}
	}
	return nil
}

// entry 构造一条带流水号的账目。
func (s *Shard) entry(uid uint64, t ledger.Type, currency uint32, amount, balance int64) *ledger.Entry {
	e := ledger.New(uid, t, currency, amount, balance)
	if id, err := s.lob.node.NextID(); err == nil {
		e.WithID(id)
	}
	return e
}

// handleJackpotInfo 返回奖池水位。只读投影，不经过 World。
func (s *Shard) handleJackpotInfo(m *bus.Msg) {
	var req pb.JackpotInfoReq
	_ = bus.Unpack(m.Env, &req)
	pool := req.GetPoolId()
	if pool == "" {
		pool = "grand"
	}
	jp := s.lob.jackpot
	if jp == nil {
		_ = m.RespondErr(protocol.ErrNotFound, "未配置奖池")
		return
	}
	cfg, ok := jp.Pool(pool)
	if !ok {
		_ = m.RespondErr(protocol.ErrNotFound, "未知奖池 %q", pool)
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		amount, err := jp.Amount(ctx, pool)
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "读取奖池失败")
			return
		}
		_ = m.Respond(&pb.JackpotInfoResp{PoolId: pool, Amount: amount, Seed: cfg.Seed})
	}()
}

// handleRGStatus 返回责任游戏限额状态。
func (s *Shard) handleRGStatus(p *Player, m *bus.Msg) {
	conf := s.lob.conf.Get()
	_ = m.Respond(rg.Status(p.RG, time.Now(), conf.RG))
}

// handleApplyJackpot 是奖池派彩的入账端（内部命令）。
//
// 与跨分片转移同一套幂等机制：txid 去重，重复投递不会重复入账。
func (s *Shard) handleApplyJackpot(p *Player, m *bus.Msg) {
	var req pb.JackpotClaimReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	amount := req.GetAmount()
	if amount <= 0 {
		_ = m.RespondErr(protocol.ErrBadRequest, "派彩金额非法")
		return
	}

	conf := s.lob.conf.Get()
	pool := req.GetPoolId()
	currency := uint32(protocol.CurrencyGold)
	if cfg, ok := conf.Jackpots[pool]; ok {
		currency = cfg.Currency
	}

	baseCopy := proto.Clone(p.Base).(*pb.PlayerBase)
	if baseCopy.Currency == nil {
		baseCopy.Currency = map[uint32]int64{}
	}
	baseCopy.Currency[currency] += amount

	entries := []*ledger.Entry{
		s.entry(p.UID, ledger.TypeJackpotWin, currency, amount, baseCopy.Currency[currency]).
			WithRef(fmt.Sprintf("pool=%s;txid=%s", pool, req.GetTxid())),
	}

	blob, err := proto.Marshal(baseCopy)
	if err != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	kv := map[string][]byte{s.lob.node.Keys.Player(p.UID, store.ModBase): blob}

	resp := &pb.JackpotClaimResp{Amount: amount, Won: true}
	payload, _ := proto.Marshal(resp)

	s.commit(p, CommitSpec{
		Op:      "jackpot",
		IdemKey: s.lob.node.Keys.Order(p.UID, "jp-"+req.GetTxid()),
		Payload: payload,
		Entries: entries,
		KV:      kv,
	}, func(res *store.CommitResult, cerr error) {
		if cerr != nil {
			_ = m.RespondErr(protocol.ErrInternal, "奖池入账失败: %v", cerr)
			return
		}
		if !res.Duplicate {
			p.Base = baseCopy
			logx.Trace(m.Env.GetTraceId()).Info("奖池派彩到账",
				"uid", p.UID, "pool", pool, "amount", amount, "txid", req.GetTxid())
		}
		_ = m.Respond(resp)
	})
}

func toUint32(in []int) []uint32 {
	out := make([]uint32, len(in))
	for i, v := range in {
		out[i] = uint32(v)
	}
	return out
}

func toLineWins(in []slots.LineWin) []*pb.LineWin {
	out := make([]*pb.LineWin, 0, len(in))
	for _, w := range in {
		out = append(out, &pb.LineWin{
			Line: uint32(w.Line), Symbol: uint32(w.Symbol),
			Count: uint32(w.Count), Payout: w.Payout,
		})
	}
	return out
}
