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

// handleRoundState 返回未结算的回合，给断线重连用
func (s *Shard) handleRoundState(p *Player, m *bus.Msg) {
	resp := &pb.RoundStateResp{}
	if p.Round != nil {
		resp.Round = p.Round
		resp.HasOpen = true
	}
	_ = m.Respond(resp)
}

// handleSpin 转一次，顺序别改
//
//  1. 检查
//  2. 在副本上算结果
//  3. 余额、回合、限额、流水一起落库
//  4. 提交成功之后才改内存并回包
func (s *Shard) handleSpin(p *Player, m *bus.Msg) {
	var req pb.SpinReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	conf := s.lob.conf.Get()
	now := time.Now()

	// 一、先把该拒的都拒掉

	gameID := req.GetGameId()
	machine, ok := conf.Machine(gameID)
	if !ok {
		_ = m.RespondErr(protocol.ErrGameNotFound, "未知游戏 %q", gameID)
		return
	}

	free := p.Round != nil && p.Round.GetFreeSpinsLeft() > 0
	if p.Round != nil && !free {
		// 有回合但没免费次数，说明上一局没收尾。直接开新局会把上一局的派彩吞掉
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
		// 免费旋转用触发时的投注额和币种，客户端说了不算
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

		// 免费旋转没投注，限额只管付费的
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

	// 二、在副本上算

	round := p.Round
	var src *rng.Source
	if free {
		// 免费旋转接着用回合的那颗种子，整局一条随机流，复算时一次重放到底
		seed, err := rng.ParseSeed(round.GetSeedHex())
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "回合种子损坏")
			return
		}
		src = rng.New(seed)
		// 先重放到当前进度，把随机流对齐
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

	// 推进回合状态（还是副本）
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

	// 算余额（还是副本）
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

	// 从投注里切一小块进奖池。这笔也要记流水，
	// 否则投注额和玩家实际支出对不上，奖池差额也没法追
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

	// 累计限额
	rgCopy := proto.Clone(p.RG).(*pb.PlayerRG)
	if !free {
		rg.ApplyBet(rgCopy, bet)
	}
	rg.ApplyWin(rgCopy, result.TotalWin)

	// 看这局完了没
	finished := next.FreeSpinsLeft == 0
	if finished {
		next.State = protocol.RoundSettled
		next.SettledAt = now.UnixMilli()
	}

	// 三、一次提交落盘

	keys := s.lob.node.Keys
	baseBlob, e1 := proto.Marshal(baseCopy)
	rgBlob, e2 := proto.Marshal(rgCopy)
	if e1 != nil || e2 != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	var roundBlob []byte
	if finished {
		// 结算了就清掉，免得下次加载当成未完成的局恢复出来
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

	// 幂等键。免费旋转用回合号加序号，天然唯一；付费旋转用客户端给的 client_id
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

		// 四、落盘成功了，换进内存
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

		// 下面这些都是尽力而为：失败了不影响玩家的钱，也都有流水或索引可以补
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

// fastForward 把随机流重放到当前进度
//
// 免费旋转得和主旋转共用一条随机流，不然「一颗种子重放整局」就不成立了。
// 代价是每次免费旋转要把前面几次重跑一遍，一局最多几十次，纯内存计算
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

func (s *Shard) entry(uid uint64, t ledger.Type, currency uint32, amount, balance int64) *ledger.Entry {
	e := ledger.New(uid, t, currency, amount, balance)
	if id, err := s.lob.node.NextID(); err == nil {
		e.WithID(id)
	}
	return e
}

// handleJackpotInfo 返回奖池水位，直接读 Redis，不经过 World
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

// handleRGStatus 返回责任游戏限额状态
func (s *Shard) handleRGStatus(p *Player, m *bus.Msg) {
	conf := s.lob.conf.Get()
	_ = m.Respond(rg.Status(p.RG, time.Now(), conf.RG))
}

// handleApplyJackpot 奖池派彩的入账端，内部命令。
// 幂等和跨分片转移一样靠 txid 去重
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
