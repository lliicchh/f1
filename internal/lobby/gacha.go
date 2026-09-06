package lobby

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/ledger"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/rng"
	"github.com/gamedev/f1/pkg/store"
)

// MaxDrawsPerRequest 一次请求最多抽几次
const MaxDrawsPerRequest = 10

// handleGacha 处理抽卡
//
// 抽卡和 slots 相同：RNG、概率公示、保底。公示了就得跟实现一致，
// 所以同样记种子、走原子提交
func (s *Shard) handleGacha(p *Player, m *bus.Msg) {
	var req pb.GachaReq
	if err := bus.Unpack(m.Env, &req); err != nil {
		_ = m.RespondErr(protocol.ErrBadRequest, "解析请求失败")
		return
	}

	conf := s.lob.conf.Get()
	pool, ok := conf.Pool(req.GetPoolId())
	if !ok {
		_ = m.RespondErr(protocol.ErrGachaPool, "未知卡池 %q", req.GetPoolId())
		return
	}

	times := int(req.GetTimes())
	if times <= 0 {
		times = 1
	}
	if times > MaxDrawsPerRequest {
		_ = m.RespondErr(protocol.ErrBadRequest, "单次最多抽 %d 次", MaxDrawsPerRequest)
		return
	}

	cost := pool.Cost * int64(times)
	if p.Currency(pool.Currency) < cost {
		_ = m.RespondErr(protocol.ErrNotEnough, "货币不足：需要 %d", cost)
		return
	}

	src, err := rng.NewRandom()
	if err != nil {
		_ = m.RespondErr(protocol.ErrInternal, "生成随机种子失败")
		return
	}

	// 全程在副本上算，提交成功才换进内存
	baseCopy := proto.Clone(p.Base).(*pb.PlayerBase)
	bagCopy := proto.Clone(p.Bag).(*pb.PlayerBag)
	gachaCopy := proto.Clone(p.Gacha).(*pb.PlayerGacha)
	if baseCopy.Currency == nil {
		baseCopy.Currency = map[uint32]int64{}
	}
	baseCopy.Currency[pool.Currency] -= cost
	if baseCopy.Currency[pool.Currency] < 0 {
		_ = m.RespondErr(protocol.ErrNotEnough, "货币不足")
		return
	}

	pity := pityStateOf(gachaCopy, pool.ID)
	tmp := &Player{UID: p.UID, Base: baseCopy, Bag: bagCopy}
	now := time.Now()

	drops := make([]*pb.GachaDrop, 0, times)
	entries := make([]*ledger.Entry, 0, times+1)
	entries = append(entries, s.entry(p.UID, ledger.TypeGachaCost, pool.Currency,
		-cost, baseCopy.Currency[pool.Currency]).
		WithRef(fmt.Sprintf("pool=%s;times=%d;seed=%s", pool.ID, times, src.SeedHex())))

	gotHigh := false
	for i := 0; i < times; i++ {
		lastOfTen := pool.TenPullGuarantee && times >= 10 && i == times-1 && !gotHigh
		entry, byPity := drawOne(pool, pity, src, lastOfTen)
		if entry == nil {
			_ = m.RespondErr(protocol.ErrInternal, "卡池权重配置异常")
			return
		}

		count := entry.Count
		if count <= 0 {
			count = 1
		}
		id, err := s.lob.node.NextID()
		if err != nil {
			_ = m.RespondErr(protocol.ErrInternal, "生成道具 ID 失败: %v", err)
			return
		}
		if _, ok := tmp.AddItem(id, entry.TplID, count, now, conf.Stackable(entry.TplID)); !ok {
			_ = m.RespondErr(protocol.ErrBagFull, "背包空间不足，请先清理")
			return
		}

		if entry.Rarity >= pool.HighRarity {
			gotHigh = true
		}
		drops = append(drops, &pb.GachaDrop{
			TplId: entry.TplID, Rarity: entry.Rarity, Count: count, ByPity: byPity,
		})
		entries = append(entries, s.entry(p.UID, ledger.TypeGachaDrop, 0, 0, 0).
			WithRef(fmt.Sprintf("pool=%s;tpl=%d;rarity=%d;pity=%v",
				pool.ID, entry.TplID, entry.Rarity, byPity)))
	}

	// 落盘
	keys := s.lob.node.Keys
	baseBlob, e1 := proto.Marshal(baseCopy)
	bagBlob, e2 := proto.Marshal(bagCopy)
	gachaBlob, e3 := proto.Marshal(gachaCopy)
	if e1 != nil || e2 != nil || e3 != nil {
		_ = m.RespondErr(protocol.ErrInternal, "序列化失败")
		return
	}
	kv := map[string][]byte{
		keys.Player(p.UID, store.ModBase):  baseBlob,
		keys.Player(p.UID, store.ModBag):   bagBlob,
		keys.Player(p.UID, store.ModGacha): gachaBlob,
	}

	resp := &pb.GachaResp{
		Drops:       drops,
		PityCounter: pool.PityTop - min32(pity.SinceTop, pool.PityTop),
		Balance:     baseCopy.Currency[pool.Currency],
	}
	payload, _ := proto.Marshal(resp)

	idem := req.GetClientId()
	if idem == "" {
		idem = fmt.Sprintf("gacha%s-%d", pool.ID, now.UnixNano())
	}

	s.commit(p, CommitSpec{
		Op:      "gacha",
		IdemKey: keys.Order(p.UID, idem),
		Payload: payload,
		Entries: entries,
		KV:      kv,
	}, func(res *store.CommitResult, cerr error) {
		if cerr != nil {
			_ = m.RespondErr(protocol.ErrInternal, "抽卡失败: %v", cerr)
			return
		}
		if res.Duplicate {
			out := &pb.GachaResp{}
			_ = proto.Unmarshal(res.Payload, out)
			out.Duplicate = true
			_ = m.Respond(out)
			return
		}
		p.Base = baseCopy
		p.Bag = bagCopy
		p.Gacha = gachaCopy

		metrics.GachaDraws.WithLabelValues(pool.ID).Add(float64(times))
		for _, d := range drops {
			if d.GetByPity() {
				kind := "high"
				if d.GetRarity() >= pool.TopRarity {
					kind = "top"
				}
				metrics.GachaPityHits.WithLabelValues(pool.ID, kind).Inc()
			}
		}
		_ = m.Respond(resp)
	})
}

// drawOne 抽一次，返回抽到的条目和是不是保底给的
//
// 优先级：最高稀有度保底 > 高稀有度保底和十连保底 > 按权重随机
func drawOne(pool *gameconf.GachaPool, pity *pb.GachaPityState, src *rng.Source, forceHigh bool) (*gameconf.GachaEntry, bool) {
	pity.TotalDraws++
	pity.SinceTop++
	pity.SinceHigh++

	// 最高稀有度保底
	if pool.PityTop > 0 && pity.SinceTop >= pool.PityTop {
		if e := pickByRarity(pool, src, pool.TopRarity); e != nil {
			pity.SinceTop = 0
			pity.SinceHigh = 0
			return e, true
		}
	}
	// 高稀有度保底和十连保底
	if (pool.PityHigh > 0 && pity.SinceHigh >= pool.PityHigh) || forceHigh {
		if e := pickByRarity(pool, src, pool.HighRarity); e != nil {
			pity.SinceHigh = 0
			if e.Rarity >= pool.TopRarity {
				pity.SinceTop = 0
			}
			return e, true
		}
	}

	// 都没触发就按权重抽
	weights := make([]int64, len(pool.Entries))
	for i, e := range pool.Entries {
		weights[i] = e.Weight
	}
	idx := src.WeightedPick(weights)
	if idx < 0 {
		return nil, false
	}
	e := &pool.Entries[idx]
	if e.Rarity >= pool.TopRarity {
		pity.SinceTop = 0
		pity.SinceHigh = 0
	} else if e.Rarity >= pool.HighRarity {
		pity.SinceHigh = 0
	}
	return e, false
}

// pickByRarity 在稀有度不低于给定值的条目里按权重抽
func pickByRarity(pool *gameconf.GachaPool, src *rng.Source, minRarity uint32) *gameconf.GachaEntry {
	var idxs []int
	var weights []int64
	for i := range pool.Entries {
		if pool.Entries[i].Rarity >= minRarity {
			idxs = append(idxs, i)
			weights = append(weights, pool.Entries[i].Weight)
		}
	}
	if len(idxs) == 0 {
		return nil
	}
	pick := src.WeightedPick(weights)
	if pick < 0 {
		return nil
	}
	return &pool.Entries[idxs[pick]]
}

// pityStateOf 取某卡池的保底状态，没有就建一个
func pityStateOf(g *pb.PlayerGacha, poolID string) *pb.GachaPityState {
	for _, s := range g.GetPools() {
		if s.GetPoolId() == poolID {
			return s
		}
	}
	st := &pb.GachaPityState{PoolId: poolID}
	g.Pools = append(g.Pools, st)
	return st
}

func min32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
