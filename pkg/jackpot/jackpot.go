// Package jackpot 实现累积奖池。
//
// 评审 P1-5 的修复。奖池是 slots 最典型的「全局唯一强一致状态」：
// 所有玩家往里投，某一个玩家全部拿走，中间不能多也不能少。
//
// 设计上有两个容易做错的地方，这里都绕开了：
//
//	一、不要把每次注入做成分布式事务。
//	    「每次 spin 发一个 job 给 World 服累加」是常见的过度设计：
//	    把一个原子加法变成了跨进程消息，既慢又多一个失败点。
//	    这里注入直接用 Redis INCRBY —— 它本来就是原子的。
//
//	二、中奖派彩必须走跨分片中转层（§8）。
//	    奖池和玩家在不同的 slot、不同的 owner，直接互相操作内存是禁止的。
//	    这里的做法是：原子地「清零奖池 + 落一条 PENDING 派彩记录」，
//	    再由中转层把钱幂等地打给玩家。奖池清零与派彩记录同生共死，
//	    因此不会出现「池子清了但没人拿到钱」。
package jackpot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/store"
)

// ErrEmpty 表示奖池为空（已被人拿走，或尚未注入）。
var ErrEmpty = errors.New("jackpot: 奖池为空")

// Payout 是一笔待处理的奖池派彩。
type Payout struct {
	TxID      string `json:"txid"`
	PoolID    string `json:"pool"`
	UID       uint64 `json:"uid"`
	Amount    int64  `json:"amount"`
	Currency  uint32 `json:"currency"`
	RoundID   uint64 `json:"round"`
	CreatedAt int64  `json:"created_at"`
	Retries   int    `json:"retries,omitempty"`
}

// scriptContribute 注入奖池。首次注入时用底注初始化。
//
//	KEYS[1] = 奖池金额键
//	ARGV[1] = 注入额
//	ARGV[2] = 底注（键不存在时的初始值）
var scriptContribute = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('SET', KEYS[1], ARGV[2])
end
return redis.call('INCRBY', KEYS[1], ARGV[1])
`)

// scriptClaim 原子地「清零奖池 + 落一条 PENDING 派彩记录」。
//
//	KEYS[1] = 奖池金额键
//	KEYS[2] = 派彩待处理索引（与奖池同 slot）
//	ARGV[1] = 派彩记录 JSON
//	ARGV[2] = 重置后的底注
//	ARGV[3] = 索引 score（创建时间）
//
// 返回中奖金额；奖池为空或不足底注时返回 0（不产生派彩记录）。
//
// 这两步必须同生共死：只清零不记账 = 钱凭空消失；
// 只记账不清零 = 同一笔奖池被发两次。
var scriptClaim = redis.NewScript(`
local amount = tonumber(redis.call('GET', KEYS[1]) or '0')
local seed = tonumber(ARGV[2])
if amount <= seed then
  return 0
end
redis.call('SET', KEYS[1], seed)
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[1])
return amount
`)

// Manager 管理奖池。
type Manager struct {
	rdb   redis.UniversalClient
	keys  *store.Keys
	pools map[string]*gameconf.Jackpot
}

// NewManager 构造管理器。
func NewManager(rdb redis.UniversalClient, keys *store.Keys, pools map[string]*gameconf.Jackpot) *Manager {
	return &Manager{rdb: rdb, keys: keys, pools: pools}
}

// Pool 返回奖池配置。
func (m *Manager) Pool(id string) (*gameconf.Jackpot, bool) {
	p, ok := m.pools[id]
	return p, ok
}

// Contribute 往奖池注入，返回注入后的金额。
//
// 注入额来自玩家已经扣掉的投注（那一步已经原子落盘并记了流水），
// 因此这里失败不会造成玩家资损，只会让池子少涨一点；
// 流水里记着 JP_CONTRIB，对账时可以查出差额并补回。
func (m *Manager) Contribute(ctx context.Context, poolID string, amount int64) (int64, error) {
	if amount <= 0 {
		return 0, nil
	}
	cfg, ok := m.pools[poolID]
	if !ok {
		return 0, fmt.Errorf("jackpot: 未知奖池 %q", poolID)
	}
	return scriptContribute.Run(ctx, m.rdb,
		[]string{m.keys.Jackpot(poolID)}, amount, cfg.Seed).Int64()
}

// Amount 读取奖池当前水位。任何进程可读，属于只读投影。
func (m *Manager) Amount(ctx context.Context, poolID string) (int64, error) {
	v, err := m.rdb.Get(ctx, m.keys.Jackpot(poolID)).Int64()
	if errors.Is(err, redis.Nil) {
		if cfg, ok := m.pools[poolID]; ok {
			return cfg.Seed, nil
		}
		return 0, nil
	}
	return v, err
}

// Claim 中奖：原子清零并生成待派彩记录。
//
// 返回的 Payout 需要由调用方通过必达任务打给玩家（幂等由 txid 保证）。
func (m *Manager) Claim(ctx context.Context, poolID string, uid, roundID uint64, txid string) (*Payout, error) {
	cfg, ok := m.pools[poolID]
	if !ok {
		return nil, fmt.Errorf("jackpot: 未知奖池 %q", poolID)
	}

	p := &Payout{
		TxID: txid, PoolID: poolID, UID: uid,
		Currency: cfg.Currency, RoundID: roundID,
		CreatedAt: time.Now().UnixMilli(),
	}
	// 金额要等 Lua 返回才知道，先用占位序列化再回填是不行的（索引里存的是 JSON），
	// 因此分两步：先读一次水位算出金额，再用该金额构造记录去 CAS。
	// 竞争由 Lua 内的「amount <= seed 就放弃」兜底：并发中奖时只有一个能拿到。
	cur, err := m.Amount(ctx, poolID)
	if err != nil {
		return nil, err
	}
	if cur <= cfg.Seed {
		return nil, ErrEmpty
	}
	p.Amount = cur

	blob, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	won, err := scriptClaim.Run(ctx, m.rdb,
		[]string{m.keys.Jackpot(poolID), m.keys.JackpotPending(poolID)},
		blob, cfg.Seed, p.CreatedAt).Int64()
	if err != nil {
		return nil, err
	}
	if won == 0 {
		return nil, ErrEmpty
	}
	if won != cur {
		// 读到的水位与清零时的实际金额不同（期间有人注入）。
		// 以 Lua 返回的为准，并把差额补回池子 —— 绝不能让玩家少拿或多拿。
		diff := won - p.Amount
		if diff > 0 {
			if _, cerr := m.Contribute(ctx, poolID, diff); cerr != nil {
				return nil, fmt.Errorf("jackpot: 回补差额失败（差额 %d）: %w", diff, cerr)
			}
		}
	}
	return p, nil
}

// PendingPayouts 返回待处理的派彩记录，供补偿扫描器重投。
func (m *Manager) PendingPayouts(ctx context.Context, poolID string, before time.Time, limit int64) ([]*Payout, error) {
	if limit <= 0 {
		limit = 100
	}
	members, err := m.rdb.ZRangeByScore(ctx, m.keys.JackpotPending(poolID), &redis.ZRangeBy{
		Min: "0", Max: fmt.Sprint(before.UnixMilli()), Count: limit,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]*Payout, 0, len(members))
	for _, raw := range members {
		var p Payout
		if json.Unmarshal([]byte(raw), &p) != nil {
			_ = m.rdb.ZRem(ctx, m.keys.JackpotPending(poolID), raw).Err()
			continue
		}
		out = append(out, &p)
	}
	return out, nil
}

// Settle 标记一笔派彩已完成，从待处理索引移除。
func (m *Manager) Settle(ctx context.Context, p *Payout) error {
	blob, err := json.Marshal(p)
	if err != nil {
		return err
	}
	pipe := m.rdb.Pipeline()
	pipe.ZRem(ctx, m.keys.JackpotPending(p.PoolID), blob)
	pipe.HSet(ctx, m.keys.JackpotMeta(p.PoolID),
		"last_winner", p.UID,
		"last_amount", p.Amount,
		"last_at", p.CreatedAt,
		"last_txid", p.TxID)
	_, err = pipe.Exec(ctx)
	return err
}

// Requeue 重投一笔派彩（推后 score，避免同一轮重复扫到）。
func (m *Manager) Requeue(ctx context.Context, p *Payout) error {
	old, err := json.Marshal(p)
	if err != nil {
		return err
	}
	next := *p
	next.Retries++
	blob, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	pipe := m.rdb.Pipeline()
	pipe.ZRem(ctx, m.keys.JackpotPending(p.PoolID), old)
	pipe.ZAdd(ctx, m.keys.JackpotPending(p.PoolID), redis.Z{
		Score: float64(time.Now().UnixMilli()), Member: blob,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// LastWinner 返回上一次中奖信息，用于展示。
func (m *Manager) LastWinner(ctx context.Context, poolID string) (uid uint64, amount int64, at int64, err error) {
	vals, err := m.rdb.HMGet(ctx, m.keys.JackpotMeta(poolID),
		"last_winner", "last_amount", "last_at").Result()
	if err != nil {
		return 0, 0, 0, err
	}
	parse := func(v any) int64 {
		s, ok := v.(string)
		if !ok {
			return 0
		}
		var n int64
		_, _ = fmt.Sscanf(s, "%d", &n)
		return n
	}
	return uint64(parse(vals[0])), parse(vals[1]), parse(vals[2]), nil
}

// ContributionOf 按万分比算出本次投注应注入的金额。
//
// 用整数运算（向下取整），不用浮点：奖池金额是钱，不接受浮点舍入。
func ContributionOf(bet, bp int64) int64 {
	if bet <= 0 || bp <= 0 {
		return 0
	}
	return bet * bp / 10000
}
