// Package jackpot 累积奖池
//
// 注入直接用 INCRBY，它本来就是原子的。每次 spin 发个 job 给 World 是把一次加法
// 做成分布式事务，慢且多一个失败点。
//
// 中奖走跨分片那一套：原子地清零奖池并落一条 PENDING 派彩记录，再由中转层幂等
// 打给玩家。两步同生共死，不会出现池子清了但没人拿到钱的情况
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

// ErrEmpty 表示奖池是空的，可能刚被人拿走
var ErrEmpty = errors.New("jackpot: 奖池为空")

// Payout 一笔待处理的奖池派彩
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

// scriptContribute 注入奖池，键不存在时先用底注初始化
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

// scriptClaim 清零奖池并落一条待派彩记录
//
//	KEYS[1] = 奖池金额键
//	KEYS[2] = 待派彩索引，与奖池同 slot
//	ARGV[1] = 派彩记录 JSON
//	ARGV[2] = 重置后的底注
//	ARGV[3] = 索引 score
//
// 返回中奖金额，池子不足底注返回 0。
// 两步得一起成：只清零钱就没了，只记账同一笔会发两次
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

// Manager 管理奖池
type Manager struct {
	rdb   redis.UniversalClient
	keys  *store.Keys
	pools map[string]*gameconf.Jackpot
}

func NewManager(rdb redis.UniversalClient, keys *store.Keys, pools map[string]*gameconf.Jackpot) *Manager {
	return &Manager{rdb: rdb, keys: keys, pools: pools}
}

func (m *Manager) Pool(id string) (*gameconf.Jackpot, bool) {
	p, ok := m.pools[id]
	return p, ok
}

// Contribute 往奖池注入，返回注入后的水位
//
// 这笔钱在扣注那一步已经落盘并记了 JP_CONTRIB，所以这里失败只是池子少涨，
// 玩家不亏，差额可以照流水补回来
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

// Amount 读奖池水位，任何进程都能读
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

// Claim 中奖：清零池子并生成待派彩记录
//
// 返回的 Payout 要由调用方通过必达任务打给玩家，幂等靠 txid
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
	// 索引里存的是 JSON，没法先占位再回填金额，所以先读一次水位再拿它去 CAS。
	// 并发中奖由 Lua 里的 amount <= seed 兜底，只有一个能拿到
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
		// 读水位到清零之间有人又注入了。以 Lua 返回的为准，差额补回池子
		diff := won - p.Amount
		if diff > 0 {
			if _, cerr := m.Contribute(ctx, poolID, diff); cerr != nil {
				return nil, fmt.Errorf("jackpot: 回补差额失败（差额 %d）: %w", diff, cerr)
			}
		}
	}
	return p, nil
}

// PendingPayouts 返回待派彩记录，给补偿扫描重投用
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

// Settle 标记一笔派彩已完成，从待处理索引移除
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

// Requeue 重投一笔派彩，score 往后推，免得同一轮又扫到
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

// LastWinner 返回上次中奖信息，给展示用
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

// ContributionOf 按万分比算注入额。整数向下取整，钱不能用浮点
func ContributionOf(bet, bp int64) int64 {
	if bet <= 0 || bp <= 0 {
		return 0
	}
	return bet * bp / 10000
}
