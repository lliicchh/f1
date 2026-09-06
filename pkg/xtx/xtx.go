// Package xtx 跨分片操作的中转层
//
// 分片之间不许直接动对方内存。没有跨分片事务，直接互操作时任一侧崩溃就会
// 资源凭空消失或者翻倍，事后还查不出来。
//
// 流程是发起方先扣再写穿 tx 记录，投一条 job，接收方用 txid 幂等去重后入账。
// 邮件附件、交易、公会仓库、组队奖励、给离线玩家发奖都走这一套
package xtx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/store"
)

// State 转移状态
type State string

const (
	StatePending   State = "PENDING"
	StateCompleted State = "COMPLETED"
)

// Record 一笔跨分片转移
type Record struct {
	TxID      string            `json:"txid"`
	FromUID   uint64            `json:"from_uid"` // 0 表示系统发放（发奖、发信）
	ToUID     uint64            `json:"to_uid"`
	FromShard uint32            `json:"from_shard"`
	ToShard   uint32            `json:"to_shard"`
	Items     []*pb.Attachment  `json:"items,omitempty"`
	CurrType  uint32            `json:"curr_type,omitempty"`
	CurrDelta int64             `json:"curr_delta,omitempty"`
	Reason    string            `json:"reason"`
	State     State             `json:"state"`
	CreatedAt int64             `json:"created_at"`
	DoneAt    int64             `json:"done_at,omitempty"`
	Retries   int               `json:"retries,omitempty"`
	Extra     map[string]string `json:"extra,omitempty"`
}

// ErrDuplicate 表示这个 txid 已经处理过了
var ErrDuplicate = errors.New("xtx: 该转移已完成（幂等去重）")

// scriptBegin 发起方写穿：tx 记录、PENDING 索引、扣减后的玩家数据一起落
//
//	KEYS[1]    = epoch 键
//	KEYS[2]    = tx 记录键
//	KEYS[3]    = tx pending 索引键
//	KEYS[4..N] = 发起方玩家数据键
//	ARGV[1]    = 调用方 epoch
//	ARGV[2]    = tx JSON
//	ARGV[3]    = created_at（索引 score）
//	ARGV[4]    = 记录 TTL 秒（0 = 不过期）
//	ARGV[5..]  = 对应 KEYS[4..] 的值
//
// 返回 1 成功，0 epoch 过期，2 txid 已存在
var scriptBegin = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
  return 2
end
if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then
  return 0
end
local ttl = tonumber(ARGV[4])
if ttl > 0 then
  redis.call('SET', KEYS[2], ARGV[2], 'EX', ttl)
else
  redis.call('SET', KEYS[2], ARGV[2])
end
redis.call('ZADD', KEYS[3], tonumber(ARGV[3]), ARGV[2])
for i = 4, #KEYS do
  redis.call('SET', KEYS[i], ARGV[i + 1])
end
return 1
`)

// scriptClaim 接收方去重加写穿
//
//	KEYS[1]    = epoch 键（接收方分片）
//	KEYS[2]    = done 标记键
//	KEYS[3..N] = 接收方玩家数据键
//	ARGV[1]    = 调用方 epoch
//	ARGV[2]    = done 标记 TTL 秒
//	ARGV[3..]  = 对应 KEYS[3..] 的值
//
// 返回 1 首次处理，2 重复投递，0 epoch 过期
var scriptClaim = redis.NewScript(`
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) == false then
  return 2
end
if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then
  redis.call('DEL', KEYS[2])
  return 0
end
for i = 3, #KEYS do
  redis.call('SET', KEYS[i], ARGV[i])
end
return 1
`)

// scriptComplete 标记完成并从 PENDING 索引里摘掉
//
//	KEYS[1] = tx 记录键
//	KEYS[2] = tx pending 索引键
//	ARGV[1] = 新的 tx JSON
//	ARGV[2] = 旧的 tx JSON（索引里的 member）
var scriptComplete = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur then
  redis.call('ZREM', KEYS[2], cur)
end
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('SET', KEYS[1], ARGV[1], 'KEEPTTL')
return 1
`)

// Manager 管理跨分片转移的 Redis 侧状态
type Manager struct {
	rdb     redis.UniversalClient
	keys    *store.Keys
	recTTL  time.Duration
	doneTTL time.Duration
}

// NewManager 构造管理器
//
// recTTL 是 tx 记录留多久，审计用。doneTTL 是幂等标记留多久，
// 必须明显长于任何可能的重投窗口，否则会重复发放
func NewManager(rdb redis.UniversalClient, keys *store.Keys, recTTL, doneTTL time.Duration) *Manager {
	if recTTL <= 0 {
		recTTL = 7 * 24 * time.Hour
	}
	if doneTTL <= 0 {
		doneTTL = 30 * 24 * time.Hour
	}
	return &Manager{rdb: rdb, keys: keys, recTTL: recTTL, doneTTL: doneTTL}
}

// Begin 发起方写穿，一次 Lua 把 tx 记录、PENDING 索引和扣减后的玩家数据都落下去
//
// playerKV 必须已经是扣减过的状态。先扣再写，资源就不会凭空多出来
func (m *Manager) Begin(ctx context.Context, epoch int64, rec *Record, playerKV map[string][]byte) error {
	if rec.TxID == "" {
		return errors.New("xtx: txid 不能为空")
	}
	if rec.State == "" {
		rec.State = StatePending
	}
	if rec.CreatedAt == 0 {
		rec.CreatedAt = time.Now().UnixMilli()
	}
	blob, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(playerKV)+3)
	args := make([]any, 0, len(playerKV)+4)
	keys = append(keys,
		m.keys.Epoch(rec.FromShard),
		m.keys.Tx(rec.FromShard, rec.TxID),
		m.keys.TxPending(rec.FromShard))
	args = append(args, epoch, blob, rec.CreatedAt, int64(m.recTTL.Seconds()))
	for k, v := range playerKV {
		keys = append(keys, k)
		args = append(args, v)
	}

	n, err := scriptBegin.Run(ctx, m.rdb, keys, args...).Int64()
	if err != nil {
		return fmt.Errorf("写穿 tx 记录失败 txid=%s: %w", rec.TxID, err)
	}
	switch n {
	case 1:
		metrics.TxPending.Inc()
		return nil
	case 2:
		return fmt.Errorf("%w: txid=%s 已存在", ErrDuplicate, rec.TxID)
	default:
		return fmt.Errorf("%w: 发起转移时 epoch 过期 shard=%d epoch=%d",
			store.ErrFenced, rec.FromShard, epoch)
	}
}

// Claim 接收方去重后入账。返回 (false, nil) 说明是重复投递，直接 Ack 就行
func (m *Manager) Claim(ctx context.Context, epoch int64, rec *Record, playerKV map[string][]byte) (bool, error) {
	keys := make([]string, 0, len(playerKV)+2)
	args := make([]any, 0, len(playerKV)+2)
	keys = append(keys, m.keys.Epoch(rec.ToShard), m.keys.TxDone(rec.ToShard, rec.TxID))
	args = append(args, epoch, int64(m.doneTTL.Seconds()))
	for k, v := range playerKV {
		keys = append(keys, k)
		args = append(args, v)
	}

	n, err := scriptClaim.Run(ctx, m.rdb, keys, args...).Int64()
	if err != nil {
		return false, fmt.Errorf("接收转移失败 txid=%s: %w", rec.TxID, err)
	}
	switch n {
	case 1:
		metrics.TxCompleted.WithLabelValues("ok").Inc()
		return true, nil
	case 2:
		metrics.TxCompleted.WithLabelValues("duplicate").Inc()
		return false, nil
	default:
		return false, fmt.Errorf("%w: 接收转移时 epoch 过期 shard=%d epoch=%d",
			store.ErrFenced, rec.ToShard, epoch)
	}
}

// Complete 标记转移完成并摘出 PENDING 索引
//
// 这一步失败也没关系：done 标记已经在了，重投会被幂等拦下，扫描器最多白跑一次
func (m *Manager) Complete(ctx context.Context, rec *Record) error {
	old, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	done := *rec
	done.State = StateCompleted
	done.DoneAt = time.Now().UnixMilli()
	blob, err := json.Marshal(&done)
	if err != nil {
		return err
	}
	if err := scriptComplete.Run(ctx, m.rdb,
		[]string{m.keys.Tx(rec.FromShard, rec.TxID), m.keys.TxPending(rec.FromShard)},
		blob, old).Err(); err != nil {
		return err
	}
	metrics.TxPending.Dec()
	return nil
}

func (m *Manager) Get(ctx context.Context, fromShard uint32, txid string) (*Record, error) {
	raw, err := m.rdb.Get(ctx, m.keys.Tx(fromShard, txid)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec Record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// ScanPending 返回某分片里创建时间早于 before 的 PENDING 转移，给补偿扫描用
func (m *Manager) ScanPending(ctx context.Context, shard uint32, before time.Time, limit int64) ([]*Record, error) {
	if limit <= 0 {
		limit = 200
	}
	members, err := m.rdb.ZRangeByScore(ctx, m.keys.TxPending(shard), &redis.ZRangeBy{
		Min:   "0",
		Max:   fmt.Sprint(before.UnixMilli()),
		Count: limit,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	out := make([]*Record, 0, len(members))
	for _, raw := range members {
		var rec Record
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			// 索引里有坏数据就摘掉，别让扫描器每轮都被它绊住
			_ = m.rdb.ZRem(ctx, m.keys.TxPending(shard), raw).Err()
			logx.Warn("PENDING 索引中存在无法解析的记录，已移除", "shard", shard, "raw", raw)
			continue
		}
		out = append(out, &rec)
	}
	return out, nil
}

// PendingCount 返回某分片的 PENDING 数量
func (m *Manager) PendingCount(ctx context.Context, shard uint32) (int64, error) {
	return m.rdb.ZCard(ctx, m.keys.TxPending(shard)).Result()
}

// Requeue 更新 PENDING 索引：重试计数加一，score 往后推，免得同一轮又扫到
func (m *Manager) Requeue(ctx context.Context, rec *Record) error {
	old, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	next := *rec
	next.Retries++
	blob, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	pipe := m.rdb.Pipeline()
	pipe.ZRem(ctx, m.keys.TxPending(rec.FromShard), old)
	pipe.ZAdd(ctx, m.keys.TxPending(rec.FromShard), redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: blob,
	})
	pipe.Set(ctx, m.keys.Tx(rec.FromShard, rec.TxID), blob, redis.KeepTTL)
	_, err = pipe.Exec(ctx)
	metrics.TxTimeout.Inc()
	return err
}

// NewTxID 用雪花生成 txid，带个前缀方便在 Redis 里认
func NewTxID(snowflake uint64) string { return fmt.Sprintf("tx%d", snowflake) }
