package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
)

// ErrFenced 表示写入被 fencing 拒了，调用方已经不是这个分片的 owner
//
// 收到它就该丢内存、停服务、告警，不要重试。内存已经过期，重试只会更糟
var ErrFenced = errors.New("store: epoch 过期，写入被 fencing 拒绝")

// scriptRaiseEpoch 抬高分片 epoch，接管方在加载数据之前调用
//
//	KEYS[1] = epoch 键
//	ARGV[1] = 新 epoch，取 etcd revision
//
// 返回 {是否写入, 当前 epoch}。已有更高的就不覆盖，免得乱序请求把 epoch 拉低
var scriptRaiseEpoch = redis.NewScript(`
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local new = tonumber(ARGV[1])
if cur >= new then
  return {0, cur}
end
redis.call('SET', KEYS[1], new)
return {1, new}
`)

// scriptWriteModules 带 epoch 校验的批量写入
//
//	KEYS[1]    = epoch 键
//	KEYS[2..N] = 数据键
//	ARGV[1]    = 调用方 epoch
//	ARGV[2..N] = 对应的值
//
// 返回 1 成功，0 表示调用方已过期
var scriptWriteModules = redis.NewScript(`
if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then
  return 0
end
for i = 2, #KEYS do
  redis.call('SET', KEYS[i], ARGV[i])
end
return 1
`)

// scriptWriteHash 带 epoch 校验的 HASH 写入，profile 摘要用
//
//	KEYS[1] = epoch 键
//	KEYS[2] = HASH 键
//	ARGV[1] = 调用方 epoch
//	ARGV[2] = TTL 秒（0 表示不设）
//	ARGV[3..] = field/value 交替
var scriptWriteHash = redis.NewScript(`
if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then
  return 0
end
local ttl = tonumber(ARGV[2])
local args = {}
for i = 3, #ARGV do
  args[#args+1] = ARGV[i]
end
if #args > 0 then
  redis.call('HSET', KEYS[2], unpack(args))
end
if ttl > 0 then
  redis.call('EXPIRE', KEYS[2], ttl)
end
return 1
`)

// scriptWriteThrough L0 写穿加幂等
//
//	KEYS[1]    = epoch 键
//	KEYS[2]    = 订单键（幂等）
//	KEYS[3..N] = 数据键
//	ARGV[1]    = 调用方 epoch
//	ARGV[2]    = 订单 TTL 秒
//	ARGV[3]    = 订单结果载荷（重复提交时原样返回）
//	ARGV[4..]  = 对应 KEYS[3..] 的值
//
// 返回 {状态, 载荷}：
//
//	1 = 首次执行，已写入
//	2 = 重复提交，返回首次结果（不重复扣减）
//	0 = epoch 过期，拒绝
var scriptWriteThrough = redis.NewScript(`
local existing = redis.call('GET', KEYS[2])
if existing then
  return {2, existing}
end
if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then
  return {0, ''}
end
local ttl = tonumber(ARGV[2])
if ttl > 0 then
  redis.call('SET', KEYS[2], ARGV[3], 'EX', ttl)
else
  redis.call('SET', KEYS[2], ARGV[3])
end
for i = 3, #KEYS do
  redis.call('SET', KEYS[i], ARGV[i + 1])
end
return {1, ARGV[3]}
`)

// scriptDeleteKeys 带 epoch 校验的删除
//
//	KEYS[1]    = epoch 键
//	KEYS[2..N] = 待删键
//	ARGV[1]    = 调用方 epoch
var scriptDeleteKeys = redis.NewScript(`
if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then
  return 0
end
for i = 2, #KEYS do
  redis.call('DEL', KEYS[i])
end
return 1
`)

// Fencer 在给定分片 epoch 下执行受保护的 Redis 写入
type Fencer struct {
	rdb  redis.UniversalClient
	keys *Keys
	kind string
}

// NewFencer 构造 Fencer
func NewFencer(rdb redis.UniversalClient, keys *Keys, kind string) *Fencer {
	return &Fencer{rdb: rdb, keys: keys, kind: kind}
}

func (f *Fencer) Keys() *Keys { return f.keys }

// RDB 返回底层客户端（只读操作直接用）
func (f *Fencer) RDB() redis.UniversalClient { return f.rdb }

// RaiseEpoch 抬高分片 epoch，必须在加载数据之前调用
//
// Redis 里已有更高的 epoch，说明有更新的 owner 接管了，本次认领作废
func (f *Fencer) RaiseEpoch(ctx context.Context, shard uint32, epoch int64) error {
	key := f.keys.Epoch(shard)
	res, err := scriptRaiseEpoch.Run(ctx, f.rdb, []string{key}, epoch).Slice()
	if err != nil {
		return fmt.Errorf("抬高 epoch 失败 (shard=%d): %w", shard, err)
	}
	ok, _ := res[0].(int64)
	cur, _ := res[1].(int64)
	if ok == 0 {
		metrics.ShardEpochRejected.WithLabelValues(f.kind, fmt.Sprint(shard)).Inc()
		logx.Error("抬高 epoch 被拒绝：Redis 中已有更高 epoch，本次认领作废（告警）",
			"kind", f.kind, "shard", shard, "my_epoch", epoch, "redis_epoch", cur)
		return fmt.Errorf("%w: shard=%d my=%d redis=%d", ErrFenced, shard, epoch, cur)
	}
	logx.Info("分片 epoch 已抬高", "kind", f.kind, "shard", shard, "epoch", epoch)
	return nil
}

// CurrentEpoch 读取 Redis 中记录的分片 epoch
func (f *Fencer) CurrentEpoch(ctx context.Context, shard uint32) (int64, error) {
	v, err := f.rdb.Get(ctx, f.keys.Epoch(shard)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return v, err
}

// WriteModules 带 epoch 校验地写入若干键值
func (f *Fencer) WriteModules(ctx context.Context, shard uint32, epoch int64, kv map[string][]byte) error {
	if len(kv) == 0 {
		return nil
	}
	keys := make([]string, 0, len(kv)+1)
	args := make([]any, 0, len(kv)+1)
	keys = append(keys, f.keys.Epoch(shard))
	args = append(args, epoch)
	for k, v := range kv {
		keys = append(keys, k)
		args = append(args, v)
	}
	n, err := scriptWriteModules.Run(ctx, f.rdb, keys, args...).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		f.reportFenced(shard, epoch)
		return fmt.Errorf("%w: shard=%d epoch=%d", ErrFenced, shard, epoch)
	}
	return nil
}

// WriteHash 带 epoch 校验地写入 HASH（profile 摘要）
func (f *Fencer) WriteHash(ctx context.Context, shard uint32, epoch int64, key string, ttlSec int64, fields []any) error {
	if len(fields) == 0 {
		return nil
	}
	args := make([]any, 0, len(fields)+2)
	args = append(args, epoch, ttlSec)
	args = append(args, fields...)
	n, err := scriptWriteHash.Run(ctx, f.rdb, []string{f.keys.Epoch(shard), key}, args...).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		f.reportFenced(shard, epoch)
		return fmt.Errorf("%w: shard=%d epoch=%d key=%s", ErrFenced, shard, epoch, key)
	}
	return nil
}

// WriteThroughResult L0 写穿的结果
type WriteThroughResult struct {
	Duplicate bool   // true = 重复提交，Payload 是首次结果
	Payload   []byte // 首次执行时即入参 payload
}

// WriteThrough 执行 L0 写穿，落盘成功后调用方才改内存回包。
// 幂等靠订单键，重复提交返回首次结果
func (f *Fencer) WriteThrough(ctx context.Context, shard uint32, epoch int64,
	orderKey string, orderTTLSec int64, payload []byte, kv map[string][]byte) (*WriteThroughResult, error) {

	keys := make([]string, 0, len(kv)+2)
	args := make([]any, 0, len(kv)+3)
	keys = append(keys, f.keys.Epoch(shard), orderKey)
	args = append(args, epoch, orderTTLSec, payload)
	for k, v := range kv {
		keys = append(keys, k)
		args = append(args, v)
	}

	res, err := scriptWriteThrough.Run(ctx, f.rdb, keys, args...).Slice()
	if err != nil {
		return nil, err
	}
	code, _ := res[0].(int64)
	var out []byte
	switch v := res[1].(type) {
	case string:
		out = []byte(v)
	case []byte:
		out = v
	}

	switch code {
	case 1:
		return &WriteThroughResult{Duplicate: false, Payload: out}, nil
	case 2:
		return &WriteThroughResult{Duplicate: true, Payload: out}, nil
	default:
		f.reportFenced(shard, epoch)
		return nil, fmt.Errorf("%w: shard=%d epoch=%d", ErrFenced, shard, epoch)
	}
}

// Delete 带 epoch 校验地删除键
func (f *Fencer) Delete(ctx context.Context, shard uint32, epoch int64, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	all := append([]string{f.keys.Epoch(shard)}, keys...)
	n, err := scriptDeleteKeys.Run(ctx, f.rdb, all, epoch).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		f.reportFenced(shard, epoch)
		return fmt.Errorf("%w: shard=%d epoch=%d", ErrFenced, shard, epoch)
	}
	return nil
}

// LoadPlayer 用 pipeline 一次读回玩家全部模块
func (f *Fencer) LoadPlayer(ctx context.Context, uid uint64) (map[Module][]byte, error) {
	pipe := f.rdb.Pipeline()
	cmds := make(map[Module]*redis.StringCmd, len(AllModules))
	for _, m := range AllModules {
		cmds[m] = pipe.Get(ctx, f.keys.Player(uid, m))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("加载玩家 %d 失败: %w", uid, err)
	}

	out := make(map[Module][]byte, len(AllModules))
	for m, cmd := range cmds {
		b, err := cmd.Bytes()
		if errors.Is(err, redis.Nil) {
			continue // 新玩家，该模块尚不存在
		}
		if err != nil {
			return nil, fmt.Errorf("读取玩家 %d 模块 %s 失败: %w", uid, m, err)
		}
		out[m] = b
	}
	return out, nil
}

func (f *Fencer) reportFenced(shard uint32, epoch int64) {
	metrics.ShardEpochRejected.WithLabelValues(f.kind, fmt.Sprint(shard)).Inc()
	logx.Error("写入被 epoch fencing 拒绝：本节点已不是该分片 owner（告警）",
		"kind", f.kind, "shard", shard, "my_epoch", epoch,
		"action", "必须丢弃该分片内存、停止服务、告警，绝不重试")
}

// ---------------------------------------------------------------------------
// 统一提交：幂等 + 流水 + 数据，一次 Lua 原子完成
// ---------------------------------------------------------------------------

// scriptCommit 资金类操作的统一入口
//
//	KEYS[1]    = epoch 键
//	KEYS[2]    = 幂等键（ARGV[2]==0 时不启用）
//	KEYS[3]    = 流水 Stream 键
//	KEYS[4..N] = 数据键
//	ARGV[1]    = 调用方 epoch
//	ARGV[2]    = 幂等 TTL 秒（0 = 不启用幂等）
//	ARGV[3]    = 幂等载荷（重复提交时原样返回）
//	ARGV[4]    = 流水保留条数（0 = 不裁剪）
//	ARGV[5]    = 流水条数 E
//	ARGV[6..5+E]   = E 条流水 JSON
//	ARGV[6+E..]    = 对应 KEYS[4..] 的值
//
// 返回 {状态, 载荷}：1 首次执行，2 重复提交，0 epoch 过期。
//
// 幂等判定、追流水、写余额放一个脚本里，是为了让它们同生共死
var scriptCommit = redis.NewScript(`
local useIdem = tonumber(ARGV[2]) > 0
if useIdem then
  local existing = redis.call('GET', KEYS[2])
  if existing then
    return {2, existing}
  end
end
if tonumber(redis.call('GET', KEYS[1]) or '0') > tonumber(ARGV[1]) then
  return {0, ''}
end
if useIdem then
  redis.call('SET', KEYS[2], ARGV[3], 'EX', tonumber(ARGV[2]))
end
local maxlen = tonumber(ARGV[4])
local n = tonumber(ARGV[5])
for i = 1, n do
  if maxlen > 0 then
    redis.call('XADD', KEYS[3], 'MAXLEN', '~', maxlen, '*', 'd', ARGV[5 + i])
  else
    redis.call('XADD', KEYS[3], '*', 'd', ARGV[5 + i])
  end
end
for i = 4, #KEYS do
  redis.call('SET', KEYS[i], ARGV[2 + n + i])
end
return {1, ARGV[3]}
`)

// CommitReq 描述一次资金类提交
type CommitReq struct {
	Shard uint32
	Epoch int64

	// IdemKey 为空表示不做幂等
	IdemKey     string
	IdemTTLSec  int64
	IdemPayload []byte

	// LedgerKey 玩家流水的 Stream，Entries 为空时不写
	LedgerKey    string
	LedgerMaxLen int64
	Entries      [][]byte

	// KV 要写的玩家数据，序列化在 Actor 里做完
	KV map[string][]byte
}

// CommitResult 提交结果
type CommitResult struct {
	Duplicate bool
	Payload   []byte
}

// Commit 做一次带幂等和流水的原子写入
func (f *Fencer) Commit(ctx context.Context, req *CommitReq) (*CommitResult, error) {
	if req.LedgerKey == "" && len(req.Entries) > 0 {
		return nil, errors.New("store: 有流水条目但未指定 Stream 键")
	}
	ledgerKey := req.LedgerKey
	if ledgerKey == "" {
		// KEYS[3] 得占个位，条目数为 0 时 Lua 不会碰它
		ledgerKey = f.keys.Epoch(req.Shard)
	}
	idemKey := req.IdemKey
	idemTTL := req.IdemTTLSec
	if idemKey == "" {
		idemKey = f.keys.Epoch(req.Shard) // 占位
		idemTTL = 0
	}

	keys := make([]string, 0, len(req.KV)+3)
	keys = append(keys, f.keys.Epoch(req.Shard), idemKey, ledgerKey)

	args := make([]any, 0, len(req.KV)+len(req.Entries)+5)
	args = append(args, req.Epoch, idemTTL, req.IdemPayload, req.LedgerMaxLen, len(req.Entries))
	for _, e := range req.Entries {
		args = append(args, e)
	}
	for k, v := range req.KV {
		keys = append(keys, k)
		args = append(args, v)
	}

	res, err := scriptCommit.Run(ctx, f.rdb, keys, args...).Slice()
	if err != nil {
		return nil, err
	}
	code, _ := res[0].(int64)
	var payload []byte
	switch v := res[1].(type) {
	case string:
		payload = []byte(v)
	case []byte:
		payload = v
	}

	switch code {
	case 1:
		return &CommitResult{Payload: payload}, nil
	case 2:
		return &CommitResult{Duplicate: true, Payload: payload}, nil
	default:
		f.reportFenced(req.Shard, req.Epoch)
		return nil, fmt.Errorf("%w: shard=%d epoch=%d", ErrFenced, req.Shard, req.Epoch)
	}
}
