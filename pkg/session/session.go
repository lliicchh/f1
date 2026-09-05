// Package session 实现 §9 的网关路由表与顶号。
//
//	session:{uid} → {gateID, connID}   (Redis，TTL + 心跳续期)
//
// 网关只订两个 subject（push.gate.{gateID} 与 push.broadcast），
// 订阅数与在线人数无关；代价是多一次查表，可本地缓存，注意顶号时失效（§9.1 / D4）。
package session

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/store"
)

// Info 是一条会话路由。
type Info struct {
	UID     uint64
	GateID  string
	ConnID  uint64
	LoginAt int64
	BeatAt  int64
}

// Empty 报告是否为空路由（玩家不在线）。
func (i Info) Empty() bool { return i.GateID == "" }

// ErrOffline 表示玩家不在线。
var ErrOffline = errors.New("session: 玩家不在线")

// scriptBind 原子替换 session，并返回被顶掉的旧 gateID/connID（§9.2）。
var scriptBind = redis.NewScript(`
local old = redis.call('HMGET', KEYS[1], 'gate_id', 'conn_id')
redis.call('HSET', KEYS[1],
  'uid', ARGV[1], 'gate_id', ARGV[2], 'conn_id', ARGV[3],
  'login_at', ARGV[4], 'beat_at', ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[5])
return {old[1] or '', old[2] or ''}
`)

// scriptUnbind 只在 gate/conn 都匹配时才删除。
//
// 这条件很关键：顶号后旧连接的清理消息会迟到，
// 若无条件删除会把刚建立的新会话一起删掉。
var scriptUnbind = redis.NewScript(`
local g = redis.call('HGET', KEYS[1], 'gate_id')
local c = redis.call('HGET', KEYS[1], 'conn_id')
if g == ARGV[1] and c == ARGV[2] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)

// scriptTouch 心跳续期，同样要求 gate/conn 匹配。
var scriptTouch = redis.NewScript(`
local g = redis.call('HGET', KEYS[1], 'gate_id')
local c = redis.call('HGET', KEYS[1], 'conn_id')
if g ~= ARGV[1] or c ~= ARGV[2] then
  return 0
end
redis.call('HSET', KEYS[1], 'beat_at', ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 1
`)

// Store 是会话路由表的 Redis 实现。
type Store struct {
	rdb  redis.UniversalClient
	keys *store.Keys
	ttl  time.Duration
}

// NewStore 构造会话表。
func NewStore(rdb redis.UniversalClient, keys *store.Keys, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Store{rdb: rdb, keys: keys, ttl: ttl}
}

// TTL 返回会话 TTL。
func (s *Store) TTL() time.Duration { return s.ttl }

// Bind 建立会话，返回被顶掉的旧会话（若有）。
//
// 顶号流程：Lua 原子替换 session，取出旧 gateID 后由调用方发 KICK（§9.2）。
func (s *Store) Bind(ctx context.Context, uid uint64, gateID string, connID uint64) (old Info, err error) {
	now := time.Now().UnixMilli()
	res, err := scriptBind.Run(ctx, s.rdb, []string{s.keys.Session(uid)},
		uid, gateID, connID, now, int64(s.ttl.Seconds())).Slice()
	if err != nil {
		return Info{}, fmt.Errorf("绑定会话失败 uid=%d: %w", uid, err)
	}

	// 索引到网关名下，供网关重启时清理（§9.4）。不同 slot，必须单独执行。
	if err := s.rdb.SAdd(ctx, s.keys.GateSessions(gateID), uid).Err(); err != nil {
		logx.Warn("写入网关会话索引失败（不阻断登录）", "gate", gateID, "uid", uid, "err", err)
	}

	oldGate, _ := res[0].(string)
	if oldGate == "" {
		return Info{}, nil
	}
	oldConn, _ := res[1].(string)
	cid, _ := strconv.ParseUint(oldConn, 10, 64)
	metrics.SessionKick.Inc()
	return Info{UID: uid, GateID: oldGate, ConnID: cid}, nil
}

// Unbind 删除会话（仅当 gate/conn 匹配）。
func (s *Store) Unbind(ctx context.Context, uid uint64, gateID string, connID uint64) (bool, error) {
	n, err := scriptUnbind.Run(ctx, s.rdb, []string{s.keys.Session(uid)}, gateID, strconv.FormatUint(connID, 10)).Int64()
	if err != nil {
		return false, err
	}
	_ = s.rdb.SRem(ctx, s.keys.GateSessions(gateID), uid).Err()
	return n == 1, nil
}

// Touch 心跳续期。返回 false 表示该连接已不是当前会话（已被顶号）。
func (s *Store) Touch(ctx context.Context, uid uint64, gateID string, connID uint64) (bool, error) {
	n, err := scriptTouch.Run(ctx, s.rdb, []string{s.keys.Session(uid)},
		gateID, strconv.FormatUint(connID, 10), time.Now().UnixMilli(), int64(s.ttl.Seconds())).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Get 查询单个会话。
func (s *Store) Get(ctx context.Context, uid uint64) (Info, error) {
	vals, err := s.rdb.HMGet(ctx, s.keys.Session(uid), "gate_id", "conn_id", "login_at", "beat_at").Result()
	if err != nil {
		return Info{}, err
	}
	return parseInfo(uid, vals), nil
}

// GetMany 批量查询会话，用于房间/公会广播按 gateID 聚合（§9.3）。
func (s *Store) GetMany(ctx context.Context, uids []uint64) (map[uint64]Info, error) {
	if len(uids) == 0 {
		return nil, nil
	}
	pipe := s.rdb.Pipeline()
	cmds := make(map[uint64]*redis.SliceCmd, len(uids))
	for _, uid := range uids {
		cmds[uid] = pipe.HMGet(ctx, s.keys.Session(uid), "gate_id", "conn_id", "login_at", "beat_at")
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make(map[uint64]Info, len(uids))
	for uid, cmd := range cmds {
		vals, err := cmd.Result()
		if err != nil {
			continue
		}
		if info := parseInfo(uid, vals); !info.Empty() {
			out[uid] = info
		}
	}
	return out, nil
}

// GroupByGate 把 uid 列表按所在网关聚合。
//
// 房间/公会广播不走 NATS 广播：由 Room 分片批量查路由表，按 gateID 聚合，
// 每个网关发一条带多个 uid 的包（§9.3）。
func (s *Store) GroupByGate(ctx context.Context, uids []uint64) (map[string][]uint64, error) {
	infos, err := s.GetMany(ctx, uids)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]uint64, 4)
	for uid, info := range infos {
		out[info.GateID] = append(out[info.GateID], uid)
	}
	return out, nil
}

// CleanGate 清理某网关名下所有会话（网关启动时调用，§9.4）。
//
// 两种清理都要做：这里是启动时的主动清理，另一条是 session TTL 由心跳续期，
// 网关崩溃后不再续期，自然过期。
func (s *Store) CleanGate(ctx context.Context, gateID string) (int, error) {
	idxKey := s.keys.GateSessions(gateID)
	uids, err := s.rdb.SMembers(ctx, idxKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}

	cleaned := 0
	for _, raw := range uids {
		uid, perr := strconv.ParseUint(raw, 10, 64)
		if perr != nil {
			continue
		}
		// 只删仍指向本网关的会话：玩家可能已经重连到别的网关了。
		info, gerr := s.Get(ctx, uid)
		if gerr != nil {
			continue
		}
		if info.GateID == gateID {
			if _, derr := s.Unbind(ctx, uid, gateID, info.ConnID); derr == nil {
				cleaned++
			}
		}
	}
	if err := s.rdb.Del(ctx, idxKey).Err(); err != nil {
		logx.Warn("删除网关会话索引失败", "gate", gateID, "err", err)
	}
	if cleaned > 0 || len(uids) > 0 {
		logx.Info("网关启动清理完成", "gate", gateID, "indexed", len(uids), "cleaned", cleaned)
	}
	return cleaned, nil
}

func parseInfo(uid uint64, vals []any) Info {
	info := Info{UID: uid}
	if len(vals) < 4 {
		return info
	}
	if g, ok := vals[0].(string); ok {
		info.GateID = g
	}
	if c, ok := vals[1].(string); ok {
		info.ConnID, _ = strconv.ParseUint(c, 10, 64)
	}
	if l, ok := vals[2].(string); ok {
		info.LoginAt, _ = strconv.ParseInt(l, 10, 64)
	}
	if b, ok := vals[3].(string); ok {
		info.BeatAt, _ = strconv.ParseInt(b, 10, 64)
	}
	return info
}

// ---------------------------------------------------------------------------

// Cache 是会话路由的本地缓存。
//
// 「代价是多一次查询（可本地缓存，注意顶号时失效）」（§9.1）。
// 失效由 evt.session.changed 事件驱动，另配一个很短的兜底 TTL。
type Cache struct {
	store *Store
	ttl   time.Duration

	mu    sync.RWMutex
	items map[uint64]cacheItem
}

type cacheItem struct {
	info Info
	exp  time.Time
}

// NewCache 构造缓存。ttl 建议 1~3s：它只是兜底，主失效路径是事件。
func NewCache(s *Store, ttl time.Duration) *Cache {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &Cache{store: s, ttl: ttl, items: make(map[uint64]cacheItem)}
}

// Get 查询会话，优先走缓存。
func (c *Cache) Get(ctx context.Context, uid uint64) (Info, error) {
	c.mu.RLock()
	it, ok := c.items[uid]
	c.mu.RUnlock()
	if ok && time.Now().Before(it.exp) {
		return it.info, nil
	}

	info, err := c.store.Get(ctx, uid)
	if err != nil {
		return Info{}, err
	}
	c.mu.Lock()
	c.items[uid] = cacheItem{info: info, exp: time.Now().Add(c.ttl)}
	c.mu.Unlock()
	return info, nil
}

// Invalidate 让某玩家的缓存失效（收到 evt.session.changed 时调用）。
func (c *Cache) Invalidate(uid uint64) {
	c.mu.Lock()
	delete(c.items, uid)
	c.mu.Unlock()
}

// Purge 清空缓存。
func (c *Cache) Purge() {
	c.mu.Lock()
	c.items = make(map[uint64]cacheItem)
	c.mu.Unlock()
}
