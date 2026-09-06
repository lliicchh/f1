// Package session 网关路由表和顶号
//
//	session:{uid} → {gateID, connID}，带 TTL，靠心跳续期
//
// 网关只订 push.gate.{gateID} 和 push.broadcast 两个 subject，订阅数与在线人数无关。
// 代价是每次推送多一次查表，可以本地缓存，但顶号时要让缓存失效
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

// Info 一条会话路由
type Info struct {
	UID     uint64
	GateID  string
	ConnID  uint64
	LoginAt int64
	BeatAt  int64
}

// Empty 报告是否为空路由（玩家不在线）
func (i Info) Empty() bool { return i.GateID == "" }

// ErrOffline 表示玩家不在线
var ErrOffline = errors.New("session: 玩家不在线")

// scriptBind 原子替换 session，顺便返回被顶掉的旧 gateID 和 connID
var scriptBind = redis.NewScript(`
local old = redis.call('HMGET', KEYS[1], 'gate_id', 'conn_id')
redis.call('HSET', KEYS[1],
  'uid', ARGV[1], 'gate_id', ARGV[2], 'conn_id', ARGV[3],
  'login_at', ARGV[4], 'beat_at', ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[5])
return {old[1] or '', old[2] or ''}
`)

// scriptUnbind 只在 gate 和 conn 都对得上时才删
//
// 顶号后旧连接的清理会迟到，无条件删会把刚建好的新会话一起删了
var scriptUnbind = redis.NewScript(`
local g = redis.call('HGET', KEYS[1], 'gate_id')
local c = redis.call('HGET', KEYS[1], 'conn_id')
if g == ARGV[1] and c == ARGV[2] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)

// scriptTouch 心跳续期，同样要求 gate 和 conn 匹配
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

// Store 会话路由表的 Redis 实现
type Store struct {
	rdb  redis.UniversalClient
	keys *store.Keys
	ttl  time.Duration
}

func NewStore(rdb redis.UniversalClient, keys *store.Keys, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Store{rdb: rdb, keys: keys, ttl: ttl}
}

func (s *Store) TTL() time.Duration { return s.ttl }

// Bind 建会话，返回被顶掉的旧会话。调用方拿到旧 gateID 后去发 KICK
func (s *Store) Bind(ctx context.Context, uid uint64, gateID string, connID uint64) (old Info, err error) {
	now := time.Now().UnixMilli()
	res, err := scriptBind.Run(ctx, s.rdb, []string{s.keys.Session(uid)},
		uid, gateID, connID, now, int64(s.ttl.Seconds())).Slice()
	if err != nil {
		return Info{}, fmt.Errorf("绑定会话失败 uid=%d: %w", uid, err)
	}

	// 挂到网关名下，重启时靠它清理。不同 slot，只能单独执行
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

// Unbind 删会话，只在 gate 和 conn 匹配时生效
func (s *Store) Unbind(ctx context.Context, uid uint64, gateID string, connID uint64) (bool, error) {
	n, err := scriptUnbind.Run(ctx, s.rdb, []string{s.keys.Session(uid)}, gateID, strconv.FormatUint(connID, 10)).Int64()
	if err != nil {
		return false, err
	}
	_ = s.rdb.SRem(ctx, s.keys.GateSessions(gateID), uid).Err()
	return n == 1, nil
}

// Touch 心跳续期。返回 false 说明这条连接已经被顶掉了
func (s *Store) Touch(ctx context.Context, uid uint64, gateID string, connID uint64) (bool, error) {
	n, err := scriptTouch.Run(ctx, s.rdb, []string{s.keys.Session(uid)},
		gateID, strconv.FormatUint(connID, 10), time.Now().UnixMilli(), int64(s.ttl.Seconds())).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) Get(ctx context.Context, uid uint64) (Info, error) {
	vals, err := s.rdb.HMGet(ctx, s.keys.Session(uid), "gate_id", "conn_id", "login_at", "beat_at").Result()
	if err != nil {
		return Info{}, err
	}
	return parseInfo(uid, vals), nil
}

// GetMany 批量查会话，房间和公会广播要用
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

// GroupByGate 把一批 uid 按所在网关分组
//
// 房间和公会广播不走 NATS 广播，而是查一遍路由表按网关聚合，每个网关发一条包
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

// CleanGate 清掉某网关名下的会话，网关启动时调
//
// 两条路都要有：这里是主动清，另一条是 TTL 到期自然消失
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
		// 只删还指向本网关的，玩家可能已经重连到别处了
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

// Cache 缓存会话路由，省掉每次推送的一次查表
//
// 失效靠 evt.session.changed 事件，另配一个很短的 TTL 兜底
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

// NewCache 构造缓存。ttl 给 1~3s 就行，它只是兜底，主要靠事件失效
func NewCache(s *Store, ttl time.Duration) *Cache {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &Cache{store: s, ttl: ttl, items: make(map[uint64]cacheItem)}
}

// Get 查会话，先看缓存
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

// Invalidate 让某个玩家的缓存失效
func (c *Cache) Invalidate(uid uint64) {
	c.mu.Lock()
	delete(c.items, uid)
	c.mu.Unlock()
}

func (c *Cache) Purge() {
	c.mu.Lock()
	c.items = make(map[uint64]cacheItem)
	c.mu.Unlock()
}
