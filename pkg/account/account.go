// Package account 渠道账号到内部 uid 的绑定
//
// 一个 uid 可以绑多个渠道，任一渠道登录都回到同一个玩家。绑定表是登录链路上
// 唯一的持久状态，写进去就不能错：同一个渠道账号绝不能指向两个 uid，
// 否则玩家换个入口进来看到的是别人的号。
//
// 所有键共用 {acct} 这一个 hash tag。绑定的正反两张表要在同一个 Lua 里改，
// 而正表按渠道账号寻址、反表按 uid 寻址，没有第三种 tag 能同时满足两边。
// 代价是账号数据不分片，收益是 Cluster 下也不会撞 CROSSSLOT。这一层的量很小：
// 只有首登和换设备会读它，重连拿旧 token 走网关本地验签，根本不到这儿
package account

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrBindConflict 表示这个渠道账号已经绑在别的 uid 上
	ErrBindConflict = errors.New("account: 渠道账号已被其他玩家绑定")
	// ErrAlreadyBound 表示本 uid 在这个渠道已经绑了另一个账号
	ErrAlreadyBound = errors.New("account: 该渠道已绑定其他账号")
	// ErrNotBound 表示要解绑的渠道本来就没绑
	ErrNotBound = errors.New("account: 该渠道未绑定")
	// ErrLastBinding 表示这是最后一个绑定，解了就再也登不进来
	ErrLastBinding = errors.New("account: 不能解绑最后一个登录方式")
	// ErrRaced 表示读到写之间绑定被改了，重试即可
	ErrRaced = errors.New("account: 绑定期间发生变更，请重试")
)

// Binding 一条渠道绑定
type Binding struct {
	Channel string
	OpenID  string
	BoundAt time.Time
}

// Store 绑定表
type Store struct {
	rdb    redis.UniversalClient
	prefix string
}

// NewStore 建绑定表，prefix 为空取 acct
func NewStore(rdb redis.UniversalClient, prefix string) *Store {
	if prefix == "" {
		prefix = "acct"
	}
	return &Store{rdb: rdb, prefix: prefix}
}

// bindKey 渠道账号到 uid 的正表键
func (s *Store) bindKey(channel, openID string) string {
	return fmt.Sprintf("%s:{acct}:bind:%s:%s", s.prefix, channel, openID)
}

// uidKey uid 名下所有绑定的反表键，HASH channel → boundAt:openid
func (s *Store) uidKey(uid uint64) string {
	return fmt.Sprintf("%s:{acct}:uid:%d", s.prefix, uid)
}

// scriptResolve 登录：绑过就返回原 uid，没绑过就用候选 uid 开号
//
//	KEYS[1] = 正表键
//	KEYS[2] = 候选 uid 的反表键
//	ARGV[1] = 候选 uid（雪花预先生成，没用上就丢掉）
//	ARGV[2] = 渠道
//	ARGV[3] = openid
//	ARGV[4] = 当前毫秒
//
// 返回 {是否新开号, uid}。并发首登时只有一个能 SET 成功，另一个读到已有 uid，
// 两边最后拿到的是同一个号
var scriptResolve = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur then
  return {0, cur}
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[4] .. ':' .. ARGV[3])
return {1, ARGV[1]}
`)

// scriptBind 给已有 uid 再绑一个渠道
//
//	KEYS[1] = 正表键
//	KEYS[2] = 本 uid 的反表键
//	ARGV[1] = uid
//	ARGV[2] = 渠道
//	ARGV[3] = openid
//	ARGV[4] = 当前毫秒
//
// 返回 0 成功，1 渠道账号已属他人，2 本 uid 该渠道已绑别的账号
var scriptBind = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur then
  if cur == ARGV[1] then return 0 end
  return 1
end
if redis.call('HEXISTS', KEYS[2], ARGV[2]) == 1 then
  return 2
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[4] .. ':' .. ARGV[3])
return 0
`)

// scriptUnbind 解绑，正反表一起删
//
//	KEYS[1] = 本 uid 的反表键
//	KEYS[2] = 正表键（按调用方读到的 openid 拼的）
//	ARGV[1] = 渠道
//	ARGV[2] = 调用方读到的 openid
//
// 返回 0 成功，1 未绑定，2 读到写之间被改过，3 这是最后一个绑定。
// openid 要先读一次才知道，所以带着它进来做 CAS，不然并发解绑会删错键
var scriptUnbind = redis.NewScript(`
local v = redis.call('HGET', KEYS[1], ARGV[1])
if not v then return 1 end
local sep = string.find(v, ':', 1, true)
if not sep or string.sub(v, sep + 1) ~= ARGV[2] then return 2 end
if redis.call('HLEN', KEYS[1]) <= 1 then return 3 end
redis.call('DEL', KEYS[2])
redis.call('HDEL', KEYS[1], ARGV[1])
return 0
`)

// Resolve 按渠道账号找 uid，没有就用 candidate 开一个新号
//
// candidate 由调用方用雪花生成。并发首登时会浪费掉一个号，没有副作用：
// 雪花号段够用，而且没写进任何表
func (s *Store) Resolve(ctx context.Context, channel, openID string, candidate uint64) (uid uint64, created bool, err error) {
	if channel == "" || openID == "" {
		return 0, false, fmt.Errorf("account: 渠道和 openid 都不能为空")
	}
	res, err := scriptResolve.Run(ctx, s.rdb,
		[]string{s.bindKey(channel, openID), s.uidKey(candidate)},
		candidate, channel, openID, time.Now().UnixMilli()).Slice()
	if err != nil {
		return 0, false, err
	}
	if len(res) != 2 {
		return 0, false, fmt.Errorf("account: Resolve 返回了 %d 个值", len(res))
	}
	flag, _ := res[0].(int64)
	uid, err = toUID(res[1])
	if err != nil {
		return 0, false, err
	}
	return uid, flag == 1, nil
}

// Bind 给 uid 绑一个新渠道。重复绑同一个账号当成功
func (s *Store) Bind(ctx context.Context, uid uint64, channel, openID string) error {
	if uid == 0 || channel == "" || openID == "" {
		return fmt.Errorf("account: uid、渠道和 openid 都不能为空")
	}
	code, err := scriptBind.Run(ctx, s.rdb,
		[]string{s.bindKey(channel, openID), s.uidKey(uid)},
		strconv.FormatUint(uid, 10), channel, openID, time.Now().UnixMilli()).Int64()
	if err != nil {
		return err
	}
	switch code {
	case 0:
		return nil
	case 1:
		return ErrBindConflict
	default:
		return ErrAlreadyBound
	}
}

// Unbind 解绑一个渠道，最后一个不让解
func (s *Store) Unbind(ctx context.Context, uid uint64, channel string) error {
	if uid == 0 || channel == "" {
		return fmt.Errorf("account: uid 和渠道都不能为空")
	}
	raw, err := s.rdb.HGet(ctx, s.uidKey(uid), channel).Result()
	if errors.Is(err, redis.Nil) {
		return ErrNotBound
	}
	if err != nil {
		return err
	}
	_, openID, ok := strings.Cut(raw, ":")
	if !ok {
		return fmt.Errorf("account: 绑定记录格式异常: %q", raw)
	}

	code, err := scriptUnbind.Run(ctx, s.rdb,
		[]string{s.uidKey(uid), s.bindKey(channel, openID)},
		channel, openID).Int64()
	if err != nil {
		return err
	}
	switch code {
	case 0:
		return nil
	case 1:
		return ErrNotBound
	case 2:
		return ErrRaced
	default:
		return ErrLastBinding
	}
}

// List 列出 uid 名下的全部绑定
func (s *Store) List(ctx context.Context, uid uint64) ([]Binding, error) {
	m, err := s.rdb.HGetAll(ctx, s.uidKey(uid)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Binding, 0, len(m))
	for ch, raw := range m {
		ms, openID, ok := strings.Cut(raw, ":")
		if !ok {
			continue
		}
		at, _ := strconv.ParseInt(ms, 10, 64)
		out = append(out, Binding{Channel: ch, OpenID: openID, BoundAt: time.UnixMilli(at)})
	}
	return out, nil
}

// UIDOf 只查不建，给运维接口用
func (s *Store) UIDOf(ctx context.Context, channel, openID string) (uint64, bool, error) {
	v, err := s.rdb.Get(ctx, s.bindKey(channel, openID)).Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

func toUID(v any) (uint64, error) {
	switch t := v.(type) {
	case int64:
		return uint64(t), nil
	case string:
		return strconv.ParseUint(t, 10, 64)
	default:
		return 0, fmt.Errorf("account: uid 类型异常 %T", v)
	}
}
