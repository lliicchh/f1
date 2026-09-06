// Package profile 玩家的只读摘要，也就是 CQRS 的读模型
//
// 查好友资料、排行榜、给离线玩家发信时都要读别人的数据。为看一眼头像就把
// 整个玩家对象加载进内存不划算，所以 owner 分片在数据变更时顺手写一份轻量快照。
//
// 只读，允许滞后几秒，任何进程都能直接读 Redis
package profile

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/store"
)

// 用 HASH 不用 protobuf，运维工具能直接看，也能只读其中几个字段
const (
	FieldNick      = "nick"
	FieldAvatar    = "avatar"
	FieldLevel     = "level"
	FieldGuildID   = "guild_id"
	FieldPower     = "power"
	FieldLastLogin = "last_login"
)

var allFields = []string{FieldNick, FieldAvatar, FieldLevel, FieldGuildID, FieldPower, FieldLastLogin}

// Fields 把 Profile 摊成 HSET 要的 field/value
func Fields(p *pb.Profile) []any {
	return []any{
		FieldNick, p.GetNick(),
		FieldAvatar, p.GetAvatar(),
		FieldLevel, strconv.FormatUint(uint64(p.GetLevel()), 10),
		FieldGuildID, strconv.FormatUint(p.GetGuildId(), 10),
		FieldPower, strconv.FormatUint(p.GetPower(), 10),
		FieldLastLogin, strconv.FormatInt(p.GetLastLogin(), 10),
	}
}

// HashWrite 构造一次带 epoch 校验的摘要写入，交给刷盘器。
// 它和玩家键同 hash tag，能进同一个 Lua
func HashWrite(keys *store.Keys, p *pb.Profile) *store.HashWrite {
	return &store.HashWrite{
		Key:    keys.Profile(p.GetUid()),
		TTLSec: 0, // 摘要不过期：离线玩家的资料也要能被查看
		Fields: Fields(p),
	}
}

// Reader 直接从 Redis 读摘要，不经过 owner
type Reader struct {
	rdb  redis.UniversalClient
	keys *store.Keys
}

func NewReader(rdb redis.UniversalClient, keys *store.Keys) *Reader {
	return &Reader{rdb: rdb, keys: keys}
}

// Get 读一个玩家的摘要，没有就返回 nil
func (r *Reader) Get(ctx context.Context, uid uint64) (*pb.Profile, error) {
	vals, err := r.rdb.HMGet(ctx, r.keys.Profile(uid), allFields...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	return decode(uid, vals), nil
}

// GetMany 批量读，好友列表、公会成员、排行榜都用它
func (r *Reader) GetMany(ctx context.Context, uids []uint64) ([]*pb.Profile, error) {
	if len(uids) == 0 {
		return nil, nil
	}
	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(uids))
	for i, uid := range uids {
		cmds[i] = pipe.HMGet(ctx, r.keys.Profile(uid), allFields...)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]*pb.Profile, 0, len(uids))
	for i, cmd := range cmds {
		vals, err := cmd.Result()
		if err != nil {
			continue
		}
		if p := decode(uids[i], vals); p != nil {
			out = append(out, p)
		}
	}
	return out, nil
}

// Exists 看这个玩家存不存在，不唤醒玩家对象
func (r *Reader) Exists(ctx context.Context, uid uint64) (bool, error) {
	n, err := r.rdb.Exists(ctx, r.keys.Profile(uid)).Result()
	return n > 0, err
}

func decode(uid uint64, vals []any) *pb.Profile {
	if len(vals) < len(allFields) {
		return nil
	}
	empty := true
	for _, v := range vals {
		if v != nil {
			empty = false
			break
		}
	}
	if empty {
		return nil
	}

	p := &pb.Profile{Uid: uid}
	if s, ok := vals[0].(string); ok {
		p.Nick = s
	}
	if s, ok := vals[1].(string); ok {
		p.Avatar = s
	}
	if s, ok := vals[2].(string); ok {
		n, _ := strconv.ParseUint(s, 10, 32)
		p.Level = uint32(n)
	}
	if s, ok := vals[3].(string); ok {
		p.GuildId, _ = strconv.ParseUint(s, 10, 64)
	}
	if s, ok := vals[4].(string); ok {
		p.Power, _ = strconv.ParseUint(s, 10, 64)
	}
	if s, ok := vals[5].(string); ok {
		p.LastLogin, _ = strconv.ParseInt(s, 10, 64)
	}
	return p
}

// ---------------------------------------------------------------------------

// Rank 基于摘要的排行榜，同样允许滞后
//
// 榜单键和玩家键不同 slot，所以没纳入 fencing。陈旧 owner 偶尔写一下只会让
// 某个名次短暂不准，下次真 owner 更新就好了，不影响资产
type Rank struct {
	rdb  redis.UniversalClient
	keys *store.Keys
	name string
}

func NewRank(rdb redis.UniversalClient, keys *store.Keys, name string) *Rank {
	return &Rank{rdb: rdb, keys: keys, name: name}
}

func (r *Rank) Update(ctx context.Context, uid uint64, score float64) error {
	return r.rdb.ZAdd(ctx, r.keys.Rank(r.name), redis.Z{Score: score, Member: uid}).Err()
}

// Entry 一条榜单记录
type Entry struct {
	Rank    int64
	UID     uint64
	Score   float64
	Profile *pb.Profile
}

// Top 取前 n 名并补上摘要
func (r *Rank) Top(ctx context.Context, n int64, reader *Reader) ([]Entry, error) {
	zs, err := r.rdb.ZRevRangeWithScores(ctx, r.keys.Rank(r.name), 0, n-1).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(zs))
	uids := make([]uint64, 0, len(zs))
	for i, z := range zs {
		uid, _ := strconv.ParseUint(z.Member.(string), 10, 64)
		entries = append(entries, Entry{Rank: int64(i) + 1, UID: uid, Score: z.Score})
		uids = append(uids, uid)
	}
	if reader == nil {
		return entries, nil
	}
	profiles, err := reader.GetMany(ctx, uids)
	if err != nil {
		return entries, nil // 榜单能用就行，摘要没读到不算失败
	}
	byUID := make(map[uint64]*pb.Profile, len(profiles))
	for _, p := range profiles {
		byUID[p.GetUid()] = p
	}
	for i := range entries {
		entries[i].Profile = byUID[entries[i].UID]
	}
	return entries, nil
}

// RankOf 返回名次，从 1 开始，不在榜返回 0
func (r *Rank) RankOf(ctx context.Context, uid uint64) (int64, error) {
	n, err := r.rdb.ZRevRank(ctx, r.keys.Rank(r.name), strconv.FormatUint(uid, 10)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return n + 1, nil
}

// FromBase 从 base 模块拼出摘要
func FromBase(b *pb.PlayerBase) *pb.Profile {
	return &pb.Profile{
		Uid:       b.GetUid(),
		Nick:      b.GetNick(),
		Avatar:    b.GetAvatar(),
		Level:     b.GetLevel(),
		GuildId:   b.GetGuildId(),
		Power:     b.GetPower(),
		LastLogin: b.GetLastLogin(),
	}
}

// StaleAfter 摘要允许滞后的上限，给监控参考用
const StaleAfter = 10 * time.Second
