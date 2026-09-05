// Package profile 实现 §6.5 的 profile 只读摘要（CQRS 读模型）。
//
// 问题：查看好友资料、排行榜、给离线玩家发邮件时，需要读一个不在本分片、
// 甚至不在线的玩家数据。为看一眼头像就加载整个玩家对象进内存是不可接受的。
//
// 方案：owner 分片在数据变更时顺带写一份轻量快照。
// 只读、允许滞后几秒、任何进程可直接读 Redis，不经过 owner，不唤醒玩家对象。
//
// 写模型在内存（owner 独占强一致），读模型在 Redis（人人可读，最终一致）。
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

// 字段名。用 HASH 而非 protobuf blob，任何进程/运维工具都能直接看懂与部分读取。
const (
	FieldNick      = "nick"
	FieldAvatar    = "avatar"
	FieldLevel     = "level"
	FieldGuildID   = "guild_id"
	FieldPower     = "power"
	FieldLastLogin = "last_login"
)

var allFields = []string{FieldNick, FieldAvatar, FieldLevel, FieldGuildID, FieldPower, FieldLastLogin}

// Fields 把 Profile 转成 HSET 的 field/value 序列。
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

// HashWrite 构造一次带 epoch 校验的 profile 写入，交给刷盘器执行。
//
// profile 与玩家数据键共用 hash tag，因此可以和玩家模块在同一个 Lua/slot 中写。
func HashWrite(keys *store.Keys, p *pb.Profile) *store.HashWrite {
	return &store.HashWrite{
		Key:    keys.Profile(p.GetUid()),
		TTLSec: 0, // 摘要不过期：离线玩家的资料也要能被查看
		Fields: Fields(p),
	}
}

// Reader 直接从 Redis 读摘要，不经过 owner 分片。
type Reader struct {
	rdb  redis.UniversalClient
	keys *store.Keys
}

// NewReader 构造读取器。
func NewReader(rdb redis.UniversalClient, keys *store.Keys) *Reader {
	return &Reader{rdb: rdb, keys: keys}
}

// Get 读取单个玩家摘要。玩家从未存在时返回 nil。
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

// GetMany 批量读取（好友列表、公会成员、排行榜展示）。
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

// Exists 报告玩家是否存在过（不唤醒玩家对象）。
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

// Rank 是基于摘要的排行榜（ZSET）。
//
// 它同样是读模型：允许滞后，读取不经过 owner。
// 排行榜键不与玩家键同 slot，因此不纳入 epoch fencing —— 陈旧 owner 偶发写入
// 只会让某个名次短暂不准，下次真 owner 更新即自愈，不构成资产不一致。
type Rank struct {
	rdb  redis.UniversalClient
	keys *store.Keys
	name string
}

// NewRank 构造排行榜。
func NewRank(rdb redis.UniversalClient, keys *store.Keys, name string) *Rank {
	return &Rank{rdb: rdb, keys: keys, name: name}
}

// Update 更新玩家分数。
func (r *Rank) Update(ctx context.Context, uid uint64, score float64) error {
	return r.rdb.ZAdd(ctx, r.keys.Rank(r.name), redis.Z{Score: score, Member: uid}).Err()
}

// Entry 是一条榜单记录。
type Entry struct {
	Rank    int64
	UID     uint64
	Score   float64
	Profile *pb.Profile
}

// Top 取前 n 名，并批量补齐摘要。
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
		return entries, nil // 榜单本体已可用，摘要缺失不算失败
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

// RankOf 返回玩家名次（从 1 开始），不在榜返回 0。
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

// FromBase 由玩家 base 模块构造摘要。
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

// StaleAfter 是摘要允许的滞后上限，仅用于文档与监控参考。
const StaleAfter = 10 * time.Second
