// Package store Redis 持久化层：key 规范、epoch fencing、分级刷盘
//
// 内存是权威副本，Redis 只是它的投影。写入都要过 epoch 校验，失败都要重新标脏，
// 绝不丢弃
package store

import (
	"fmt"
	"strconv"
)

// Module 玩家数据的模块划分，一个模块一个 Redis key。
// 拆开是为了不让改一个字段重写整个玩家
type Module string

const (
	ModBase   Module = "base"   // 等级、经验、货币   高频、小
	ModBag    Module = "bag"    // 背包              中频、大
	ModQuest  Module = "quest"  // 任务              中频
	ModSocial Module = "social" // 好友、公会         低频
	ModMail   Module = "mail"   // 邮件              低频

	// ModRound 未结算的回合，必须和投注在同一次提交里落盘。
	// 回合落后于扣款，玩家断线重连就是「钱扣了免费旋转没了」
	ModRound Module = "round"
	// ModRG 责任游戏限额累计，同样要和投注一起推进
	ModRG Module = "rg"
	// ModGacha 抽卡保底计数，要和抽卡消耗一起推进
	ModGacha Module = "gacha"
)

// AllModules 全部玩家模块，登录时一次 pipeline 读回
var AllModules = []Module{
	ModBase, ModBag, ModQuest, ModSocial, ModMail,
	ModRound, ModRG, ModGacha,
}

// HashTagMode 决定 Redis key 的 hash tag 取法
type HashTagMode string

const (
	// TagUID 形如 p:{1001}:base，同一玩家的 key 落同一个 slot，
	// 单玩家的 Lua 和 pipeline 才能用
	TagUID HashTagMode = "uid"

	// TagShard 形如 p:{s7}:1001:base，给 Redis Cluster 用：
	// epoch 键和该分片下所有玩家键同 slot，fencing 的 Lua 才不会撞 CROSSSLOT。
	// 单实例或主从哨兵没这个约束，用 TagUID 就行
	TagShard HashTagMode = "shard"
)

// Keys 按配置生成 Redis key
type Keys struct {
	mode       HashTagMode
	shardCount uint32
	kind       string // "lobby" / "room"，用于 epoch 键
}

// NewKeys 构造 key 生成器
func NewKeys(mode HashTagMode, shardCount uint32, kind string) *Keys {
	if mode == "" {
		mode = TagUID
	}
	return &Keys{mode: mode, shardCount: shardCount, kind: kind}
}

func (k *Keys) Mode() HashTagMode { return k.mode }

func (k *Keys) Shard(id uint64) uint32 { return uint32(id % uint64(k.shardCount)) }

// tag 返回 hash tag 的内容，不含大括号
func (k *Keys) tag(id uint64) string {
	if k.mode == TagShard {
		return "s" + strconv.FormatUint(uint64(k.Shard(id)), 10)
	}
	return strconv.FormatUint(id, 10)
}

// Player 返回玩家某模块的 key，形如 p:{1001}:base
//
// 前缀用 p: 不用 lobby:：key 跟数据走不跟服务走，以后 Lobby 改名不用迁数据
func (k *Keys) Player(uid uint64, m Module) string {
	if k.mode == TagShard {
		return fmt.Sprintf("p:{%s}:%d:%s", k.tag(uid), uid, m)
	}
	return fmt.Sprintf("p:{%d}:%s", uid, m)
}

// PlayerAll 返回玩家全部模块的 key，顺序同 AllModules
func (k *Keys) PlayerAll(uid uint64) []string {
	out := make([]string, 0, len(AllModules))
	for _, m := range AllModules {
		out = append(out, k.Player(uid, m))
	}
	return out
}

// Profile 返回只读摘要的 key，与玩家键同 tag
func (k *Keys) Profile(uid uint64) string {
	if k.mode == TagShard {
		return fmt.Sprintf("profile:{%s}:%d", k.tag(uid), uid)
	}
	return fmt.Sprintf("profile:{%d}", uid)
}

func (k *Keys) Room(roomID uint64) string {
	return fmt.Sprintf("room:{s%d}:%d", k.Shard(roomID), roomID)
}

// RoomIndex 分片的房间索引，接管时靠它恢复房间列表
func (k *Keys) RoomIndex(shard uint32) string {
	return fmt.Sprintf("room:{s%d}:index", shard)
}

// Epoch 返回分片 epoch 键。加 hash tag 是为了 Cluster 下与数据键同 slot，
// 单实例下大括号就是普通字符
func (k *Keys) Epoch(shard uint32) string {
	return fmt.Sprintf("shard:%s:{s%d}:epoch", k.kind, shard)
}

func (k *Keys) EpochOf(id uint64) string { return k.Epoch(k.Shard(id)) }

func (k *Keys) Session(uid uint64) string { return fmt.Sprintf("session:{%d}", uid) }

// GateSessions 某网关名下的会话索引，网关重启时靠它清理
func (k *Keys) GateSessions(gateID string) string { return "gate:" + gateID + ":sessions" }

// Tx 返回跨分片转移记录的 key
//
// hash tag 用发起方分片而不是 txid：扣道具和写 tx 记录要进同一个 Lua，
// 必须同 slot。pending 索引同理
func (k *Keys) Tx(fromShard uint32, txid string) string {
	return fmt.Sprintf("tx:{s%d}:%s", fromShard, txid)
}

// TxDone 接收方的幂等标记。hash tag 用接收方分片，
// 让 SET done NX 和加道具能进同一个 Lua
func (k *Keys) TxDone(toShard uint32, txid string) string {
	return fmt.Sprintf("txdone:{s%d}:%s", toShard, txid)
}

// TxPending 某分片的 PENDING 转移索引，score 为创建时间。
// 按分片切分，每个 owner 只重投自己发起的那些
func (k *Keys) TxPending(fromShard uint32) string {
	return fmt.Sprintf("tx:pending:{s%d}", fromShard)
}

// Order L0 的幂等订单 key，与玩家键同 tag
func (k *Keys) Order(uid uint64, orderID string) string {
	if k.mode == TagShard {
		return fmt.Sprintf("order:{%s}:%d:%s", k.tag(uid), uid, orderID)
	}
	return fmt.Sprintf("order:{%d}:%s", uid, orderID)
}

// MailPending 离线玩家的待领取邮件队列
func (k *Keys) MailPending(uid uint64) string {
	if k.mode == TagShard {
		return fmt.Sprintf("p:{%s}:%d:mailq", k.tag(uid), uid)
	}
	return fmt.Sprintf("p:{%d}:mailq", uid)
}

// Ledger 返回玩家流水的 Stream key。
// 必须与玩家数据同 hash tag，否则流水和余额进不了同一个 Lua
func (k *Keys) Ledger(uid uint64) string {
	if k.mode == TagShard {
		return fmt.Sprintf("ledger:{%s}:%d", k.tag(uid), uid)
	}
	return fmt.Sprintf("ledger:{%d}", uid)
}

// LedgerCursor 记录导出器已导出到的位置
func (k *Keys) LedgerCursor(uid uint64) string {
	if k.mode == TagShard {
		return fmt.Sprintf("ledger:{%s}:%d:cursor", k.tag(uid), uid)
	}
	return fmt.Sprintf("ledger:{%d}:cursor", uid)
}

// LedgerDirty 待导出流水的玩家集合，按分片切分
func (k *Keys) LedgerDirty(shard uint32) string {
	return fmt.Sprintf("ledger:dirty:{s%d}", shard)
}

// Jackpot 返回奖池金额 key
func (k *Keys) Jackpot(pool string) string {
	return fmt.Sprintf("jackpot:{%s}:amount", pool)
}

// JackpotMeta 返回奖池元信息（上次中奖时间、中奖者等）
func (k *Keys) JackpotMeta(pool string) string {
	return fmt.Sprintf("jackpot:{%s}:meta", pool)
}

// JackpotPending 待派彩索引，与奖池同 slot，保证清零和记账一起成
func (k *Keys) JackpotPending(pool string) string {
	return fmt.Sprintf("jackpot:{%s}:pending", pool)
}

// WorldBoss 返回世界 BOSS 状态 key，固定占 0 号分片位
func (k *Keys) WorldBoss() string { return "world:{s0}:boss" }

// GuildMembers 公会成员索引，公会广播靠它查路由表
func (k *Keys) GuildMembers(guildID uint64) string {
	return fmt.Sprintf("guild:{%d}:members", guildID)
}

func (k *Keys) Rank(name string) string { return "rank:" + name }
