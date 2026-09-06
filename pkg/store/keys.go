// Package store 是 Redis 持久化层：key 规范、epoch fencing、分级刷盘。
//
// 记住第一条设计取向：进程内存是数据的权威副本，Redis 是它的持久化投影。
// 因此这里的所有写入都必须经过 epoch 校验，所有失败都必须回报重新标脏，绝不丢弃。
package store

import (
	"fmt"
	"strconv"
)

// Module 是玩家数据的模块划分，与 Redis key 一一对应（§6.1）。
//
// 按模块拆分是为了避免「改一个字段重写整个玩家」。
type Module string

const (
	ModBase   Module = "base"   // 等级、经验、货币   高频、小
	ModBag    Module = "bag"    // 背包              中频、大
	ModQuest  Module = "quest"  // 任务              中频
	ModSocial Module = "social" // 好友、公会         低频
	ModMail   Module = "mail"   // 邮件              低频

	// ModRound 是未结算的游戏回合。
	//
	// 它必须与投注同一次原子提交落盘：回合状态落后于扣款，
	// 玩家断线重连就会「钱扣了但免费旋转没了」——
	// 这在监管口径下属于 incomplete round 处理失败。
	ModRound Module = "round"
	// ModRG 是责任游戏限额累计，同样必须与投注原子推进。
	ModRG Module = "rg"
	// ModGacha 是抽卡保底计数，必须与抽卡消耗原子推进。
	ModGacha Module = "gacha"
)

// AllModules 是全部玩家模块，登录时 pipeline 读回（§10.1）。
var AllModules = []Module{
	ModBase, ModBag, ModQuest, ModSocial, ModMail,
	ModRound, ModRG, ModGacha,
}

// HashTagMode 决定 Redis key 的 hash tag 取法。
type HashTagMode string

const (
	// TagUID 是设计文档的默认写法：p:{1001}:base。
	// 保证同一玩家所有 key 同 slot，从而支持单玩家 Lua 和 pipeline（§6.1）。
	TagUID HashTagMode = "uid"

	// TagShard 把 hash tag 换成分片号：p:{s7}:1001:base。
	//
	// 用于 Redis Cluster 部署：epoch 键 shard:lobby:{s7}:epoch 与该分片下所有玩家键
	// 落在同一 slot，fencing 的 Lua 才不会撞上 CROSSSLOT。
	// 单实例 / 主从哨兵部署下无此约束，用 TagUID 即可。
	TagShard HashTagMode = "shard"
)

// Keys 按配置生成 Redis key。
type Keys struct {
	mode       HashTagMode
	shardCount uint32
	kind       string // "lobby" / "room"，用于 epoch 键
}

// NewKeys 构造 key 生成器。
func NewKeys(mode HashTagMode, shardCount uint32, kind string) *Keys {
	if mode == "" {
		mode = TagUID
	}
	return &Keys{mode: mode, shardCount: shardCount, kind: kind}
}

// Mode 返回 hash tag 模式。
func (k *Keys) Mode() HashTagMode { return k.mode }

// Shard 计算 uid / roomID 所属分片。
func (k *Keys) Shard(id uint64) uint32 { return uint32(id % uint64(k.shardCount)) }

// tag 返回某实体的 hash tag 内容（不含大括号）。
func (k *Keys) tag(id uint64) string {
	if k.mode == TagShard {
		return "s" + strconv.FormatUint(uint64(k.Shard(id)), 10)
	}
	return strconv.FormatUint(id, 10)
}

// Player 返回玩家某模块的 key。
//
//	TagUID   → p:{1001}:base
//	TagShard → p:{s9}:1001:base
//
// key 用 p: 而非 lobby: —— key 跟着数据实体走，不跟服务名走，
// 将来 Lobby 拆分或改名时不用迁移数据（§6.1）。
func (k *Keys) Player(uid uint64, m Module) string {
	if k.mode == TagShard {
		return fmt.Sprintf("p:{%s}:%d:%s", k.tag(uid), uid, m)
	}
	return fmt.Sprintf("p:{%d}:%s", uid, m)
}

// PlayerAll 返回玩家全部模块的 key，顺序与 AllModules 一致。
func (k *Keys) PlayerAll(uid uint64) []string {
	out := make([]string, 0, len(AllModules))
	for _, m := range AllModules {
		out = append(out, k.Player(uid, m))
	}
	return out
}

// Profile 返回只读摘要 key（§6.5）。与玩家键同 tag，保证同 slot。
func (k *Keys) Profile(uid uint64) string {
	if k.mode == TagShard {
		return fmt.Sprintf("profile:{%s}:%d", k.tag(uid), uid)
	}
	return fmt.Sprintf("profile:{%d}", uid)
}

// Room 返回房间快照 key：room:{shard}:{roomID}。
func (k *Keys) Room(roomID uint64) string {
	return fmt.Sprintf("room:{s%d}:%d", k.Shard(roomID), roomID)
}

// RoomIndex 返回某分片的房间索引（SET），接管时用它恢复房间列表。
// 与房间快照、epoch 键同 tag，保证同 slot。
func (k *Keys) RoomIndex(shard uint32) string {
	return fmt.Sprintf("room:{s%d}:index", shard)
}

// Epoch 返回分片 epoch 键：shard:lobby:{shard}:epoch。
//
// 加 hash tag 是为了 Cluster 下与该分片的数据键同 slot；
// 单实例部署下大括号只是普通字符，无副作用。
func (k *Keys) Epoch(shard uint32) string {
	return fmt.Sprintf("shard:%s:{s%d}:epoch", k.kind, shard)
}

// EpochOf 返回某实体所属分片的 epoch 键。
func (k *Keys) EpochOf(id uint64) string { return k.Epoch(k.Shard(id)) }

// Session 返回会话路由 key：session:{uid}（§9.1）。
func (k *Keys) Session(uid uint64) string { return fmt.Sprintf("session:{%d}", uid) }

// GateSessions 返回某网关名下 session 索引，用于网关重启时清理（§9.4）。
func (k *Keys) GateSessions(gateID string) string { return "gate:" + gateID + ":sessions" }

// Tx 返回跨分片转移记录 key（§8）。
//
// hash tag 取「发起方分片」而不是 txid 本身：发起方要把「扣道具」和「写穿 tx 记录」
// 放在同一个 Lua 里原子完成，二者必须同 slot。tx:pending 索引同理。
func (k *Keys) Tx(fromShard uint32, txid string) string {
	return fmt.Sprintf("tx:{s%d}:%s", fromShard, txid)
}

// TxDone 返回接收方幂等标记 key。
//
// hash tag 取「接收方分片」：接收方要把「SET done NX」和「加道具写穿」原子完成。
func (k *Keys) TxDone(toShard uint32, txid string) string {
	return fmt.Sprintf("txdone:{s%d}:%s", toShard, txid)
}

// TxPending 返回某分片的 PENDING 转移有序索引（score = 创建时间毫秒）。
//
// 按分片切分让补偿扫描器天然分布：每个 owner 只负责重投自己分片发起的转移。
func (k *Keys) TxPending(fromShard uint32) string {
	return fmt.Sprintf("tx:pending:{s%d}", fromShard)
}

// Order 返回 L0 幂等订单 key。与玩家键同 tag，写穿可在同一 Lua 中完成。
func (k *Keys) Order(uid uint64, orderID string) string {
	if k.mode == TagShard {
		return fmt.Sprintf("order:{%s}:%d:%s", k.tag(uid), uid, orderID)
	}
	return fmt.Sprintf("order:{%d}:%s", uid, orderID)
}

// MailPending 返回离线玩家待领取邮件队列（§6.5）。
func (k *Keys) MailPending(uid uint64) string {
	if k.mode == TagShard {
		return fmt.Sprintf("p:{%s}:%d:mailq", k.tag(uid), uid)
	}
	return fmt.Sprintf("p:{%d}:mailq", uid)
}

// Ledger 返回玩家流水的 Redis Stream key。
//
// 与玩家数据同 hash tag 是硬要求：流水必须与余额变更在同一个 Lua 里原子写入，
// 不同 slot 就做不到原子，也就无法保证「有扣款必有流水」。
func (k *Keys) Ledger(uid uint64) string {
	if k.mode == TagShard {
		return fmt.Sprintf("ledger:{%s}:%d", k.tag(uid), uid)
	}
	return fmt.Sprintf("ledger:{%d}", uid)
}

// LedgerCursor 记录导出器已导出到的位置。
func (k *Keys) LedgerCursor(uid uint64) string {
	if k.mode == TagShard {
		return fmt.Sprintf("ledger:{%s}:%d:cursor", k.tag(uid), uid)
	}
	return fmt.Sprintf("ledger:{%d}:cursor", uid)
}

// LedgerDirty 是「有新流水待导出」的玩家集合，按分片切分。
func (k *Keys) LedgerDirty(shard uint32) string {
	return fmt.Sprintf("ledger:dirty:{s%d}", shard)
}

// Jackpot 返回奖池金额 key。
func (k *Keys) Jackpot(pool string) string {
	return fmt.Sprintf("jackpot:{%s}:amount", pool)
}

// JackpotMeta 返回奖池元信息（上次中奖时间、中奖者等）。
func (k *Keys) JackpotMeta(pool string) string {
	return fmt.Sprintf("jackpot:{%s}:meta", pool)
}

// JackpotPending 是奖池派彩的待处理索引，与奖池同 slot，保证「清零 + 记账」原子。
func (k *Keys) JackpotPending(pool string) string {
	return fmt.Sprintf("jackpot:{%s}:pending", pool)
}

// WorldBoss 返回世界 BOSS 状态 key。世界服是全局唯一逻辑，固定用 0 号分片位。
func (k *Keys) WorldBoss() string { return "world:{s0}:boss" }

// GuildMembers 返回公会成员索引（SET），公会广播按它查路由表后聚合（§9.3）。
func (k *Keys) GuildMembers(guildID uint64) string {
	return fmt.Sprintf("guild:{%d}:members", guildID)
}

// Rank 返回排行榜 key（读 profile 摘要拼装）。
func (k *Keys) Rank(name string) string { return "rank:" + name }
