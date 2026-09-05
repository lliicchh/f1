// Package subject 实现 §5.1 的 NATS subject 命名规范。
//
//	req.   请求-应答    Core NATS
//	evt.   事件广播     Core NATS
//	push.  下行推送     Core NATS
//	job.   必达任务     JetStream
//	ctl.   控制面       Core NATS
//
// 前缀语义即投递保证：任何「丢了会导致资产不一致」的操作必须用 job. 前缀，
// 在命名上强制体现，降低误用（§5.2）。
package subject

import (
	"fmt"
	"strconv"
	"strings"
)

// 前缀常量。
const (
	PrefixReq  = "req"
	PrefixEvt  = "evt"
	PrefixPush = "push"
	PrefixJob  = "job"
	PrefixCtl  = "ctl"
)

// 服务名（与 ident.SvcType.Name() 一致，这里避免包依赖单独写常量）。
const (
	KindLobby = "lobby"
	KindRoom  = "room"
	KindMatch = "match"
	KindChat  = "chat"
	KindWorld = "world"
)

// ---------------------------- req. ----------------------------

// LobbyReq 返回 req.lobby.{shard}.{cmd}，shard = uid % ShardCount。
func LobbyReq(shard uint32, cmd string) string {
	return fmt.Sprintf("req.lobby.%d.%s", shard, cmd)
}

// LobbyShardWildcard 返回某个 lobby 分片的订阅通配 subject。
//
// 注意：订阅时不能加 queue group。分片独占性正是靠「同一 subject 只有一个订阅者」
// 保证的，加了反而允许多实例同时订阅，破坏独占语义（§4.3）。
func LobbyShardWildcard(shard uint32) string {
	return fmt.Sprintf("req.lobby.%d.>", shard)
}

// RoomReq 返回 req.room.{shard}.{cmd}，shard = roomID % ShardCount。
func RoomReq(shard uint32, cmd string) string {
	return fmt.Sprintf("req.room.%d.%s", shard, cmd)
}

// RoomShardWildcard 返回某个 room 分片的订阅通配 subject（同样不带 queue group）。
func RoomShardWildcard(shard uint32) string {
	return fmt.Sprintf("req.room.%d.>", shard)
}

// MatchReq 返回 req.match.{mode}.{tier}。
func MatchReq(mode, tier uint32) string {
	return fmt.Sprintf("req.match.%d.%d", mode, tier)
}

// MatchWildcard 返回匹配服的订阅通配（按 模式×段位 分片）。
func MatchWildcard() string { return "req.match.>" }

// MatchModeWildcard 订阅某个模式下所有段位。
func MatchModeWildcard(mode uint32) string { return fmt.Sprintf("req.match.%d.*", mode) }

// ChatReq 返回 req.chat.{cmd}。Chat 不持有数据，可以且应该用 queue group（§2.1）。
func ChatReq(cmd string) string { return "req.chat." + cmd }

// ChatWildcard 返回聊天服订阅通配。
func ChatWildcard() string { return "req.chat.>" }

// WorldReq 返回 req.world.{cmd}。World 选主主备，只有 leader 订阅。
func WorldReq(cmd string) string { return "req.world." + cmd }

// WorldWildcard 返回世界服订阅通配。
func WorldWildcard() string { return "req.world.>" }

// ---------------------------- push. ----------------------------

// GatePush 返回 push.gate.{gateID}，定向推送。
//
// 网关只订两个 subject，订阅数与在线人数无关（§9.1）。
func GatePush(gateID string) string { return "push.gate." + gateID }

// Broadcast 返回 push.broadcast，全服广播。
const Broadcast = "push.broadcast"

// ---------------------------- evt. ----------------------------

// PlayerEvt 返回 evt.player.{uid}.{event}。
func PlayerEvt(uid uint64, event string) string {
	return fmt.Sprintf("evt.player.%d.%s", uid, event)
}

// PlayerEvtWildcard 订阅某玩家全部事件。
func PlayerEvtWildcard(uid uint64) string { return fmt.Sprintf("evt.player.%d.>", uid) }

// AllPlayerEvt 订阅全部玩家事件（慎用，量大）。
const AllPlayerEvt = "evt.player.>"

// RoomEvt 返回 evt.room.{roomID}.{event}。
func RoomEvt(roomID uint64, event string) string {
	return fmt.Sprintf("evt.room.%d.%s", roomID, event)
}

// MatchEvt 匹配成功事件。
const MatchFoundEvt = "evt.match.found"

// SessionChangedEvt 会话变更事件（登录/顶号/下线）。
//
// 网关与 Room 会本地缓存 session 路由以省一次 Redis 查询，
// 顶号时必须让缓存失效（§9.1），靠这个事件广播。
const SessionChangedEvt = "evt.session.changed"

// ShardFreedEvt 分片被主动释放，提示其他实例尽快重扫认领。
func ShardFreedEvt(kind string) string { return "evt.shard." + kind + ".freed" }

// ---------------------------- job.（JetStream）----------------------------

// JobTransfer 返回 job.transfer.{txid}，跨分片资源转移（§8）。
func JobTransfer(txid string) string { return "job.transfer." + txid }

// JobTransferWildcard 消费端订阅通配。
const JobTransferWildcard = "job.transfer.*"

// JobMailSend 发信。
const JobMailSend = "job.mail.send"

// JobBattleSettle 战斗发奖。
//
// 发奖属于「丢了会导致资产不一致」的操作，必须走 JetStream 而非 Core NATS（§5.2）。
const JobBattleSettle = "job.battle.settle"

// JobWildcard 覆盖全部必达任务，用于建 JetStream Stream。
const JobWildcard = "job.>"

// ---------------------------- ctl. ----------------------------

// CtlHandoff 返回 ctl.node.{nodeID}.handoff，分片交接（§10.2）。
func CtlHandoff(nodeID string) string { return "ctl.node." + nodeID + ".handoff" }

// CtlShutdown 返回 ctl.node.{nodeID}.shutdown，优雅下线。
func CtlShutdown(nodeID string) string { return "ctl.node." + nodeID + ".shutdown" }

// CtlNodeWildcard 返回某节点全部控制面消息。
func CtlNodeWildcard(nodeID string) string { return "ctl.node." + nodeID + ".>" }

// ---------------------------- 解析 ----------------------------

// Prefix 取 subject 的第一段，用于指标打标（避免 label 基数爆炸）。
func Prefix(subj string) string {
	if i := strings.IndexByte(subj, '.'); i > 0 {
		head := subj[:i]
		rest := subj[i+1:]
		if j := strings.IndexByte(rest, '.'); j > 0 {
			return head + "." + rest[:j]
		}
		return head + "." + rest
	}
	return subj
}

// ParseShardReq 解析 req.{kind}.{shard}.{cmd}，返回 kind、shard、cmd。
func ParseShardReq(subj string) (kind string, shard uint32, cmd string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) < 4 || parts[0] != PrefixReq {
		return "", 0, "", false
	}
	n, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return "", 0, "", false
	}
	return parts[1], uint32(n), strings.Join(parts[3:], "."), true
}

// ParseMatchReq 解析 req.match.{mode}.{tier}。
func ParseMatchReq(subj string) (mode, tier uint32, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 4 || parts[0] != PrefixReq || parts[1] != KindMatch {
		return 0, 0, false
	}
	m, err1 := strconv.ParseUint(parts[2], 10, 32)
	t, err2 := strconv.ParseUint(parts[3], 10, 32)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return uint32(m), uint32(t), true
}
