// Package subject NATS subject 的命名规范
//
//	req.   请求-应答    Core NATS
//	evt.   事件广播     Core NATS
//	push.  下行推送     Core NATS
//	job.   必达任务     JetStream
//	ctl.   控制面       Core NATS
//
// 前缀就是投递保证。丢了会造成资产不一致的操作必须用 job.
package subject

import (
	"fmt"
	"strconv"
	"strings"
)

// 前缀常量
const (
	PrefixReq  = "req"
	PrefixEvt  = "evt"
	PrefixPush = "push"
	PrefixJob  = "job"
	PrefixCtl  = "ctl"
)

// 服务名，和 ident.SvcType.Name() 一致，单独写一份免得引包
const (
	KindLobby = "lobby"
	KindRoom  = "room"
	KindMatch = "match"
	KindChat  = "chat"
	KindWorld = "world"
)

// ---------------------------- req. ----------------------------

func LobbyReq(shard uint32, cmd string) string {
	return fmt.Sprintf("req.lobby.%d.%s", shard, cmd)
}

// LobbyShardWildcard 某个 lobby 分片的订阅通配
//
// 订阅时别加 queue group，分片独占靠的就是同一 subject 只有一个订阅者
func LobbyShardWildcard(shard uint32) string {
	return fmt.Sprintf("req.lobby.%d.>", shard)
}

func RoomReq(shard uint32, cmd string) string {
	return fmt.Sprintf("req.room.%d.%s", shard, cmd)
}

// RoomShardWildcard 某个 room 分片的订阅通配，同样不带 queue group
func RoomShardWildcard(shard uint32) string {
	return fmt.Sprintf("req.room.%d.>", shard)
}

func MatchReq(mode, tier uint32) string {
	return fmt.Sprintf("req.match.%d.%d", mode, tier)
}

func MatchWildcard() string { return "req.match.>" }

func MatchModeWildcard(mode uint32) string { return fmt.Sprintf("req.match.%d.*", mode) }

// ChatReq Chat 不持有数据，可以用 queue group
func ChatReq(cmd string) string { return "req.chat." + cmd }

func ChatWildcard() string { return "req.chat.>" }

// AccountReq Account 不持有玩家数据，同样可以用 queue group
func AccountReq(cmd string) string { return "req.account." + cmd }

func AccountWildcard() string { return "req.account.>" }

// WorldReq 只有 leader 订
func WorldReq(cmd string) string { return "req.world." + cmd }

func WorldWildcard() string { return "req.world.>" }

// ---------------------------- push. ----------------------------

func GatePush(gateID string) string { return "push.gate." + gateID }

// Broadcast 全服广播
const Broadcast = "push.broadcast"

// ---------------------------- evt. ----------------------------

func PlayerEvt(uid uint64, event string) string {
	return fmt.Sprintf("evt.player.%d.%s", uid, event)
}

func PlayerEvtWildcard(uid uint64) string { return fmt.Sprintf("evt.player.%d.>", uid) }

// AllPlayerEvt 订阅所有玩家事件，量很大，慎用
const AllPlayerEvt = "evt.player.>"

func RoomEvt(roomID uint64, event string) string {
	return fmt.Sprintf("evt.room.%d.%s", roomID, event)
}

// MatchFoundEvt 匹配成功事件
const MatchFoundEvt = "evt.match.found"

// SessionChangedEvt 会话变更事件。网关和 Room 缓存了路由，靠它失效
const SessionChangedEvt = "evt.session.changed"

// ShardFreedEvt 提示其他实例有分片被释放了，可以来抢
func ShardFreedEvt(kind string) string { return "evt.shard." + kind + ".freed" }

// ---------------------------- job.（JetStream）----------------------------

func JobTransfer(txid string) string { return "job.transfer." + txid }

// JobTransferWildcard 消费端订阅通配
const JobTransferWildcard = "job.transfer.*"

// JobMailSend 发信
const JobMailSend = "job.mail.send"

// JobBattleSettle 战斗发奖，丢了会造成资产不一致，所以走 JetStream
const JobBattleSettle = "job.battle.settle"

// JobWildcard 覆盖全部必达任务，建 Stream 用
const JobWildcard = "job.>"

// ---------------------------- ctl. ----------------------------

func CtlHandoff(nodeID string) string { return "ctl.node." + nodeID + ".handoff" }

func CtlShutdown(nodeID string) string { return "ctl.node." + nodeID + ".shutdown" }

func CtlNodeWildcard(nodeID string) string { return "ctl.node." + nodeID + ".>" }

// ---------------------------- 解析 ----------------------------

// Prefix 取 subject 前两段做指标 label，免得基数爆炸
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

// ParseShardReq 解析 req.{kind}.{shard}.{cmd}，返回 kind、shard、cmd
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

// ParseMatchReq 解析 req.match.{mode}.{tier}
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
