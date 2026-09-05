// Package protocol 定义命令号、错误码与 push 号。
//
// 命令号既作为 Envelope.cmd 的数值，也作为 NATS subject 的最后一段（名字），
// 二者由同一张表维护，避免两处漂移。
package protocol

// Cmd 是请求命令号。
type Cmd uint32

// 段位划分：1xx 会话 / 2xx 玩家 / 3xx 房间 / 4xx 匹配 / 5xx 聊天 / 6xx 世界 / 9xx 控制面
const (
	CmdUnknown Cmd = 0

	// --- 会话（Lobby 处理）---
	CmdLogin     Cmd = 101
	CmdLogout    Cmd = 102
	CmdHeartbeat Cmd = 103

	// --- 玩家逻辑（Lobby）---
	CmdAddCurrency   Cmd = 201
	CmdAddItem       Cmd = 202
	CmdUseItem       Cmd = 203
	CmdGetBag        Cmd = 204
	CmdAcceptQuest   Cmd = 205
	CmdQuestProgress Cmd = 206
	CmdGetSocial     Cmd = 207
	CmdAddFriend     Cmd = 208
	CmdGetMail       Cmd = 209
	CmdClaimMail     Cmd = 210
	CmdPurchase      Cmd = 211 // L0 写穿，幂等
	CmdGetProfile    Cmd = 212 // 读 profile 只读摘要
	CmdTransfer      Cmd = 213 // 发起跨分片转移
	CmdApplyTransfer Cmd = 214 // 接收跨分片转移（job 投递）
	CmdApplyMail     Cmd = 215 // 接收邮件（job 投递）
	CmdBattleSettle  Cmd = 216 // 战斗结算回写

	// --- 房间（Room）---
	CmdCreateRoom  Cmd = 301
	CmdJoinRoom    Cmd = 302
	CmdLeaveRoom   Cmd = 303
	CmdRoomOp      Cmd = 304
	CmdRoomInfo    Cmd = 305
	CmdStartBattle Cmd = 306

	// --- 匹配（Match）---
	CmdMatchEnqueue Cmd = 401
	CmdMatchCancel  Cmd = 402

	// --- 聊天（Chat）---
	CmdChatSend Cmd = 501

	// --- 世界（World）---
	CmdWorldBossState Cmd = 601
	CmdWorldBossHit   Cmd = 602

	// --- 控制面 ---
	CmdHandoff  Cmd = 901
	CmdShutdown Cmd = 902
)

var cmdNames = map[Cmd]string{
	CmdLogin: "login", CmdLogout: "logout", CmdHeartbeat: "heartbeat",
	CmdAddCurrency: "add_currency", CmdAddItem: "add_item", CmdUseItem: "use_item",
	CmdGetBag: "get_bag", CmdAcceptQuest: "accept_quest", CmdQuestProgress: "quest_progress",
	CmdGetSocial: "get_social", CmdAddFriend: "add_friend", CmdGetMail: "get_mail",
	CmdClaimMail: "claim_mail", CmdPurchase: "purchase", CmdGetProfile: "get_profile",
	CmdTransfer: "transfer", CmdApplyTransfer: "apply_transfer", CmdApplyMail: "apply_mail",
	CmdBattleSettle: "battle_settle",
	CmdCreateRoom:   "create_room", CmdJoinRoom: "join_room", CmdLeaveRoom: "leave_room",
	CmdRoomOp: "room_op", CmdRoomInfo: "room_info", CmdStartBattle: "start_battle",
	CmdMatchEnqueue: "match_enqueue", CmdMatchCancel: "match_cancel",
	CmdChatSend:       "chat_send",
	CmdWorldBossState: "world_boss_state", CmdWorldBossHit: "world_boss_hit",
	CmdHandoff: "handoff", CmdShutdown: "shutdown",
}

var cmdByName = func() map[string]Cmd {
	m := make(map[string]Cmd, len(cmdNames))
	for c, n := range cmdNames {
		m[n] = c
	}
	return m
}()

// Name 返回命令名，用作 subject 最后一段。
func (c Cmd) Name() string {
	if n, ok := cmdNames[c]; ok {
		return n
	}
	return "cmd_unknown"
}

func (c Cmd) String() string { return c.Name() }

// ParseCmd 按名字解析命令号。
func ParseCmd(name string) (Cmd, bool) {
	c, ok := cmdByName[name]
	return c, ok
}

// Push 是下行推送号（服务端主动下发）。
type Push uint32

const (
	PushKick         Push = 1001 // 顶号
	PushMailNew      Push = 1002
	PushChat         Push = 1003
	PushMatchFound   Push = 1004
	PushRoomEvent    Push = 1005
	PushBattleResult Push = 1006
	PushAnnounce     Push = 1007 // 全服公告
	PushMaintenance  Push = 1008 // 服务器维护，请重连（§10.3）
	PushItemChanged  Push = 1009
	PushWorldBoss    Push = 1010
)

// ErrCode 是应答错误码（Envelope.err_code）。
type ErrCode uint32

const (
	ErrOK ErrCode = 0

	ErrInternal      ErrCode = 500
	ErrBadRequest    ErrCode = 400
	ErrNotFound      ErrCode = 404
	ErrTimeout       ErrCode = 408
	ErrUnavailable   ErrCode = 503 // 分片正在交接 / 未认领，客户端应重试
	ErrNotOwner      ErrCode = 409 // 本实例不是该分片 owner
	ErrEpochStale    ErrCode = 410 // epoch 过期，写入被 fencing 拒绝
	ErrNotEnough     ErrCode = 1001
	ErrBagFull       ErrCode = 1002
	ErrItemNotFound  ErrCode = 1003
	ErrDuplicate     ErrCode = 1004
	ErrPlayerOffline ErrCode = 1005
	ErrRoomFull      ErrCode = 1006
	ErrRoomNotFound  ErrCode = 1007
	ErrPermission    ErrCode = 1008
	ErrRateLimited   ErrCode = 1009
)

var errText = map[ErrCode]string{
	ErrOK: "ok", ErrInternal: "内部错误", ErrBadRequest: "请求非法", ErrNotFound: "对象不存在",
	ErrTimeout: "超时", ErrUnavailable: "分片不可用，请重试", ErrNotOwner: "非本分片 owner",
	ErrEpochStale: "epoch 过期", ErrNotEnough: "资源不足", ErrBagFull: "背包已满",
	ErrItemNotFound: "道具不存在", ErrDuplicate: "重复请求", ErrPlayerOffline: "玩家不在线",
	ErrRoomFull: "房间已满", ErrRoomNotFound: "房间不存在", ErrPermission: "无权限",
	ErrRateLimited: "请求过于频繁",
}

func (e ErrCode) String() string {
	if s, ok := errText[e]; ok {
		return s
	}
	return "未知错误"
}

// CurrencyType 货币类型。
type CurrencyType uint32

const (
	CurrencyGold    CurrencyType = 1
	CurrencyDiamond CurrencyType = 2
	CurrencyStamina CurrencyType = 3
)

// 房间状态
const (
	RoomStateWaiting  uint32 = 0
	RoomStateBattling uint32 = 1
	RoomStateSettling uint32 = 2
)

// 聊天频道
const (
	ChanWorld   uint32 = 1
	ChanGuild   uint32 = 2
	ChanPrivate uint32 = 3
	ChanRoom    uint32 = 4
)
