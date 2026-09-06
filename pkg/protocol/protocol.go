// Package protocol 命令号、权限级别、错误码和推送号
//
// 命令号既是 Envelope.cmd 的数值，也是 subject 的最后一段，同一张表维护，
// 免得两边漂移
package protocol

// Cmd 请求命令号
type Cmd uint32

// 号段：1xx 会话 2xx 玩家 3xx 房间 4xx 匹配 5xx 聊天 6xx 世界 7xx 内部 8xx GM 9xx 控制面
const (
	CmdUnknown Cmd = 0

	// --- 会话（Lobby 处理）---
	CmdLogin     Cmd = 101
	CmdLogout    Cmd = 102
	CmdHeartbeat Cmd = 103

	// --- 账号（Account）---
	//
	// 都是客户端级。渠道校验不过就什么都拿不到，没有可提权的东西；
	// 而且网关不持有内部密钥，定成 Internal 它根本调不出去
	CmdAuthChannel   Cmd = 104 // 渠道凭证换我们自己的 token，登录前调
	CmdBindChannel   Cmd = 105 // 已登录状态下再绑一个渠道
	CmdUnbindChannel Cmd = 106
	CmdListBindings  Cmd = 107

	// --- 玩家逻辑（Lobby）---
	CmdUseItem       Cmd = 203
	CmdGetBag        Cmd = 204
	CmdAcceptQuest   Cmd = 205
	CmdQuestProgress Cmd = 206
	CmdGetSocial     Cmd = 207
	CmdAddFriend     Cmd = 208
	CmdGetMail       Cmd = 209
	CmdClaimMail     Cmd = 210
	CmdPurchase      Cmd = 211 // L0 写穿，需渠道回执
	CmdGetProfile    Cmd = 212
	CmdTransfer      Cmd = 213

	// --- slots / 抽卡（Lobby）---
	CmdSpin        Cmd = 220 // 一次旋转（含免费旋转）
	CmdRoundState  Cmd = 221 // 查询未结算回合（断线恢复）
	CmdGacha       Cmd = 222 // 抽卡
	CmdJackpotInfo Cmd = 223 // 查询奖池水位
	CmdRGStatus    Cmd = 224 // 查询责任游戏限额状态

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
	CmdJackpotClaim   Cmd = 603 // 由 Lobby 发起的奖池派彩申领（内部）

	// --- 内部命令：只允许服务端之间调用，绝不对客户端开放 ---
	CmdAddCurrency   Cmd = 701 // 发奖/结算加减货币
	CmdAddItem       Cmd = 702 // 发奖加道具
	CmdApplyTransfer Cmd = 703 // 接收跨分片转移
	CmdApplyMail     Cmd = 704 // 接收邮件
	CmdBattleSettle  Cmd = 705 // 战斗结算回写
	CmdApplyJackpot  Cmd = 706 // 奖池派彩入账

	// --- GM / 运营后台 ---
	CmdGMGrant Cmd = 801 // 补单发放
	CmdGMQuery Cmd = 802 // 查询玩家（含流水）
	CmdGMKick  Cmd = 803 // 强制下线
	CmdGMSetRG Cmd = 804 // 设置责任游戏限额 / 自我排除

	// --- 控制面 ---
	CmdHandoff  Cmd = 901
	CmdShutdown Cmd = 902
)

// Level 命令的调用权限
//
// 级别标在命令定义处，不散落在网关的 switch 里。网关只放行 Client，
// Lobby 对 Internal 和 GM 再验一次签名。忘了登记的默认落到最严的 Internal，
// 漏配的后果是调不通，而不是被人刷钱
type Level uint8

const (
	// LevelClient 客户端可直接调用
	LevelClient Level = iota
	// LevelInternal 仅服务端内部调用，需携带内部签名
	LevelInternal
	// LevelGM 仅 GM / 运营后台调用，需携带内部签名 + GM 身份
	LevelGM
)

func (l Level) String() string {
	switch l {
	case LevelClient:
		return "client"
	case LevelGM:
		return "gm"
	default:
		return "internal"
	}
}

// clientCmds 客户端可调用的白名单
//
// 往里加之前先问一句：这个命令能不能让玩家自己决定给自己加多少钱？
var clientCmds = map[Cmd]bool{
	CmdLogin: true, CmdLogout: true, CmdHeartbeat: true,
	CmdAuthChannel: true, CmdBindChannel: true,
	CmdUnbindChannel: true, CmdListBindings: true,
	CmdUseItem: true, CmdGetBag: true,
	CmdAcceptQuest: true, CmdQuestProgress: true,
	CmdGetSocial: true, CmdAddFriend: true,
	CmdGetMail: true, CmdClaimMail: true,
	CmdPurchase: true, CmdGetProfile: true, CmdTransfer: true,
	CmdSpin: true, CmdRoundState: true, CmdGacha: true,
	CmdJackpotInfo: true, CmdRGStatus: true,
	CmdCreateRoom: true, CmdJoinRoom: true, CmdLeaveRoom: true,
	CmdRoomOp: true, CmdRoomInfo: true, CmdStartBattle: true,
	CmdMatchEnqueue: true, CmdMatchCancel: true,
	CmdChatSend:       true,
	CmdWorldBossState: true, CmdWorldBossHit: true,
}

var gmCmds = map[Cmd]bool{
	CmdGMGrant: true, CmdGMQuery: true, CmdGMKick: true, CmdGMSetRG: true,
}

// Level 返回命令的权限级别，没登记的一律当 Internal
func (c Cmd) Level() Level {
	if clientCmds[c] {
		return LevelClient
	}
	if gmCmds[c] {
		return LevelGM
	}
	return LevelInternal
}

func (c Cmd) ClientCallable() bool { return c.Level() == LevelClient }

var cmdNames = map[Cmd]string{
	CmdLogin: "login", CmdLogout: "logout", CmdHeartbeat: "heartbeat",
	CmdAuthChannel: "auth_channel", CmdBindChannel: "bind_channel",
	CmdUnbindChannel: "unbind_channel", CmdListBindings: "list_bindings",
	CmdUseItem: "use_item", CmdGetBag: "get_bag",
	CmdAcceptQuest: "accept_quest", CmdQuestProgress: "quest_progress",
	CmdGetSocial: "get_social", CmdAddFriend: "add_friend",
	CmdGetMail: "get_mail", CmdClaimMail: "claim_mail",
	CmdPurchase: "purchase", CmdGetProfile: "get_profile", CmdTransfer: "transfer",
	CmdSpin: "spin", CmdRoundState: "round_state", CmdGacha: "gacha",
	CmdJackpotInfo: "jackpot_info", CmdRGStatus: "rg_status",
	CmdCreateRoom: "create_room", CmdJoinRoom: "join_room", CmdLeaveRoom: "leave_room",
	CmdRoomOp: "room_op", CmdRoomInfo: "room_info", CmdStartBattle: "start_battle",
	CmdMatchEnqueue: "match_enqueue", CmdMatchCancel: "match_cancel",
	CmdChatSend:       "chat_send",
	CmdWorldBossState: "world_boss_state", CmdWorldBossHit: "world_boss_hit",
	CmdJackpotClaim: "jackpot_claim",
	CmdAddCurrency:  "add_currency", CmdAddItem: "add_item",
	CmdApplyTransfer: "apply_transfer", CmdApplyMail: "apply_mail",
	CmdBattleSettle: "battle_settle", CmdApplyJackpot: "apply_jackpot",
	CmdGMGrant: "gm_grant", CmdGMQuery: "gm_query", CmdGMKick: "gm_kick", CmdGMSetRG: "gm_set_rg",
	CmdHandoff: "handoff", CmdShutdown: "shutdown",
}

var cmdByName = func() map[string]Cmd {
	m := make(map[string]Cmd, len(cmdNames))
	for c, n := range cmdNames {
		m[n] = c
	}
	return m
}()

// Name 返回命令名，用作 subject 最后一段
func (c Cmd) Name() string {
	if n, ok := cmdNames[c]; ok {
		return n
	}
	return "cmd_unknown"
}

func (c Cmd) String() string { return c.Name() }

// ParseCmd 按名字解析命令号
func ParseCmd(name string) (Cmd, bool) {
	c, ok := cmdByName[name]
	return c, ok
}

// Push 下行推送号（服务端主动下发）
type Push uint32

const (
	PushKick         Push = 1001 // 顶号
	PushMailNew      Push = 1002
	PushChat         Push = 1003
	PushMatchFound   Push = 1004
	PushRoomEvent    Push = 1005
	PushBattleResult Push = 1006
	PushAnnounce     Push = 1007 // 全服公告
	PushMaintenance  Push = 1008 // 服务器维护，请重连
	PushItemChanged  Push = 1009
	PushWorldBoss    Push = 1010
	PushJackpotWon   Push = 1011 // 有人中了奖池
	PushJackpotTick  Push = 1012 // 奖池水位变化
	PushRGNotice     Push = 1013 // 责任游戏提醒（会话时长、接近限额）
)

// ErrCode 应答错误码（Envelope.err_code）
type ErrCode uint32

const (
	ErrOK ErrCode = 0

	ErrInternal    ErrCode = 500
	ErrBadRequest  ErrCode = 400
	ErrNotFound    ErrCode = 404
	ErrTimeout     ErrCode = 408
	ErrUnavailable ErrCode = 503 // 分片正在交接 / 未认领，客户端应重试
	ErrNotOwner    ErrCode = 409 // 本实例不是该分片 owner
	ErrEpochStale  ErrCode = 410 // epoch 过期，写入被 fencing 拒绝

	ErrNotEnough     ErrCode = 1001
	ErrBagFull       ErrCode = 1002
	ErrItemNotFound  ErrCode = 1003
	ErrDuplicate     ErrCode = 1004
	ErrPlayerOffline ErrCode = 1005
	ErrRoomFull      ErrCode = 1006
	ErrRoomNotFound  ErrCode = 1007
	ErrPermission    ErrCode = 1008
	ErrRateLimited   ErrCode = 1009

	// --- slots / 合规 ---
	ErrRoundOpen      ErrCode = 1100 // 还有未结算的回合，必须先打完
	ErrRoundNotFound  ErrCode = 1101
	ErrBetInvalid     ErrCode = 1102 // 下注额不在允许档位内
	ErrGameNotFound   ErrCode = 1103
	ErrRGLimit        ErrCode = 1104 // 触发责任游戏限额
	ErrSelfExcluded   ErrCode = 1105 // 自我排除期内
	ErrReceiptInvalid ErrCode = 1106 // 充值回执无效
	ErrProductUnknown ErrCode = 1107 // 未知商品
	ErrJackpotEmpty   ErrCode = 1108
	ErrGachaPool      ErrCode = 1109

	// --- 账号 ---
	ErrChannelUnknown ErrCode = 1200 // 渠道没配 provider
	ErrCredentialBad  ErrCode = 1201 // 渠道说这份凭证不认
	ErrUpstreamFailed ErrCode = 1202 // 渠道自己挂了或超时，可重试
	ErrBindConflict   ErrCode = 1203 // 该渠道账号已绑在别的 uid 上
	ErrAlreadyBound   ErrCode = 1204 // 本 uid 在该渠道已经绑过别的账号
	ErrNotBound       ErrCode = 1205
	ErrLastBinding    ErrCode = 1206 // 最后一个绑定不能解，解了账号就找不回来
)

var errText = map[ErrCode]string{
	ErrOK: "ok", ErrInternal: "内部错误", ErrBadRequest: "请求非法", ErrNotFound: "对象不存在",
	ErrTimeout: "超时", ErrUnavailable: "分片不可用，请重试", ErrNotOwner: "非本分片 owner",
	ErrEpochStale: "epoch 过期", ErrNotEnough: "资源不足", ErrBagFull: "背包已满",
	ErrItemNotFound: "道具不存在", ErrDuplicate: "重复请求", ErrPlayerOffline: "玩家不在线",
	ErrRoomFull: "房间已满", ErrRoomNotFound: "房间不存在", ErrPermission: "无权限",
	ErrRateLimited: "请求过于频繁",
	ErrRoundOpen:   "存在未结算的回合", ErrRoundNotFound: "回合不存在",
	ErrBetInvalid: "下注额非法", ErrGameNotFound: "游戏不存在",
	ErrRGLimit: "已达限额", ErrSelfExcluded: "账号处于自我排除期",
	ErrReceiptInvalid: "支付回执无效", ErrProductUnknown: "未知商品",
	ErrJackpotEmpty: "奖池为空", ErrGachaPool: "卡池不存在",
	ErrChannelUnknown: "渠道不支持", ErrCredentialBad: "渠道凭证无效",
	ErrUpstreamFailed: "渠道暂时不可用，请重试", ErrBindConflict: "该渠道账号已被占用",
	ErrAlreadyBound: "该渠道已绑定其他账号", ErrNotBound: "该渠道未绑定",
	ErrLastBinding: "不能解绑最后一个登录方式",
}

func (e ErrCode) String() string {
	if s, ok := errText[e]; ok {
		return s
	}
	return "未知错误"
}

// CurrencyType 货币类型
//
// 金额统一用 int64 的最小单位，不用浮点，浮点的舍入误差会直接变成对账差额
type CurrencyType uint32

const (
	CurrencyGold    CurrencyType = 1 // 游戏币
	CurrencyDiamond CurrencyType = 2 // 付费币
	CurrencyStamina CurrencyType = 3 // 体力
	CurrencyCash    CurrencyType = 4 // 真钱余额（最小单位：分）
)

// 房间状态
const (
	RoomStateWaiting  uint32 = 0
	RoomStateBattling uint32 = 1
	RoomStateSettling uint32 = 2
)

// 回合状态
const (
	RoundOpen    uint32 = 0
	RoundSettled uint32 = 1
)

// 聊天频道
const (
	ChanWorld   uint32 = 1
	ChanGuild   uint32 = 2
	ChanPrivate uint32 = 3
	ChanRoom    uint32 = 4
)
