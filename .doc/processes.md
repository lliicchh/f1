# 进程走读

逐个走读仓库里的每个可执行进程：它是干什么的、怎么启动、进程模型是什么、
处理哪些消息、故障时怎么表现。架构层面的取舍在 `.doc/README.md`，这里只谈
「一个进程跑起来之后发生了什么」。

代码位置：入口在 `cmd/<name>/main.go`，业务在 `internal/<name>/`。

## 目录

- [共用骨架 pkg/node](#共用骨架-pkgnode)
- [gateway 网关](#gateway-网关)
- [account 账号服](#account-账号服)
- [lobby 大厅](#lobby-大厅)
- [room 房间](#room-房间)
- [match 匹配](#match-匹配)
- [chat 聊天](#chat-聊天)
- [world 世界服](#world-世界服)
- [gameconfctl 配置工具](#gameconfctl-配置工具)
- [一次登录的全链路](#一次登录的全链路)

---

## 共用骨架 pkg/node

每个服务进程的 `main.go` 都只有十几行，真正的启动、停机顺序固化在
`pkg/node`（`pkg/node/node.go`），六个服务共用。典型入口：

```go
func main() {
    n, err := node.Bootstrap("lobby", "lobby") // svcName, hashtag-kind
    if err != nil { node.Fatal(err) }
    if err := n.Run(lobby.New()); err != nil { node.Fatal(err) }
}
```

`Bootstrap` 按固定顺序做启动检查，任何一步失败直接退进程，绝不降级：

1. `config.Load` 装载 env，其中含 `NODE_SEQ` 越界校验（必须在 `[1,127]`）。
2. 连 etcd，`nodeid.Claim` 做 nodeID 唯一性检查。撞号就拒绝启动并打印冲突方
   host/pid；抢到后 30s 一次心跳。这是脑裂防护里唯一没有存储层兜底的洞，只能靠
   启动检查拦住。
3. 用 `ident.Identity` 派生雪花发号器（`workerID = svcType<<7 | nodeSeq`）。
4. 连 Redis，校验 `maxmemory-policy`，预加载 Lua 脚本。
5. 最后连 NATS。

`Run` 起服务后阻塞，直到收到 SIGINT/SIGTERM，或 nodeID 心跳丢失（`lostCh`）。
两种情况都走同一套**优雅下线，顺序不能颠倒**（`node.shutdown`）：

```
NotifyClients   → 网关先告诉客户端去重连别的网关（只有 gateway 有内容）
StopAccepting   → 摘注册、发起分片交接、停止接新请求
nc.Drain()      → 处理完 pending 再关连接
FlushAll        → 全量刷盘并确认成功（刷盘没成功就不删注册键，留给运维排查）
注销 nodeID     → 删 etcd 注册键
Close           → 收尾
```

每个 `internal/<svc>` 都实现 `node.Service` 这五个钩子。下面各节说的就是各服务
往这些钩子里填了什么。所有服务都不用 `deploy.replicas`，靠 compose 里手写
`NODE_SEQ` 区分实例。

---

## gateway 网关

- 入口：`cmd/gateway/main.go`（hashtag-kind 传 `lobby`）
- 业务：`internal/gateway/{gateway,dispatch,conn}.go`
- 实例模型：**无状态，LB 轮询**，compose 里 `gateway-1/2`，对外暴露 7001/7002
- 唯一面向公网的进程，也是唯一**拿不到 `INTERNAL_SECRET`** 的进程（`deploy/gateway.env`
  把它覆盖成空），物理上签不出内部命令

**职责。** 管 TCP 连接、编解码、鉴权、把客户端请求路由到后端、把下行推送发给客户端。
不持有任何玩家数据。

**订阅只有两个 subject**（`Start`）：`push.gate.{gateID}` 定向推送和 `push.broadcast`
全服广播。订阅数与在线人数无关，这是刻意的设计。另外订一个 `evt.session.changed`
用来在顶号时让本地 session 缓存失效。

**连接生命周期。** `acceptLoop` 每来一个连接分配一个雪花 connID（不用自增，避免重启
撞号），起 `readLoop`/`writeLoop` 两个 goroutine。`heartbeatLoop` 按 `SessionTTL/3`
给在线会话续期；续期时发现会话已被别人接管（顶号）就把这条连接踢下线。

**请求分发**（`dispatch.go`）。每条请求单独起 goroutine 同步等应答，慢请求不阻塞读取：

- `CmdLogin` 走单独路径（见下）。
- 其余先过 `authz.CheckClient`，只放行 Client 级命令。这是权限两道校验的第一道。
- uid 强制取服务端会话值，清空 `Auth`/`Operator`，客户端说自己是谁不算数。
- `route` 按命令决定 subject：房间类解出 roomID 按 roomID 分片，匹配类按 mode×tier，
  聊天/账号/世界各走自己的 wildcard，默认玩家逻辑按 uid 分片投给 Lobby。
- `forward` 带有限次重试（`ErrUnavailable`/`ErrNotOwner`/超时/无响应者时重投），
  兜住分片交接窗口。

**登录与顶号**（`handleLogin`）。带 `channel` 的走渠道登录：转给 account 服换票据
（网关自己不打外部网络）；带 token 的走纯本地 HMAC 验签。无论哪条路，uid 只认票据
这一个来源。验签通过后用 Lua 原子替换 `session:{uid}`，拿到被顶掉的旧 gateID，
给旧网关发 KICK。会话建好才把登录请求转给 Lobby（带上 gateID/connID 供定向推送）。

**下线钩子。** `NotifyClients` 给所有连接发维护通知让客户端重连别的网关；
`FlushAll` 清掉自己名下的 session 路由，让玩家不用等 TTL 就能连别的网关；
崩溃重启后 `Start` 里的 `CleanGate` 兜底清理残留脏路由。

---

## account 账号服

- 入口：`cmd/account/main.go`（hashtag-kind 传 `lobby`）
- 业务：`internal/account/account.go`
- 实例模型：**不持有玩家数据，用 queue group**，按渠道响应时间加实例
- 与网关是仅有的两个需要 `LOGIN_SECRET` 的进程：**这边签发，网关校验**

> 注意：account 在 compose 里没有对应的 service 块（当前 `docker-compose.yml`
> 起的是 gateway/lobby/room/match/chat/world）。代码与入口都在，部署时需要按需
> 补一个 service 块。

**为什么单独拆。** 登录要打外部渠道网络，渠道慢一秒网关就得替它挂一秒连接，
开服高峰会把网关 goroutine 吃光。拆出去后网关的登录路径重新变成纯本地 HMAC 验签。

**启动约束。** 没配 `LOGIN_SECRET` 直接启动失败——签不出票据的账号服没有意义，
而且那种配置下网关也在拒绝一切登录。渠道 provider 通过 `idp.Registry` 注册，
带超时和熔断；没注册任何 provider 时所有渠道登录被拒（打告警日志）。

**处理的命令**（`onReq`，queue subscribe 在 `req.account.>`）：

- `CmdAuthChannel`：渠道凭证换自家 token，顺带首登开号。**顺序不能改**——先过渠道
  校验，再落账号绑定，最后才签票据；反过来先签票据的话，绑定写失败会让玩家手握一张
  指向不存在账号的合法票据。
- `CmdBindChannel` / `CmdUnbindChannel`：给已登录 uid 增删渠道绑定。uid 取 Envelope
  （网关按会话填的），不信客户端自报。解绑最后一个登录方式会被拒（不可逆）。
- `CmdListBindings`：列绑定。

**错误分级**（`replyIDPErr`）。「凭证不对」和「渠道挂了」必须用不同错误码：前者让
客户端换凭证重来，后者让它稍后重试。合成一个码的话，渠道抖动期间玩家会以为号没了。

---

## lobby 大厅

- 入口：`cmd/lobby/main.go`（hashtag-kind 传 `lobby`）
- 业务：`internal/lobby/{service,shard,player,handlers,slots,gacha,gm}.go`
- 实例模型：**分片独占，不用 queue group**，compose 里 `lobby-1/2/3` 各持一批分片
- 分片键：`lobbyShard(uid) = uid % 1024`

**职责。** 玩家对象常驻内存的容器，同时跑玩家的全部非战斗玩法：背包、任务、邮件、
货币、社交、slots、抽卡、充值、GM。没有单独的玩家数据服务，逻辑和数据在一个进程里。

**分片模型**（`Start` 里建 `shardsvc.Service`，kind=Lobby）。用 `pkg/shardsvc` 的
Actor 框架：每个分片一条 goroutine 独占该分片全部玩家内存，无锁串行，同一玩家请求
天然不并发。订阅 `req.lobby.{shard}.>` **不带 queue group**——分片独占靠的就是同一
subject 只有一个订阅者。玩家数据懒加载（登录时 pipeline 读回全部模块 key），下线后
保留 5~10 分钟再最终刷盘删除。

**权限第二道校验。** Lobby 收到内部/GM 命令要再验一次 HMAC 签名（`authz.Signer`）。
这道存在的理由是不信任网关——网关部署时不持有 `INTERNAL_SECRET`，物理上签不出合法
内部命令。没配密钥时本进程也发不出发奖/结算/跨分片入账（打告警）。

**job 中转层**（`startJobConsumers`）。作为 JetStream 竞争消费者跑三个 durable
consumer，多个 Lobby 实例共用：

- `lobby-transfer`（`job.transfer.*`）：跨分片资源转移
- `lobby-mail`（`job.mail.send`）：发信
- `lobby-battle`（`job.battle.settle`）：战斗发奖

竞争的是「谁来搬运」，搬运动作是把任务转发给目标 uid 所在分片的 owner（`forward`），
转发的是带签名的内部命令，成功才 Ack，失败按 `backoff` 阶梯 Nak 重投。`jobSem`
（容量 64）限制并发转发数，避免 JetStream 一次推一大批把出向请求打爆。

**其它内存外的活儿。** 补偿扫描器 `scanPendingTx` 由分片 Tick 触发（只有 owner 扫
自己的分片），重投超时的 PENDING 转移；奖池注入/清算/派彩补偿（`contributeJackpot`/
`claimJackpot`/`scanJackpotPayouts`）；下行推送 `pushToPlayer` 走 `push.gate.{gateID}`，
全服公告 `Broadcast` 走 `push.broadcast`；配置热替换 `ReloadConf`（版本跟着回合走，
校验不过保持原样）。所有 IO 都甩到独立 goroutine，Actor 回调里只投递不阻塞。

**slots 合规要点**（`slots.go`）。下注/派彩全程 L0 写穿，一个 Lua 里搞定幂等判定、
追流水、写余额、写回合，提交成功才改内存回包。未结算回合随投注原子落盘，登录时
`LoginResp.open_round` 带回，凭 `seed_hex`+`config_version` 可位对位复算。

---

## room 房间

- 入口：`cmd/room/main.go`（hashtag-kind 传 `room`）
- 业务：`internal/room/{service,shard,room,handlers}.go`
- 实例模型：**分片独占**，compose 里 `room-1/2`
- 分片键：`roomShard(roomID) = roomID % 1024`，与 Lobby 是两套独立分片空间

**职责。** 持有房间和战斗状态。用和 Lobby 相同的 `pkg/shardsvc` Actor 框架，但
`Tick` 更密（200ms，要推战斗帧）。

**房间对象**（`room.go`）。`Room` 分两类数据：可落盘的（成员、模式、状态、帧号，
`Snapshot`/`Marshal`）和不落盘的战斗中间态（`scores`/`inputs`，L3 临时数据）。
关键设计：`FromSnapshot` 接管时如果房间处于战斗中，一律回退到等待状态重新开局——
中间态没落盘，接不回来。`BattleTimeout` 5 分钟强制结算，`IdleTimeout` 2 分钟回收
空房间。

**广播与发奖**（`service.go`）。`broadcastRoom` 不走 NATS 广播，而是查路由表按
gateID 聚合，每个网关发一条带多个 uid 的包（查表 IO 放独立 goroutine）。
`publishSettle` 战斗结束后把发奖投进 JetStream（`job.battle.settle`），每位成员
一条，msgID 用 `battle-{roomID}-{uid}` 去重，最终由 Lobby 的 battle consumer 领走
发到玩家分片。

---

## match 匹配

- 入口：`cmd/match/main.go`（hashtag-kind 传 `match`）
- 业务：`internal/match/match.go`
- 实例模型：**按 模式×段位 分桶独占认领**，compose 里 `match-1/2`
- 分桶：`Buckets = ModeCount(8) × TierCount(16) = 128`

**为什么还是分片独占。** 匹配池纯内存不刷盘，实例挂了池子清空玩家重排即可，所以
`SkipEpoch: true` 跳过 fencing。但「同一个桶只能有一个实例在撮合」必须成立，否则
两个实例会把同一批人撮进两个房间，所以仍走分片认领。

**撮合逻辑**（`Bucket`）。每个桶维护一个 `queue` 和 `inPool` 去重集。`tryMatch`
按战力排序取相邻的一组（战力差最小），战力差超过 `tolerance` 就等下次；`tolerance`
随等待时间放宽（每秒 +500），等够 `MaxWait`（30s）无条件放行，避免高战力永远匹配
不上。`Tick`（500ms）周期性尝试撮合。

**撮合成功后**（`createRoom`）。生成 roomID → 按 roomID 选房间分片 → 发 `CmdCreateRoom`
建房 → 推 `PushMatchFound` → 发 `CmdStartBattle` 开打。建房失败（分片在交接）不能把
人丢了——他们已从池子取出，`requeue` 放回本桶，放不回去（桶已不在本实例）就通知
玩家重新入队。桶被释放时（`Close`）同样通知排队玩家重排。

---

## chat 聊天

- 入口：`cmd/chat/main.go`（hashtag-kind 传 `lobby`）
- 业务：`internal/chat/chat.go`
- 实例模型：**不持有数据，用 queue group**，compose 里 `chat-1/2`

**职责。** 纯消息中继，不存数据，所以多实例竞争消费（`req.chat.>` queue group
`chat`）正好就是要的负载均衡。单条上限 `MaxTextLen` 512，超长截断。

**分频道转发**（`relay`，昵称从 profile 只读摘要拿，不唤醒发送者玩家对象）：

- `ChanWorld` 世界频道 → `push.broadcast` 全服广播
- `ChanPrivate` 私聊 → 查路由表按 gateID 聚合定向推
- `ChanGuild` 公会 → 查公会成员索引，按 gateID 聚合推（不走 NATS 广播）
- `ChanRoom` 房间 → 转给房间分片（只有它知道成员列表），包成一个 `RoomOp`

---

## world 世界服

- 入口：`cmd/world/main.go`（hashtag-kind 传 `world`）
- 业务：`internal/world/world.go`
- 实例模型：**选主主备**，compose 里 `world-1/2`，同一时刻只有 leader 对外服务

**职责。** 世界 BOSS、活动调度这类全局唯一逻辑。同一时刻只能有一个实例改 BOSS
血量，不然两边各扣各的都不对。

**两道保护**，跟分片那套相同，只是空间只有一格（`worldShard = 0`）：

1. `pkg/leader` 选主决定谁订阅 `req.world.>`——备机压根收不到消息。
2. epoch fencing 在落盘时做最终裁决。

**自身也是个 Actor**（`loop`）。单 goroutine 串行，`mbox` 有界（4096），不用锁。
`onElected` 的顺序和分片接管一致：抬 epoch → 加载 BOSS 状态 → 装进 Actor → 订阅
subject → 服务。`onResigned` 失去领导权就退订，内存里没落盘的血量**直接丢**——
这会儿已不是权威副本，再写会盖掉新 leader 的数据。

**BOSS 逻辑**（`handleHit`/`tick`）。受击扣血，`markDirty` + 3s ticker 落盘；击杀是
关键跃迁，立刻落盘不等 ticker，并广播公告；死亡后 10 分钟 `respawn` 重生。
`flushBoss` 带 epoch 校验，被 fencing 拒了主动 `elector.Stop` 让位，绝不重试。

---

## gameconfctl 配置工具

- 入口：`cmd/gameconfctl/main.go`
- 不是常驻服务，是命令行运维工具，不连 etcd/NATS/Redis

三个子命令，配置版本号是内容哈希（改一个数字就变）：

- `dump [-o 文件]`：导出内置默认配置（`gameconf.Default`）。
- `lint -f 文件`：校验配置并打印版本号、各老虎机/卡池摘要。发布前必跑。
- `rtp [-f 文件] [-n 次数]`：蒙特卡洛实测 RTP，对每台机器跑 N 次 `slots.Spin`
  （含免费旋转与倍率），实测偏离配置值超过 0.5% 以非零码退出。改数学模型后必跑。

对应 Makefile：`make conf-dump` / `make conf-lint` / `make rtp`。

---

## 一次登录的全链路

把前面的进程串起来，一次渠道登录经过：

```
client ──login(channel,cred)──► gateway
                                   │  网关不打外部网络
                                   ├─ req.account.auth_channel ──► account
                                   │                                  ├─ idp 校验渠道凭证
                                   │                                  ├─ Resolve 开号/取 uid
                                   │                                  └─ 签发 token（LOGIN_SECRET）
                                   │◄──── uid + token ────────────────┘
                                   ├─ 本地再验一次 token（uid 只认票据）
                                   ├─ Lua 原子写 session:{uid}，顶掉旧连接发 KICK
                                   └─ req.lobby.{uid%1024}.login ──► lobby(owner 分片)
                                                                       ├─ 懒加载玩家全部模块 key
                                                                       └─ LoginResp（含 open_round）
```

### 数据帧格式

客户端与网关之间是裸 TCP，帧格式（`internal/gateway/conn.go`）：

```
[4 字节大端长度][Envelope protobuf]      上限 1MB，与 NATS max_payload 对齐
```

`pb.Envelope` 是贯穿全链路的信封，关键字段：`Cmd` 命令号、`Uid`、`Body` 业务
payload（也是 protobuf）、`TraceId`、`FromNode`、`Shard`、`Auth` 内部命令签名、
`Seq`、`ErrCode/ErrMsg`。进程之间经 NATS 传的也是这同一个 Envelope，所以
「客户端 → 网关 → 后端」是一套编码，只是承载从 TCP 换成 NATS。

### 逐步的数据落点

1. **网关分发**（`dispatch`）。`CmdLogin` 是唯一在鉴权前就分流的命令，因为它要先
   建会话。此时 `Envelope.Uid` 还是 0。
2. **账号服校验 + 开号**（account `handleAuth`）。链路里唯一读写**账号身份数据**的
   地方：`idp.Verify` 打外部渠道拿到 `(channel, openID)`；`store.Resolve` 在 Redis
   查 `acct:{acct}:bind:{channel}:{openID} → uid`，没有就用预生成的雪花候选 uid 开号。
3. **签 token**。`issuer.Issue(uid, ttl)` 用 `LOGIN_SECRET` 做 HMAC。**token 不落
   任何库**，它是无状态自证的，谁有同一把密钥都能验。
4. **网关二次验签**（`handleLogin`）。账号服换回的 token 网关必须再本地验一遍——
   uid 只认票据这一个来源，多一条旁路就多一种伪造姿势。验完清空 channel/credential。
5. **建会话**（`session.Bind` → `scriptBind` Lua）。登录链路第一处写「运行时状态」：
   ```
   session:{uid} = { uid, gate_id, conn_id, login_at, beat_at }   # hash，带 TTL
   SADD gate:sessions:{gateID} uid                                 # 网关名下索引，重启清理用
   ```
   Lua 原子返回被顶掉的旧 gate_id/conn_id，这就是顶号——新登录直接覆盖 session。
6. **顶号处理**。有旧连接就往 `push.gate.{旧gate}` 发 KICK；网关本地 `byUID[uid]=c`
   登记连接（内存 map，下行推送时 uid→连接的定位）。
7. **Lobby 加载玩家**（lobby `handleLogin`）。请求按 `uid%1024` 路由到 owner 分片的
   Actor。玩家对象**懒加载**——框架在交给 handler 前 pipeline 从 Redis 读回该玩家所有
   模块 key（`p:{uid}:base/bag/quest/round`…）反序列化成内存 `Player`。handler 做：
   查自我排除、置 Online/GateID/ConnID、起责任游戏计时、收离线邮件、标脏。`LoginResp`
   带回 base/bag/quest 与 `open_round`（免费旋转中途断线的未结算回合，可接着打完）。

### token 重连（更短的路）

重连带上次的 token、不带 channel，**跳过账号服**：

```
client ─login(uid, token)─► gateway ─(本地验 token)─► sessions.Bind ─► req.lobby.*.login ─► lobby
```

省掉「打外部渠道 + 签新 token」。这正是账号服要单独拆出去的原因：只有首登/换设备
才走它那条慢路径，日常重连是纯本地 HMAC 验签。

### 游客账号

游客登录**不是独立链路，是渠道登录的一个特例**——用 `DeviceID` 当 openid
（`idp.Credential` 已预留该字段）。绑定表正表键变成
`acct:{acct}:bind:guest:{deviceID}`，除此之外 uid 生成、token 签发、session 建立、
Lobby 加载全部复用，游客号在 Lobby 眼里和正式号没有区别。

现状与注意点：

- 代码里**没有内置 guest provider**，真实 provider 只有本地开发用的 `sandbox`
  （`IDP_SANDBOX=true`，把凭证原样当 openid，生产严禁开启）。要正式支持游客需加一个
  `idp.Provider`，`Verify` 里把 deviceID 当 openid 返回，在 account 服注册即可，下游无感。
- 换设备 = 丢号：游客身份绑死 deviceID，清数据/重装会变成新号。找回靠 `CmdBindChannel`
  引导游客绑定正式渠道，之后用正式渠道登录回到同一 uid。
- 刷号：每个新 deviceID 开一个新号，配合首登奖励易被薅，正式渠道靠 OAuth 天然限制，
  游客这层代码没有防护，上线需在 provider/account 层补设备号风控。

### 回包走的是请求-应答，不是推送

登录成功后 Lobby**不是直连客户端**，而是把 `LoginResp` 作为 **NATS 请求-应答**返回给
发起请求的那个网关，网关再经 TCP 编码发回客户端。Lobby 全程不认识客户端连接。

```
client ─TCP─► gateway ─NATS request(req.lobby.7.login)─► lobby(owner 分片)
                                                             │ handleLogin
                                                             │ m.Respond(LoginResp)
client ◄TCP─ gateway ◄─NATS reply(临时 inbox)───────────────┘
                 │ forward 拿到 resp → c.SendEnv(resp)
```

**应答为什么能回到正确网关**：NATS request-reply 是点对点的，谁发的 request、reply
就回给谁，和 gateID、session 路由表**无关**。网关用自己进程的 NATS 连接发起 request，
应答自然回到这个连接。所以登录**回包不查 session**。

登录过程中建立的 `session:{uid}={gateID,connID}` 是给**另一类消息**用的——主动推送
`push.*`（邮件、聊天、顶号 KICK、全服公告）。这些不是玩家请求的应答，Lobby/Chat 必须
主动查 session 拿 gateID，才走 `push.gate.{gateID}` 找到玩家。两条路分工：

| 场景 | 机制 | 怎么找到客户端 |
|---|---|---|
| 登录回包、玩法请求回包 | NATS 请求-应答 | 不用找，reply 自动回到发起请求的网关 |
| 邮件 / 聊天 / 踢人 / 公告 | NATS `push.*` | 查 `session:{uid}` 拿 gateID，发 `push.gate.{gateID}` |

网关不是纯透传：登录应答在 `forward` 之前就已完成验签、换 token、建 session、发 KICK、
`byUID` 登记；`forward` 还带有限次重试（`ErrUnavailable`/`ErrNotOwner`/超时时重投），
兜住 Lobby 分片交接的几百毫秒窗口，客户端无感知。

### 三处存储，别混

登录链路碰了三种数据，各有各的家：

| 数据 | 存在哪 | 谁写 | 形式 |
|---|---|---|---|
| 账号身份（channel↔uid 绑定） | Redis `acct:*` | account 服 | 首登时写一次 |
| 登录票据 token | 不落库 | account 签，gateway + account 都能验 | 无状态 HMAC |
| 会话路由 `session:{uid}` | Redis，带 TTL | gateway（Bind/Touch/Unbind） | hash，心跳续期 |
| 玩家游戏数据 `p:{uid}:*` | Redis + Lobby 内存 | Lobby owner 分片 | 内存权威，Redis 是投影 |

一句话：**账号服认「你是谁」（channel→uid），token 是这个结论的可验证凭证，session
记「你现在连在哪个网关」，Lobby 才真正把游戏数据从 Redis 拉进内存开始服务。**

### 后续玩法请求

`gateway` 做 Client 级鉴权 + 按 uid/roomID/mode-tier 路由 → 对应服务的 owner 分片
处理 → 回包同样走请求-应答。服务端主动下发（发奖、公告等）才走 `push.gate.{gateID}`。
内部命令（发奖、跨分片入账）额外带 HMAC 签名过 Lobby 第二道校验。
