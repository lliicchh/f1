# 游戏服务器（slots + RPG）

分区分服的游戏服务器，Go / NATS / Redis / etcd / Docker Compose。
玩家对象常驻内存、按分片独占、etcd 选主与 fencing、Redis 分级刷盘，
业务面覆盖 slots 的一整套（RNG、回合、账本、RTP、奖池、合规）与 RPG 养成。

两条取向贯穿全部代码：

1. 进程内存是数据的权威副本，Redis 是它的持久化投影。任何时刻一份玩家数据
   只能被一个进程持有。这是强约束，不是性能优化。
2. 分片是一切的调度单位。玩家和房间统一映射到分片，分片被实例独占认领，
   资源分配、故障转移、扩缩容、刷盘都以分片为粒度。

不做的事：不引入 MySQL，不做跨服玩法，不提供跨分片强一致事务。

文档里的路径都相对仓库根目录。

```bash
make test    # 单元 + 集成测试，内嵌 etcd / NATS / Redis，不需要 Docker
make build   # 构建全部服务
make up ENV_FILE=deploy/s1.env PROJECT=game-s1 TAG=v1.0.0   # 起一个区服
make rtp     # 蒙特卡洛实测 RTP，改了轴带或赔付表必跑
```

## 1. 架构

```
client ─┬─ Gateway ×2 ─┐
        └─ ...         │
                 ┌─────┴─────┐
                 │NATS Cluster│
                 └─────┬─────┘
    ┌───────┬───────┬──┼───┬───────┬───────┐
  Account  Lobby   Room  Match  Chat   World
   queue  ×3分片  ×2分片  分桶   queue   主备
    └───────┴───────┴──────┴───────┴───────┘
                       │
             ┌─────────┴─────────┐
           Redis              etcd
      玩家数据/session      分片/nodeID
```

| 服务 | 职责 | 实例模型 | 代码 |
|---|---|---|---|
| Gateway | 连接、编解码、下行推送 | 无数据，LB 轮询 | `internal/gateway` |
| Account | 渠道校验、开号、多渠道绑定 | 无玩家数据，queue group | `internal/account` |
| Lobby | 玩家对象常驻内存 + 非战斗玩法 | 分片独占 | `internal/lobby` |
| Room | 房间和战斗状态 | 分片独占 | `internal/room` |
| Match | 匹配池 | 按 `模式×段位` 分桶独占 | `internal/match` |
| Chat | 消息中继，不存数据 | queue group | `internal/chat` |
| World | 世界 BOSS、活动调度 | 选主主备 | `internal/world` |

**Lobby 是玩家对象的容器，同时跑玩家的业务逻辑。** 背包、任务、邮件、货币、社交的
数据和逻辑都在这个进程里，没有单独的玩家数据服务。拆成「逻辑服 + 数据服」的话，
每次读背包都要一次 RPC，内存态的好处全部抵消，而数据服仍然需要分片独占，
复杂度只是往后挪了一层。用 Redis 当数据源、逻辑服无状态是另一套成立的架构，
两套可以选，不能混。

**持有玩家数据的服务不能用 queue group。** queue group 只保证负载均衡，
不保证同一玩家路由到同一实例，内存态下会出现双副本互相覆盖刷盘。

## 2. ID

三层结构，前两层派生出进程标识和雪花的 workerID：

| 层 | 例 | 来源 |
|---|---|---|
| 区服 `serverID` | 1 | 手动配置，运营概念 |
| 服务类型 `svcType` | 2（lobby） | 代码枚举，3 bit（1..7 已占满） |
| 进程序号 `nodeSeq` | 1 | 手动配置，每服务从 1 开始 |

```
nodeID   = s{serverID}-{svcName}-{nodeSeq}     例 s1-lobby-1
workerID = svcType<<7 | nodeSeq                10 bit

1 符号 | 41 时间戳(ms) | 10 workerID | 12 序列号
           69 年          1024 节点     4096/ms/节点
```

serverID 不进 workerID。分区分服数据隔离，uid 只要区服内唯一，
一区和二区各有 uid=10001 完全没问题，它们在不同 Redis 里永不相遇。
真要全局唯一时用 `(serverID, uid)` 二元组。

3 bit 的服务号段已经占满（gateway/account/lobby/room/match/chat/world）。
再加第八种服务必须先加宽 `svcType`，那会改变 workerID 的布局，进而改变已发出去的
雪花 ID 的取值空间，是个破坏性变更，要连带考虑存量 ID。

workerID 用位拼接而不是人工号段。若 nodeSeq 直接当 workerID，各服务都从 1 开始会互撞，
只能靠约定「lobby 从 100 起、room 从 600 起」错开，依赖人的纪律。
位拼接把这件事变成编译期常量，compose 里每个服务都能从 1 开始编号。

**两道启动检查，缺一不可。** 代码：`pkg/ident`、`pkg/nodeid`、`pkg/node`。

第一道是越界，`nodeSeq` 必须在 [1,127]。128 会溢出到相邻服务的号段，
静默产生重复 workerID。

第二道是 etcd 上的唯一性检查。手写序号最怕撞号：复制 compose 配置忘改数字，
两个进程用同一个 nodeID。此时 etcd 认为是同一个 owner 在续租，续租不会失败，
两个进程同时持有同一分片，epoch 还一样，fencing 也拦不住。
这是全套设计里唯一没有兜底的洞，只能在启动时拦住。

```
key = {ETCD_PREFIX}/node/{svcName}/{nodeSeq}
val = {"pid":..., "host":..., "addr":..., "beat_at":...}

键不存在              → 抢占成功
键在但 beat_at 超 10min → 按僵尸记录接管
键在且心跳新鲜         → 打印冲突方 host/pid，拒绝启动
```

抢占后 30s 一次心跳，正常退出时删键。抢不到就退出，
不要降级用随机数或 hostname 哈希兜底，那就是在生产制造重复 ID。
风险因此从静默数据损坏降为发布时启动失败，后者改配置重启就行。

**时钟回拨。** 序号固定绑容器，workerID 不会被别的进程复用，跨进程回拨不存在。
单进程内回拨 ≤10ms 自旋等待追平，>10ms 停止发号并告警，宁可这个进程不服务。
运维配合项：NTP 必须用 slew 模式。

**什么时候改成自动分配。** 上第二台物理机、实例数超过十个左右、或者上 K8s。
etcd 注册结构与自动分配完全一致，切过去只是在前面多一个扫空位的步骤，
但那时 workerID 会被复用，得补上跨进程时钟回拨防护（键里维护 `last_ts`）。
`nodeid.Record.LastTS` 已经就位。

## 3. 分片

```
ShardCount = 1024        // 常量，一经确定永不变更
lobbyShard(uid)   = uid % 1024
roomShard(roomID) = roomID % 1024
```

Lobby 与 Room 用独立分片空间。1024 远大于预期实例数保证均匀分布，
分片数变更等价于全量迁移，一次定死。

**认领。** etcd 每分片一个键，带 lease，TTL 8s，续租间隔 2.5s。
启动时扫描空闲分片，用 `CreateRevision == 0` 的事务 CAS 抢占。
续租失败立即停止处理该分片消息，lease 过期键自动删除，其他实例接管。
1024 个分片并发认领（16 路），不然串行的 etcd 往返会拖到几十秒。
代码：`pkg/shard`。

epoch 直接取键的 `CreateRevision`，不写回值里。它天然单调递增，
还省掉一次「先占位再回写」的写入，接管时间减半。
`etcdctl get --write-out=json` 依然能直接看到。

**分片 lease 和 nodeID 心跳的阈值取舍相反，不要混用同一套：**

| | 超时 | 回收慢的代价 | 回收快的代价 |
|---|---|---|---|
| 分片 | 8s | 玩家不可用 | 误接管，有 fencing 兜底 |
| nodeID | 10min | 浪费一个槽位 | 误判冲突，拒绝启动 |

**订阅不带 queue group。** 分片独占靠的就是同一 subject 只有一个订阅者，
加了 queue group 反而允许多实例同时订阅。

**Actor 执行模型。** 每个分片一条 goroutine，独占该分片全部内存数据，
有界 mailbox，积压超阈值告警。无锁，同一玩家的请求天然串行，
不存在并发扣道具类问题。回调内禁止阻塞：序列化在 Actor 里做（读内存必须如此），
真正的 IO 甩给独立 goroutine，结果再投递回来。

## 4. 消息

subject 前缀就是投递保证。代码：`pkg/subject`、`pkg/bus`。

| 前缀 | 语义 | 投递 |
|---|---|---|
| `req.` | 请求-应答 | Core NATS |
| `evt.` | 事件广播 | Core NATS |
| `push.` | 下行推送 | Core NATS |
| `job.` | 必达任务 | JetStream |
| `ctl.` | 控制面 | Core NATS |

```
req.lobby.{shard}.{cmd}      玩家逻辑
req.room.{shard}.{cmd}       房间逻辑
req.match.{mode}.{tier}      匹配
push.gate.{gateID}           定向推送
push.broadcast               全服广播
evt.player.{uid}.{event}     玩家事件
job.transfer.{txid}          跨分片资源转移
job.battle.settle            战斗发奖
ctl.node.{nodeID}.handoff    分片交接
ctl.node.{nodeID}.shutdown   优雅下线
```

默认 Core NATS，状态同步类消息丢了由下一帧覆盖，用 JetStream 是浪费。
跨分片转移、发奖、发信、充值到账这些丢了会造成资产不一致的操作走 `job.`，
在命名上就区分开。

Envelope 带 `trace_id` 和 `from_node`，排查问题全靠这两个串起来。

几个坑：slow consumer 会静默丢消息，ErrorHandler 必须装并告警；
回调是单 goroutine 串行的，里面只能投递不能干活；payload 有 1MB 上限；
没有订阅者的消息直接丢，交接窗口靠客户端重试兜。

## 5. 数据与刷盘

key 按模块拆，避免改一个字段重写整个玩家。代码：`pkg/store`。

```
p:{1001}:base       等级、经验、货币
p:{1001}:bag        背包
p:{1001}:quest      任务
p:{1001}:social     好友、公会
p:{1001}:round      未结算回合
profile:{1001}      只读摘要
ledger:{1001}       资金流水（Redis Stream）
room:{shard}:{roomID}        房间快照
shard:lobby:{shard}:epoch    分片 epoch
session:{uid}                → {gateID, connID}
tx:{s<发起方分片>}:{txid}     跨分片转移记录
```

`{1001}` 是 hash tag，保证同一玩家所有 key 同 slot，单玩家的 Lua 和 pipeline 才能用。
数据 key 用 `p:` 而不是 `lobby:`，key 跟着数据实体走不跟服务名走，
将来 Lobby 拆分或改名不用迁数据。

序列化用 protobuf，只增字段不改 tag，废弃字段用 `reserved`，
滚动发版时新旧版本进程会同时读写同一批数据。

| 级别 | 数据 | 策略 | 最大丢失 |
|---|---|---|---|
| L0 写穿 | 充值、开箱、交易、下注 | 落盘成功才改内存回包 | 0 |
| L1 高频 | 货币、等级、背包 | 脏标记 + 5s | 5s |
| L2 低频 | 设置、社交 | 脏标记 + 60s | 60s |
| L3 临时 | 战斗中间态 | 不刷盘 | 全部 |

L0 必须幂等，客户端带唯一订单号，重复提交返回首次结果。

```
Actor goroutine                      IO pool
   ├─ ticker 触发
   ├─ 遍历 dirty 序列化（读内存，必须在 Actor 内）
   ├─ 清 dirty
   ├─ batch → flushCh ──────────► pipeline 写 Redis + Lua 校验 epoch
   └─ 立即返回                       失败回报，重新标脏
```

flushCh 满时走 default 重新标脏并告警，绝不阻塞 Actor。
积压说明 Redis 已经扛不住，阻塞会把故障扩散成全服卡死。
ticker 初始偏移 `shardID % interval`，免得 1024 个分片同秒刷出尖峰。
刷盘失败重新标脏绝不丢弃，内存还是权威副本，Redis 挂着也能继续服务。

**Redis 配置。** 它是唯一的持久化后端，配错等于丢全服数据。
`maxmemory-policy` 必须 `noeviction`，设 LRU 会在内存满时静默淘汰玩家数据，
服务启动时校验，不满足直接拒绝启动。`appendonly yes` + `appendfsync everysec`。
部署用主从哨兵或 Cluster，定期导出 RDB，那是业务 bug 写坏数据时唯一的回滚手段。

Cluster 部署要把 `REDIS_HASHTAG` 从 `uid` 改成 `shard`，让 epoch 键与该分片下
所有玩家键落在同一 slot，否则 fencing 的 Lua 会撞 CROSSSLOT。
单实例与主从哨兵无此约束。

**profile 只读摘要。** 查好友资料、排行榜、给离线玩家发信时要读别人的数据，
为看一眼头像就把整个玩家对象加载进内存不划算。owner 分片在数据变更时
顺手写一份轻量快照，只读、允许滞后几秒、任何进程直接读 Redis，不唤醒玩家对象。
修改离线玩家仍然必须走 `job.` 中转层。代码：`pkg/profile`。

这就是 CQRS：写模型在内存（owner 独占强一致），读模型在 Redis（人人可读，最终一致）。

## 6. 脑裂防护

```
T1  节点 A 持有分片 7
T2  A 发生 GC 长停顿或网络分区
T3  lease 过期，分片释放
T4  节点 B 接管，从 Redis 加载，玩家产生新数据
T5  A 恢复，还不知道自己已经失去所有权，把旧内存刷进 Redis
    → 覆盖 B 的新数据，玩家进度回滚
```

内存态数据没有 DB 行级 version 兜底，只能在设计层解决。两道保护。

软保护是 lease 检查：续租失败立即停止处理消息和刷盘。覆盖大部分情况，
但拦不住进程完全冻结后恢复。

硬保护是 epoch fencing：所有写入过 Lua，先比 epoch。

```lua
-- KEYS[1]=shard:lobby:7:epoch  ARGV[1]=调用方 epoch
if tonumber(redis.call('GET', KEYS[1]) or 0) > tonumber(ARGV[1]) then
    return 0        -- 调用方已过期，拒绝写入
end
```

接管方在加载数据之前先抬高 epoch，此后旧 owner 任何写入被拒。
旧 owner 收到拒绝就丢弃该分片内存、停止服务、告警，绝不重试。
内存已经过期，重试只会更糟。代码：`pkg/store/fencing.go`。

fencing 的失效前提是两个进程 nodeID 相同，那时它们的 epoch 也相同，
分不出谁是谁。这就是第 2 节那道启动检查必须存在的原因。

一致性边界：单分片内强一致（单 goroutine 串行），跨分片最终一致，
崩溃恢复时 L0 零丢失、L1/L2 按刷盘间隔回退。

## 7. 跨分片

分片之间不许直接动对方内存。没有跨分片事务，直接互操作时任一侧崩溃就会
资源凭空消失或者翻倍，事后还查不出来。代码：`pkg/xtx`。

```
A 分片                      Redis                   B 分片
  ├─ 扣道具（内存）
  ├─ 写穿 tx 记录 ─────► {from,to,item,PENDING}
  ├─ job.transfer.{txid} ──────────────────────►  ├─ SET done NX（幂等）
                                                  ├─ 加道具（内存）
                                                  └─ 标记 COMPLETED
```

发起方先扣除并写穿，资源不会凭空增加；接收方用 txid 幂等去重；
后台扫描器定期重投超时的 PENDING。邮件附件、交易、公会仓库、组队奖励、
给离线玩家发奖全走这一套。

tx 键的 hash tag 按「哪一侧要原子」来定：
`tx:{s<发起方分片>}:<txid>` 与 `tx:pending:{s<发起方分片>}` 同 slot，
让「扣道具 + 写 tx 记录 + 入 PENDING 索引」在一个 Lua 里完成；
`txdone:{s<接收方分片>}:<txid>` 与接收方玩家键同 slot，让「幂等标记 + 入账」也原子。

`job.` 中转层用 JetStream 的竞争消费，多个 Lobby 实例共用同一个 durable consumer。
竞争的是谁来搬运，搬运的动作是转发给目标分片的 owner，不是谁持有数据。

## 8. 网关与会话

```
session:{uid} → {gateID, connID}     Redis，TTL 由心跳续期
```

网关只订 `push.gate.{gateID}` 和 `push.broadcast` 两个 subject，
订阅数与在线人数无关。按 uid 订阅的话订阅数会等于在线人数，
NATS 集群内 interest 传播开销随在线量线性增长。代价是每次推送多一次查表，
可以本地缓存，但顶号时要让缓存失效。代码：`pkg/session`。

顶号用 Lua 原子替换 session，取出旧 gateID 后发 KICK，
旧连接所在的 Lobby 分片保存并卸载上下文。

房间和公会广播不走 NATS 广播，由分片批量查路由表按 gateID 聚合，
每个网关发一条带多个 uid 的包。全服公告才走 `push.broadcast`。

网关重启后 session 变脏数据，两种清理都做：启动时清掉自己 gateID 名下所有 session，
以及给 session 设 TTL 靠心跳续期。

### 登录与账号

登录拆成两段，慢的那段不在网关上。

```
渠道登录（首次、换设备）        重连（绝大多数）
客户端 → 网关 → 账号服 → 渠道    客户端 → 网关
        ↑ 换回我们自己的 token           ↑ 本地 HMAC 验签，零外部 IO
```

客户端 `CmdLogin` 带 `channel` + `credential` 时走上面那条：网关转 `req.account.auth_channel`，
账号服打渠道、查绑定表、没绑过就用雪花开个号，签一张 `uid.exp.sig` 回来。
网关拿到票据之后仍然本地验一遍才认 uid，票据是 uid 唯一的来源，不留旁路。

带 `uid` + `token` 时走下面那条，完全不碰账号服。渠道挂了只影响首登，
在线玩家和重连都不受影响。客户端应当缓存票据，别每次都打渠道。

账号服不持有玩家数据，所以能用 queue group，按渠道的响应时间加实例就行。
它和网关是仅有的两个需要 `LOGIN_SECRET` 的进程，一个签发一个校验；
两个都不该拿到 `INTERNAL_SECRET`（见 `deploy/gateway.env`、`deploy/account.env`）。

**外部 IO 全都关在 `pkg/idp` 里。** 一个渠道一个 `Provider`，注册表统一负责三件事：

- 硬超时。provider 不看 ctx 也拖不住调用方，到点就不等了。
  第三方 SDK 忽略 ctx 太常见，靠约定守不住
- 熔断。连续失败 N 次就断开该渠道，冷却期满只放一个探针。
  渠道挂了的时候，没有熔断就是「所有人卡 3 秒」，goroutine 会先被吃光
- 错误分级。「凭证不对」不重试也不计入熔断，「渠道挂了」可重试并计入。
  合成一个码的话，渠道抖动期间玩家收到的是「账号无效」

没注册 provider 的渠道一律拒绝。沙箱要 `IDP_SANDBOX=true` 显式开，忘了配不能变成谁都能登。

**绑定表** 在 `pkg/account`，一个 uid 可以绑多个渠道，任一渠道登录都回到同一个号。
两条规则写死在 Lua 里：同一个渠道账号绝不能指向两个 uid，
最后一个绑定不能解绑（解了这个号就再也登不进来）。
首登开号用 CAS，并发首登只会开出一个号。

所有账号键共用 `{acct}` 一个 hash tag。绑定的正反两张表要进同一个 Lua，
而正表按渠道账号寻址、反表按 uid 寻址，没有第三种 tag 能同时满足两边。
代价是账号数据不分片，量很小：只有首登和换设备会读它。

## 9. 生命周期

**玩家数据。** 懒加载，登录时由所属 Lobby 分片 pipeline 读回全部模块 key。
下线后保留 5~10 分钟（断线重连、离线结算、好友查看），超时后最终刷盘并删除。
这段冗余要计入内存估算。

**分片交接。** 靠 lease 自然过期有 8~15s 不可用窗口，主动交接能压到百毫秒。

```
1. 新节点发 ctl.node.{old}.handoff
2. 旧节点停止处理该分片新消息
3. 旧节点全量刷盘 → 释放 etcd 锁 → 回 READY
4. 新节点抢占，拿新 revision 当 epoch
5. 新节点抬高 Redis epoch → 加载数据 → 订阅 subject → 服务
```

再平衡由想要分片的一方发起交接，不是持有方主动让出。让出会制造一段没人服务的窗口。
只有服务没接入 handoff 时才退化为主动让出。交接窗口内的请求会被 Core NATS 丢弃，
客户端要实现超时重试，网关也做了有限次重投兜底。

**优雅下线，顺序不能颠倒。** 框架固化在 `pkg/node`，服务只填内容。

```
1. 网关先告诉客户端去重连别的网关
2. 摘注册、发起 handoff，停止接新请求
3. nc.Drain()，处理完 pending 再关连接
4. 全量刷盘并确认成功
5. 删除 etcd 里的 nodeID 注册键
6. 退出
```

## 10. slots 与合规

slots 有四条普通 MMO 没有的硬约束，它们直接改变了架构取舍。

**每一次 spin 都是一笔金融事务。** 不是掉个道具丢了下次再刷，是钱。
丢一次是客诉加对账差异，重复一次是资损。所以下注和派彩不适用 L1 的脏标记，
必须全程 L0，且必须有流水。

**回合是可中断的状态机，监管要求必须能恢复。** 免费旋转打到一半断线，
玩家重连后要能接着打完，GLI-19 对 incomplete round 有明文要求。
所以需要一个持久化的回合对象，而不是一次请求一个结果。

**事后必须能复算。** 客服接到「这把该中没中」的投诉时，要凭 round_id 取出当时的
种子、paytable 版本、reel 停位，重新算一遍，得到位对位一致的结果。
所以 RNG 要可记录可复现，配置要版本化，流水要不可变。

**RTP 是要被监控和审计的生产指标。** 偏离预期不是体验问题，
是配置错误或作弊的第一信号，也是监管核查项。所以投注额和派彩额要实时聚合。

抽卡与 slots 同源，也是 RNG 加概率公示加保底，同样需要流水与可复算。

### 命令权限分级

每个命令在 `pkg/protocol` 的定义处标注 `Client / Internal / GM`。
**没登记的默认落到最严的 Internal**，漏配的后果是调不通，而不是被人刷钱。

两道校验：网关只放行 Client 级；Lobby 再验一次 HMAC 签名。
第二道存在的理由是不信任网关，部署时网关不持有 `INTERNAL_SECRET`
（见 `deploy/gateway.env`），它在物理上签不出一条合法的内部命令。
代码：`pkg/authz`、`pkg/authn`。

### 一次 spin 的四步，顺序不能改

```
1. 检查（限额、档位、回合冲突），拒绝不产生任何副作用
2. 在副本上算结果，内存此时未被改动
3. 一次原子提交，余额、回合、限额、流水一起落库
4. 提交成功之后才改内存并回包
```

第 3 步落在 `pkg/store` 的 `scriptCommit` 里，一个 Lua 搞定幂等判定、追流水、写余额。
分两步写一定会出现「扣了钱没流水」或「有流水没扣钱」，
而这两种情况事后无法区分是 bug 还是欺诈。代码：`internal/lobby/slots.go`。

### 可复算

未结算回合随投注原子落盘，登录时由 `LoginResp.open_round` 带回。
回合里记着 `seed_hex` 与 `config_version`，`slots.Replay` 凭这两样重放出
位对位一致的结果，这是客服查单与监管抽查的入口。

RNG 用 ChaCha8 而不是 `math/rand`，后者的状态能从少量输出反推出来，
玩家能算出下一把开什么。种子由 crypto/rand 生成，不用时间戳。代码：`pkg/rng`。

免费旋转与主旋转共用一条随机流。代价是每次免费旋转要把前面几次重放一遍
（纯内存，开销可忽略），换来「一颗种子重放整局」这个性质，分成两条流就复算不了。

配置版本是内容哈希（sha256 前缀），改一个数字就变。人工维护的版本号总有人忘了改。
调参走配置发布，不走发版。代码：`pkg/gameconf`、`cmd/gameconfctl`。

### 奖池

注入用 Redis `INCRBY`，它本来就是原子的。中奖时原子地清零并落一条 PENDING 派彩记录，
再由第 7 节的中转层幂等打给玩家，两步同生共死。

不要走「每次 spin 发个 job 给 World 累加」，那是把一次加法做成分布式事务，
既慢又多一个失败点。代码：`pkg/jackpot`。

### RTP 由测试守着

`make rtp` 跑蒙特卡洛，实测偏离配置值 0.5% 以上即以非零码退出。
轴带改错一个符号 RTP 能从 96% 跳到 130%，这种错误必须在 CI 拦住，
不能等上线后靠报表发现。

`internal/slots` 保持纯函数，没有 IO、没有时间、没有全局状态。
一旦塞进时间戳或玩家实时数据，「同种子重放整局」就不成立。

### 责任游戏与抽卡保底

会话时长、日投注、日亏损、自我排除在下注前的统一检查点拦截，
限额状态随玩家数据持久化，跨日按配置时区重置（不用服务器本地零点，
换个部署地域窗口就偏了）。代码：`pkg/rg`。

抽卡有保底，概率与保底规则多地要求公示，公示了就必须与实现一致。
代码：`internal/lobby/gacha.go`。

### 钱

金额一律 `int64` 最小单位，不用浮点，浮点的舍入误差会变成对账差额。
下注额、到账数量只能来自服务端配置，客户端给什么都不算。
商品必须查配置表，未知商品直接拒绝；充值必须带渠道回执并由服务端验签。
订单幂等防的是重复，防不了伪造，两个不能互相替代。代码：`pkg/payment`、`pkg/ledger`。

## 11. 部署与运维

所有区服共用一份 `docker-compose.yml`，靠 env 文件和 project 名区分：

```bash
ENV_FILE=deploy/s1.env docker compose -p game-s1 up -d
ENV_FILE=deploy/s2.env docker compose -p game-s2 up -d
```

- 服务名与 `NODE_SEQ` 对齐（`lobby-2` 配 `NODE_SEQ=2`），看日志不用做映射
- 不用 `deploy.replicas`，副本之间没法携带不同的 `NODE_SEQ`。
  扩容手工加一个 service 块，配合 YAML 锚点只需三行
- `stop_grace_period: 60s` 必须设，默认 10s 会在优雅下线刷盘途中 SIGKILL。
  `SHUTDOWN_GRACE` 相应设 55s，按实际分片数与数据量调
- 用 `docker compose` v2

**上线前必须确认的事：**

0. 三把密钥改掉，且网关拿不到 `INTERNAL_SECRET`。`deploy/s1.env` 里的 `CHANGE-ME-*`
   是占位值。`deploy/gateway.env` 把内部密钥覆盖成空，这不是可选项，
   它是「网关被攻破也签不出内部命令」这一保证的物理基础。
   没配 `LOGIN_SECRET` 又没显式设 `ALLOW_DEV_AUTH=true` 时所有登录都会被拒绝，有意如此；
   没配 `PAYMENT_SECRET` 又没开沙箱时所有充值都会被拒绝，同样有意。
1. `IDP_SANDBOX=false`。置 true 后任何字符串都是合法 openid，谁都能登进任何号。
   `deploy/account.env` 里默认关着，别在生产打开。
2. Redis `maxmemory-policy` 是 `noeviction`。要放行得显式设
   `REDIS_REQUIRE_NOEVICTION=false`，那意味着接受静默数据丢失的风险。
3. NTP 用 slew 模式（chrony 配 `maxslewrate`，或 ntpd 加 `-x`）。
   默认配置在偏差大时会直接 step，那会触发发号停止。
4. 这几个指标非零即有事，必须接告警：`game_shard_epoch_rejected_total`、
   `game_nats_slow_consumer_total`、`game_id_clock_backwards_total`、
   `game_authz_rejected_total`。
5. `game_idp_breaker_open` 持续为 1 说明那个渠道已经登不进来了。
   配合 `game_idp_verify_total{outcome}` 看是渠道挂了还是凭证批量出错。
6. RTP 接监控大盘：`game_slots_win_amount_total / game_slots_bet_amount_total`
   按 `game` 与 `config_version` 分组，与配置的理论 RTP 做偏差告警。
7. `game_slots_jackpot_pending` 长期非零说明奖池派彩链路卡住了，钱在半路上。
8. 账本是过渡方案。流水目前写在 Redis Stream（每玩家约 2000 条），
   真钱上线前必须导出到不可篡改的长期存储，Redis 可被 FLUSH，不是审计级存储。

其余指标：分片的 mailbox 长度、处理延迟 P99、owner 变更次数；
刷盘的耗时、失败次数、flushCh 积压次数、脏数据滞留时长 P99（真实丢失窗口）；
NATS 的重连次数与各 subject 速率；ID 的序列号溢出次数；
业务的在线人数、实例内存、tx PENDING 超时数。代码：`pkg/metrics`。

**容量估算，待压测校准：**

```
单玩家内存 ≈ 200KB（含 map/slice 开销）
5 万在线 + 20% 卸载冗余 → 约 12GB → 3 个 Lobby 实例各 ~4GB
L1 刷盘带宽（5s 间隔，10% 玩家有变更）→ 5e4 × 10% × 20KB / 5s ≈ 20MB/s
雪花序列号 4096/ms/节点 → 需按开箱、扫荡等批量产道具 ID 的峰值复核
```

## 12. 关键决策

**持有数据的服务不用 queue group。** 它不保证同玩家同实例，内存态下会双副本互相覆盖。
代价是自建分片认领，换取数据正确性，不可妥协。

**分片数固定 1024。** 变更等价于全量迁移。1024 远大于实例数保证均匀，
又不至于让 etcd 键数、订阅数、ticker 数失控。

**epoch fencing 而不是只靠 lease。** lease 只能检测「我认为我还持有」，
拦不住进程冻结恢复后的滞后写入。fencing 在存储层做最终裁决，开销是一次 Lua 比较。

**网关不按 uid 订阅。** 订阅数会随在线量线性增长。固定 gateID 加路由表，
订阅数恒定，代价是一次查询。

**跨分片走中转层。** 没有跨分片事务，直接互操作崩溃会导致资源消失或翻倍。
中转层换来可审计、可补偿、可幂等。

**nodeSeq 手写而不是自动分配。** 当前单机 compose、实例数个位数。
自动分配约 250 行代码，解决的是尚未出现的问题，还会因为 workerID 可复用
引入跨进程时钟回拨这个新复杂度。代价里唯一致命的撞号已经由启动检查
转成了启动失败，迁移时机见第 2 节。

**匹配池用分片认领而不是选主。** 按 `模式×段位` 分成 128 个桶独占认领。
匹配池不落盘所以跳过 fencing，但「同一个桶只能有一个实例在撮合」仍靠分片独占保证，
否则两个实例会把同一批玩家撮进两个房间。

## 13. 故障行为

| 场景 | 行为 | 恢复时间 | 代码 |
|---|---|---|---|
| Lobby 实例崩溃 | lease 过期，其他实例扫到空闲分片，抬 epoch，从 Redis 加载 | 8~15s | `shard.Claimer.scanAndClaim` |
| 续租失败 | 立即停止处理该分片消息并丢弃内存，不做任何写入 | 立即 | `Claimer.keepAlive` |
| GC 长停顿 | lease 没过期就自愈，过期则接管加 fencing | 视停顿 | |
| 网络分区双 owner | 旧 owner 写入被 Lua 拒绝，丢内存、停服务、告警，绝不重试 | 立即 | `store.Fencer` |
| nodeID 撞号 | 启动检查拒绝启动，日志直接给出冲突方 host/pid | 人工 | `nodeid.Claim` |
| 时钟大幅回拨 | 停止发号加告警，时钟修正后自动恢复 | 待修正 | `idgen.Generator.Next` |
| Gateway 崩溃 | 客户端重连其他网关，残留 session 靠启动清理加 TTL | 重连时间 | `session.CleanGate` |
| NATS 单节点故障 | 客户端库自动重连 | 秒级 | |
| Redis 主故障 | 刷盘失败重新标脏，内存仍是权威副本，服务继续 | 30s 内 | `DirtySet.ReMark` |
| Redis 全丢 | 冷备 RDB 恢复，回滚到备份时点 | 小时级 | |
| flushCh 积压 | 走 default 重新标脏并告警，绝不阻塞 Actor | 立即 | `Flusher.Submit` |
| 单分片过热 | mailbox 积压超阈值持续告警 | 人工 | `Actor.warnBacklog` |

## 14. 目录与测试

```
cmd/                    六个服务的入口，每个十几行
  gateway/ account/ lobby/ room/ match/ chat/ world/
  gameconfctl/          配置表运维工具：dump / lint / rtp

pkg/                    与业务无关的基础设施
  authz/ authn/ payment/     权限分级、登录票据、充值验签
  idp/ account/              渠道校验（唯一打外部网络的地方）、账号绑定表
  rng/ gameconf/             可审计随机源、配置表与内容哈希版本
  ledger/ jackpot/ rg/       流水账本、累积奖池、责任游戏限额
  ident/ idgen/ nodeid/      ID 派生、雪花、nodeID 唯一性检查
  config/ node/              env 装载、启动与下线骨架
  shard/ shardsvc/           分片认领、Actor、服务骨架
  store/ profile/ xtx/       Redis 层、只读摘要、跨分片
  session/ leader/           会话路由、选主
  bus/ subject/ protocol/    NATS 封装、subject 规范、命令号
  metrics/ logx/             指标、结构化日志（zap）

internal/
  account/              账号服：渠道校验、开号、绑定
  slots/                老虎机数学引擎（纯函数，可复算）
  lobby/                玩家对象 + slots/抽卡/充值/GM
  room/ gateway/ match/ chat/ world/

test/harness/           进程内 etcd + NATS + Redis
test/integration/       端到端测试
proto/                  .proto 源文件（生成物在 pkg/pb）
deploy/                 各区服的 env 文件
```

集成测试把 etcd、NATS（含 JetStream）、Redis 全跑在测试进程里，
`make test` 即可，不需要 Docker。

```
pkg/authz        造币类命令绝不能是客户端级、签名绑定 cmd/uid/时效、无密钥签不出
pkg/authn        无效、过期、张冠李戴的登录票据必须被拒
pkg/idp          未注册渠道被拒、错误分级、provider 卡死能被超时掐断、
                 熔断触发与半开探针、脏凭证不该把渠道打熔
pkg/account      并发首登只开一个号、渠道账号不能指向两个 uid、
                 最后一个绑定不能解、解绑要清正反两张表
pkg/payment      未配置时默认拒绝；换单号、换 uid、换商品、改价的回执都要被识破
pkg/rng          同种子可复现、不同种子必不同
pkg/gameconf     版本是内容哈希、非法配置被拦下、热替换失败不影响现有配置
pkg/rg           日投注、日亏损、会话时长、自我排除、跨日重置、玩家限额只能更严
internal/slots   RTP 蒙特卡洛回归、重放确定性、连线规则、免费旋转不参与奖池
pkg/ident        workerID 全服务唯一性、越界溢出会撞车的反证
pkg/idgen        并发唯一性、小回拨自旋、大回拨停止发号、序列号溢出
pkg/store        fencing 拒绝陈旧 owner、L0 幂等、刷盘失败必须回报
pkg/session      顶号原子替换、迟到的解绑不能删新会话、网关重启清理
pkg/xtx          发起方原子写穿、接收方幂等、fencing 时 done 标记回滚
pkg/shard        Actor 串行性、mailbox 满不阻塞、panic 隔离

test/integration
  分片认领        单实例吃满分片空间；两实例不会有同一分片被双持
  故障接管        owner 断连后另一实例接管，epoch 严格抬高
  脑裂拦截        真实接管后旧 owner 的写入被拒，数据未被回滚
  nodeID 检查     撞号拒绝启动；僵尸记录可接管；时钟倒退拒绝接管
  端到端 Lobby    登录 → 改数据 → L1 刷盘 → Redis + profile 摘要
  L0 幂等         同一订单号重复提交返回首次结果，余额不翻倍
  跨分片转移      扣除与入账总量守恒，失败时整体回滚
  分片交接        扩容后收敛到公平份额，交接过程中数据不丢
  优雅下线        关掉定时刷盘后，下线仍把内存改动全部落盘
  网关            TCP 端到端登录、路由、顶号、未登录拒绝
  全链路          匹配 → 建房 → 开打 → 结算 → 发奖回到 Lobby
  世界服          选主后唯一实例对外服务
  客户端越权      add_currency / add_item / apply_* / gm_* 一律被拒，且确认没到账
  伪造签名        错误密钥、无签名、缺 operator 的内部命令都被拒
  登录认证        空票据、乱填、别人的票据都被拒
  spin 原子性     扣款、派彩、流水三者一致，流水求和 == 当前余额
  spin 幂等       重发同一次 spin 不重复扣费，返回首次结果
  未完成回合      免费旋转中途重启进程，重连后取回并打完，期间不扣费
  责任游戏        触发日限额后拒绝下注；自我排除期内拒绝登录与下注
  奖池            并发注入总额守恒；中奖清零只发生一次且必留派彩记录
  抽卡保底        保底周期内必出最高稀有度；十连必出高稀有度；消耗与产出都有流水
  充值            未知商品、无效回执、未授权渠道都被拒，且一分钱不到账
  GM              补单幂等、留下带操作者的流水、缺 operator 直接拒绝
  渠道登录        首登开号、同账号回到同一 uid、不同账号是不同号
  登录降级        账号服下线后，带票据的重连照常成功
  渠道失败分级    凭证不对 / 渠道故障 / 渠道没配 三个不同错误码
  账号服不可达    报「服务不可用」而不是「认证失败」
  绑定            重复绑定幂等、抢别人的渠道账号被拒、解绑最后一个被拒
```

## 15. 已知缺口与待拍板

**还没做的：**

- 账本长期存储。当前只有 Redis Stream 加导出接口，没有实际的导出目标。
- group commit。每笔 L0 仍是一次独立的 Redis 往返。10 万在线、3 秒一次 spin
  约 33k 次/秒同步 Lua，单 Redis 会吃紧；同分片攒批可提升一个数量级，
  代价是几毫秒延迟。`maxWaitersPerPlayer = 64` 要跟攒批一起调。
- TLS。网关仍是明文 TCP，需在部署层加 TLS 终结。
- 多币种与精度。金额已统一 `int64` 最小单位，但汇率、分币种限额还没做。
- NATS 账号隔离。内部命令签名已能挡住伪造，但生产仍应给 NATS 配账号，
  限制谁能往 `req.lobby.*`、`req.account.*` 发消息。账号服信任 Envelope 里的 uid
  是网关按会话填的，这条信任目前只靠内网。
- 真实渠道 provider。`pkg/idp` 的框架、超时、熔断、错误分级和沙箱都在，
  但只有沙箱一个实现。接具体渠道时实现 `idp.Provider` 并在 `internal/account`
  里注册，HTTP 那层用 `idp.NewHTTPDoer`，它已经把状态码到错误类型的映射定死了。
- 设备指纹与风控。账号服是放这些东西的地方，现在只透传了 `device_id`，没用它做判断。

**代码留了口子但没替业务做决定的：**

1. Lobby 与 Room 是否合并部署。目前分开，两套独立分片空间。
   合并的话改动集中在分片键选择上（按 roomID 而不是 uid）。
   这项会反向影响分片键设计，建议优先拍板。
2. 雪花的使用范围。现在 uid、道具实例 ID、房间 ID、订单 txid、connID 都用它，
   峰值需求要压测校准，`idgen` 已有 `seq_overflow_total` 指标。
3. 是否有跨服需求。现在按 `(serverID, uid)` 二元组区分，serverID 不进 workerID。
   将来做跨服战场或全服排行，ID 要全局唯一，后期改代价很大。
4. 单玩家内存实测值。`game_biz_players_resident` 与 `game_biz_mem_alloc_bytes` 已在，
   压测时直接读。
5. 客户端重试策略。网关已实现有限次重投兜底，但客户端自己也应实现超时重试。
6. 服务名是否改回 player。数据 key 用的是 `p:` 不是 `lobby:`，改名不用迁数据。

## 16. 代码约定

### 注释

导出符号按 Go 惯例给一行摘要，格式 `// 名字 描述`。不用系动词，不加句号，
一行讲得完就一行，讲不完空一行写正文：

```go
// Package ledger 资金流水账本
// Entry 一条流水
// New 构造发号器，workerID 来自 ident.Identity.WorkerID()
```

正文内部的分句保留句号，注释块的最后一行不加。

**不用破折号解释。** 后半句解释前半句就用逗号或句号接下去，不写 `——`：

```go
// 任何一步不过就退出，不降级 —— 降级就是在生产制造重复 ID 或者丢档   ← 别这样
// 任何一步不过就退出，而非降级，降级会造成生产环境重复 ID 或者数据丢失   ← 这样
```

**写完回头砍一遍。** 四种东西砍掉。

铺垫和自我评价：「这段是整个服务里最要小心的地方」「这正是……的原因」，
读的人自己会判断重不重要。

同义反复，后半句只是把前半句反过来说，或者推不出新东西：

```go
// 只有明确返回 1 才算成功，其余一律当失败              ← 后半句是废话
// 和分片认领是同一套东西，只是分片空间只有一格，
// 所以阈值取舍也一样：lease 短，丢租就立刻停手          ← 说了「同一套」就不必展开
```

列表项里的理由，步骤列表只留动作：

```go
// handleSpin 转一次，顺序别改
//
//  1. 检查
//  2. 在副本上算结果
//  3. 余额、回合、限额、流水一起落库
//  4. 提交成功之后才改内存并回包
```

自我辩解：「唯一可以放心用的」「这跟 X 不冲突」「所以不违反 Y」删掉，
规则冲不冲突读的人自己会查。

顺手清掉的词：层面、方面、进行、相关、的话、而言、来说、所谓。

**不必要就不写。** 从名字和签名看得出来的不要再写一遍，
构造函数、纯 getter、只为满足接口的空实现一律不加注释：

```go
func New() *Service { return &Service{} }
func (s *Service) Name() string { return "chat" }
func (k *Keys) Rank(name string) string { return "rank:" + name }
```

留注释的理由只有一个，有代码之外的信息：

```go
// Drain 优雅关闭，把 pending 处理完再断        和 Close 的区别
// Shuffle 原地洗牌                             会改入参
// Registry 返回内部注册表，测试用               限定用途
// Clear 清掉某实体的全部脏标记，卸载时用         什么时候该调
```

导出符号缺注释 `go vet` 不管，别为了凑 godoc 写废话。

**用词平实：**

| 别写 | 写 |
|---|---|
| 启动自检 | 启动检查 |
| 丢档、清档 | 数据丢失、清空存档 |
| 把滞留时长洗白 | 把滞留时长重置 |
| 会把 goroutine 拖死 | 会阻塞 goroutine |
| 此处必须保证原子性，否则将导致数据不一致 | 这两步得一起成，不然会出现扣了钱没流水 |
| 该操作是幂等的 | 重复调不会发两份 |
| 需要注意的是，本方法不支持并发调用 | 别并发调，会打乱随机流的顺序 |

**该写的：** 代码看不出来的东西，为什么这么写、改坏了会怎样、调用方要注意什么。

```go
// 只有明确返回 1 才算成功
//
// 连接出错时 go-redis 只在 Exec 上返回错误，不给每条命令挂 err，
// Result() 会是 (nil, nil)。当成功处理就是静默丢弃
```

Lua 脚本的 KEYS / ARGV 表、生命周期顺序表这类是参考资料，长一点没关系。

**不该写的：** 外部文档的章节号（引用会随文档改版失效）、复述代码
（`// 加锁` 写在 `mu.Lock()` 上面等于没写）、加粗和排比。

中文注释，专有名词和标识符保持英文（`epoch`、`fencing`、`slot`、`goroutine`、`paytable`）。
`pkg/pb/` 是 protoc 生成的，别手改，那里的注释改 `proto/*.proto`。

### 破坏了不会编译失败的约束

下面这些只能靠人守，破坏它们未必测试失败，但会在生产上表现为丢钱、
数据丢失或者对不上账。

**钱：** 一律 `int64` 最小单位不用浮点。每一笔资金变动都要有流水，
流水必须和余额在同一个 Lua 里写，资金路径走 `Shard.commit` 不走 `writeThrough`。
资金操作要有幂等键，订单号防重复，签名防伪造，两个不能互相替代。
下注额、到账数量只能来自服务端配置。

**命令权限：** 新增命令必须在 `pkg/protocol` 里定级。往 `clientCmds` 白名单里
加东西之前问一句：这个命令能不能让玩家自己决定给自己加多少钱。
网关部署时不给 `INTERNAL_SECRET`。

**分片与内存：** Actor goroutine 里不能有阻塞 IO。改动先算在副本上，
提交成功之后才换进内存。被 fencing 拒了就丢内存、停服务，不要重试。
刷盘失败重新标脏，绝不丢弃。持有玩家数据的服务不能用 queue group。

**Redis key：** 要进同一个 Lua 的 key 必须同 hash tag。加新 key 时想一下
它要不要和别的 key 一起原子写，要就对齐 tag。

**slots：** 改轴带或赔付表之后必须跑 `make rtp`。`internal/slots` 保持纯函数。
随机数的消费顺序是复算契约的一部分，别随手调整。配置版本是内容哈希，
改一个数字就变，每个回合落盘时记的就是它。

### 提交前

```bash
make fmt vet test     # 全绿再提
make rtp              # 只在动了 slots 数学模型时跑
make conf-lint        # 只在动了配置表时跑
```

自查五条：

1. 新增的注释里有没有章节号、评审编号、加粗
2. 摘要行有没有多余的「是」和句号，块尾有没有多余的句号
3. 有没有用 `——` 引出解释
4. 有没有写从名字就能看出来的废话
5. 碰了钱、分片所有权或者随机数吗，碰了就回头看上一节
