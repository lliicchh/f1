# 游戏服务器架构设计文档

| 版本 | v0.3 |
|---|---|
| 技术栈 | Go / NATS / Redis / etcd / Docker Compose |
| 本版变更 | 新增 ID 分配完整方案（方案 B）；明确 Lobby 定位；新增 profile 只读摘要；新增部署形态 |

---

## 1. 设计取向

两个基础判断，后续设计全部由此推导：

**一、进程内存是数据的权威副本，Redis 是它的持久化投影。**
任何时刻一份玩家数据只能被一个进程持有。这是强约束，不是性能优化。

**二、分片（shard）是一切的调度单位。**
玩家、房间统一映射到分片，分片被实例独占认领。资源分配、故障转移、扩缩容、刷盘都以分片为粒度。

**非目标**：本期不引入 MySQL；不做跨服玩法；不提供跨分片强一致事务。

---

## 2. 整体架构

```
client ─┬─ Gateway ×2 ─┐
        └─ ...         │
                 ┌─────┴─────┐
                 │NATS Cluster│
                 └─────┬─────┘
        ┌───────┬──────┼──────┬───────┐
      Lobby   Room   Match  Chat   World
      ×3分片  ×2分片   主备   queue   主备
        └───────┴──────┴──────┴───────┘
                       │
             ┌─────────┴─────────┐
           Redis              etcd
      玩家数据/session      分片/nodeID
```

### 2.1 服务职责

| 服务 | 职责 | 实例模型 |
|---|---|---|
| **Gateway** | 连接管理、编解码、下行推送 | 无数据，LB 轮询 |
| **Lobby** | 玩家对象常驻内存 + 非战斗玩法逻辑 | **分片独占** |
| **Room** | 房间/战斗状态 | **分片独占** |
| **Match** | 匹配池 | 按 `模式×段位` 分片，或选主 |
| **Chat** | 消息中继，不存数据 | queue group |
| **World** | 全局唯一逻辑（世界 BOSS、活动调度） | 选主主备 |

### 2.2 关于 Lobby 的定位（重要）

**Lobby 是玩家对象的容器，同时跑玩家的业务逻辑，二者在同一进程内。** 背包、任务、邮件、货币、社交这些模块的数据和逻辑都在这里，不存在独立的"玩家数据服务"。

**明确不采用「数据服务 + 逻辑服务」拆分：**

```
❌ Lobby 逻辑服 ──网络──► Player 数据服 ──► 内存
   每次读背包都要一次 RPC，内存态的好处全部抵消，
   且数据服仍需分片独占，复杂度只是往后挪了一层

✅ Lobby 进程 ──► 分片 goroutine ──► 内存数据
   函数调用，纳秒级；无锁；业务代码接近单机写法
```

内存态设计的全部价值是**逻辑在数据所在的地方就地执行**。拆成两个进程后每次访问都要过网络和序列化，还不如直接用 Redis 当数据源、逻辑服无状态——那是另一套成立的架构，但不是本方案。**两套可以选，不能混**，混的结果是既有内存态的复杂度，又有 DB 方案的延迟。

### 2.3 核心约束

**持有玩家数据的服务（Lobby / Room）不得使用 queue group。**

Queue group 只保证负载均衡，不保证同一玩家路由到同一实例。在内存态数据下这会直接导致双副本互相覆盖刷盘。

---

## 3. ID 分配

### 3.1 三层结构

| 层 | 例子 | 来源 | 说明 |
|---|---|---|---|
| 区服 ID `serverID` | `1` | **手动配置** | 运营概念，程序无法推断 |
| 服务类型 `svcType` | `2`(lobby) | 代码枚举 | 编译期常量 |
| 进程序号 `nodeSeq` | `1` | **手动配置** | 每服务从 1 开始 |

派生出两个标识：

```go
nodeID   = fmt.Sprintf("s%d-%s-%d", serverID, svcName, nodeSeq)  // s1-lobby-1
workerID = svcType<<7 | nodeSeq                                   // 雪花用
```

### 3.2 区服 ID：手动配置

一个「区服」= 一整套独立进程 + 独立 Redis + 独立 etcd 前缀，区服之间数据完全隔离。配置按区服分组：

```bash
# s1.env
SERVER_ID=1
ETCD_PREFIX=/game/s1
REDIS_ADDR=redis:6379
NATS_URL=nats://nats:4222
```

一区内所有进程（gateway / lobby / room）共用同一个 `SERVER_ID`。开新区复制一份改号即可。

**serverID 不进 workerID。** 本方案分区分服、数据隔离，uid 只需区服内唯一——一区和二区各有 uid=10001 完全没问题，它们在不同 Redis 里永不相遇。把 serverID 塞进 workerID 会白白吃掉几个 bit（4 bit 只够 16 个区，远远不够）。真需要全局唯一时用 `(serverID, uid)` 二元组。

### 3.3 进程序号：手动配置 + 启动自检（方案 B）

**决策：现阶段采用手写序号，不做自动分配。** 理由见 §12 D6。

每个服务从 1 开始独立编号，唯一性由代码的位拼接保证，**不需要人为划分号段**：

```yaml
gateway-1:  NODE_SEQ=1   →  workerID = 1<<7 | 1 = 129
lobby-1:    NODE_SEQ=1   →  workerID = 2<<7 | 1 = 257
lobby-2:    NODE_SEQ=2   →  workerID = 2<<7 | 2 = 258
room-1:     NODE_SEQ=1   →  workerID = 3<<7 | 1 = 385
```

```go
const (
    SvcGateway = 1
    SvcLobby   = 2
    SvcRoom    = 3
    SvcMatch   = 4
    SvcChat    = 5
    SvcWorld   = 6
)   // 最多 8 种服务（3 bit）
```

**为什么必须拼接而不是直接用 nodeSeq**：workerID 必须在所有服务之间唯一。若每个服务都从 1 开始且直接当 workerID，`gateway-1`、`lobby-1`、`room-1` 的 workerID 都是 1，同一毫秒会生成完全相同的雪花 ID。拼接把「保证不重叠」这件事从人的纪律变成了编译期常量。

### 3.4 启动校验（两道，缺一不可）

**第一道 · 越界校验**

```go
if nodeSeq < 1 || nodeSeq >= 128 {
    log.Fatal("NODE_SEQ 越界，必须在 [1,127]")
}
```

`nodeSeq >= 128` 会溢出到相邻服务的号段，静默产生重复 workerID。这道校验漏了，位拼接的安全性就没了。

**第二道 · etcd 唯一性自检**

手写序号唯一的致命风险是撞号：复制 compose 配置忘改数字，两个进程用同一个 nodeID。此时 etcd 会认为是同一个 owner 在续租，续租不会失败，两进程同时持有同一分片、**epoch 还相同，fencing 也拦不住**。这是全套设计里唯一没有兜底的洞，必须在启动时堵住。

```
key = {ETCD_PREFIX}/node/{svcName}/{nodeSeq}
val = {"pid":..., "host":..., "addr":..., "beat_at":...}

CAS 抢占：
  - key 不存在                    → 成功
  - key 存在但 beat_at 超时 10min → 成功（接管僵尸记录）
  - key 存在且心跳新鲜             → 打印冲突方 host/pid，拒绝启动
```

- 抢占成功后启动心跳协程，30s 更新一次 `beat_at`
- **抢不到必须 panic 退出**，绝不能降级用随机数或 hostname 哈希兜底——那就是在生产重复 ID
- 进程正常退出时主动删除该 key

风险因此从「静默数据损坏」降为「发布时启动失败」，后者改配置重启即可。

### 3.5 雪花位分配

```
1 符号 | 41 时间戳(ms) | 10 workerID | 12 序列号
                69年      1024 节点     4096/ms/节点

workerID 10 bit = svcType(3) | nodeSeq(7)
                  8 种服务      每服务 127 实例
```

### 3.6 时钟回拨

因为序号固定绑定容器、**workerID 不会被其他进程复用**，跨进程回拨问题不存在，只需处理单进程内：

```go
if now < lastTimestamp {
    diff := lastTimestamp - now
    if diff <= 10 {
        // NTP 微调，自旋等待追平
    } else {
        // 停止发号 + 告警，绝不能继续
    }
}
```

大回拨绝不容忍继续发号，宁可该进程停止服务。

**运维配合项：NTP 必须用 slew 模式**（chrony 配 `maxslewrate`，或 ntpd 加 `-x`），让时钟平滑漂移而非跳变。默认配置在偏差大时会直接 step，那是灾难。这条要提前跟运维确认。

### 3.7 迁移到自动分配的时机

出现以下任一情况时切换（届时序号从 etcd 池动态抢占）：

- 上第二台物理机（手工维护号段开始容易错）
- 实例数超过十个左右
- 上 Swarm / K8s，编排层本身要动态调度

迁移是平滑的：§3.4 的 etcd 注册结构与自动分配完全一致，自动分配只是在前面多一个「扫描空位」步骤。**但需注意**：一旦 workerID 可被复用，就必须补上跨进程时钟回拨防护（在 etcd key 中维护 `last_ts`，新进程抢到后校验本机时间不早于它）。

---

## 4. 分片模型

### 4.1 划分

```
ShardCount = 1024        // 常量，一经确定永不变更

lobbyShard(uid)   = uid % 1024
roomShard(roomID) = roomID % 1024
```

Lobby 与 Room 使用独立分片空间。1024 远大于预期实例数，保证均匀分布；分片数变更等价于全量迁移，一次定死。

### 4.2 认领

etcd 每分片一个键，带 lease（TTL 8s，续租间隔 2.5s）：

```
/game/s1/shard/lobby/0007 → {"node":"s1-lobby-1", "epoch":8842}
```

- 启动时扫描空闲分片，etcd 事务（`CreateRevision == 0`）CAS 抢占
- `epoch` 取事务返回的 **revision**，天然全局单调递增，无需额外分配器
- 续租失败 → 立即停止处理该分片消息
- lease 过期 → 键自动删除 → 其他实例接管

**分片 lease 与 nodeID 心跳的阈值取舍是相反的**，不要混用同一套：

| | 超时阈值 | 回收慢的代价 | 回收快的代价 |
|---|---|---|---|
| 分片 | 8s | 玩家不可用 | 误接管 → 有 fencing 兜底 |
| nodeID | 10min | 浪费一个槽位 | **误判冲突 → 拒绝启动** |

### 4.3 订阅

```go
nc.Subscribe("req.lobby.7.>", handler)   // 不带 queue group
```

**不能加 queue group**。分片独占性正是靠「同一 subject 只有一个订阅者」保证的，加了反而允许多实例同时订阅，破坏独占语义。

### 4.4 Actor 执行模型

每个分片一条 goroutine，独占该分片全部内存数据：

```
Shard {
    id      uint32
    epoch   int64
    mbox    chan *Msg           // 有容量上限
    players map[uid]*Player     // 仅本 goroutine 访问
    dirty   map[uid]DirtyFlag
}
```

- 无锁、无竞态，业务代码是纯同步的
- 同一玩家请求天然串行，不存在并发扣道具类问题
- **回调内禁止任何阻塞操作**。刷盘的序列化在 Actor 内做（读内存必须如此），实际 IO 甩给独立 goroutine

mailbox 需监控积压，超阈值告警。

---

## 5. NATS Subject 规范

### 5.1 命名

| 前缀 | 语义 | 投递保证 |
|---|---|---|
| `req.` | 请求-应答 | Core NATS |
| `evt.` | 事件广播 | Core NATS |
| `push.` | 下行推送 | Core NATS |
| `job.` | 必达任务 | JetStream |

```
req.lobby.{shard}.{cmd}      玩家逻辑，shard = uid % 1024
req.room.{shard}.{cmd}       房间逻辑
req.match.{mode}.{tier}      匹配

push.gate.{gateID}           定向推送
push.broadcast               全服广播

evt.player.{uid}.{event}     玩家事件
job.transfer.{txid}          跨分片资源转移
job.mail.send                发信

ctl.node.{nodeID}.handoff    分片交接
ctl.node.{nodeID}.shutdown   优雅下线
```

### 5.2 Core NATS 与 JetStream

**默认 Core NATS。** 状态同步类消息丢了由下一帧覆盖，用 JetStream 是纯浪费。

**必须走 JetStream 或写穿**：跨分片转移、发奖、发信、充值到账——任何「丢了会导致资产不一致」的操作。通过 `job.` 前缀在命名上强制体现，降低误用。

### 5.3 Envelope

```protobuf
message Envelope {
  uint32 cmd       = 1;
  uint64 uid       = 2;
  string trace_id  = 3;   // 全链路追踪
  string from_node = 4;   // 发送方 nodeID
  int64  ts_ms     = 5;
  uint32 seq       = 6;   // 应答匹配
  bytes  body      = 7;
}
```

`trace_id` 和 `from_node` 是排查问题的唯一抓手，从第一天就带上。

### 5.4 使用注意

| 项 | 应对 |
|---|---|
| Slow consumer（pending 超限静默丢弃） | **必须注册 ErrorHandler 并告警** |
| 回调单 goroutine 串行 | 回调内只投递入 mailbox，不做业务 |
| Payload 默认 1MB 上限 | 大包分片或调 `max_payload` |
| 无订阅者直接丢弃 | 分片交接窗口靠客户端重试兜底 |

---

## 6. 数据与刷盘

### 6.1 Redis Key

按模块拆分，避免改一个字段重写整个玩家：

```
p:{1001}:base       等级、经验、货币     高频、小
p:{1001}:bag        背包                中频、大
p:{1001}:quest      任务                中频
p:{1001}:social     好友、公会           低频

profile:{1001}      只读摘要（见 6.5）
room:{shard}:{roomID}        房间快照
shard:lobby:{shard}:epoch    分片 epoch
session:{uid}                → {gateID, connID}
tx:{txid}                    跨分片转移记录
```

`{1001}` 是 Redis Cluster hash tag，保证同一玩家所有 key 同 slot，从而支持单玩家 Lua 和 pipeline。

数据 key 用 `p:` 而非 `lobby:`——**key 跟着数据实体走，不跟服务名走**，将来 Lobby 拆分或改名时不用迁移数据。

序列化用 protobuf，**只增字段不改 tag**，废弃字段用 `reserved`——滚动发版时新旧版本进程会同时读写同一批数据。

### 6.2 分级刷盘

| 级别 | 数据 | 策略 | 最大丢失 |
|---|---|---|---|
| L0 写穿 | 充值、开箱、交易 | 同步落 Redis 成功后才改内存回包 | 0 |
| L1 高频 | 货币、等级、背包 | 脏标记 + 3~5s | 5s |
| L2 低频 | 设置、社交 | 脏标记 + 60s | 60s |
| L3 临时 | 战斗中间态 | 不刷盘 | 全部 |

L0 必须幂等：客户端带唯一订单号，重复提交返回首次结果。

### 6.3 刷盘流程

```
Actor goroutine                      IO pool
   ├─ ticker 触发
   ├─ 遍历 dirty 序列化（内存读，必须在 Actor 内）
   ├─ 清 dirty
   ├─ batch → flushCh ──────────────► pipeline 写 Redis
   └─ 立即返回                          + Lua 校验 epoch
                                       失败 → 回报重新标脏
```

- **flushCh 满时走 default 重新标脏并告警**，绝不阻塞 Actor。积压说明 Redis 已扛不住，阻塞会让故障扩散成全服卡死
- **刷盘时刻打散**：ticker 初始偏移 `shardID % interval`，避免 1024 分片同秒刷造成 Redis 尖峰
- **刷盘失败重新标脏，绝不丢弃**。内存仍是权威副本，Redis 不可用期间服务可继续

### 6.4 Redis 配置（关键）

Redis 是唯一持久化后端，配错等于丢全服数据：

| 配置 | 要求 |
|---|---|
| `maxmemory-policy` | **必须 `noeviction`**。设 LRU 会在内存满时静默淘汰玩家数据 |
| `appendonly` | `yes` + `appendfsync everysec` |
| 部署 | 主从 + 哨兵，或 Cluster |
| 冷备 | 定期导出 RDB。业务 bug 写坏数据时唯一回滚手段 |

### 6.5 profile 只读摘要（CQRS 读模型）

**问题**：查看好友资料、排行榜、给离线玩家发邮件时，需要读一个不在本分片、甚至不在线的玩家数据。为看一眼头像就加载整个玩家对象进内存是不可接受的。

**方案**：owner 分片在数据变更时顺带写一份轻量快照：

```
profile:{uid} → {nick, avatar, level, guildID, power, lastLogin}
```

- **只读、允许滞后几秒、任何进程可直接读 Redis**，不经过 owner，不唤醒玩家对象
- 适用：查看资料、排行榜、好友列表、公会成员列表
- **修改离线玩家必须走 `job.` 中转层**（见 §8），由 owner 分片处理，或写入待领取队列等上线消费

这是 CQRS：写模型在内存（owner 独占强一致），读模型在 Redis（人人可读，最终一致）。

---

## 7. 脑裂防护（Epoch Fencing）

### 7.1 风险

```
T1  节点 A 持有分片 7
T2  A 发生 GC 长停顿 / 网络分区
T3  lease 过期，分片释放
T4  节点 B 接管，从 Redis 加载，玩家产生新数据
T5  A 恢复，尚不知已失去所有权，将旧内存刷入 Redis
    → 覆盖 B 的新数据，玩家进度回滚
```

内存态数据没有 DB 行级 version 兜底，必须在设计层解决。

### 7.2 双重防护

**软保护 —— lease 自检**：续租失败立即停止处理消息和刷盘。覆盖大部分情况，但拦不住进程完全冻结后恢复。

**硬保护 —— epoch fencing**：所有刷盘经 Lua，先校验 epoch：

```lua
-- KEYS[1]=shard:lobby:7:epoch  ARGV[1]=调用方 epoch
if tonumber(redis.call('GET', KEYS[1]) or 0) > tonumber(ARGV[1]) then
    return 0        -- 调用方已过期，拒绝写入
end
-- 执行写入
return 1
```

接管方在**加载数据之前**先抬高 epoch，此后旧 owner 任何写入被拒。

**旧 owner 收到拒绝 → 丢弃该分片内存、停止服务、告警。绝不重试。** 此时内存已过期，重试只会加重损坏。

**注意 fencing 的失效前提**：若两个进程 nodeID 相同（撞号），它们持有的 epoch 也相同，fencing 无法区分。这正是 §3.4 启动自检必须存在的原因。

### 7.3 一致性边界

- 单分片内：**强一致**（单 goroutine 串行）
- 跨分片：**最终一致**
- 崩溃恢复：L0 零丢失，L1/L2 按刷盘间隔回退

---

## 8. 跨分片操作

**禁止分片之间直接操作对方内存。** 无跨分片事务，直接互操作在任一侧崩溃时会导致资源蒸发或复制，且事后无法审计。

```
A 分片                      Redis                   B 分片
  ├─ 扣道具（内存）
  ├─ 写穿 tx:{txid} ────► {from,to,item,PENDING}
  ├─ job.transfer.{txid} ───────────────────────►  ├─ SET tx:{txid}:done NX（幂等）
                                                   ├─ 加道具（内存）
                                                   └─ 标记 COMPLETED
```

- 发起方**先扣除并写穿**，确保资源不会凭空增加
- 接收方**用 txid 幂等去重**
- 后台扫描器兜底，定期重投超时 PENDING

邮件附件、交易、公会仓库、组队奖励、给离线玩家发奖全部走这一套。

---

## 9. 网关与会话

### 9.1 路由表

```
session:{uid} → {gateID, connID}   (Redis，TTL + 心跳续期)
```

网关只订两个 subject，订阅数与在线人数无关：

```
push.gate.{gateID}      定向推送
push.broadcast          全服广播
```

**为什么不按 uid 订阅**：订阅数会等于在线人数，NATS 集群内 interest 传播开销随在线量线性增长。固定 gateID + Redis 查表，订阅数恒定，代价是多一次查询（可本地缓存，注意顶号时失效）。

### 9.2 顶号

Lua 脚本原子替换 session，取出旧 gateID 后发 KICK，旧连接所在 Lobby 分片保存并卸载上下文。

### 9.3 广播

- **全服公告**：走 `push.broadcast`
- **房间/公会广播**：**不走 NATS 广播**。由 Room 分片批量查路由表，按 gateID 聚合，每个网关发一条带多个 uid 的包

### 9.4 网关重启

session 变脏数据。两种清理都做：启动时清理自己 gateID 名下所有 session；session 设 TTL 由心跳续期。

---

## 10. 生命周期

### 10.1 玩家数据

- **加载**：懒加载。登录时由所属 Lobby 分片 pipeline 读回全部模块 key
- **卸载**：下线后保留 5~10 分钟（断线重连、离线结算、好友查看），超时后最终刷盘并删除。这段冗余计入内存估算

### 10.2 分片交接（Handoff）

靠 lease 自然过期有 8~10s 不可用窗口，主动交接可压到百毫秒：

```
1. 新节点发起 ctl.node.{old}.handoff
2. 旧节点停止处理该分片新消息
3. 旧节点全量刷盘 → 释放 etcd 锁 → 回复 READY
4. 新节点抢占，拿新 revision 作为 epoch
5. 新节点抬高 Redis epoch → 加载数据 → 订阅 subject → 服务
```

交接窗口内的请求会被 Core NATS 丢弃，**客户端必须实现超时重试**（或网关做 500ms 缓冲重投）。

### 10.3 优雅下线

顺序不可颠倒：

```
1. 摘除注册 / 发起 handoff   → 停止接新请求
2. nc.Drain()               → 处理完 pending 后关连接
3. 全量刷盘
4. 确认刷盘成功（epoch 校验通过）
5. 删除 etcd 中的 nodeID 注册键
6. 退出
```

网关额外多一步：**先向客户端发「服务器维护，请重连」**，让客户端主动重连到其他网关，体验远好于直接断开。

---

## 11. 部署形态（Docker Compose）

### 11.1 编排文件

所有区服共用一份 yml，靠 env 文件和 project 名区分：

```yaml
x-common: &common
  env_file: [./${ENV_FILE}]
  depends_on: [etcd, nats, redis]
  stop_grace_period: 60s
  restart: unless-stopped

x-lobby: &lobby
  <<: *common
  image: game/lobby:${TAG}

services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.15
    volumes: [etcd-data:/etcd-data]

  nats:
    image: nats:2.10-alpine
    command: ["-js", "-m", "8222"]

  redis:
    image: redis:7-alpine
    command: >
      redis-server --appendonly yes
                   --appendfsync everysec
                   --maxmemory-policy noeviction
    volumes: [redis-data:/data]

  gateway-1:
    <<: *common
    image: game/gateway:${TAG}
    environment: [NODE_SEQ=1]
    ports: ["7001:7000"]
  gateway-2:
    <<: *common
    image: game/gateway:${TAG}
    environment: [NODE_SEQ=2]
    ports: ["7002:7000"]

  lobby-1:
    <<: *lobby
    environment: [NODE_SEQ=1]
  lobby-2:
    <<: *lobby
    environment: [NODE_SEQ=2]
  lobby-3:
    <<: *lobby
    environment: [NODE_SEQ=3]

  room-1:
    <<: *common
    image: game/room:${TAG}
    environment: [NODE_SEQ=1]
  room-2:
    <<: *common
    image: game/room:${TAG}
    environment: [NODE_SEQ=2]

volumes:
  etcd-data:
  redis-data:
```

```bash
ENV_FILE=s1.env TAG=v1.2.0 docker compose -p game-s1 up -d
ENV_FILE=s2.env TAG=v1.2.0 docker compose -p game-s2 up -d
```

### 11.2 要点

- **服务名与 NODE_SEQ 对齐**（`lobby-2` 配 `NODE_SEQ=2`），看日志不需要做映射
- **不使用 `deploy.replicas`**，因为副本之间无法携带不同的 `NODE_SEQ`。扩容时手工增加一个 service 块（配合 YAML 锚点，只需三行）
- **Gateway 需固定端口**（要被外部访问 + LB 配置），本来就得展开写
- **`stop_grace_period: 60s`** 必须设置。默认 10s 会在优雅下线刷盘途中 SIGKILL，按实际分片数和数据量调整
- 使用 `docker compose`（v2），v1 已 EOL

---

## 12. 关键决策

**D1 · 持有数据的服务不用 queue group**
Queue group 不保证同玩家同实例，内存态下会双副本互相覆盖。代价是自建分片认领，换取数据正确性——不可妥协。

**D2 · 分片数固定 1024**
变更等价于全量迁移。1024 远大于实例数保证均匀，又不至于让 etcd 键数、订阅数、ticker 数失控。

**D3 · epoch fencing 而非只靠 lease**
Lease 只能检测「我认为我还持有」，拦不住进程冻结恢复后的滞后写入。Fencing 在存储层做最终裁决。开销是一次 Lua 比较，可忽略。

**D4 · 网关不按 uid 订阅**
订阅数会随在线量线性增长。固定 gateID + 路由表，订阅数恒定，代价是一次查询。

**D5 · 跨分片走中转层**
无跨分片事务，直接互操作崩溃会导致资源蒸发或复制。中转层换来可审计、可补偿、可幂等。

**D6 · nodeSeq 手写而非自动分配**
当前单机 compose、实例数个位数。自动分配约 250 行代码（扫描/抢占/心跳/回收/`last_ts` 校验），解决的是尚未出现的问题，且因 workerID 可复用而引入跨进程时钟回拨这一新复杂度——为自动化去处理 workerID 复用，而复用本就是自动化带来的。不划算。

代价（compose 需展开写、多机需人工分号段、撞号风险）中，唯一致命的撞号已由启动自检（§3.4）转为启动失败。迁移时机与路径见 §3.7。

**D7 · workerID 用位拼接而非人工号段偏移**
若 nodeSeq 直接当 workerID，各服务从 1 开始会互撞，只能靠约定「lobby 从 100 起、room 从 600 起」错开——依赖人的纪律。位拼接把同一件事变成编译期常量，同时让 compose 里每个服务都能从 1 开始编号。代价是多一层映射和一道越界校验。

---

## 13. 故障处理

| 场景 | 处理 | 恢复时间 |
|---|---|---|
| Lobby 实例崩溃 | lease 过期后其他实例接管，从 Redis 加载 | 8~15s |
| GC 长停顿 | lease 未过期则自愈；过期则接管 + fencing | 视停顿 |
| 网络分区双 owner | fencing 拒绝旧 owner 写入，旧 owner 丢弃内存自杀 | 立即 |
| **nodeID 撞号** | **启动自检拒绝启动，改配置重启** | 人工 |
| 时钟大幅回拨 | 停止发号 + 告警，该进程不可用 | 待时钟修正 |
| Gateway 崩溃 | 客户端重连其他网关，session TTL 清理 | 客户端重连时间 |
| NATS 单节点故障 | 客户端库自动重连 | 秒级 |
| Redis 主故障 | 哨兵切换；刷盘失败重新标脏，服务继续 | 30s 内 |
| Redis 全丢 | 冷备 RDB 恢复，回滚到备份时点 | 小时级 |
| 单分片过热 | mailbox 积压告警，人工介入 | — |

---

## 14. 监控指标

- **分片**：mailbox 长度、处理延迟 P99、owner 变更次数、**epoch 校验失败次数（非零即告警）**
- **刷盘**：耗时、失败次数、flushCh 积压次数、脏数据滞留时长 P99（真实丢失窗口）
- **NATS**：**slow consumer 次数（必须告警）**、重连次数、各 subject 速率
- **ID**：**时钟回拨触发次数（非零即告警）**、序列号溢出次数
- **业务**：在线人数、实例内存、tx PENDING 超时数

---

## 15. 容量估算（待压测校准）

```
单玩家内存 ≈ 200KB（含 map/slice 开销）
5 万在线 + 20% 卸载冗余 → 约 12GB
  → 3 个 Lobby 实例需 ~4GB/实例；实例数按实测调整

L1 刷盘带宽（5s 间隔，10% 玩家有变更）
  → 5e4 × 10% × 20KB / 5s ≈ 20MB/s

雪花序列号：4096/ms/节点
  → 需评估开箱、扫荡等批量产出道具 ID 的峰值
```

---

## 16. 待定事项

1. **Lobby 与 Room 是否合并部署**。分开清晰但一次战斗结算可能十几次跨进程调用；重战斗轻养成的玩法应考虑合并。**此项会反向影响分片键设计（若合并，应按房间而非 uid 分片），建议优先拍板**
2. **雪花的使用范围**。只生成 uid，还是道具实例 ID、订单号也用？影响序列号位宽评估
3. **是否有跨服需求**。若将来做跨服战场/全服排行，ID 需全局唯一，现在就要决定塞 serverID 还是用二元组。后期改代价很大
4. **单玩家内存实测值**。需真实结构压测，直接决定实例数
5. **客户端重试策略**。分片交接窗口依赖客户端重试，需与客户端约定超时和次数；若客户端不做，需在网关加缓冲队列
6. **服务名是否改回 player**。Lobby 现在的语义是「玩家对象容器」，若团队惯常理解 lobby 为「匹配前大厅」易生歧义
7. **NTP slew 模式**。需与运维确认，很多默认配置是 step 模式

---

## 17. 实施顺序

| 阶段 | 内容 | 验证目标 |
|---|---|---|
| P0 | NATS 封装、Envelope、ID 分配 + 启动自检 | 进程间能通信、能追踪、撞号能拦截 |
| **P1** | **分片认领（etcd lease + CAS）** | 多实例正确分片，挂了能接管 |
| **P2** | **Actor + 刷盘 + epoch fencing** | 数据正确落盘，脑裂能被拦截 |
| P3 | Gateway + session + 顶号 | 端到端连通 |
| P4 | Handoff + 优雅下线 | 滚动发版不掉线 |
| P5 | 跨分片 tx + 补偿扫描 + profile 摘要 | 交易/邮件/好友类玩法可用 |

**P1、P2 是地基，出问题最贵、事后最难补，建议投入最多评审和测试。**
