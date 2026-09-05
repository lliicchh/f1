# 游戏服务器（game-server-design v0.3 实现）

按 [`game-server-design-v3.md`](game-server-design-v3.md) 实现的分区分服游戏服务器。
技术栈：Go / NATS / Redis / etcd / Docker Compose。

两条设计取向贯穿全部代码，读代码前先记住它们：

1. **进程内存是数据的权威副本，Redis 是它的持久化投影。** 任何时刻一份玩家数据只能被一个进程持有。
2. **分片（shard）是一切的调度单位。** 资源分配、故障转移、扩缩容、刷盘都以分片为粒度。

---

## 快速开始

```bash
# 单元测试 + 集成测试（集成测试内嵌 etcd / NATS / Redis，不需要外部依赖）
make test

# 构建全部服务
make build

# 起一整个区服
make up ENV_FILE=deploy/s1.env PROJECT=game-s1 TAG=v1.0.0

# 再开一个区
make up ENV_FILE=deploy/s2.env PROJECT=game-s2 TAG=v1.0.0
```

---

## 目录结构

```
cmd/                    六个服务的入口，每个只有十几行
  gateway/ lobby/ room/ match/ chat/ world/

pkg/                    与业务无关的基础设施
  ident/       §3   三层 ID 结构、nodeID / workerID 派生、越界校验
  idgen/       §3.5 雪花发号、时钟回拨处理
  nodeid/      §3.4 nodeID 的 etcd 唯一性自检与心跳
  config/      §3.2 env 配置装载与一致性校验
  shard/       §4   分片认领（etcd lease + CAS）、Actor 执行模型
  shardsvc/    §4/§10 分片服务骨架：认领 + Actor + 订阅 + 交接
  store/       §6/§7 Redis key、分级刷盘、epoch fencing
  session/     §9   会话路由表、顶号
  profile/     §6.5 只读摘要（CQRS 读模型）与排行榜
  xtx/         §8   跨分片转移与补偿
  leader/      §2.1 选主（World 用）
  bus/         §5   NATS 封装、Envelope、JetStream
  subject/     §5.1 subject 命名规范
  protocol/         命令号、错误码、推送号
  node/        §10.3 启动流程与优雅下线骨架
  metrics/     §14  全部监控指标
  logx/             结构化日志

internal/               各服务的业务实现
  lobby/ room/ gateway/ match/ chat/ world/

test/
  harness/            进程内 etcd + NATS + Redis
  integration/        端到端测试

proto/                  .proto 源文件（生成物在 pkg/pb）
deploy/                 各区服的 env 文件
```

---

## 设计文档到代码的对照

| 设计章节 | 实现位置 | 说明 |
|---|---|---|
| §2.3 持有数据的服务不用 queue group | `bus.Subscribe` / `shardsvc` | `Subscribe` 与 `QueueSubscribe` 分开两个方法，注释写明谁能用哪个 |
| §3.1–3.3 三层 ID | `pkg/ident` | `nodeID = s{serverID}-{svc}-{seq}`，`workerID = svcType<<7 \| nodeSeq` |
| §3.4 两道启动校验 | `ident.New` + `pkg/nodeid` | 越界校验在配置装载期；etcd 唯一性自检在 `node.Bootstrap` |
| §3.5 雪花位分配 | `pkg/idgen` | 41 时间戳 / 10 workerID / 12 序列号 |
| §3.6 时钟回拨 | `idgen.Generator.Next` | ≤10ms 自旋，>10ms 停止发号 + 告警 |
| §3.7 迁移到自动分配 | `nodeid.Record.LastTS` | `last_ts` 校验已经就位，切换时不用改数据结构 |
| §4.1 分片划分 | `shard.Of` | `ShardCount` 常量，Lobby 与 Room 独立分片空间 |
| §4.2 分片认领 | `shard.Claimer` | lease TTL 8s、续租 2.5s、CAS `CreateRevision == 0` |
| §4.3 订阅不带 queue group | `subject.LobbyShardWildcard` + `shardsvc.onAcquire` | 每认领一个分片才订阅一个 subject |
| §4.4 Actor 执行模型 | `shard.Runtime` / `shard.Actor` | 每分片一条 goroutine，有界 mailbox，积压告警 |
| §5.1 subject 规范 | `pkg/subject` | `req. / evt. / push. / job. / ctl.` |
| §5.2 Core NATS vs JetStream | `bus.JS` | `job.` 前缀走 WorkQueue Stream |
| §5.3 Envelope | `proto/envelope.proto` | `trace_id` / `from_node` 全链路带上 |
| §5.4 使用注意 | `bus.Connect` | ErrorHandler 必装、payload 上限校验、回调只投递 |
| §6.1 Redis key | `store.Keys` | 按模块拆分，hash tag，`p:` 而非 `lobby:` |
| §6.2 分级刷盘 | `store.Level` / `Shard.flushLevel` | L0 写穿、L1 5s、L2 60s、L3 不刷 |
| §6.3 刷盘流程 | `store.Flusher` | Actor 内序列化，IO pool 写盘，满则重新标脏 |
| §6.4 Redis 配置 | `store.VerifyConfig` | `maxmemory-policy != noeviction` 直接拒绝启动 |
| §6.5 profile 摘要 | `pkg/profile` | 随 base 刷盘顺带写，任何进程可直读 |
| §7 epoch fencing | `store.Fencer` | 所有写入过 Lua 校验 epoch，被拒即丢弃内存自杀 |
| §8 跨分片转移 | `pkg/xtx` + Lobby job 中转层 | 写穿 → JetStream → 幂等入账 → 补偿扫描 |
| §9.1 路由表 | `session.Store` | 网关只订两个 subject |
| §9.2 顶号 | `session.scriptBind` | Lua 原子替换，取旧 gateID 发 KICK |
| §9.3 广播 | `room.broadcastRoom` / `chat` | 房间与公会广播按 gateID 聚合 |
| §9.4 网关重启 | `session.CleanGate` | 启动清理 + TTL 双保险 |
| §10.1 玩家生命周期 | `lobby.Shard.startLoad` / `unloadIdle` | 懒加载、下线保留 10 分钟 |
| §10.2 分片交接 | `shardsvc.handleHandoff` / `requestHandoff` | 新节点发起，旧节点刷盘后回 READY |
| §10.3 优雅下线 | `node.Node.shutdown` | 五步顺序固化在框架里，服务只填内容 |
| §11 部署 | `docker-compose.yml` / `deploy/*.env` | YAML 锚点、不使用 replicas、`stop_grace_period: 60s` |
| §13 故障处理 | 见下方「故障行为」 | |
| §14 监控指标 | `pkg/metrics` | 三个「必须告警」的指标同时打 Error 日志 |

---

## 几处需要知道的实现选择

**分片 epoch 直接取 etcd 键的 `CreateRevision`。**
设计说「epoch 取事务返回的 revision」。实现上没有把 epoch 再写回值里，而是读键的
`CreateRevision` —— 它就是那个 revision。这样认领一个分片只需一次写，
1024 个分片的接管时间减半；`etcdctl get --write-out=json` 依然能直接看到。

**全节点共用一个分片 lease。**
lease 死了意味着本进程整体失联，所有分片一起释放正是想要的行为；
单个分片的主动释放通过删键完成，不影响其他分片。

**再平衡由「想要分片的一方」发起交接，而不是持有方主动让出。**
让出会制造一段没人服务的窗口；交接是「旧节点刷完盘回 READY，新节点立刻接手」，
窗口压到百毫秒（§10.2）。只有在服务没接入 handoff 时才退化为主动让出。

**`job.` 中转层用 JetStream 的竞争消费。**
多个 Lobby 实例共用同一个 durable consumer，竞争的是「谁来搬运这条任务」，
搬运的动作是转发给目标分片的 owner。这不违反 §2.3 —— 竞争的不是数据持有权。

**匹配池用分片认领而不是选主。**
按 `模式×段位` 分成 128 个桶独占认领（§2.1 的第一种方案）。
匹配池不落盘，所以跳过 epoch fencing；但「同一个桶只能有一个实例在撮合」
仍然靠分片独占保证，否则两个实例会把同一批玩家撮进两个房间。

**Redis Cluster 需要把 hash tag 切成分片粒度。**
默认 `REDIS_HASHTAG=uid`，即文档写的 `p:{1001}:base`。
Cluster 部署要改成 `shard`，让 epoch 键与该分片下所有玩家键落在同一 slot，
否则 fencing 的 Lua 会撞上 CROSSSLOT。单实例与主从哨兵部署无此约束。

**跨分片 tx 的键按「哪一侧要原子」来定 hash tag。**
`tx:{s<发起方分片>}:<txid>` 与 `tx:pending:{s<发起方分片>}` 同 slot，
让「扣道具 + 写 tx 记录 + 入 PENDING 索引」在一个 Lua 里完成；
`txdone:{s<接收方分片>}:<txid>` 与接收方玩家键同 slot，让「幂等标记 + 入账」也原子。

---

## 故障行为（§13）

| 场景 | 实际行为 | 代码位置 |
|---|---|---|
| Lobby 实例崩溃 | lease 过期 → 其他实例扫描到空闲分片 → 抬高 epoch → 从 Redis 加载 | `shard.Claimer.scanAndClaim` |
| 续租失败 | 立即停止处理该分片消息并丢弃内存，不做任何写入 | `Claimer.keepAlive` → `ReleaseLeaseLost` |
| 网络分区双 owner | 旧 owner 的写入被 Lua 拒绝 → 丢弃内存、停止服务、告警，绝不重试 | `store.Fencer` → `Claimer.Fence` |
| nodeID 撞号 | 启动自检拒绝启动，日志里直接给出冲突方 host/pid | `nodeid.Claim` |
| 时钟大幅回拨 | 停止发号 + 告警；时钟修正后自动恢复 | `idgen.Generator.Next` |
| Gateway 崩溃 | 客户端重连其他网关；残留 session 靠启动清理 + TTL 双保险 | `session.CleanGate` |
| Redis 主故障 | 刷盘失败重新标脏，内存仍是权威副本，服务继续 | `store.Flusher` → `DirtySet.ReMark` |
| flushCh 积压 | 走 default 重新标脏并告警，绝不阻塞 Actor | `Flusher.Submit` |
| 单分片过热 | mailbox 积压超阈值持续告警 | `Actor.warnBacklog` |

---

## 测试

集成测试把 etcd、NATS（含 JetStream）、Redis 全部跑在测试进程里，`make test` 即可，
不需要 Docker。设计文档把 P1、P2 列为地基，测试也压在那里：

```
pkg/ident        workerID 全服务唯一性、越界溢出会撞车的反证
pkg/idgen        并发唯一性、小回拨自旋、大回拨停止发号、序列号溢出
pkg/store        epoch fencing 拒绝陈旧 owner、L0 幂等、刷盘失败必须回报
pkg/session      顶号原子替换、迟到的解绑不能删新会话、网关重启清理
pkg/xtx          发起方原子写穿、接收方幂等、fencing 时 done 标记回滚
pkg/shard        Actor 串行性、mailbox 满不阻塞、panic 隔离

test/integration
  分片认领        单实例吃满分片空间；两实例不会有同一分片被双持
  故障接管        owner 断连后另一实例接管，epoch 严格抬高
  脑裂拦截        真实接管后旧 owner 的写入被拒，数据未被回滚
  nodeID 自检     撞号拒绝启动；僵尸记录可接管；时钟倒退拒绝接管
  端到端 Lobby    登录 → 改数据 → L1 刷盘 → Redis + profile 摘要
  L0 幂等         同一订单号重复提交返回首次结果，余额不翻倍
  跨分片转移      扣除与入账总量守恒，失败时整体回滚
  分片交接        扩容后收敛到公平份额，交接过程中数据不丢
  优雅下线        关掉定时刷盘后，下线仍把内存改动全部落盘
  网关            TCP 端到端登录、路由、顶号、未登录拒绝
  全链路          匹配 → 建房 → 开打 → 结算 → 发奖回到 Lobby
  世界服          选主后唯一实例对外服务
```

---

## 运维必须确认的事

1. **Redis `maxmemory-policy` 必须是 `noeviction`。** 服务启动时会校验，不满足直接拒绝启动
   （要放行需显式设 `REDIS_REQUIRE_NOEVICTION=false`，但那意味着接受静默丢档的风险）。
2. **NTP 必须用 slew 模式**（chrony 配 `maxslewrate`，或 ntpd 加 `-x`）。
   默认配置在偏差大时会直接 step，那会触发发号停止。
3. **`stop_grace_period` 要给够。** 默认 10s 会在优雅下线刷盘途中 SIGKILL；
   compose 里已设 60s，`SHUTDOWN_GRACE` 相应设为 55s，按实际分片数与数据量调整。
4. **告警必须接这三个指标**（非零即有事）：
   `game_shard_epoch_rejected_total`、`game_nats_slow_consumer_total`、`game_id_clock_backwards_total`。

---

## 设计文档里仍待拍板的事（§16）

代码在这些点上留了口子，但没有替业务做决定：

1. **Lobby 与 Room 是否合并部署。** 目前分开，两套独立分片空间。
   若要合并，改动集中在分片键选择上（按 roomID 而不是 uid）。
2. **雪花的使用范围。** 现在 uid、道具实例 ID、房间 ID、订单 txid、connID 都用它，
   峰值需求要压测校准（`idgen` 已有 `seq_overflow_total` 指标）。
3. **是否有跨服需求。** 现在按 `(serverID, uid)` 二元组区分，serverID 不进 workerID。
4. **单玩家内存实测值。** `game_biz_players_resident` 与 `game_biz_mem_alloc_bytes` 已在，压测时直接读。
5. **客户端重试策略。** 网关已实现有限次重投（`retryMax` / `retryWait`）作为兜底，
   但客户端自己也应实现超时重试。
6. **服务名是否改回 player。** 代码里数据 key 用的是 `p:` 而不是 `lobby:`，
   改服务名不需要迁移数据。
7. **NTP slew 模式。** 见上一节第 2 条。
