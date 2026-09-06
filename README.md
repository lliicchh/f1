# 游戏服务器（slots + RPG）

按 [`game-server-design-v3.md`](game-server-design-v3.md) 实现的分区分服游戏服务器，
并按 [`REVIEW.md`](REVIEW.md) 的评审结论补齐了 slots 品类所需的安全与合规能力。
技术栈：Go / NATS / Redis / etcd / Docker Compose。

> 先读 [`REVIEW.md`](REVIEW.md)：它说明了为什么同一份架构代码，
> 放到 slots 品类下风险等级完全不同，以及本仓库据此做了哪些改动。

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
  authz/       P0-1 命令权限分级与内部命令签名
  authn/       P0-3 登录票据校验
  payment/     P0-2 充值回执验签
  rng/         P0-4 可审计、可复现的随机数源
  gameconf/    P1-3 配置表 + 内容哈希版本
  ledger/      P1-2 资金流水账本
  jackpot/     P1-5 累积奖池
  rg/          P1-6 责任游戏限额
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
  slots/              老虎机数学引擎（纯函数，可复算）
  lobby/              玩家对象 + slots/抽卡/充值/GM 逻辑
  room/ gateway/ match/ chat/ world/

cmd/gameconfctl/        配置表运维工具：dump / lint / rtp

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

## slots 相关的实现要点

这一节对应 [`REVIEW.md`](REVIEW.md) 的修改清单。

**命令权限分级（P0-1）。**
每个命令在 `pkg/protocol` 的定义处标注 `Client / Internal / GM`，
**未登记的命令默认落到最严的 Internal** —— 漏配的后果是「调不通」而不是「被刷钱」。
两道校验：网关只放行 Client 级；Lobby 再验一次 HMAC 签名。
第二道存在的理由是不信任网关 —— 部署时网关**不持有** `INTERNAL_SECRET`
（见 `deploy/gateway.env`），因此它在物理上签不出一条合法的内部命令。

**一次 spin 的四个步骤，顺序不可打乱**（`internal/lobby/slots.go`）：

```
1. 拒绝性检查（限额、档位、回合冲突）  —— 拒绝不产生任何副作用
2. 在副本上算结果                      —— 内存此时未被改动
3. 一次原子提交                        —— 余额 + 回合 + 限额 + 流水，同生共死
4. 提交成功后才改内存、才回包
```

**流水与余额必须在同一个 Lua 里写入**（`pkg/store` 的 `scriptCommit`）。
分两步写一定会出现「扣了钱没流水」或「有流水没扣钱」，
而这两种情况事后无法区分是 bug 还是欺诈。

**回合可中断、可恢复、可复算。**
未结算回合随投注原子落盘，登录时由 `LoginResp.open_round` 带回；
回合里记着 `seed_hex` 与 `config_version`，
`slots.Replay` 能凭这两样重放出位对位一致的结果 —— 这是客服查单与监管抽查的入口。

**免费旋转与主旋转共用一条随机流。**
代价是每次免费旋转要把前面几次重放一遍（纯内存，开销可忽略），
换来的是「一颗种子重放整局」这个性质 —— 分成两条流就复算不了了。

**奖池不做成分布式事务。**
注入用 Redis `INCRBY`（本来就是原子的）；
中奖时原子地「清零 + 落 PENDING 派彩记录」，再由 §8 的中转层幂等打给玩家。
「每次 spin 发个 job 给 World 累加」是常见的过度设计，既慢又多一个失败点。

**RTP 由测试守着。**
`make rtp` 或 `make test-rtp` 跑蒙特卡洛，实测偏离配置值 0.5% 以上即失败。
轴带改错一个符号，RTP 可能从 96% 跳到 130%，这种错误必须在 CI 拦住。

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
pkg/authz        造币类命令绝不能是客户端级、签名绑定 cmd/uid/时效、无密钥签不出
pkg/authn        无效/过期/张冠李戴的登录票据必须被拒
pkg/payment      未配置时默认拒绝；换单号/换 uid/换商品/改价的回执都要被识破
pkg/rng          同种子可复现、不同种子必不同
pkg/gameconf     版本是内容哈希、非法配置被拦下、热替换失败不影响现有配置
pkg/rg           日投注/日亏损/会话时长/自我排除、跨日按配置时区重置、玩家限额只能更严
internal/slots   RTP 蒙特卡洛回归、重放确定性、连线规则、免费旋转不参与奖池
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

  —— 以下对应 REVIEW.md 的验收标准 ——
  客户端越权      add_currency / add_item / apply_* / gm_* 一律被拒，且确认没到账
  伪造签名        错误密钥、无签名、缺 operator 的内部命令都被拒
  登录认证        空票据 / 乱填 / 别人的票据都被拒
  spin 原子性     扣款、派彩、流水三者一致，流水求和 == 当前余额
  spin 幂等       重发同一次 spin 不重复扣费，返回首次结果
  未完成回合      免费旋转中途重启进程，重连后取回并打完，期间不扣费
  责任游戏        触发日限额后拒绝下注；自我排除期内拒绝登录与下注
  奖池            并发注入总额守恒；中奖清零只发生一次且必留派彩记录
  抽卡保底        保底周期内必出最高稀有度；十连必出高稀有度；消耗与产出都有流水
  充值            未知商品 / 无效回执 / 未授权渠道都被拒，且一分钱不到账
  GM              补单幂等、留下带操作者的流水、缺 operator 直接拒绝
```

---

## 运维必须确认的事

0. **三把密钥必须改掉，且网关不能拿到 `INTERNAL_SECRET`。**
   `deploy/s1.env` 里的 `CHANGE-ME-*` 是占位值。
   网关通过 `deploy/gateway.env` 把内部密钥覆盖成空 —— 这不是可选项，
   它是「网关被攻破也签不出内部命令」这一保证的物理基础。
   未配置 `LOGIN_SECRET` 且未显式设 `ALLOW_DEV_AUTH=true` 时，**所有登录都会被拒绝**（有意如此）。
   未配置 `PAYMENT_SECRET` 且未开沙箱时，**所有充值都会被拒绝**（同样有意）。
1. **Redis `maxmemory-policy` 必须是 `noeviction`。** 服务启动时会校验，不满足直接拒绝启动
   （要放行需显式设 `REDIS_REQUIRE_NOEVICTION=false`，但那意味着接受静默丢档的风险）。
2. **NTP 必须用 slew 模式**（chrony 配 `maxslewrate`，或 ntpd 加 `-x`）。
   默认配置在偏差大时会直接 step，那会触发发号停止。
3. **`stop_grace_period` 要给够。** 默认 10s 会在优雅下线刷盘途中 SIGKILL；
   compose 里已设 60s，`SHUTDOWN_GRACE` 相应设为 55s，按实际分片数与数据量调整。
4. **告警必须接这几个指标**（非零即有事）：
   `game_shard_epoch_rejected_total`、`game_nats_slow_consumer_total`、
   `game_id_clock_backwards_total`、`game_authz_rejected_total`。
5. **RTP 要接监控大盘**：
   `game_slots_win_amount_total / game_slots_bet_amount_total`（按 `game` 与 `config_version` 分组），
   与配置的理论 RTP 做偏差告警。RTP 异常是配置错误或作弊的第一信号。
6. **`game_slots_jackpot_pending` 长期非零说明奖池派彩链路卡住了**，钱在半路上。
7. **账本是过渡方案。** 流水目前写在 Redis Stream（每玩家保留约 2000 条）。
   真钱上线前必须把它导出到不可篡改的长期存储 —— Redis 可被 FLUSH，不是审计级存储。

---

## 评审中已知但本次未做的事

诚实列出来，避免给人「都做完了」的错觉：

- **账本长期存储**。当前只有 Redis Stream + 导出接口，没有实际的导出目标。
- **group commit（P2-1）**。每笔 L0 仍是一次独立的 Redis 往返。
  10 万在线、3 秒一次 spin ≈ 33k 次/秒同步 Lua，单 Redis 会吃紧；
  同分片攒批可提升一个数量级，代价是几毫秒延迟。
- **TLS（P2-3）**。网关仍是明文 TCP，需在部署层加 TLS 终结。
- **多币种与精度（P2-5）**。金额已统一用 int64 最小单位（绝不用浮点），
  但汇率、分币种限额还没做。
- **NATS 账号隔离（P2-7）**。内部命令签名已能挡住伪造，
  但生产仍应给 NATS 配账号，限制谁能往 `req.lobby.*` 发消息。

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
