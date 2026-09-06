// Package metrics 全部监控指标
//
// 几个非零就该看一眼的：epoch 校验失败、NATS slow consumer、时钟回拨、鉴权拒绝。
// 这些同时也打 Error 日志，没接 Prometheus 时靠日志也能发现
package metrics

import (
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "game"

// ---------------------------- 分片 ----------------------------

var (
	// ShardMailboxLen 当前积压
	ShardMailboxLen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "shard", Name: "mailbox_len",
		Help: "mailbox 中待处理的消息数",
	}, []string{"kind", "shard"})

	// ShardMailboxCap mailbox 容量上限
	ShardMailboxCap = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "shard", Name: "mailbox_cap",
		Help: "mailbox 容量上限",
	}, []string{"kind"})

	// ShardMailboxDropped mailbox 满丢掉的消息数
	ShardMailboxDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "mailbox_dropped_total",
		Help: "mailbox 满被丢弃的消息数",
	}, []string{"kind", "shard"})

	// ShardHandleLatency 消息处理延迟，用于 P99
	ShardHandleLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "shard", Name: "handle_latency_seconds",
		Help:    "Actor 内单条消息处理耗时",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"kind", "cmd"})

	// ShardOwnerChanges 分片 owner 变更次数
	ShardOwnerChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "owner_changes_total",
		Help: "本进程获得/释放分片所有权的次数",
	}, []string{"kind", "action"}) // action: acquire / release / lost

	// ShardOwned 当前持有的分片数
	ShardOwned = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "shard", Name: "owned",
		Help: "当前持有的分片数量",
	}, []string{"kind"})

	// ShardEpochRejected epoch 校验失败次数，非零就是发生过脑裂
	ShardEpochRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "epoch_rejected_total",
		Help: "epoch fencing 拒绝写入的次数，非零即代表发生过脑裂",
	}, []string{"kind", "shard"})

	// ShardLeaseLost 续租失败次数
	ShardLeaseLost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "lease_lost_total",
		Help: "分片 lease 丢失次数",
	}, []string{"kind"})

	// ShardHandoff 主动交接次数
	ShardHandoff = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "handoff_total",
		Help: "分片主动交接次数",
	}, []string{"kind", "role", "result"}) // role: source/target
)

// ---------------------------- 刷盘 ----------------------------

var (
	FlushDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "flush", Name: "duration_seconds",
		Help:    "一批刷盘（pipeline 写 Redis）耗时",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"kind", "level"})

	FlushFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "flush", Name: "failures_total",
		Help: "刷盘失败次数（失败后重新标脏，绝不丢弃）",
	}, []string{"kind", "cause"}) // cause: redis / epoch / serialize

	// FlushChBacklog flushCh 满、只能重新标脏的次数
	FlushChBacklog = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "flush", Name: "chan_backlog_total",
		Help: "flushCh 满走 default 重新标脏的次数，说明 Redis 已扛不住",
	}, []string{"kind"})

	FlushChLen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "flush", Name: "chan_len",
		Help: "flushCh 当前积压批次数",
	}, []string{"kind"})

	// DirtyAge 脏数据滞留了多久，也就是真实的丢失窗口
	DirtyAge = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "flush", Name: "dirty_age_seconds",
		Help:    "从标脏到成功落盘的时长，即真实数据丢失窗口",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 14),
	}, []string{"kind", "level"})

	FlushEntities = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "flush", Name: "entities_total",
		Help: "累计刷盘实体数",
	}, []string{"kind", "level"})

	// WriteThrough L0 写穿耗时
	WriteThrough = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "flush", Name: "write_through_seconds",
		Help:    "L0 写穿耗时（同步落 Redis 成功后才改内存回包）",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"op"})
)

// ---------------------------- NATS ----------------------------

var (
	// NATSSlowConsumer slow consumer 次数，出现就说明在丢消息
	NATSSlowConsumer = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "nats", Name: "slow_consumer_total",
		Help: "NATS slow consumer 次数，pending 超限会静默丢弃消息",
	}, []string{"subject"})

	NATSAsyncErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "nats", Name: "async_errors_total",
		Help: "NATS 异步错误数",
	}, []string{"kind"})

	NATSReconnects = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "nats", Name: "reconnects_total",
		Help: "NATS 重连次数",
	})

	NATSDisconnects = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "nats", Name: "disconnects_total",
		Help: "NATS 断连次数",
	})

	NATSMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "nats", Name: "messages_total",
		Help: "各 subject 前缀收发消息数",
	}, []string{"direction", "prefix"}) // direction: in/out

	NATSRequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "nats", Name: "request_latency_seconds",
		Help:    "req.* 请求-应答往返耗时",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 15),
	}, []string{"prefix"})

	NATSPayloadTooLarge = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "nats", Name: "payload_too_large_total",
		Help: "超过 max_payload 被拒绝发送的消息数",
	}, []string{"prefix"})
)

// ---------------------------- ID ----------------------------

var (
	// IDClockBackwards 时钟回拨次数
	IDClockBackwards = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "id", Name: "clock_backwards_total",
		Help: "雪花检测到时钟回拨的次数",
	}, []string{"severity"}) // severity: minor(自旋追平) / major(停止发号)

	// IDSeqOverflow 同毫秒内序列号用尽的次数
	IDSeqOverflow = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "id", Name: "seq_overflow_total",
		Help: "同一毫秒内序列号用尽、需等待下一毫秒的次数",
	})

	IDGenerated = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "id", Name: "generated_total",
		Help: "累计生成的雪花 ID 数",
	})

	IDHalted = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "id", Name: "halted",
		Help: "1 表示因大幅时钟回拨已停止发号",
	})
)

// ---------------------------- 业务 ----------------------------

var (
	Online = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "biz", Name: "online",
		Help: "本实例在线人数 / 连接数",
	}, []string{"kind"})

	PlayersResident = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "biz", Name: "players_resident",
		Help: "常驻内存的玩家对象数（含下线后未卸载的冗余）",
	})

	RoomsResident = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "biz", Name: "rooms_resident",
		Help: "常驻内存的房间数",
	})

	// TxPending 当前 PENDING 的跨分片转移数
	TxPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "biz", Name: "tx_pending",
		Help: "当前 PENDING 状态的跨分片转移数",
	})

	TxTimeout = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "biz", Name: "tx_timeout_total",
		Help: "超时被补偿扫描器重投的跨分片转移数",
	})

	TxCompleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "biz", Name: "tx_completed_total",
		Help: "完成的跨分片转移数",
	}, []string{"result"}) // result: ok / duplicate

	MatchQueueLen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "biz", Name: "match_queue_len",
		Help: "匹配池排队人数",
	}, []string{"mode", "tier"})

	MatchWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "biz", Name: "match_wait_seconds",
		Help:    "匹配等待时长",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 12),
	}, []string{"mode"})

	MemAlloc = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "biz", Name: "mem_alloc_bytes",
		Help: "实例堆内存占用（runtime.MemStats.Alloc）",
	})

	// NodeIDConflict 撞号次数。进程会直接退出，这个指标主要用来触发告警
	NodeIDConflict = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "node", Name: "id_conflict_total",
		Help: "nodeID 撞号导致启动失败的次数",
	})

	// ---- slots / 运营指标----

	// Spins 旋转次数，按游戏和是否免费分
	Spins = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "slots", Name: "spins_total",
		Help: "旋转次数",
	}, []string{"game", "kind"}) // kind: paid / free

	// BetAmount 和 WinAmount RTP 的分子分母，要能实时看。
	// RTP 偏了通常不是体验问题，而是配置错了或者有人在薅
	BetAmount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "slots", Name: "bet_amount_total",
		Help: "累计投注额（最小货币单位）",
	}, []string{"game", "config_version"})

	WinAmount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "slots", Name: "win_amount_total",
		Help: "累计派彩额（最小货币单位）",
	}, []string{"game", "config_version"})

	SpinWinRatio = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "slots", Name: "win_multiple",
		Help:    "单次旋转派彩相对总注的倍数分布",
		Buckets: []float64{0, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
	}, []string{"game"})

	RoundsOpen = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "slots", Name: "rounds_open",
		Help: "当前未结算的回合数（含免费旋转进行中）",
	})

	RoundRecovered = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "slots", Name: "rounds_recovered_total",
		Help: "登录时恢复的未结算回合数",
	})

	JackpotAmount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "slots", Name: "jackpot_amount",
		Help: "奖池当前水位",
	}, []string{"pool"})

	JackpotWins = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "slots", Name: "jackpot_wins_total",
		Help: "奖池中奖次数",
	}, []string{"pool"})

	JackpotPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "slots", Name: "jackpot_pending",
		Help: "待派彩的奖池记录数，长期非零说明派彩链路卡住了",
	}, []string{"pool"})

	// LedgerEntries 流水条数，应该和资金变动次数同步涨
	LedgerEntries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "ledger", Name: "entries_total",
		Help: "写入的流水条数",
	})

	// RGBlocked 责任游戏拦了多少次
	RGBlocked = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "rg", Name: "blocked_total",
		Help: "被责任游戏限额拦截的次数",
	}, []string{"reason"})

	// AuthzRejected 鉴权拒绝次数。非零要查：要么有人在试探，要么哪个服务配错了
	AuthzRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "authz", Name: "rejected_total",
		Help: "命令鉴权拒绝次数",
	}, []string{"cmd", "reason"})

	// AuthnFailed 登录认证失败次数
	AuthnFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "authn", Name: "failed_total",
		Help: "登录认证失败次数",
	}, []string{"reason"})

	// GachaDraws 抽卡次数
	GachaDraws = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "gacha", Name: "draws_total",
		Help: "抽卡次数",
	}, []string{"pool"})

	GachaPityHits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "gacha", Name: "pity_hits_total",
		Help: "保底触发次数",
	}, []string{"pool", "kind"})

	// ConfigVersion 当前生效的配置版本，用来回溯某段时间跑的是哪一版
	ConfigVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "conf", Name: "active",
		Help: "当前生效的配置版本",
	}, []string{"version"})

	// ---------------------------- 账号 ----------------------------

	// IDPVerify 渠道校验结果。outcome 取 ok / bad_credential / upstream / timeout
	IDPVerify = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "idp", Name: "verify_total",
		Help: "渠道凭证校验次数",
	}, []string{"channel", "outcome"})

	// IDPLatency 渠道校验耗时。外部 IO，慢和抖都要看得见
	IDPLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "idp", Name: "verify_seconds",
		Help:    "渠道凭证校验耗时",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2, 5},
	}, []string{"channel"})

	// IDPBreakerOpen 熔断器是否打开。持续为 1 说明这个渠道已经登不进来了
	IDPBreakerOpen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "idp", Name: "breaker_open",
		Help: "渠道熔断器是否打开",
	}, []string{"channel"})

	// AccountCreated 开号数
	AccountCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "account", Name: "created_total",
		Help: "首次登录开号数",
	}, []string{"channel"})

	// AccountBindConflict 绑定冲突次数，非零说明有人在拿别人的渠道账号试
	AccountBindConflict = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "account", Name: "bind_conflict_total",
		Help: "渠道绑定冲突次数",
	}, []string{"channel", "reason"})

	SessionKick = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "biz", Name: "session_kick_total",
		Help: "顶号次数",
	})
)

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(
		ShardMailboxLen, ShardMailboxCap, ShardMailboxDropped, ShardHandleLatency,
		ShardOwnerChanges, ShardOwned, ShardEpochRejected, ShardLeaseLost, ShardHandoff,
		FlushDuration, FlushFailures, FlushChBacklog, FlushChLen, DirtyAge, FlushEntities, WriteThrough,
		NATSSlowConsumer, NATSAsyncErrors, NATSReconnects, NATSDisconnects, NATSMessages,
		NATSRequestLatency, NATSPayloadTooLarge,
		IDClockBackwards, IDSeqOverflow, IDGenerated, IDHalted,
		Online, PlayersResident, RoomsResident, TxPending, TxTimeout, TxCompleted,
		MatchQueueLen, MatchWait, MemAlloc, NodeIDConflict, SessionKick,
		Spins, BetAmount, WinAmount, SpinWinRatio, RoundsOpen, RoundRecovered,
		JackpotAmount, JackpotWins, JackpotPending, LedgerEntries,
		RGBlocked, AuthzRejected, AuthnFailed, GachaDraws, GachaPityHits, ConfigVersion,
		IDPVerify, IDPLatency, IDPBreakerOpen, AccountCreated, AccountBindConflict,
	)
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
}

// Registry 返回内部注册表，测试用
func Registry() *prometheus.Registry { return registry }

// Serve 暴露 /metrics 和 /healthz，addr 为空就不起
func Serve(addr string) *http.Server {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()
	go sampleMem()
	return srv
}

func sampleMem() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	var ms runtime.MemStats
	for range t.C {
		runtime.ReadMemStats(&ms)
		MemAlloc.Set(float64(ms.Alloc))
	}
}
