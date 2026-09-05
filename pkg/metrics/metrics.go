// Package metrics 汇总 §14 的全部监控指标。
//
// 标注「必须告警」的指标在代码中同时打 Error 日志，方便没接 Prometheus 时也能发现：
//   - epoch 校验失败次数（非零即告警）
//   - NATS slow consumer 次数
//   - 时钟回拨触发次数（非零即告警）
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
	// ShardMailboxLen mailbox 当前长度，需监控积压（§4.4）。
	ShardMailboxLen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "shard", Name: "mailbox_len",
		Help: "当前 mailbox 中待处理消息数",
	}, []string{"kind", "shard"})

	// ShardMailboxCap mailbox 容量上限。
	ShardMailboxCap = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "shard", Name: "mailbox_cap",
		Help: "mailbox 容量上限",
	}, []string{"kind"})

	// ShardMailboxDropped mailbox 满导致的丢弃数（背压）。
	ShardMailboxDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "mailbox_dropped_total",
		Help: "mailbox 满被丢弃的消息数",
	}, []string{"kind", "shard"})

	// ShardHandleLatency 消息处理延迟，用于 P99。
	ShardHandleLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "shard", Name: "handle_latency_seconds",
		Help:    "Actor 内单条消息处理耗时",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"kind", "cmd"})

	// ShardOwnerChanges 分片 owner 变更次数。
	ShardOwnerChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "owner_changes_total",
		Help: "本进程获得/释放分片所有权的次数",
	}, []string{"kind", "action"}) // action: acquire / release / lost

	// ShardOwned 当前持有的分片数。
	ShardOwned = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "shard", Name: "owned",
		Help: "当前持有的分片数量",
	}, []string{"kind"})

	// ShardEpochRejected epoch 校验失败次数 —— 非零即告警（§14）。
	ShardEpochRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "epoch_rejected_total",
		Help: "epoch fencing 拒绝写入的次数，非零即代表发生过脑裂",
	}, []string{"kind", "shard"})

	// ShardLeaseLost 续租失败次数。
	ShardLeaseLost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "shard", Name: "lease_lost_total",
		Help: "分片 lease 丢失次数",
	}, []string{"kind"})

	// ShardHandoff 主动交接次数。
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

	// FlushChBacklog flushCh 满导致重新标脏的次数（§6.3，必须告警）。
	FlushChBacklog = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "flush", Name: "chan_backlog_total",
		Help: "flushCh 满走 default 重新标脏的次数，说明 Redis 已扛不住",
	}, []string{"kind"})

	FlushChLen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "flush", Name: "chan_len",
		Help: "flushCh 当前积压批次数",
	}, []string{"kind"})

	// DirtyAge 脏数据滞留时长 —— 真实丢失窗口（§14）。
	DirtyAge = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "flush", Name: "dirty_age_seconds",
		Help:    "从标脏到成功落盘的时长，即真实数据丢失窗口",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 14),
	}, []string{"kind", "level"})

	FlushEntities = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "flush", Name: "entities_total",
		Help: "累计刷盘实体数",
	}, []string{"kind", "level"})

	// WriteThrough L0 写穿次数与耗时。
	WriteThrough = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "flush", Name: "write_through_seconds",
		Help:    "L0 写穿耗时（同步落 Redis 成功后才改内存回包）",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"op"})
)

// ---------------------------- NATS ----------------------------

var (
	// NATSSlowConsumer slow consumer 次数 —— 必须告警（§5.4）。
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
	// IDClockBackwards 时钟回拨触发次数 —— 非零即告警（§14）。
	IDClockBackwards = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "id", Name: "clock_backwards_total",
		Help: "雪花检测到时钟回拨的次数",
	}, []string{"severity"}) // severity: minor(自旋追平) / major(停止发号)

	// IDSeqOverflow 序列号溢出次数（同毫秒内 4096 用尽）。
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

	// TxPending 跨分片转移 PENDING 数与超时数（§8）。
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

	// NodeIDConflict 启动自检撞号次数（§3.4）。进程会直接退出，这里主要用于 push 到告警。
	NodeIDConflict = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "node", Name: "id_conflict_total",
		Help: "nodeID 撞号导致启动失败的次数",
	})

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
	)
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
}

// Registry 返回内部注册表，便于测试。
func Registry() *prometheus.Registry { return registry }

// Serve 在给定地址上暴露 /metrics 与 /healthz。addr 为空则不启动。
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
