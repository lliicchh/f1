// Package config 从环境变量读配置
//
// 一个区服里所有进程共用同一份 SERVER_ID 和 ETCD_PREFIX，各自用不同的 NODE_SEQ。
// 开新区复制一份 env 改个号就行
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gamedev/f1/pkg/ident"
)

// Config 所有服务共用的基础配置
type Config struct {
	// ---- 身份----
	ServerID int    // SERVER_ID，手动配
	NodeSeq  int    // NODE_SEQ，手动配，1~127
	SvcName  string // 各服务 main 传进来，不从 env 读

	// ---- 基础设施 ----
	EtcdEndpoints []string      // ETCD_ENDPOINTS
	EtcdPrefix    string        // ETCD_PREFIX，例 /game/s1
	EtcdTimeout   time.Duration // ETCD_TIMEOUT
	RedisAddr     string        // REDIS_ADDR
	RedisPassword string        // REDIS_PASSWORD
	RedisDB       int           // REDIS_DB
	RedisPoolSize int           // REDIS_POOL_SIZE
	NatsURL       string        // NATS_URL

	// ---- 运行 ----
	LogLevel      string // LOG_LEVEL
	LogFormat     string // LOG_FORMAT: text/json
	MetricsAddr   string // METRICS_ADDR，例 :9100
	ListenAddr    string // LISTEN_ADDR，仅 gateway 用
	AdvertiseAddr string // ADVERTISE_ADDR，写入 etcd node 记录，便于排查

	// ---- 分片----
	ShardCount     uint32        // SHARD_COUNT，定了就别改
	ShardLeaseTTL  time.Duration // SHARD_LEASE_TTL，默认 8s
	ShardKeepAlive time.Duration // SHARD_KEEPALIVE，默认 2.5s
	ShardMaxOwn    int           // SHARD_MAX_OWN，单实例最多认领分片数，0=不限
	MailboxSize    int           // MAILBOX_SIZE，Actor mailbox 容量
	MailboxWarnPct int           // MAILBOX_WARN_PCT，积压告警阈值百分比

	// nodeID 检查
	NodeStaleTimeout time.Duration // NODE_STALE_TIMEOUT，默认 10min
	NodeBeatInterval time.Duration // NODE_BEAT_INTERVAL，默认 30s

	// ---- 刷盘----
	FlushL1Interval time.Duration // FLUSH_L1_INTERVAL，默认 5s
	FlushL2Interval time.Duration // FLUSH_L2_INTERVAL，默认 60s
	FlushChanSize   int           // FLUSH_CHAN_SIZE
	FlushWorkers    int           // FLUSH_WORKERS，IO pool 大小
	FlushBatchSize  int           // FLUSH_BATCH_SIZE，单次 pipeline 实体数

	// ---- 生命周期----
	UnloadIdle    time.Duration // UNLOAD_IDLE，下线后保留时长，默认 10min
	SessionTTL    time.Duration // SESSION_TTL
	ShutdownGrace time.Duration // SHUTDOWN_GRACE，优雅下线总预算

	// ---- 跨分片----
	TxScanInterval time.Duration // TX_SCAN_INTERVAL，补偿扫描间隔
	TxTimeout      time.Duration // TX_TIMEOUT，PENDING 超时重投阈值

	// ---- 安全与合规 ----

	// InternalSecret 内部命令的 HMAC 密钥
	//
	// 别下发给网关。网关没有它就签不出合法的内部命令，
	// 就算有人绕过白名单，Lobby 那边的签名校验也拦得住
	InternalSecret string
	// LoginSecret 登录票据密钥
	//
	// 账号服务用它签发，网关用它校验。除这两个进程外谁都不该有
	LoginSecret string
	// AllowDevAuth 开启无密钥放行登录的开发模式，默认关
	AllowDevAuth bool
	// PaymentSecret 充值回执的验签密钥
	PaymentSecret string
	// PaymentSandbox 开启支付沙箱，只给本地开发用
	PaymentSandbox bool
	// GMSecret GM 后台的额外凭证，为空就关掉 GM 通道
	GMSecret string

	// IDPSandbox 开启渠道沙箱，任何凭证都当合法，只给本地开发用
	IDPSandbox bool
	// IDPTimeout 单次渠道校验的上限
	IDPTimeout time.Duration
	// IDPBreakerFails 连续失败多少次熔断该渠道
	IDPBreakerFails int
	// IDPBreakerCooldown 熔断后多久放探针
	IDPBreakerCooldown time.Duration
	// TokenTTL 账号服务签发的登录票据有效期
	TokenTTL time.Duration

	// ---- 游戏配置 ----

	// GameConfPath 配置表路径，为空用内置默认值
	GameConfPath string
}

// Load 读环境变量并校验，svcName 由各服务 main 传
func Load(svcName string) (*Config, *ident.Identity, error) {
	svc, ok := ident.ParseSvcType(svcName)
	if !ok {
		return nil, nil, fmt.Errorf("未知服务名 %q", svcName)
	}

	serverID, err := envInt("SERVER_ID", -1)
	if err != nil {
		return nil, nil, err
	}
	if serverID < 0 {
		return nil, nil, fmt.Errorf("SERVER_ID 未配置：区服 ID 是运营概念，程序推不出来，必须手动配")
	}

	nodeSeq, err := envInt("NODE_SEQ", -1)
	if err != nil {
		return nil, nil, err
	}
	if nodeSeq < 0 {
		return nil, nil, fmt.Errorf("NODE_SEQ 未配置：进程序号要手动配，每个服务从 1 开始")
	}

	// 第一道越界校验在 ident.New 里做
	id, err := ident.New(serverID, svc, nodeSeq)
	if err != nil {
		return nil, nil, err
	}

	c := &Config{
		ServerID: serverID,
		NodeSeq:  nodeSeq,
		SvcName:  svcName,

		EtcdEndpoints: envList("ETCD_ENDPOINTS", []string{"127.0.0.1:2379"}),
		EtcdPrefix:    strings.TrimRight(envStr("ETCD_PREFIX", fmt.Sprintf("/game/s%d", serverID)), "/"),
		EtcdTimeout:   envDur("ETCD_TIMEOUT", 5*time.Second),
		RedisAddr:     envStr("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword: envStr("REDIS_PASSWORD", ""),
		NatsURL:       envStr("NATS_URL", "nats://127.0.0.1:4222"),

		LogLevel:      envStr("LOG_LEVEL", "info"),
		LogFormat:     envStr("LOG_FORMAT", "text"),
		MetricsAddr:   envStr("METRICS_ADDR", ":9100"),
		ListenAddr:    envStr("LISTEN_ADDR", ":7000"),
		AdvertiseAddr: envStr("ADVERTISE_ADDR", ""),
	}

	if c.RedisDB, err = envInt("REDIS_DB", 0); err != nil {
		return nil, nil, err
	}
	if c.RedisPoolSize, err = envInt("REDIS_POOL_SIZE", 64); err != nil {
		return nil, nil, err
	}

	sc, err := envInt("SHARD_COUNT", 1024)
	if err != nil {
		return nil, nil, err
	}
	if sc <= 0 || sc&(sc-1) != 0 {
		return nil, nil, fmt.Errorf("SHARD_COUNT 必须是正的 2 的幂，当前 %d", sc)
	}
	c.ShardCount = uint32(sc)

	c.ShardLeaseTTL = envDur("SHARD_LEASE_TTL", 8*time.Second)
	c.ShardKeepAlive = envDur("SHARD_KEEPALIVE", 2500*time.Millisecond)
	if c.ShardKeepAlive >= c.ShardLeaseTTL {
		return nil, nil, fmt.Errorf("SHARD_KEEPALIVE(%s) 必须显著小于 SHARD_LEASE_TTL(%s)", c.ShardKeepAlive, c.ShardLeaseTTL)
	}
	if c.ShardMaxOwn, err = envInt("SHARD_MAX_OWN", 0); err != nil {
		return nil, nil, err
	}
	if c.MailboxSize, err = envInt("MAILBOX_SIZE", 4096); err != nil {
		return nil, nil, err
	}
	if c.MailboxWarnPct, err = envInt("MAILBOX_WARN_PCT", 70); err != nil {
		return nil, nil, err
	}

	// 分片 lease 与 nodeID 心跳的阈值取舍是相反的，不要混用同一套
	c.NodeStaleTimeout = envDur("NODE_STALE_TIMEOUT", 10*time.Minute)
	c.NodeBeatInterval = envDur("NODE_BEAT_INTERVAL", 30*time.Second)
	if c.NodeBeatInterval >= c.NodeStaleTimeout {
		return nil, nil, fmt.Errorf("NODE_BEAT_INTERVAL(%s) 必须远小于 NODE_STALE_TIMEOUT(%s)", c.NodeBeatInterval, c.NodeStaleTimeout)
	}

	c.FlushL1Interval = envDur("FLUSH_L1_INTERVAL", 5*time.Second)
	c.FlushL2Interval = envDur("FLUSH_L2_INTERVAL", 60*time.Second)
	if c.FlushChanSize, err = envInt("FLUSH_CHAN_SIZE", 1024); err != nil {
		return nil, nil, err
	}
	if c.FlushWorkers, err = envInt("FLUSH_WORKERS", 8); err != nil {
		return nil, nil, err
	}
	if c.FlushBatchSize, err = envInt("FLUSH_BATCH_SIZE", 128); err != nil {
		return nil, nil, err
	}

	c.UnloadIdle = envDur("UNLOAD_IDLE", 10*time.Minute)
	c.SessionTTL = envDur("SESSION_TTL", 5*time.Minute)
	c.ShutdownGrace = envDur("SHUTDOWN_GRACE", 55*time.Second)

	c.TxScanInterval = envDur("TX_SCAN_INTERVAL", 30*time.Second)
	c.TxTimeout = envDur("TX_TIMEOUT", 60*time.Second)

	c.InternalSecret = envStr("INTERNAL_SECRET", "")
	c.LoginSecret = envStr("LOGIN_SECRET", "")
	c.AllowDevAuth = envBool("ALLOW_DEV_AUTH", false)
	c.PaymentSecret = envStr("PAYMENT_SECRET", "")
	c.PaymentSandbox = envBool("PAYMENT_SANDBOX", false)
	c.GMSecret = envStr("GM_SECRET", "")

	c.IDPSandbox = envBool("IDP_SANDBOX", false)
	c.IDPTimeout = envDur("IDP_TIMEOUT", 3*time.Second)
	if c.IDPBreakerFails, err = envInt("IDP_BREAKER_FAILS", 5); err != nil {
		return nil, nil, err
	}
	c.IDPBreakerCooldown = envDur("IDP_BREAKER_COOLDOWN", 10*time.Second)
	c.TokenTTL = envDur("TOKEN_TTL", 30*time.Minute)
	c.GameConfPath = envStr("GAME_CONF", "")

	// 网关拿到内部密钥，那道物理隔离就没了。单机 compose 共用一份 env 很常见，
	// 所以这里只告警不阻断，但生产必须分开，得让它显眼
	if svcName == "gateway" && c.InternalSecret != "" {
		fmt.Fprintln(os.Stderr,
			"[警告] 网关进程持有 INTERNAL_SECRET：生产环境必须把它从网关的 env 中移除，"+
				"否则网关一旦被攻破就能签发内部命令")
	}

	return c, id, nil
}

// EtcdKey 拼一个带前缀的 etcd 键
func (c *Config) EtcdKey(parts ...string) string {
	return c.EtcdPrefix + "/" + strings.Join(parts, "/")
}

func envStr(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) (int, error) {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("环境变量 %s=%q 不是合法整数: %w", k, v, err)
	}
	return n, nil
}

func envDur(k string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return def
	}
	return d
}

func envBool(k string, def bool) bool {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}

func envList(k string, def []string) []string {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}
