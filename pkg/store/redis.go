package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/logx"
)

// NewRedis 创建 Redis 客户端并执行 §6.4 的关键配置校验。
//
// Redis 是唯一持久化后端，配错等于丢全服数据：
//   - maxmemory-policy 必须 noeviction。设 LRU 会在内存满时静默淘汰玩家数据
//   - appendonly yes + appendfsync everysec
//
// 默认严格模式：maxmemory-policy 不是 noeviction 时拒绝启动。
// 托管 Redis 禁用 CONFIG 命令时只告警不阻断（拿不到就没法判断）。
func NewRedis(ctx context.Context, cfg *config.Config) (redis.UniversalClient, error) {
	addrs := strings.Split(cfg.RedisAddr, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:           addrs,
		Password:        cfg.RedisPassword,
		DB:              cfg.RedisDB,
		PoolSize:        cfg.RedisPoolSize,
		MinIdleConns:    cfg.RedisPoolSize / 8,
		DialTimeout:     3 * time.Second,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    3 * time.Second,
		PoolTimeout:     4 * time.Second,
		MaxRetries:      3,
		MinRetryBackoff: 20 * time.Millisecond,
		MaxRetryBackoff: 300 * time.Millisecond,
	})

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("Redis 连接失败 (%s): %w", cfg.RedisAddr, err)
	}

	if err := VerifyConfig(ctx, rdb, strictRedis()); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return rdb, nil
}

func strictRedis() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("REDIS_REQUIRE_NOEVICTION")))
	return v != "false" && v != "0" && v != "no"
}

// VerifyConfig 校验 Redis 关键配置（§6.4）。strict 为真时 noeviction 不满足即报错。
func VerifyConfig(ctx context.Context, rdb redis.UniversalClient, strict bool) error {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	policy, err := configGet(cctx, rdb, "maxmemory-policy")
	if err != nil {
		logx.Warn("无法读取 Redis maxmemory-policy（CONFIG 可能被禁用），跳过校验",
			"err", err, "requirement", "必须为 noeviction，否则内存满时会静默淘汰玩家数据")
	} else if policy != "noeviction" {
		msg := fmt.Sprintf("Redis maxmemory-policy=%q，必须为 noeviction —— "+
			"设 LRU/LFU 会在内存满时静默淘汰玩家数据，等于丢档（§6.4）", policy)
		if strict {
			return fmt.Errorf("%s；确认要放行请设 REDIS_REQUIRE_NOEVICTION=false", msg)
		}
		logx.Error("Redis 配置危险（告警）", "detail", msg)
	} else {
		logx.Info("Redis maxmemory-policy 校验通过", "policy", policy)
	}

	if ao, err := configGet(cctx, rdb, "appendonly"); err == nil {
		if ao != "yes" {
			logx.Error("Redis appendonly 未开启（告警）：崩溃将丢失自上次 RDB 以来的全部数据，"+
				"要求 appendonly yes + appendfsync everysec（§6.4）", "appendonly", ao)
		} else if fs, err := configGet(cctx, rdb, "appendfsync"); err == nil && fs != "everysec" {
			logx.Warn("Redis appendfsync 非 everysec", "appendfsync", fs, "recommend", "everysec")
		}
	}
	return nil
}

func configGet(ctx context.Context, rdb redis.UniversalClient, param string) (string, error) {
	res, err := rdb.ConfigGet(ctx, param).Result()
	if err != nil {
		return "", err
	}
	v, ok := res[param]
	if !ok {
		return "", fmt.Errorf("参数 %s 不存在", param)
	}
	return strings.ToLower(strings.TrimSpace(v)), nil
}
