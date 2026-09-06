// Package etcdx 建 etcd 客户端
package etcdx

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/gamedev/f1/pkg/config"
)

// New 按配置建客户端，顺带探一下连通性
func New(cfg *config.Config) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            cfg.EtcdEndpoints,
		DialTimeout:          cfg.EtcdTimeout,
		DialKeepAliveTime:    10 * time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
		AutoSyncInterval:     5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd 连接失败: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.EtcdTimeout)
	defer cancel()
	if _, err := cli.Status(ctx, cfg.EtcdEndpoints[0]); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("etcd 探测失败 (%s): %w", cfg.EtcdEndpoints[0], err)
	}
	return cli, nil
}
