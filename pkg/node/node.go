// Package node 各服务共用的启动和停机骨架
//
// 启动检查和下线顺序放在这里统一做，不让每个服务各写一遍，顺序写错就是数据丢失
package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/etcdx"
	"github.com/gamedev/f1/pkg/ident"
	"github.com/gamedev/f1/pkg/idgen"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/nodeid"
	"github.com/gamedev/f1/pkg/store"
)

// Service 各服务要实现的生命周期，由 Node 按固定顺序驱动：
//
//	NotifyClients   网关先告诉客户端去重连别的网关
//	StopAccepting   摘注册、发起交接，停止接新请求
//	（Node 做 Drain）
//	FlushAll        全量刷盘并确认成功
//	（Node 删 nodeID 注册键）
//	Close           收尾
type Service interface {
	Name() string
	Start(ctx context.Context, n *Node) error
	NotifyClients(ctx context.Context)
	StopAccepting(ctx context.Context)
	FlushAll(ctx context.Context) error
	Close(ctx context.Context)
}

// Node 持有一个进程的基础设施句柄
type Node struct {
	Cfg   *config.Config
	ID    *ident.Identity
	Etcd  *clientv3.Client
	Bus   *bus.Conn
	Redis redis.UniversalClient
	Keys  *store.Keys
	Snow  *idgen.Generator

	reg     *nodeid.Registration
	metrics *http.Server
	lostCh  chan error
}

// Bootstrap 走完整个启动流程，任何一步失败都要退进程
//
// 顺序是有讲究的：先做两道 ID 校验，撞号得在碰业务依赖之前就拦下；
// 再连 Redis 顺带校验 maxmemory-policy；最后才连 NATS
func Bootstrap(svcName string, kind string) (*Node, error) {
	cfg, id, err := config.Load(svcName)
	if err != nil {
		// NODE_SEQ 越界校验在这里面
		return nil, fmt.Errorf("配置校验失败: %w", err)
	}

	logx.Init(id.NodeID(), cfg.LogLevel, cfg.LogFormat)
	logx.Info("进程启动",
		"svc", svcName, "node", id.NodeID(), "server_id", cfg.ServerID,
		"node_seq", cfg.NodeSeq, "worker_id", id.WorkerID(),
		"etcd_prefix", cfg.EtcdPrefix, "shard_count", cfg.ShardCount)

	n := &Node{Cfg: cfg, ID: id, lostCh: make(chan error, 1)}
	n.metrics = metrics.Serve(cfg.MetricsAddr)

	// etcd
	cli, err := etcdx.New(cfg)
	if err != nil {
		return nil, err
	}
	n.Etcd = cli

	// 第二道：nodeID 在 etcd 上的唯一性检查。
	// 抢不到就退出，不能降级用随机数或 hostname 哈希
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	reg, err := nodeid.Claim(ctx, cli, cfg, id, func(err error) {
		select {
		case n.lostCh <- err:
		default:
		}
	})
	cancel()
	if err != nil {
		_ = cli.Close()
		if errors.Is(err, nodeid.ErrConflict) {
			return nil, fmt.Errorf("启动检查失败: %w", err)
		}
		return nil, err
	}
	n.reg = reg

	// 雪花
	snow, err := idgen.NewFromIdentity(id)
	if err != nil {
		return nil, err
	}
	n.Snow = snow

	// Redis
	rctx, rcancel := context.WithTimeout(context.Background(), 15*time.Second)
	rdb, err := store.NewRedis(rctx, cfg)
	rcancel()
	if err != nil {
		return nil, err
	}
	n.Redis = rdb

	tagMode := store.HashTagMode(os.Getenv("REDIS_HASHTAG"))
	if tagMode == "" {
		tagMode = store.TagUID
	}
	n.Keys = store.NewKeys(tagMode, cfg.ShardCount, kind)

	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := store.EnsureScripts(sctx, rdb); err != nil {
		logx.Warn("预加载 Lua 脚本失败，运行时会回退到 EVAL", "err", err)
	}
	scancel()

	// NATS
	conn, err := bus.Connect(bus.Options{URL: cfg.NatsURL, NodeID: id.NodeID()})
	if err != nil {
		return nil, err
	}
	n.Bus = conn

	return n, nil
}

func (n *Node) NodeID() string { return n.ID.NodeID() }

func (n *Node) NextID() (uint64, error) { return n.Snow.Next() }

// Run 启动服务并阻塞，收到停机信号后按固定顺序下线
func (n *Node) Run(svc Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx, n); err != nil {
		n.emergencyClose()
		return fmt.Errorf("服务启动失败: %w", err)
	}
	logx.Info("服务已就绪", "svc", svc.Name(), "node", n.NodeID())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case s := <-sig:
		logx.Info("收到停机信号，开始优雅下线", "signal", s.String())
	case err := <-n.lostCh:
		// nodeID 心跳一直失败，多半是撞号或 etcd 挂太久。
		// 再跑下去有生成重复 ID 的风险，不如主动下线
		logx.Error("nodeID 注册丢失，主动下线", "err", err)
	}

	cancel()
	return n.shutdown(svc)
}

// shutdown 按固定顺序走，不能颠倒
func (n *Node) shutdown(svc Service) error {
	ctx, cancel := context.WithTimeout(context.Background(), n.Cfg.ShutdownGrace)
	defer cancel()

	start := time.Now()

	// 网关先让客户端去连别的网关，比直接断开体验好得多
	svc.NotifyClients(ctx)

	// 停止接新请求
	logx.Info("下线步骤 1/5：停止接收新请求")
	svc.StopAccepting(ctx)

	// 处理完 pending 再断连接
	logx.Info("下线步骤 2/5：NATS Drain")
	if err := n.Bus.Drain(); err != nil {
		logx.Warn("NATS Drain 失败", "err", err)
	}
	n.waitDrain(ctx)

	// 全量刷盘并确认成功
	logx.Info("下线步骤 3/5：全量刷盘")
	if err := svc.FlushAll(ctx); err != nil {
		// 刷盘没成就别删注册键，留着让运维看见异常
		logx.Error("全量刷盘失败，数据可能丢失（告警）", "err", err)
	} else {
		logx.Info("下线步骤 4/5：刷盘已确认")
	}

	// 注销 nodeID
	logx.Info("下线步骤 5/5：注销 nodeID")
	if n.reg != nil {
		if err := n.reg.Release(ctx); err != nil {
			logx.Warn("注销 nodeID 失败", "err", err)
		}
	}

	// 收尾
	svc.Close(ctx)
	n.closeInfra()
	logx.Info("优雅下线完成", "elapsed", time.Since(start).Round(time.Millisecond))
	logx.Sync()
	return nil
}

// waitDrain 等 NATS 连接真的关掉
func (n *Node) waitDrain(ctx context.Context) {
	deadline := time.Now().Add(10 * time.Second)
	for !n.Bus.IsClosed() && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
	}
}

func (n *Node) closeInfra() {
	if n.Bus != nil && !n.Bus.IsClosed() {
		n.Bus.Close()
	}
	if n.Redis != nil {
		_ = n.Redis.Close()
	}
	if n.Etcd != nil {
		_ = n.Etcd.Close()
	}
	if n.metrics != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = n.metrics.Shutdown(sctx)
		cancel()
	}
}

// emergencyClose 启动失败时把已经拿到的资源放掉
func (n *Node) emergencyClose() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n.reg != nil {
		_ = n.reg.Release(ctx)
	}
	n.closeInfra()
}

// Fatal 打印错误后退出，给 main 用
func Fatal(err error) {
	logx.Fatal("启动失败，进程退出", "err", err)
}
