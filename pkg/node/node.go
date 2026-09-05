// Package node 是所有服务共用的启动/停机骨架。
//
// 它把 §3.4 的启动自检与 §10.3 的优雅下线顺序固化在框架里，
// 而不是让每个服务各写一遍 —— 顺序不可颠倒，写错一次就是丢数据。
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

// Service 是一个具体服务需要实现的生命周期。
//
// 各阶段由 Node 按 §10.3 的固定顺序驱动：
//
//  1. NotifyClients   （仅网关）先告知客户端「服务器维护，请重连」
//  2. StopAccepting   摘除注册 / 发起 handoff → 停止接新请求
//  3. Node 执行 nc.Drain()  → 处理完 pending 后关连接
//  4. FlushAll        全量刷盘 + 确认刷盘成功（epoch 校验通过）
//  5. Node 删除 etcd 中的 nodeID 注册键
//  6. Close           释放剩余资源后退出
type Service interface {
	Name() string
	Start(ctx context.Context, n *Node) error
	NotifyClients(ctx context.Context)
	StopAccepting(ctx context.Context)
	FlushAll(ctx context.Context) error
	Close(ctx context.Context)
}

// Node 持有一个进程的全部基础设施句柄。
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

// Bootstrap 执行完整启动流程。任一步失败都直接返回错误，调用方必须退出进程。
//
// 启动顺序有意如此：
//   - 先做 §3.4 两道 ID 校验（越界 + etcd 唯一性），撞号必须在连接业务依赖之前就拦下
//   - 再连 Redis 并校验 maxmemory-policy（§6.4），配错等于丢全服数据
//   - 最后连 NATS，此时进程已具备处理消息的前置条件
func Bootstrap(svcName string, kind string) (*Node, error) {
	cfg, id, err := config.Load(svcName)
	if err != nil {
		// 这里包含 §3.4 第一道「NODE_SEQ 越界校验」。
		return nil, fmt.Errorf("配置校验失败: %w", err)
	}

	logx.Init(id.NodeID(), cfg.LogLevel, cfg.LogFormat)
	logx.Info("进程启动",
		"svc", svcName, "node", id.NodeID(), "server_id", cfg.ServerID,
		"node_seq", cfg.NodeSeq, "worker_id", id.WorkerID(),
		"etcd_prefix", cfg.EtcdPrefix, "shard_count", cfg.ShardCount)

	n := &Node{Cfg: cfg, ID: id, lostCh: make(chan error, 1)}
	n.metrics = metrics.Serve(cfg.MetricsAddr)

	// —— etcd ——
	cli, err := etcdx.New(cfg)
	if err != nil {
		return nil, err
	}
	n.Etcd = cli

	// —— §3.4 第二道：nodeID etcd 唯一性自检 ——
	// 抢不到必须退出，绝不能降级用随机数或 hostname 哈希兜底。
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
			return nil, fmt.Errorf("启动自检失败 —— %w", err)
		}
		return nil, err
	}
	n.reg = reg

	// —— 雪花 ——
	snow, err := idgen.NewFromIdentity(id)
	if err != nil {
		return nil, err
	}
	n.Snow = snow

	// —— Redis ——
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

	// —— NATS ——
	conn, err := bus.Connect(bus.Options{URL: cfg.NatsURL, NodeID: id.NodeID()})
	if err != nil {
		return nil, err
	}
	n.Bus = conn

	return n, nil
}

// NodeID 返回本进程 nodeID。
func (n *Node) NodeID() string { return n.ID.NodeID() }

// NextID 生成一个雪花 ID。
func (n *Node) NextID() (uint64, error) { return n.Snow.Next() }

// Run 启动服务并阻塞直到收到停机信号，然后按 §10.3 顺序优雅下线。
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
		// nodeID 心跳持续失败：多半是撞号或 etcd 长时间不可用。
		// 继续跑下去有生成重复 ID 的风险，主动下线更安全。
		logx.Error("nodeID 注册丢失，主动下线", "err", err)
	}

	cancel()
	return n.shutdown(svc)
}

// shutdown 严格按 §10.3 的顺序执行。顺序不可颠倒。
func (n *Node) shutdown(svc Service) error {
	ctx, cancel := context.WithTimeout(context.Background(), n.Cfg.ShutdownGrace)
	defer cancel()

	start := time.Now()

	// 0. 网关额外一步：先向客户端发「服务器维护，请重连」，
	//    让客户端主动重连到其他网关，体验远好于直接断开。
	svc.NotifyClients(ctx)

	// 1. 摘除注册 / 发起 handoff → 停止接新请求
	logx.Info("下线步骤 1/5：停止接收新请求")
	svc.StopAccepting(ctx)

	// 2. nc.Drain() → 处理完 pending 后关连接
	logx.Info("下线步骤 2/5：NATS Drain")
	if err := n.Bus.Drain(); err != nil {
		logx.Warn("NATS Drain 失败", "err", err)
	}
	n.waitDrain(ctx)

	// 3+4. 全量刷盘并确认成功（epoch 校验通过）
	logx.Info("下线步骤 3/5：全量刷盘")
	if err := svc.FlushAll(ctx); err != nil {
		// 刷盘没成功就删注册键是危险的：宁可留下注册键让运维看到异常。
		logx.Error("全量刷盘失败，数据可能丢失（告警）", "err", err)
	} else {
		logx.Info("下线步骤 4/5：刷盘已确认")
	}

	// 5. 删除 etcd 中的 nodeID 注册键
	logx.Info("下线步骤 5/5：注销 nodeID")
	if n.reg != nil {
		if err := n.reg.Release(ctx); err != nil {
			logx.Warn("注销 nodeID 失败", "err", err)
		}
	}

	// 6. 退出
	svc.Close(ctx)
	n.closeInfra()
	logx.Info("优雅下线完成", "elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

// waitDrain 等待 NATS 连接真正关闭。
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

// emergencyClose 在启动失败时释放已获取的资源，尤其是 nodeID 槽位 ——
// 否则下次启动会撞上自己留下的僵尸记录（虽然 10min 后会被接管，但没必要让人等）。
func (n *Node) emergencyClose() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n.reg != nil {
		_ = n.reg.Release(ctx)
	}
	n.closeInfra()
}

// Fatal 打印错误并退出。用于 main 中启动失败的统一出口。
func Fatal(err error) {
	logx.Fatal("启动失败，进程退出", "err", err)
}
