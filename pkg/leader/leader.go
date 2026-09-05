// Package leader 实现 etcd 选主，用于「全局唯一逻辑」的主备服务（§2.1 World）。
//
// 与分片认领是同一套机制的退化形式：分片空间只有一个格子。
// 因此阈值取舍也一致 —— lease 短（秒级），丢租立即停止处理。
package leader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/logx"
)

// Record 是写入 etcd 的 leader 记录。
type Record struct {
	Node  string `json:"node"`
	Epoch int64  `json:"epoch"` // etcd revision，单调递增
	Since int64  `json:"since"`
}

// Hooks 是选主回调。
type Hooks struct {
	// OnElected 当选后调用：订阅 subject、加载状态、开始服务。
	// 返回错误会放弃本次当选并重新参选。
	OnElected func(ctx context.Context, epoch int64) error
	// OnResigned 失去领导权时调用：立即停止处理、退订。
	OnResigned func(reason string)
}

// Elector 是选主器。
type Elector struct {
	cli    *clientv3.Client
	cfg    *config.Config
	key    string
	nodeID string
	hooks  Hooks
	ttl    time.Duration
	beat   time.Duration

	mu       sync.RWMutex
	isLeader bool
	epoch    int64
	lease    clientv3.LeaseID

	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// New 构造选主器。name 是被选举的角色名，例如 "world"。
func New(cli *clientv3.Client, cfg *config.Config, name, nodeID string, hooks Hooks) *Elector {
	return &Elector{
		cli:    cli,
		cfg:    cfg,
		key:    cfg.EtcdKey("leader", name),
		nodeID: nodeID,
		hooks:  hooks,
		ttl:    cfg.ShardLeaseTTL,
		beat:   cfg.ShardKeepAlive,
	}
}

// Start 开始参选。
func (e *Elector) Start(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.wg.Add(1)
	go e.loop(cctx)
	return nil
}

// IsLeader 报告本实例当前是否是 leader。
func (e *Elector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

// Epoch 返回当选时的 epoch（etcd revision）。
func (e *Elector) Epoch() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.epoch
}

func (e *Elector) loop(ctx context.Context) {
	defer e.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := e.campaign(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			logx.Debug("参选失败，稍后重试", "key", e.key, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.beat):
		}
	}
}

// campaign 尝试当选；当选后阻塞续租直到失去领导权。
func (e *Elector) campaign(ctx context.Context) error {
	ttl := int64(e.ttl.Seconds())
	if ttl < 1 {
		ttl = 1
	}
	gctx, gcancel := context.WithTimeout(ctx, e.cfg.EtcdTimeout)
	gr, err := e.cli.Grant(gctx, ttl)
	gcancel()
	if err != nil {
		return fmt.Errorf("申请 leader lease 失败: %w", err)
	}

	rec := Record{Node: e.nodeID, Since: time.Now().UnixMilli()}
	val, _ := json.Marshal(rec)

	tctx, tcancel := context.WithTimeout(ctx, e.cfg.EtcdTimeout)
	resp, err := e.cli.Txn(tctx).
		If(clientv3.Compare(clientv3.CreateRevision(e.key), "=", 0)).
		Then(clientv3.OpPut(e.key, string(val), clientv3.WithLease(gr.ID))).
		Commit()
	tcancel()
	if err != nil {
		_, _ = e.cli.Revoke(context.Background(), gr.ID)
		return err
	}
	if !resp.Succeeded {
		_, _ = e.cli.Revoke(context.Background(), gr.ID)
		return e.waitForVacancy(ctx)
	}

	epoch := resp.Header.Revision
	rec.Epoch = epoch
	val, _ = json.Marshal(rec)
	if _, err := e.cli.Put(ctx, e.key, string(val), clientv3.WithLease(gr.ID)); err != nil {
		_, _ = e.cli.Revoke(context.Background(), gr.ID)
		return err
	}

	e.mu.Lock()
	e.isLeader, e.epoch, e.lease = true, epoch, gr.ID
	e.mu.Unlock()

	logx.Info("当选 leader", "key", e.key, "node", e.nodeID, "epoch", epoch)

	if e.hooks.OnElected != nil {
		if err := e.hooks.OnElected(ctx, epoch); err != nil {
			logx.Error("当选后初始化失败，主动让位", "key", e.key, "err", err)
			e.resign(ctx, "初始化失败")
			return err
		}
	}

	e.keepAlive(ctx, gr.ID)
	return nil
}

// waitForVacancy 等待现任 leader 的键消失。
func (e *Elector) waitForVacancy(ctx context.Context) error {
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	wch := e.cli.Watch(wctx, e.key)
	for resp := range wch {
		for _, ev := range resp.Events {
			if ev.Type == clientv3.EventTypeDelete {
				return nil
			}
		}
	}
	return nil
}

// keepAlive 续租，直到失败或 ctx 结束。
func (e *Elector) keepAlive(ctx context.Context, id clientv3.LeaseID) {
	t := time.NewTicker(e.beat)
	defer t.Stop()

	fails := 0
	for {
		select {
		case <-ctx.Done():
			e.resign(context.Background(), "进程退出")
			return
		case <-t.C:
			kctx, cancel := context.WithTimeout(ctx, e.beat)
			_, err := e.cli.KeepAliveOnce(kctx, id)
			cancel()
			if err == nil {
				fails = 0
				continue
			}
			fails++
			gone := errors.Is(err, rpctypes.ErrLeaseNotFound)
			logx.Error("leader 续租失败", "key", e.key, "fails", fails, "lease_gone", gone, "err", err)
			if fails >= 2 || gone {
				e.resign(context.Background(), "续租失败")
				return
			}
		}
	}
}

// resign 主动让位。
func (e *Elector) resign(ctx context.Context, reason string) {
	e.mu.Lock()
	if !e.isLeader {
		e.mu.Unlock()
		return
	}
	e.isLeader = false
	lease := e.lease
	e.lease = 0
	e.mu.Unlock()

	logx.Warn("失去领导权", "key", e.key, "reason", reason)
	if e.hooks.OnResigned != nil {
		e.hooks.OnResigned(reason)
	}
	if lease != 0 {
		rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, _ = e.cli.Revoke(rctx, lease)
		cancel()
	}
}

// Stop 停止参选并让位。
func (e *Elector) Stop(ctx context.Context) {
	e.once.Do(func() {
		if e.cancel != nil {
			e.cancel()
		}
	})
	e.resign(ctx, "优雅下线")
	e.wg.Wait()
}

// Current 查询当前 leader。
func (e *Elector) Current(ctx context.Context) (Record, bool, error) {
	gctx, cancel := context.WithTimeout(ctx, e.cfg.EtcdTimeout)
	defer cancel()
	resp, err := e.cli.Get(gctx, e.key)
	if err != nil {
		return Record{}, false, err
	}
	if len(resp.Kvs) == 0 {
		return Record{}, false, nil
	}
	var rec Record
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return Record{}, false, err
	}
	return rec, true, nil
}
