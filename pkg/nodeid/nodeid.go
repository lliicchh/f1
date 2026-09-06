// Package nodeid 在 etcd 上给进程序号做唯一性检查
//
// 手写序号最怕撞号：复制 compose 配置忘了改数字，两个进程用同一个 nodeID。
// 这时 etcd 以为是同一个 owner 在续租，续租不会失败，两个进程同时持有同一分片，
// epoch 还一样，fencing 也拦不住，只能在启动时拦住。
//
//	key = {ETCD_PREFIX}/node/{svcName}/{nodeSeq}
//	val = {"pid":..., "host":..., "addr":..., "beat_at":...}
//
// 键不存在就抢占成功；beat_at 超时按僵尸记录接管；心跳新鲜就拒绝启动。
// 抢不到就退出，不要降级用随机数或 hostname 哈希
package nodeid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/ident"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
)

// ErrConflict 表示撞号了，另一个心跳新鲜的进程正在用同一个序号
var ErrConflict = errors.New("nodeID 撞号")

// ErrClockRegressed 表示本机时间早于这个槽位上一任的最后心跳
//
// 现在 workerID 绑容器不复用，跨进程回拨本来不存在。留着这道校验是为了以后
// 改成自动分配，那时 workerID 会被复用，这个问题就真了
var ErrClockRegressed = errors.New("本机时钟早于该槽位上一任的最后心跳")

// Record 写入 etcd 的节点注册记录
type Record struct {
	NodeID   string `json:"node_id"`
	PID      int    `json:"pid"`
	Host     string `json:"host"`
	Addr     string `json:"addr"`
	WorkerID uint32 `json:"worker_id"`
	StartAt  int64  `json:"start_at"` // 毫秒
	BeatAt   int64  `json:"beat_at"`  // 毫秒
	// LastTS 记录该槽位上一次心跳时的本机时间，用于 workerID 复用后的跨进程回拨校验
	LastTS int64 `json:"last_ts"`
}

func (r Record) String() string {
	return fmt.Sprintf("node=%s host=%s pid=%d addr=%s beat_at=%s",
		r.NodeID, r.Host, r.PID, r.Addr, time.UnixMilli(r.BeatAt).Format(time.RFC3339))
}

// Registration 一次成功的注册，内部跑着心跳协程
type Registration struct {
	cli      *clientv3.Client
	key      string
	id       *ident.Identity
	rec      Record
	interval time.Duration
	timeout  time.Duration

	mu     sync.Mutex
	rev    int64 // 最近一次写入的 ModRevision，心跳用它做 CAS
	closed bool
	cancel context.CancelFunc
	done   chan struct{}
	onLost func(error)
}

func Key(cfg *config.Config, id *ident.Identity) string {
	return cfg.EtcdKey("node", id.SvcName(), fmt.Sprintf("%d", id.NodeSeq))
}

// Claim 做启动检查并抢占槽位
//
// 成功后要在退出前 Release。失败尤其是 ErrConflict 时必须直接退进程，不能降级
func Claim(ctx context.Context, cli *clientv3.Client, cfg *config.Config, id *ident.Identity, onLost func(error)) (*Registration, error) {
	key := Key(cfg, id)
	host, _ := os.Hostname()

	now := time.Now().UnixMilli()
	rec := Record{
		NodeID:   id.NodeID(),
		PID:      os.Getpid(),
		Host:     host,
		Addr:     cfg.AdvertiseAddr,
		WorkerID: id.WorkerID(),
		StartAt:  now,
		BeatAt:   now,
		LastTS:   now,
	}

	r := &Registration{
		cli:      cli,
		key:      key,
		id:       id,
		rec:      rec,
		interval: cfg.NodeBeatInterval,
		timeout:  cfg.NodeStaleTimeout,
		done:     make(chan struct{}),
		onLost:   onLost,
	}

	// 重试几次，应付僵尸记录刚好被别人接管的情况
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		rev, err := r.tryClaim(ctx)
		if err == nil {
			r.rev = rev
			r.start()
			logx.Info("nodeID 检查通过，槽位已抢占",
				"key", key, "node", id.NodeID(), "worker_id", id.WorkerID(), "rev", rev)
			return r, nil
		}
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrClockRegressed) {
			return nil, err // 不重试，直接失败
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	return nil, fmt.Errorf("nodeID 抢占失败: %w", lastErr)
}

// tryClaim 做一次 CAS 抢占，返回写入后的 revision
func (r *Registration) tryClaim(ctx context.Context) (int64, error) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	r.mu.Lock()
	r.rec.BeatAt = time.Now().UnixMilli()
	r.rec.LastTS = r.rec.BeatAt
	val, err := json.Marshal(r.rec)
	r.mu.Unlock()
	if err != nil {
		return 0, err
	}

	// 键不存在，直接创建
	resp, err := r.cli.Txn(cctx).
		If(clientv3.Compare(clientv3.CreateRevision(r.key), "=", 0)).
		Then(clientv3.OpPut(r.key, string(val))).
		Else(clientv3.OpGet(r.key)).
		Commit()
	if err != nil {
		return 0, fmt.Errorf("etcd 事务失败: %w", err)
	}
	if resp.Succeeded {
		return resp.Header.Revision, nil
	}

	// 键已存在，看看心跳新不新鲜
	getResp := resp.Responses[0].GetResponseRange()
	if len(getResp.Kvs) == 0 {
		return 0, errors.New("key 刚被删除，重试")
	}
	kv := getResp.Kvs[0]

	var old Record
	if err := json.Unmarshal(kv.Value, &old); err != nil {
		// 记录坏了，当僵尸处理，但要留个日志
		logx.Warn("nodeID 注册记录无法解析，按僵尸记录接管", "key", r.key, "raw", string(kv.Value), "err", err)
		old = Record{BeatAt: 0}
	}

	age := time.Since(time.UnixMilli(old.BeatAt))
	if old.BeatAt > 0 && age < r.timeout {
		// 心跳还新鲜，那就是撞号了
		metrics.NodeIDConflict.Inc()
		return 0, fmt.Errorf("%w: %s/%d 已被占用，冲突方 host=%s pid=%d addr=%s，"+
			"最后心跳 %s 前（阈值 %s）。请检查 compose 中 NODE_SEQ 是否重复；"+
			"绝不能降级用随机数或 hostname 哈希兜底",
			ErrConflict, r.id.SvcName(), r.id.NodeSeq, old.Host, old.PID, old.Addr,
			age.Round(time.Second), r.timeout)
	}

	// 僵尸记录，可以接管。接管前先看一眼时钟有没有倒退
	if old.LastTS > 0 && time.Now().UnixMilli() < old.LastTS {
		return 0, fmt.Errorf("%w: 上一任 last_ts=%s，本机 now=%s，拒绝启动以免生成重复雪花 ID",
			ErrClockRegressed,
			time.UnixMilli(old.LastTS).Format(time.RFC3339Nano),
			time.Now().Format(time.RFC3339Nano))
	}

	logx.Warn("接管僵尸 nodeID 记录",
		"key", r.key, "stale_owner", old.String(), "age", age.Round(time.Second), "threshold", r.timeout)

	// 用 ModRevision 做 CAS，免得两个同时启动的进程都以为自己接管成功
	tres, err := r.cli.Txn(cctx).
		If(clientv3.Compare(clientv3.ModRevision(r.key), "=", kv.ModRevision)).
		Then(clientv3.OpPut(r.key, string(val))).
		Commit()
	if err != nil {
		return 0, fmt.Errorf("接管僵尸记录事务失败: %w", err)
	}
	if !tres.Succeeded {
		return 0, errors.New("接管僵尸记录时被抢先，重试")
	}
	return tres.Header.Revision, nil
}

// start 起心跳协程，定期刷 beat_at
func (r *Registration) start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() {
		defer close(r.done)
		t := time.NewTicker(r.interval)
		defer t.Stop()
		fails := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := r.beat(ctx); err != nil {
					fails++
					logx.Error("nodeID 心跳失败", "key", r.key, "fails", fails, "err", err)
					// 连续失败要么是被别人改写了（撞号），要么是 etcd 挂久了。
					// 这里回收慢只是浪费一个槽位，所以可以多试几次
					if fails >= 5 && r.onLost != nil {
						r.onLost(err)
						return
					}
				} else {
					fails = 0
				}
			}
		}
	}()
}

func (r *Registration) beat(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	now := time.Now().UnixMilli()
	r.rec.BeatAt = now
	if now > r.rec.LastTS {
		r.rec.LastTS = now
	}
	val, err := json.Marshal(r.rec)
	rev := r.rev
	r.mu.Unlock()
	if err != nil {
		return err
	}

	// 只有记录还是我上次写的那一版才更新。被别人改过就说明撞号了，
	// 这时要暴露出来，不能覆盖过去
	resp, err := r.cli.Txn(cctx).
		If(clientv3.Compare(clientv3.ModRevision(r.key), "=", rev)).
		Then(clientv3.OpPut(r.key, string(val))).
		Else(clientv3.OpGet(r.key)).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		metrics.NodeIDConflict.Inc()
		detail := "key 已被删除"
		if g := resp.Responses[0].GetResponseRange(); len(g.Kvs) > 0 {
			var other Record
			if json.Unmarshal(g.Kvs[0].Value, &other) == nil {
				detail = other.String()
			}
		}
		return fmt.Errorf("%w: 注册键被他人改写，%s", ErrConflict, detail)
	}

	r.mu.Lock()
	r.rev = resp.Header.Revision
	r.mu.Unlock()
	return nil
}

// Release 停心跳并删掉注册键
func (r *Registration) Release(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	// 得先停心跳再读 rev。心跳每成功一次就推进 ModRevision，
	// 先读后停会拿到旧版本，删除的 CAS 失败，键留在 etcd 里，
	// 下次启动就要白等一个僵尸阈值
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}

	r.mu.Lock()
	rev := r.rev
	r.mu.Unlock()

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 只删自己写的那一版，别误删已经被别人接管的槽位
	resp, err := r.cli.Txn(cctx).
		If(clientv3.Compare(clientv3.ModRevision(r.key), "=", rev)).
		Then(clientv3.OpDelete(r.key)).
		Commit()
	if err != nil {
		return fmt.Errorf("删除 nodeID 注册键失败: %w", err)
	}
	if !resp.Succeeded {
		logx.Warn("nodeID 注册键已被他人接管，跳过删除", "key", r.key)
		return nil
	}
	logx.Info("nodeID 注册键已删除", "key", r.key)
	return nil
}

// Record 返回注册记录的快照
func (r *Registration) Record() Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rec
}

// CountLive 数一下某个服务有几个实例还活着，分片认领拿它算公平份额
func CountLive(ctx context.Context, cli *clientv3.Client, cfg *config.Config, svcName string, stale time.Duration) int {
	prefix := cfg.EtcdKey("node", svcName) + "/"
	gctx, cancel := context.WithTimeout(ctx, cfg.EtcdTimeout)
	defer cancel()

	resp, err := cli.Get(gctx, prefix, clientv3.WithPrefix())
	if err != nil {
		logx.Warn("统计存活实例失败", "svc", svcName, "err", err)
		return 0
	}

	// 用心跳间隔的几倍判存活，不用那个 10 分钟的僵尸阈值。后者是为拒绝启动服务的，
	// 故意很保守。这里只是算份额，判早了最多多一次再平衡，判晚了新实例要空转很久
	fresh := 3 * cfg.NodeBeatInterval
	if fresh > stale {
		fresh = stale
	}

	n := 0
	for _, kv := range resp.Kvs {
		var rec Record
		if json.Unmarshal(kv.Value, &rec) != nil {
			continue
		}
		if time.Since(time.UnixMilli(rec.BeatAt)) <= fresh {
			n++
		}
	}
	return n
}

// ListLive 列出注册记录，给运维看
func ListLive(ctx context.Context, cli *clientv3.Client, cfg *config.Config, svcName string) ([]Record, error) {
	prefix := cfg.EtcdKey("node", svcName) + "/"
	gctx, cancel := context.WithTimeout(ctx, cfg.EtcdTimeout)
	defer cancel()

	resp, err := cli.Get(gctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var rec Record
		if json.Unmarshal(kv.Value, &rec) != nil {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}
