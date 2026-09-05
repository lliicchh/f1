// Package nodeid 实现 §3.4 第二道启动校验：nodeID 在 etcd 上的唯一性自检。
//
// 手写序号唯一的致命风险是撞号：复制 compose 配置忘改数字，两个进程用同一个 nodeID。
// 此时 etcd 会认为是同一个 owner 在续租，续租不会失败，两进程同时持有同一分片、
// epoch 还相同，fencing 也拦不住。这是全套设计里唯一没有兜底的洞，必须在启动时堵住。
//
//	key = {ETCD_PREFIX}/node/{svcName}/{nodeSeq}
//	val = {"pid":..., "host":..., "addr":..., "beat_at":...}
//
//	CAS 抢占：
//	  - key 不存在                    → 成功
//	  - key 存在但 beat_at 超时 10min → 成功（接管僵尸记录）
//	  - key 存在且心跳新鲜             → 打印冲突方 host/pid，拒绝启动
//
// 抢不到必须退出，绝不能降级用随机数或 hostname 哈希兜底 —— 那就是在生产重复 ID。
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

// ErrConflict 表示 nodeID 撞号：另一个心跳新鲜的进程正在使用同一序号。
var ErrConflict = errors.New("nodeID 撞号")

// ErrClockRegressed 表示本机时间早于该槽位上一任持有者的最后心跳。
//
// 当前方案下 workerID 绑定容器、不被复用，跨进程回拨本不存在（§3.6）；
// 这道校验是为 §3.7 迁移到自动分配（workerID 可复用）预留的钩子，成本几乎为零。
var ErrClockRegressed = errors.New("本机时钟早于该槽位上一任的最后心跳")

// Record 是写入 etcd 的节点注册记录。
type Record struct {
	NodeID   string `json:"node_id"`
	PID      int    `json:"pid"`
	Host     string `json:"host"`
	Addr     string `json:"addr"`
	WorkerID uint32 `json:"worker_id"`
	StartAt  int64  `json:"start_at"` // 毫秒
	BeatAt   int64  `json:"beat_at"`  // 毫秒
	// LastTS 记录该槽位上一次心跳时的本机时间，用于 workerID 复用后的跨进程回拨校验（§3.7）。
	LastTS int64 `json:"last_ts"`
}

func (r Record) String() string {
	return fmt.Sprintf("node=%s host=%s pid=%d addr=%s beat_at=%s",
		r.NodeID, r.Host, r.PID, r.Addr, time.UnixMilli(r.BeatAt).Format(time.RFC3339))
}

// Registration 表示一次成功的注册，持有心跳协程。
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

// Key 返回注册键。
func Key(cfg *config.Config, id *ident.Identity) string {
	return cfg.EtcdKey("node", id.SvcName(), fmt.Sprintf("%d", id.NodeSeq))
}

// Claim 执行启动自检并抢占 nodeID 槽位。
//
// 成功后返回 Registration，调用方必须在进程退出前调用 Release（§10.3 第 5 步）。
// 失败（尤其是 ErrConflict）时调用方必须直接退出进程，不得降级。
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

	// 最多重试几次，应对「刚好在僵尸记录被别人接管」的竞争。
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		rev, err := r.tryClaim(ctx)
		if err == nil {
			r.rev = rev
			r.start()
			logx.Info("nodeID 自检通过，槽位已抢占",
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

// tryClaim 做一次 CAS 抢占，返回写入后的 revision。
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

	// 情况一：key 不存在 → 直接创建。
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

	// key 已存在，检查心跳新鲜度。
	getResp := resp.Responses[0].GetResponseRange()
	if len(getResp.Kvs) == 0 {
		return 0, errors.New("key 刚被删除，重试")
	}
	kv := getResp.Kvs[0]

	var old Record
	if err := json.Unmarshal(kv.Value, &old); err != nil {
		// 记录损坏：按僵尸处理，但要留痕。
		logx.Warn("nodeID 注册记录无法解析，按僵尸记录接管", "key", r.key, "raw", string(kv.Value), "err", err)
		old = Record{BeatAt: 0}
	}

	age := time.Since(time.UnixMilli(old.BeatAt))
	if old.BeatAt > 0 && age < r.timeout {
		// 情况三：心跳新鲜 → 撞号，拒绝启动。
		metrics.NodeIDConflict.Inc()
		return 0, fmt.Errorf("%w: %s/%d 已被占用 —— 冲突方 host=%s pid=%d addr=%s，"+
			"最后心跳 %s 前（阈值 %s）。请检查 compose 中 NODE_SEQ 是否重复；"+
			"绝不能降级用随机数或 hostname 哈希兜底",
			ErrConflict, r.id.SvcName(), r.id.NodeSeq, old.Host, old.PID, old.Addr,
			age.Round(time.Second), r.timeout)
	}

	// 情况二：僵尸记录 → 接管。先做时钟回拨校验（§3.7 前瞻）。
	if old.LastTS > 0 && time.Now().UnixMilli() < old.LastTS {
		return 0, fmt.Errorf("%w: 上一任 last_ts=%s，本机 now=%s，拒绝启动以免生成重复雪花 ID",
			ErrClockRegressed,
			time.UnixMilli(old.LastTS).Format(time.RFC3339Nano),
			time.Now().Format(time.RFC3339Nano))
	}

	logx.Warn("接管僵尸 nodeID 记录",
		"key", r.key, "stale_owner", old.String(), "age", age.Round(time.Second), "threshold", r.timeout)

	// 用 ModRevision CAS 接管，避免与另一个同时启动的进程双双成功。
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

// start 启动心跳协程：每 interval 更新一次 beat_at（§3.4）。
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
					// 心跳连续失败说明记录被别人改写（撞号）或 etcd 长时间不可用。
					// 与分片 lease 不同，这里回收慢的代价只是浪费槽位，因此容忍多次重试。
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

	// CAS：只有当记录仍是我上次写的那一版时才更新。
	// 若别的进程改写了这个 key，说明发生了撞号，必须暴露而不是覆盖。
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
		return fmt.Errorf("%w: 注册键被他人改写 —— %s", ErrConflict, detail)
	}

	r.mu.Lock()
	r.rev = resp.Header.Revision
	r.mu.Unlock()
	return nil
}

// Release 停止心跳并删除注册键（§10.3 第 5 步：进程正常退出时主动删除）。
func (r *Registration) Release(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	// 必须先停掉心跳协程再读 rev：心跳每次成功都会推进 ModRevision，
	// 先读 rev 再停会拿到过期版本，删除的 CAS 就会失败，注册键被留在 etcd 里，
	// 下次启动要白等一个僵尸阈值（10 分钟）。
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}

	r.mu.Lock()
	rev := r.rev
	r.mu.Unlock()

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 只删自己写的那一版，避免误删已被别人接管的槽位。
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

// Record 返回当前注册记录的快照。
func (r *Registration) Record() Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rec
}

// CountLive 统计某服务当前存活的实例数（心跳新鲜的注册记录数）。
//
// 分片认领用它计算「公平份额」：新实例上线后老实例才知道该让出多少分片。
func CountLive(ctx context.Context, cli *clientv3.Client, cfg *config.Config, svcName string, stale time.Duration) int {
	prefix := cfg.EtcdKey("node", svcName) + "/"
	gctx, cancel := context.WithTimeout(ctx, cfg.EtcdTimeout)
	defer cancel()

	resp, err := cli.Get(gctx, prefix, clientv3.WithPrefix())
	if err != nil {
		logx.Warn("统计存活实例失败", "svc", svcName, "err", err)
		return 0
	}

	// 存活判定用「心跳间隔的若干倍」，而不是 §3.4 的 10min 僵尸阈值：
	// 那个阈值是为「拒绝启动」服务的，故意取得很保守；
	// 这里只是算份额，判早了最多多一次再平衡，判晚了却会让新实例长期空转。
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

// ListLive 返回存活实例的注册记录，便于运维查看。
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
