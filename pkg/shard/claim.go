// Package shard 实现 §4 的分片模型：划分、认领、订阅、Actor 执行模型。
package shard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
)

// Kind 是分片空间。Lobby 与 Room 使用独立分片空间（§4.1）。
type Kind string

const (
	KindLobby Kind = "lobby"
	KindRoom  Kind = "room"
	// KindMatch 是匹配池的桶空间（模式 × 段位），不落盘。
	KindMatch Kind = "match"
)

// Of 计算实体所属分片。
//
//	lobbyShard(uid)   = uid % ShardCount
//	roomShard(roomID) = roomID % ShardCount
//
// ShardCount 是常量，一经确定永不变更 —— 分片数变更等价于全量迁移（§4.1 / D2）。
func Of(id uint64, shardCount uint32) uint32 { return uint32(id % uint64(shardCount)) }

// Record 是 etcd 中的分片认领记录：
//
//	/game/s1/shard/lobby/0007 → {"node":"s1-lobby-1", "since":...}
//
// epoch 不写进值里，而是直接取这个键的 CreateRevision ——
// 它就是「事务返回的 revision」，天然全局单调递增（§4.2），
// 而且省掉一次「先占位再回写 epoch」的额外写入，认领快一倍。
// 用 `etcdctl get --write-out=json` 可以看到 create_revision。
type Record struct {
	Node string `json:"node"`
	// Since 便于运维观察 owner 持有时长，不参与任何判定。
	Since int64 `json:"since"`
}

// Ownership 描述一个已认领的分片。
type Ownership struct {
	Kind  Kind
	Shard uint32
	Epoch int64 // 取 etcd 事务返回的 revision，天然全局单调递增（§4.2）
	Node  string
}

func (o Ownership) String() string {
	return fmt.Sprintf("%s/%04d@epoch=%d", o.Kind, o.Shard, o.Epoch)
}

// ReleaseReason 说明分片为何被释放。
type ReleaseReason string

const (
	// ReleaseGraceful 主动释放：handoff、再平衡、优雅下线。数据已刷盘。
	ReleaseGraceful ReleaseReason = "graceful"
	// ReleaseLeaseLost 续租失败：必须立即停止处理该分片消息（§4.2 软保护）。
	ReleaseLeaseLost ReleaseReason = "lease_lost"
	// ReleaseFenced epoch fencing 拒绝写入：内存已过期，丢弃内存、停止服务、告警（§7.2）。
	ReleaseFenced ReleaseReason = "fenced"
	// ReleaseStartFailed 认领后初始化失败，回滚认领。
	ReleaseStartFailed ReleaseReason = "start_failed"
)

// Hooks 是认领/释放的回调。均在 Claimer 自己的 goroutine 中调用。
type Hooks struct {
	// OnAcquire 在成功认领后调用：抬高 Redis epoch → 加载数据 → 订阅 subject（§10.2 步骤 5）。
	// 返回错误则回滚认领（删除 etcd 键）。
	OnAcquire func(ctx context.Context, o Ownership) error
	// OnRelease 在释放前调用：停止处理消息、刷盘（graceful 时）、清理内存。
	OnRelease func(ctx context.Context, o Ownership, reason ReleaseReason)
	// OnFreed 在本节点释放分片后通知其他节点尽快重扫（可选）。
	OnFreed func(kind Kind, shard uint32)
	// LiveNodes 返回同类服务当前存活实例数，用于计算公平份额。返回 <=0 表示未知。
	LiveNodes func(ctx context.Context) int
	// RequestHandoff 向当前 owner 发起主动交接（§10.2）。返回 nil 表示对方已 READY，
	// 分片已释放，可以立即抢占。未设置时退化为「持有过多的节点主动让出」。
	RequestHandoff func(ctx context.Context, shard uint32, fromNode string) error
}

// Claimer 负责分片的认领、续租与释放。
type Claimer struct {
	cli    *clientv3.Client
	cfg    *config.Config
	kind   Kind
	nodeID string
	hooks  Hooks

	prefix string
	// space 是分片空间大小。默认取 cfg.ShardCount（Lobby/Room 用 1024），
	// Match 这类按「模式×段位」分桶的服务可以传入自己的空间大小。
	space uint32

	mu      sync.RWMutex
	owned   map[uint32]*Ownership
	leaseID clientv3.LeaseID
	stopped bool

	scanInterval time.Duration
	rescan       chan struct{}

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewClaimer 构造分片认领器，使用 cfg.ShardCount 作为分片空间。
func NewClaimer(cli *clientv3.Client, cfg *config.Config, kind Kind, nodeID string, hooks Hooks) *Claimer {
	return NewClaimerWithSpace(cli, cfg, kind, nodeID, cfg.ShardCount, hooks)
}

// NewClaimerWithSpace 构造使用自定义分片空间的认领器。
func NewClaimerWithSpace(cli *clientv3.Client, cfg *config.Config, kind Kind, nodeID string, space uint32, hooks Hooks) *Claimer {
	if space == 0 {
		space = cfg.ShardCount
	}
	return &Claimer{
		space:        space,
		cli:          cli,
		cfg:          cfg,
		kind:         kind,
		nodeID:       nodeID,
		hooks:        hooks,
		prefix:       cfg.EtcdKey("shard", string(kind)) + "/",
		owned:        make(map[uint32]*Ownership),
		scanInterval: 3 * time.Second,
		rescan:       make(chan struct{}, 1),
	}
}

// Space 返回分片空间大小。
func (c *Claimer) Space() uint32 { return c.space }

// Key 返回某分片的 etcd 键，序号补零到 4 位，保证前缀扫描有序。
func (c *Claimer) Key(shard uint32) string { return c.prefix + fmt.Sprintf("%04d", shard) }

func (c *Claimer) shardFromKey(key string) (uint32, bool) {
	if len(key) <= len(c.prefix) {
		return 0, false
	}
	n, err := strconv.ParseUint(key[len(c.prefix):], 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

// Start 建立 lease 并开始认领循环。
func (c *Claimer) Start(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	if err := c.newLease(cctx); err != nil {
		cancel()
		return err
	}

	metrics.ShardMailboxCap.WithLabelValues(string(c.kind)).Set(float64(c.cfg.MailboxSize))

	c.wg.Add(1)
	go c.loop(cctx)
	return nil
}

// newLease 创建一个新的 lease 并启动续租。
//
// 全节点共用一个 lease：lease 死了意味着本进程整体失联，所有分片一起释放才是正确行为。
// 单个分片的主动释放通过删除键完成，不影响 lease。
func (c *Claimer) newLease(ctx context.Context) error {
	ttl := int64(c.cfg.ShardLeaseTTL.Seconds())
	if ttl < 1 {
		ttl = 1
	}
	lctx, cancel := context.WithTimeout(ctx, c.cfg.EtcdTimeout)
	defer cancel()

	gr, err := c.cli.Grant(lctx, ttl)
	if err != nil {
		return fmt.Errorf("申请分片 lease 失败: %w", err)
	}

	c.mu.Lock()
	c.leaseID = gr.ID
	c.mu.Unlock()

	logx.Info("分片 lease 已建立", "kind", c.kind, "lease", fmt.Sprintf("%x", gr.ID), "ttl_s", ttl)

	c.wg.Add(1)
	go c.keepAlive(ctx, gr.ID)
	return nil
}

// keepAlive 以固定间隔续租。
//
// 这里不用 clientv3 的自动 KeepAlive 流，而是自己按 ShardKeepAlive 间隔调用 KeepAliveOnce：
// 续租间隔（2.5s）与 TTL（8s）的比例是设计约定的一部分，显式控制更可控，
// 且失败时能立刻拿到错误 —— 「续租失败 → 立即停止处理该分片消息」（§4.2）。
func (c *Claimer) keepAlive(ctx context.Context, id clientv3.LeaseID) {
	defer c.wg.Done()
	t := time.NewTicker(c.cfg.ShardKeepAlive)
	defer t.Stop()

	fails := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			kctx, cancel := context.WithTimeout(ctx, c.cfg.ShardKeepAlive)
			_, err := c.cli.KeepAliveOnce(kctx, id)
			cancel()

			if err == nil {
				fails = 0
				continue
			}

			fails++
			logx.Error("分片 lease 续租失败", "kind", c.kind, "fails", fails, "err", err)

			// TTL 8s / 间隔 2.5s → 连续 2 次失败已逼近过期，必须立刻停手。
			// 宁可早停：软保护的意义就是在 lease 真正过期、别人接管之前主动退出。
			// lease 已被服务端回收时无需再等第二次。
			leaseGone := errors.Is(err, rpctypes.ErrLeaseNotFound)
			if fails >= 2 || leaseGone {
				metrics.ShardLeaseLost.WithLabelValues(string(c.kind)).Inc()
				logx.Error("分片 lease 判定丢失，立即释放全部分片（软保护，§7.2）",
					"kind", c.kind, "owned", c.Count(), "lease_gone", leaseGone)
				c.releaseAll(context.Background(), ReleaseLeaseLost)

				// 尝试重建 lease 后重新参与认领。
				if ctx.Err() != nil {
					return
				}
				if err := c.newLease(ctx); err != nil {
					logx.Error("重建分片 lease 失败，将在下次扫描重试", "err", err)
				}
				return
			}
		}
	}
}

// loop 是认领主循环：定期扫描空闲分片并抢占。
func (c *Claimer) loop(ctx context.Context) {
	defer c.wg.Done()

	// 启动时立刻扫一次。
	c.scanAndClaim(ctx)

	t := time.NewTicker(c.scanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.scanAndClaim(ctx)
		case <-c.rescan:
			c.scanAndClaim(ctx)
		}
	}
}

// TriggerRescan 请求尽快重扫（收到「某分片被释放」事件时调用）。
func (c *Claimer) TriggerRescan() {
	select {
	case c.rescan <- struct{}{}:
	default:
	}
}

// scanAndClaim 扫描分片空间，抢占空闲分片；持有过多时主动让出。
func (c *Claimer) scanAndClaim(ctx context.Context) {
	c.mu.RLock()
	stopped, lease := c.stopped, c.leaseID
	c.mu.RUnlock()
	if stopped || lease == 0 {
		return
	}

	gctx, cancel := context.WithTimeout(ctx, c.cfg.EtcdTimeout)
	resp, err := c.cli.Get(gctx, c.prefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		logx.Error("扫描分片失败", "kind", c.kind, "err", err)
		return
	}

	taken := make(map[uint32]Record, len(resp.Kvs))
	epochs := make(map[uint32]int64, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		s, ok := c.shardFromKey(string(kv.Key))
		if !ok {
			continue
		}
		var rec Record
		if json.Unmarshal(kv.Value, &rec) != nil {
			continue
		}
		taken[s] = rec
		epochs[s] = kv.CreateRevision
	}

	// 与本地视图对账：etcd 上已不属于我的分片，说明被抢走了（lease 过期后被接管）。
	c.reconcile(ctx, taken, epochs)

	target := c.targetCount(ctx)
	cur := c.Count()

	// 统计每个 owner 持有多少分片，供再平衡判断。
	byNode := make(map[string]int, 8)
	for _, rec := range taken {
		byNode[rec.Node]++
	}

	free := make([]uint32, 0, int(c.space)-len(taken))
	for s := uint32(0); s < c.space; s++ {
		if _, ok := taken[s]; !ok {
			free = append(free, s)
		}
	}

	switch {
	case cur < target && len(free) > 0:
		// 优先吃空闲分片：没有 owner 在服务它们，抢占没有任何不可用窗口。
		rand.Shuffle(len(free), func(i, j int) { free[i], free[j] = free[j], free[i] })
		got := c.claimMany(ctx, free, target-cur)
		if got > 0 {
			logx.Info("分片认领完成", "kind", c.kind, "acquired", got, "owned", c.Count(), "target", target)
		}

	case cur < target && len(free) == 0:
		// 分片已被瓜分完，但本节点不足份额（典型场景：扩容后新实例上线）。
		// 靠 lease 自然过期有 8~10s 不可用窗口，主动交接可压到百毫秒（§10.2），
		// 因此这里由「想要的一方」发起 handoff，而不是让持有方盲目让出。
		c.requestRebalance(ctx, taken, byNode, target, cur)

	case cur > target && c.hooks.RequestHandoff == nil:
		// 未接入 handoff 的服务退化为主动让出：动作同样是
		// 「停止处理 → 全量刷盘 → 释放锁」，只是新 owner 要等下一轮扫描才接手。
		if !c.someoneBelow(ctx, byNode, target) {
			return
		}
		shed := cur - target
		list := c.Owned()
		sort.Slice(list, func(i, j int) bool { return list[i] > list[j] })
		for i := 0; i < shed && i < len(list); i++ {
			if err := c.Release(ctx, list[i], ReleaseGraceful); err != nil {
				logx.Error("让出分片失败", "kind", c.kind, "shard", list[i], "err", err)
			}
		}
		logx.Info("分片再平衡：主动让出", "kind", c.kind, "shed", shed, "owned", c.Count(), "target", target)
	}
}

// maxHandoffPerRound 限制单轮主动交接数量，避免扩容瞬间大批分片同时抖动。
const maxHandoffPerRound = 8

// requestRebalance 向持有超额分片的节点发起主动交接。
func (c *Claimer) requestRebalance(ctx context.Context, taken map[uint32]Record, byNode map[string]int, target, cur int) {
	if c.hooks.RequestHandoff == nil {
		return
	}

	// 只找「持有数严格大于份额」的节点要分片。
	// 1024 无法被实例数整除时，各节点会稳定在 target 与 target-1 之间，
	// 这个条件保证不会出现「谁都想再要一个」的永久抖动。
	type cand struct {
		shard uint32
		node  string
		count int
	}
	var cands []cand
	for s, rec := range taken {
		if rec.Node == c.nodeID {
			continue
		}
		if byNode[rec.Node] > target {
			cands = append(cands, cand{shard: s, node: rec.Node, count: byNode[rec.Node]})
		}
	}
	if len(cands) == 0 {
		return
	}
	// 优先从最重的节点上要。
	sort.Slice(cands, func(i, j int) bool { return cands[i].count > cands[j].count })

	want := target - cur
	if want > maxHandoffPerRound {
		want = maxHandoffPerRound
	}

	taking := make(map[string]int, len(byNode))
	got := 0
	for _, cd := range cands {
		if got >= want || ctx.Err() != nil {
			break
		}
		// 别把同一个节点掏到低于份额。
		if byNode[cd.node]-taking[cd.node] <= target {
			continue
		}

		hctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := c.hooks.RequestHandoff(hctx, cd.shard, cd.node)
		cancel()
		if err != nil {
			logx.Warn("发起分片交接失败，本轮跳过",
				"kind", c.kind, "shard", cd.shard, "from", cd.node, "err", err)
			metrics.ShardHandoff.WithLabelValues(string(c.kind), "target", "fail").Inc()
			continue
		}

		// 对方已 READY 并释放，立刻抢占。
		if ok, cerr := c.claim(ctx, cd.shard); ok {
			taking[cd.node]++
			got++
			metrics.ShardHandoff.WithLabelValues(string(c.kind), "target", "ok").Inc()
		} else if cerr != nil {
			logx.Warn("交接后抢占失败", "kind", c.kind, "shard", cd.shard, "err", cerr)
		}
	}
	if got > 0 {
		logx.Info("分片再平衡：主动交接完成",
			"kind", c.kind, "acquired", got, "owned", c.Count(), "target", target)
	}
}

// someoneBelow 报告是否存在低于份额的存活节点（含尚未持有任何分片的新实例）。
func (c *Claimer) someoneBelow(ctx context.Context, byNode map[string]int, target int) bool {
	live := 0
	if c.hooks.LiveNodes != nil {
		live = c.hooks.LiveNodes(ctx)
	}
	if live > len(byNode) {
		return true // 有实例一个分片都没拿到
	}
	for node, n := range byNode {
		if node != c.nodeID && n < target {
			return true
		}
	}
	return false
}

// reconcile 处理「本地认为持有、etcd 上却不是我」的分片。
func (c *Claimer) reconcile(ctx context.Context, taken map[uint32]Record, epochs map[uint32]int64) {
	c.mu.RLock()
	stale := make([]*Ownership, 0)
	for s, o := range c.owned {
		rec, ok := taken[s]
		// epoch 变了说明键被重建过 —— 即使 node 名字相同（进程重启复用了 nodeID），
		// 那也是另一任 owner，本地这份内存已经不作数了。
		if !ok || rec.Node != c.nodeID || epochs[s] != o.Epoch {
			stale = append(stale, o)
		}
	}
	c.mu.RUnlock()

	for _, o := range stale {
		logx.Error("分片所有权已丢失（etcd 上不再是本节点），立即停止服务该分片",
			"kind", c.kind, "shard", o.Shard, "epoch", o.Epoch, "now_owner", taken[o.Shard].Node)
		metrics.ShardLeaseLost.WithLabelValues(string(c.kind)).Inc()
		c.dropLocal(ctx, o, ReleaseLeaseLost)
	}
}

// targetCount 计算本实例应持有的分片数。
func (c *Claimer) targetCount(ctx context.Context) int {
	if c.cfg.ShardMaxOwn > 0 {
		return c.cfg.ShardMaxOwn
	}
	n := 0
	if c.hooks.LiveNodes != nil {
		n = c.hooks.LiveNodes(ctx)
	}
	if n <= 0 {
		n = 1
	}
	total := int(c.space)
	return (total + n - 1) / n // 向上取整，保证 1024 能被全部覆盖
}

// claim 用 etcd 事务 CAS 抢占一个分片（§4.2）。
func (c *Claimer) claim(ctx context.Context, s uint32) (bool, error) {
	c.mu.RLock()
	lease := c.leaseID
	c.mu.RUnlock()
	if lease == 0 {
		return false, errors.New("lease 未就绪")
	}

	key := c.Key(s)
	val, _ := json.Marshal(Record{Node: c.nodeID, Since: time.Now().UnixMilli()})

	tctx, cancel := context.WithTimeout(ctx, c.cfg.EtcdTimeout)
	defer cancel()

	resp, err := c.cli.Txn(tctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(val), clientv3.WithLease(lease))).
		Commit()
	if err != nil {
		return false, fmt.Errorf("抢占分片 %d 事务失败: %w", s, err)
	}
	if !resp.Succeeded {
		return false, nil // 已被别人持有，正常情况
	}

	// 事务成功时 Header.Revision 即这个键的 CreateRevision，用它当 epoch。
	epoch := resp.Header.Revision
	o := &Ownership{Kind: c.kind, Shard: s, Epoch: epoch, Node: c.nodeID}

	c.mu.Lock()
	c.owned[s] = o
	c.mu.Unlock()
	metrics.ShardOwnerChanges.WithLabelValues(string(c.kind), "acquire").Inc()
	metrics.ShardOwned.WithLabelValues(string(c.kind)).Set(float64(c.Count()))

	// 抬高 Redis epoch → 加载数据 → 订阅 subject。失败则回滚。
	if c.hooks.OnAcquire != nil {
		if err := c.hooks.OnAcquire(ctx, *o); err != nil {
			logx.Error("分片初始化失败，回滚认领", "kind", c.kind, "shard", s, "epoch", epoch, "err", err)
			c.dropLocal(ctx, o, ReleaseStartFailed)
			_, _ = c.cli.Delete(context.Background(), key)
			return false, err
		}
	}

	logx.Debug("分片已认领", "kind", c.kind, "shard", s, "epoch", epoch)
	return true, nil
}

// claimConcurrency 是并发抢占的上限。
//
// 逐个串行抢占时每个分片要一次 etcd 往返，1024 个分片会拖到几十秒，
// 直接把 §13 承诺的「8~15s 接管」打穿。这些事务彼此独立，可以放心并发。
const claimConcurrency = 16

// claimMany 并发抢占一批分片，返回成功数。
func (c *Claimer) claimMany(ctx context.Context, shards []uint32, want int) int {
	if want <= 0 || len(shards) == 0 {
		return 0
	}

	var (
		mu      sync.Mutex
		got     int
		wg      sync.WaitGroup
		sem     = make(chan struct{}, claimConcurrency)
		stopped bool
	)

	for _, s := range shards {
		mu.Lock()
		if got >= want || stopped {
			mu.Unlock()
			break
		}
		mu.Unlock()
		if ctx.Err() != nil {
			break
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(shard uint32) {
			defer wg.Done()
			defer func() { <-sem }()

			mu.Lock()
			over := got >= want
			mu.Unlock()
			if over {
				return
			}

			ok, err := c.claim(ctx, shard)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				got++
				return
			}
			if err != nil && errors.Is(err, context.Canceled) {
				stopped = true
			}
		}(s)
	}
	wg.Wait()
	return got
}

// Release 主动释放一个分片：停止处理 → 刷盘 → 删除 etcd 键（§10.2 步骤 2~3）。
func (c *Claimer) Release(ctx context.Context, s uint32, reason ReleaseReason) error {
	c.mu.RLock()
	o, ok := c.owned[s]
	c.mu.RUnlock()
	if !ok {
		return nil
	}

	c.dropLocal(ctx, o, reason)

	dctx, cancel := context.WithTimeout(ctx, c.cfg.EtcdTimeout)
	defer cancel()
	if _, err := c.cli.Delete(dctx, c.Key(s)); err != nil {
		return fmt.Errorf("删除分片键失败: %w", err)
	}
	if c.hooks.OnFreed != nil {
		c.hooks.OnFreed(c.kind, s)
	}
	return nil
}

// dropLocal 只做本地清理（回调 + 移除记录），不碰 etcd。
func (c *Claimer) dropLocal(ctx context.Context, o *Ownership, reason ReleaseReason) {
	c.mu.Lock()
	if _, ok := c.owned[o.Shard]; !ok {
		c.mu.Unlock()
		return
	}
	delete(c.owned, o.Shard)
	c.mu.Unlock()

	if c.hooks.OnRelease != nil {
		c.hooks.OnRelease(ctx, *o, reason)
	}
	metrics.ShardOwnerChanges.WithLabelValues(string(c.kind), "release").Inc()
	metrics.ShardOwned.WithLabelValues(string(c.kind)).Set(float64(c.Count()))
	logx.Info("分片已释放", "kind", c.kind, "shard", o.Shard, "epoch", o.Epoch, "reason", reason)
}

// releaseAll 释放全部分片（lease 丢失或下线）。
func (c *Claimer) releaseAll(ctx context.Context, reason ReleaseReason) {
	for _, s := range c.Owned() {
		if reason == ReleaseGraceful {
			_ = c.Release(ctx, s, reason)
		} else {
			c.mu.RLock()
			o := c.owned[s]
			c.mu.RUnlock()
			if o != nil {
				c.dropLocal(ctx, o, reason)
			}
		}
	}
}

// Fence 因 epoch fencing 被拒而立即放弃某分片。
//
// 「旧 owner 收到拒绝 → 丢弃该分片内存、停止服务、告警。绝不重试」（§7.2）。
// 这里刻意不去删除 etcd 键：此刻该键要么已因 lease 过期消失，要么已属于新 owner，
// 删它反而可能误删别人的所有权。
func (c *Claimer) Fence(shard uint32) {
	c.mu.RLock()
	o, ok := c.owned[shard]
	c.mu.RUnlock()
	if !ok {
		return
	}
	c.dropLocal(context.Background(), o, ReleaseFenced)
}

// Owned 返回当前持有的分片列表。
func (c *Claimer) Owned() []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]uint32, 0, len(c.owned))
	for s := range c.owned {
		out = append(out, s)
	}
	return out
}

// Count 返回持有分片数。
func (c *Claimer) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.owned)
}

// Epoch 返回某分片的 epoch。
func (c *Claimer) Epoch(s uint32) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	o, ok := c.owned[s]
	if !ok {
		return 0, false
	}
	return o.Epoch, true
}

// Owns 报告是否持有该分片。
func (c *Claimer) Owns(s uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.owned[s]
	return ok
}

// Lookup 查询某分片当前的 owner（可能是别的节点）及其 epoch。
func (c *Claimer) Lookup(ctx context.Context, s uint32) (Record, int64, bool, error) {
	gctx, cancel := context.WithTimeout(ctx, c.cfg.EtcdTimeout)
	defer cancel()
	resp, err := c.cli.Get(gctx, c.Key(s))
	if err != nil {
		return Record{}, 0, false, err
	}
	if len(resp.Kvs) == 0 {
		return Record{}, 0, false, nil
	}
	var rec Record
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return Record{}, 0, false, err
	}
	return rec, resp.Kvs[0].CreateRevision, true, nil
}

// Stop 优雅停止：释放全部分片、撤销 lease。
func (c *Claimer) Stop(ctx context.Context) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	lease := c.leaseID
	c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()

	c.releaseAll(ctx, ReleaseGraceful)

	if lease != 0 {
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := c.cli.Revoke(rctx, lease); err != nil {
			logx.Warn("撤销分片 lease 失败", "err", err)
		}
		cancel()
	}
	logx.Info("分片认领器已停止", "kind", c.kind)
}
