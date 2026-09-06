// Package shard 分片模型：划分、认领、订阅、Actor 执行
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

// Kind 分片空间。Lobby 和 Room 各用一套，互不干扰
type Kind string

const (
	KindLobby Kind = "lobby"
	KindRoom  Kind = "room"
	// KindMatch 匹配池的桶空间，按模式 × 段位切，不落盘
	KindMatch Kind = "match"
)

// Of 算出实体落在哪个分片，就是取模
//
// ShardCount 定了就不能改，改一次等于全量迁移
func Of(id uint64, shardCount uint32) uint32 { return uint32(id % uint64(shardCount)) }

// Record etcd 里的分片认领记录，形如
//
//	/game/s1/shard/lobby/0007 → {"node":"s1-lobby-1", "since":...}
//
// epoch 不写进值里，直接取键的 CreateRevision，它天然单调递增，
// 还省掉一次「先占位再回写」的写入。etcdctl 加 --write-out=json 能看到
type Record struct {
	Node string `json:"node"`
	// Since 只给运维看持有时长，不参与判定
	Since int64 `json:"since"`
}

// Ownership 描述一个已认领的分片
type Ownership struct {
	Kind  Kind
	Shard uint32
	Epoch int64 // etcd 事务返回的 revision，单调递增
	Node  string
}

func (o Ownership) String() string {
	return fmt.Sprintf("%s/%04d@epoch=%d", o.Kind, o.Shard, o.Epoch)
}

// ReleaseReason 说明分片为何被释放
type ReleaseReason string

const (
	// ReleaseGraceful 主动释放，数据已经刷完盘
	ReleaseGraceful ReleaseReason = "graceful"
	// ReleaseLeaseLost 续租失败，得立刻停手
	ReleaseLeaseLost ReleaseReason = "lease_lost"
	// ReleaseFenced 被 fencing 拒了，内存已过期，丢掉就是
	ReleaseFenced ReleaseReason = "fenced"
	// ReleaseStartFailed 初始化失败，回滚认领
	ReleaseStartFailed ReleaseReason = "start_failed"
)

// Hooks 认领和释放的回调，都在 Claimer 自己的 goroutine 里跑
type Hooks struct {
	// OnAcquire 认领成功后调用，顺序是抬 epoch、加载数据、订阅 subject。
	// 返回错误就回滚认领
	OnAcquire func(ctx context.Context, o Ownership) error
	// OnRelease 释放前调用：停手、刷盘、清内存
	OnRelease func(ctx context.Context, o Ownership, reason ReleaseReason)
	// OnFreed 释放后通知别的节点尽快重扫，可选
	OnFreed func(kind Kind, shard uint32)
	// LiveNodes 返回同类服务的存活实例数，用来算公平份额，<=0 表示不知道
	LiveNodes func(ctx context.Context) int
	// RequestHandoff 向当前 owner 要分片。返回 nil 表示对方已释放，可以直接抢。
	// 不设置就退化成「持有过多的节点主动让出」
	RequestHandoff func(ctx context.Context, shard uint32, fromNode string) error
}

// Claimer 负责分片的认领、续租与释放
type Claimer struct {
	cli    *clientv3.Client
	cfg    *config.Config
	kind   Kind
	nodeID string
	hooks  Hooks

	prefix string
	// space 分片空间大小，默认 cfg.ShardCount。Match 按模式 × 段位分桶，空间小得多
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

// NewClaimer 构造认领器，分片空间取 cfg.ShardCount
func NewClaimer(cli *clientv3.Client, cfg *config.Config, kind Kind, nodeID string, hooks Hooks) *Claimer {
	return NewClaimerWithSpace(cli, cfg, kind, nodeID, cfg.ShardCount, hooks)
}

// NewClaimerWithSpace 用自定义分片空间构造认领器
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

func (c *Claimer) Space() uint32 { return c.space }

// Key 返回分片的 etcd 键，序号补零到 4 位，前缀扫描才有序
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

// Start 建 lease 并开始认领
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

// newLease 建 lease 并起续租协程
//
// 全节点共用一个 lease：它死了说明进程整体失联，所有分片一起释放才对。
// 单个分片的主动释放走删键，不动 lease
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

// keepAlive 定期续租
//
// 没用 clientv3 的自动 KeepAlive 流，自己按固定间隔调 KeepAliveOnce：
// 间隔和 TTL 的比例是设计的一部分，显式控制更清楚，失败也能立刻拿到错误
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

			// TTL 8s、间隔 2.5s，连丢两次就已经逼近过期，必须立刻停手。
			// 宁可早停，也不要等到别人接管了还在写。
			// lease 已经被服务端收走了，不用再等第二次
			leaseGone := errors.Is(err, rpctypes.ErrLeaseNotFound)
			if fails >= 2 || leaseGone {
				metrics.ShardLeaseLost.WithLabelValues(string(c.kind)).Inc()
				logx.Error("分片 lease 判定丢失，立即释放全部分片",
					"kind", c.kind, "owned", c.Count(), "lease_gone", leaseGone)
				c.releaseAll(context.Background(), ReleaseLeaseLost)

				// 重建 lease 再回来参与认领
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

// loop 定期扫描空闲分片并抢占
func (c *Claimer) loop(ctx context.Context) {
	defer c.wg.Done()

	// 启动时立刻扫一次
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

// TriggerRescan 让下一轮扫描提前，收到释放事件时调
func (c *Claimer) TriggerRescan() {
	select {
	case c.rescan <- struct{}{}:
	default:
	}
}

// scanAndClaim 扫一遍分片空间，抢空闲的，持有过多就让出去
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

	// 和本地视图对一下：etcd 上已经不是我的，说明被接管了
	c.reconcile(ctx, taken, epochs)

	target := c.targetCount(ctx)
	cur := c.Count()

	// 统计每个 owner 拿了多少，给再平衡用
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
		// 先吃空闲的，这些没人在服务，抢了不会造成不可用
		rand.Shuffle(len(free), func(i, j int) { free[i], free[j] = free[j], free[i] })
		got := c.claimMany(ctx, free, target-cur)
		if got > 0 {
			logx.Info("分片认领完成", "kind", c.kind, "acquired", got, "owned", c.Count(), "target", target)
		}

	case cur < target && len(free) == 0:
		// 分片被瓜分完了但自己不够份额，典型是扩容后新实例上线。
		// 等 lease 过期要 8~10s 不可用，主动交接能压到百毫秒，
		// 所以由想要的一方发起，而不是让持有方盲目让出
		c.requestRebalance(ctx, taken, byNode, target, cur)

	case cur > target && c.hooks.RequestHandoff == nil:
		// 没接 handoff 的服务退化成主动让出，动作一样，只是新 owner 要等下一轮扫描
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

// maxHandoffPerRound 限制单轮交接数，免得扩容瞬间大批分片一起抖
const maxHandoffPerRound = 8

// requestRebalance 向持有超额的节点要分片
func (c *Claimer) requestRebalance(ctx context.Context, taken map[uint32]Record, byNode map[string]int, target, cur int) {
	if c.hooks.RequestHandoff == nil {
		return
	}

	// 只找持有数严格大于份额的节点。
	// 1024 除不尽实例数时各节点会停在 target 和 target-1 之间，
	// 这个条件保证不会出现谁都想再要一个的永久抖动
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
	// 优先从最重的节点上要
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
		// 别把同一个节点掏到低于份额
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

		// 对方已经放手了，立刻抢
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

// someoneBelow 报告有没有节点低于份额，包括一个分片都没拿到的新实例
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

// reconcile 处理本地以为持有、etcd 上却不是我的分片
func (c *Claimer) reconcile(ctx context.Context, taken map[uint32]Record, epochs map[uint32]int64) {
	c.mu.RLock()
	stale := make([]*Ownership, 0)
	for s, o := range c.owned {
		rec, ok := taken[s]
		// epoch 变了说明键被重建过。就算 node 名字一样，那也是另一任 owner，
		// 本地这份内存不作数了
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

// targetCount 算本实例该持有多少分片
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
	return (total + n - 1) / n // 向上取整，保证全部覆盖到
}

// claim 用 etcd 事务抢一个分片
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

	// 事务成功时 Header.Revision 就是这个键的 CreateRevision，拿它当 epoch
	epoch := resp.Header.Revision
	o := &Ownership{Kind: c.kind, Shard: s, Epoch: epoch, Node: c.nodeID}

	c.mu.Lock()
	c.owned[s] = o
	c.mu.Unlock()
	metrics.ShardOwnerChanges.WithLabelValues(string(c.kind), "acquire").Inc()
	metrics.ShardOwned.WithLabelValues(string(c.kind)).Set(float64(c.Count()))

	// 抬 epoch、加载数据、订阅 subject，失败就回滚
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

// claimConcurrency 并发抢占上限
//
// 串行抢每个分片一次 etcd 往返，1024 个要拖几十秒。
// 这些事务互相独立，可以并发
const claimConcurrency = 16

// claimMany 并发抢一批，返回成功数
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

// Release 主动释放一个分片：停手、刷盘、删键
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

// dropLocal 只清本地，不碰 etcd
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

// releaseAll 释放全部分片
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

// Fence 因被 fencing 拒而立刻放弃某分片
//
// 故意不删 etcd 键：这时候键要么已经过期消失，要么属于新 owner，删了反而误伤
func (c *Claimer) Fence(shard uint32) {
	c.mu.RLock()
	o, ok := c.owned[shard]
	c.mu.RUnlock()
	if !ok {
		return
	}
	c.dropLocal(context.Background(), o, ReleaseFenced)
}

// Owned 返回当前持有的分片列表
func (c *Claimer) Owned() []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]uint32, 0, len(c.owned))
	for s := range c.owned {
		out = append(out, s)
	}
	return out
}

func (c *Claimer) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.owned)
}

// Epoch 返回某分片的 epoch
func (c *Claimer) Epoch(s uint32) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	o, ok := c.owned[s]
	if !ok {
		return 0, false
	}
	return o.Epoch, true
}

func (c *Claimer) Owns(s uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.owned[s]
	return ok
}

// Lookup 查某分片现在归谁，以及它的 epoch
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

// Stop 释放全部分片并撤销 lease
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
