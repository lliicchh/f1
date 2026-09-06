package integration

import (
	"context"
	"testing"
	"time"

	"github.com/gamedev/f1/pkg/config"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/test/harness"
)

// 测试用小一点的分片空间，认领逻辑跟 1024 一样，但快得多
const testShards = 32

func smallShards(c *config.Config) {
	c.ShardCount = testShards
	c.ShardLeaseTTL = 2 * time.Second
	c.ShardKeepAlive = 500 * time.Millisecond
}

type recorder struct {
	acquired chan uint32
	released chan uint32
}

func newRecorder() *recorder {
	return &recorder{
		acquired: make(chan uint32, 4096),
		released: make(chan uint32, 4096),
	}
}

func (r *recorder) hooks() shard.Hooks {
	return shard.Hooks{
		OnAcquire: func(ctx context.Context, o shard.Ownership) error {
			r.acquired <- o.Shard
			return nil
		},
		OnRelease: func(ctx context.Context, o shard.Ownership, reason shard.ReleaseReason) {
			r.released <- o.Shard
		},
		LiveNodes: func(ctx context.Context) int { return 1 },
	}
}

// 单实例应该把整个分片空间吃下来
func TestClaimerTakesWholeSpace(t *testing.T) {
	env := harness.Start(t)
	n := env.Node(t, "lobby", "lobby", 1, smallShards)

	rec := newRecorder()
	c := shard.NewClaimer(n.Etcd, n.Cfg, shard.KindLobby, n.NodeID(), rec.hooks())
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Stop(context.Background())

	harness.Eventually(t, 15*time.Second, "单实例认领全部分片", func() bool {
		return c.Count() == testShards
	})

	// 每个分片的 epoch 都必须是真实的 etcd revision，不能是占位的 0，
	// epoch=0 会让 fencing 完全失效
	for s := uint32(0); s < testShards; s++ {
		epoch, ok := c.Epoch(s)
		if !ok {
			t.Fatalf("分片 %d 未被认领", s)
		}
		if epoch <= 0 {
			t.Fatalf("分片 %d 的 epoch = %d，必须是正的 etcd revision", s, epoch)
		}
	}
}

// 核心不变式：任一时刻一个分片只能被一个实例持有
func TestNoShardIsOwnedTwice(t *testing.T) {
	env := harness.Start(t)

	n1 := env.Node(t, "lobby", "lobby", 1, smallShards)
	n2 := env.Node(t, "lobby", "lobby", 2, smallShards)

	live := func(ctx context.Context) int { return 2 }

	r1, r2 := newRecorder(), newRecorder()
	h1, h2 := r1.hooks(), r2.hooks()
	h1.LiveNodes, h2.LiveNodes = live, live

	c1 := shard.NewClaimer(n1.Etcd, n1.Cfg, shard.KindLobby, n1.NodeID(), h1)
	c2 := shard.NewClaimer(n2.Etcd, n2.Cfg, shard.KindLobby, n2.NodeID(), h2)

	ctx := context.Background()
	if err := c1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c1.Stop(ctx)
	if err := c2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c2.Stop(ctx)

	harness.Eventually(t, 20*time.Second, "两个实例合计认领全部分片", func() bool {
		return c1.Count()+c2.Count() == testShards
	})

	owned := map[uint32]string{}
	for _, s := range c1.Owned() {
		owned[s] = n1.NodeID()
	}
	for _, s := range c2.Owned() {
		if prev, dup := owned[s]; dup {
			t.Fatalf("分片 %d 被 %s 与 %s 同时持有，内存态下这会导致双副本互相覆盖刷盘",
				s, prev, n2.NodeID())
		}
		owned[s] = n2.NodeID()
	}
	if len(owned) != testShards {
		t.Fatalf("覆盖的分片数 = %d，期望 %d", len(owned), testShards)
	}

	// 两边都应拿到接近公平份额，不能一个吃光另一个空转
	if c1.Count() == 0 || c2.Count() == 0 {
		t.Fatalf("分片分配不均：c1=%d c2=%d", c1.Count(), c2.Count())
	}
}

// 实例崩了，lease 过期后别人接管
func TestTakeoverAfterOwnerDies(t *testing.T) {
	env := harness.Start(t)

	n1 := env.Node(t, "lobby", "lobby", 1, smallShards)
	n2 := env.Node(t, "lobby", "lobby", 2, smallShards)

	r1 := newRecorder()
	h1 := r1.hooks()
	h1.LiveNodes = func(ctx context.Context) int { return 1 }
	c1 := shard.NewClaimer(n1.Etcd, n1.Cfg, shard.KindLobby, n1.NodeID(), h1)

	ctx := context.Background()
	if err := c1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	harness.Eventually(t, 15*time.Second, "c1 认领全部分片", func() bool {
		return c1.Count() == testShards
	})

	// 模拟崩溃：直接切断 etcd 连接，不做优雅释放，让 lease 自然过期
	_ = n1.Etcd.Close()

	r2 := newRecorder()
	h2 := r2.hooks()
	h2.LiveNodes = func(ctx context.Context) int { return 1 }
	c2 := shard.NewClaimer(n2.Etcd, n2.Cfg, shard.KindLobby, n2.NodeID(), h2)
	if err := c2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c2.Stop(ctx)

	// lease TTL 2s + 扫描间隔 3s，给足余量
	harness.Eventually(t, 30*time.Second, "c2 接管全部分片", func() bool {
		return c2.Count() == testShards
	})

	// 接管方的 epoch 必须严格大于原 owner，这是 fencing 能拦住旧 owner 的前提
	for s := uint32(0); s < testShards; s++ {
		oldEpoch, _ := c1.Epoch(s)
		newEpoch, ok := c2.Epoch(s)
		if !ok {
			t.Fatalf("分片 %d 未被接管", s)
		}
		if newEpoch <= oldEpoch {
			t.Fatalf("分片 %d 接管后 epoch 未抬高：old=%d new=%d", s, oldEpoch, newEpoch)
		}
	}
}

// 接管之后旧 owner 再写必须被拒，端到端走一遍
func TestFencingBlocksOldOwnerAfterRealTakeover(t *testing.T) {
	env := harness.Start(t)

	n1 := env.Node(t, "lobby", "lobby", 1, smallShards)
	n2 := env.Node(t, "lobby", "lobby", 2, smallShards)

	f1 := store.NewFencer(n1.Redis, n1.Keys, "lobby")
	f2 := store.NewFencer(n2.Redis, n2.Keys, "lobby")

	ctx := context.Background()
	const target = uint32(3)

	// c1 认领并抬高 epoch（模拟正常服务）
	h1 := shard.Hooks{
		OnAcquire: func(ctx context.Context, o shard.Ownership) error {
			if o.Shard == target {
				return f1.RaiseEpoch(ctx, o.Shard, o.Epoch)
			}
			return nil
		},
		LiveNodes: func(ctx context.Context) int { return 1 },
	}
	c1 := shard.NewClaimer(n1.Etcd, n1.Cfg, shard.KindLobby, n1.NodeID(), h1)
	if err := c1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	harness.Eventually(t, 15*time.Second, "c1 认领目标分片", func() bool { return c1.Owns(target) })

	oldEpoch, _ := c1.Epoch(target)
	key := n1.Keys.Player(1001, store.ModBase)
	if err := f1.WriteModules(ctx, target, oldEpoch, map[string][]byte{key: []byte("c1-data")}); err != nil {
		t.Fatalf("持有期内写入应成功: %v", err)
	}

	// c1「冻结」：断开 etcd，lease 过期
	_ = n1.Etcd.Close()

	h2 := shard.Hooks{
		OnAcquire: func(ctx context.Context, o shard.Ownership) error {
			if o.Shard == target {
				return f2.RaiseEpoch(ctx, o.Shard, o.Epoch)
			}
			return nil
		},
		LiveNodes: func(ctx context.Context) int { return 1 },
	}
	c2 := shard.NewClaimer(n2.Etcd, n2.Cfg, shard.KindLobby, n2.NodeID(), h2)
	if err := c2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c2.Stop(ctx)

	harness.Eventually(t, 30*time.Second, "c2 接管目标分片", func() bool { return c2.Owns(target) })

	newEpoch, _ := c2.Epoch(target)
	if err := f2.WriteModules(ctx, target, newEpoch, map[string][]byte{key: []byte("c2-data")}); err != nil {
		t.Fatalf("新 owner 写入应成功: %v", err)
	}

	// c1 恢复后带着旧 epoch 刷盘，必须被拒
	err := f1.WriteModules(ctx, target, oldEpoch, map[string][]byte{key: []byte("c1-stale")})
	if err == nil {
		t.Fatal("旧 owner 的滞后写入必须被 epoch fencing 拒绝")
	}
	got, _ := n2.Redis.Get(ctx, key).Result()
	if got != "c2-data" {
		t.Fatalf("玩家进度被旧 owner 回滚了：%q", got)
	}
}
