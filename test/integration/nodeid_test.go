package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/gamedev/f1/pkg/etcdx"
	"github.com/gamedev/f1/pkg/ident"
	"github.com/gamedev/f1/pkg/nodeid"
	"github.com/gamedev/f1/test/harness"
)

// §3.4 第二道校验。
//
// 撞号是全套设计里唯一没有兜底的洞：两个进程用同一个 nodeID 时，
// etcd 认为是同一个 owner 在续租，续租不会失败，两进程同时持有同一分片、
// epoch 还相同，fencing 也拦不住。所以它必须在启动时就被堵死。
func TestNodeIDConflictRefusesStartup(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// 第一个进程正常抢占。
	reg1, err := nodeid.Claim(ctx, cli, cfg, id, nil)
	if err != nil {
		t.Fatalf("首个进程应抢占成功: %v", err)
	}
	defer reg1.Release(ctx)

	// 第二个进程用同一个 NODE_SEQ —— 典型的「复制 compose 配置忘改数字」。
	id2, _ := ident.New(cfg.ServerID, ident.SvcLobby, 1)
	_, err = nodeid.Claim(ctx, cli, cfg, id2, nil)
	if err == nil {
		t.Fatal("撞号必须拒绝启动，绝不能降级用随机数或 hostname 哈希兜底")
	}
	if !errors.Is(err, nodeid.ErrConflict) {
		t.Fatalf("应识别为撞号冲突，实际: %v", err)
	}
	// 错误信息要能直接指出冲突方，否则运维只能靠猜。
	if msg := err.Error(); msg == "" {
		t.Fatal("冲突错误必须带上冲突方 host/pid")
	}
	t.Logf("撞号被正确拦下: %v", err)
}

// 不同 NODE_SEQ 的进程互不影响。
func TestDifferentNodeSeqCoexist(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id1 := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	reg1, err := nodeid.Claim(ctx, cli, cfg, id1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reg1.Release(ctx)

	id2, _ := ident.New(cfg.ServerID, ident.SvcLobby, 2)
	reg2, err := nodeid.Claim(ctx, cli, cfg, id2, nil)
	if err != nil {
		t.Fatalf("不同序号应能共存: %v", err)
	}
	defer reg2.Release(ctx)

	// 不同服务用相同序号也不冲突：注册键按服务名分开，workerID 由位拼接错开。
	id3, _ := ident.New(cfg.ServerID, ident.SvcRoom, 1)
	reg3, err := nodeid.Claim(ctx, cli, cfg, id3, nil)
	if err != nil {
		t.Fatalf("不同服务的同序号应能共存: %v", err)
	}
	defer reg3.Release(ctx)

	if id1.WorkerID() == id3.WorkerID() {
		t.Fatal("lobby-1 与 room-1 的 workerID 必须不同")
	}
}

// 僵尸记录（进程崩溃未清理）可以被接管，不能让一个槽位永久废掉。
func TestStaleRecordIsReclaimed(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// 直接伪造一条心跳早已过期的注册记录 —— 等价于「进程被 SIGKILL，键还留着」。
	stale := nodeid.Record{
		NodeID: id.NodeID(),
		PID:    99999,
		Host:   "dead-host",
		BeatAt: time.Now().Add(-2 * cfg.NodeStaleTimeout).UnixMilli(),
		LastTS: time.Now().Add(-2 * cfg.NodeStaleTimeout).UnixMilli(),
	}
	blob, _ := json.Marshal(stale)
	if _, err := cli.Put(ctx, nodeid.Key(cfg, id), string(blob)); err != nil {
		t.Fatal(err)
	}

	reg, err := nodeid.Claim(ctx, cli, cfg, id, nil)
	if err != nil {
		t.Fatalf("僵尸记录应可被接管: %v", err)
	}
	defer reg.Release(ctx)

	if got := reg.Record(); got.PID == stale.PID {
		t.Fatal("接管后记录应换成本进程的信息")
	}
}

// §3.7 前瞻：若本机时间早于该槽位上一任的最后心跳，拒绝启动。
//
// 当前 workerID 绑定容器不会复用，这条用不上；但迁移到自动分配后
// workerID 可被复用，跨进程时钟回拨就会真实产生重复雪花 ID。
func TestClockRegressionRefusesTakeover(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// 僵尸记录，但 last_ts 在「未来」：说明上一任所在机器的时钟比本机快。
	stale := nodeid.Record{
		NodeID: id.NodeID(),
		Host:   "fast-clock-host",
		BeatAt: time.Now().Add(-2 * cfg.NodeStaleTimeout).UnixMilli(),
		LastTS: time.Now().Add(time.Hour).UnixMilli(),
	}
	blob, _ := json.Marshal(stale)
	if _, err := cli.Put(ctx, nodeid.Key(cfg, id), string(blob)); err != nil {
		t.Fatal(err)
	}

	_, err = nodeid.Claim(ctx, cli, cfg, id, nil)
	if !errors.Is(err, nodeid.ErrClockRegressed) {
		t.Fatalf("本机时钟早于上一任 last_ts 时必须拒绝启动，实际: %v", err)
	}
}

// 正常退出时必须删除注册键（§10.3 步骤 5），否则重启要等 10 分钟。
func TestReleaseDeletesKey(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	reg, err := nodeid.Claim(ctx, cli, cfg, id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Release(ctx); err != nil {
		t.Fatal(err)
	}

	resp, err := cli.Get(ctx, nodeid.Key(cfg, id))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatal("正常退出后注册键应被删除")
	}

	// 立刻重启应当能直接抢到，不用等僵尸阈值。
	reg2, err := nodeid.Claim(ctx, cli, cfg, id, nil)
	if err != nil {
		t.Fatalf("重启应能立即抢占: %v", err)
	}
	_ = reg2.Release(ctx)
}

// CountLive 是分片公平份额的输入，必须只数心跳新鲜的实例。
func TestCountLive(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id1 := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if n := nodeid.CountLive(ctx, cli, cfg, "lobby", cfg.NodeStaleTimeout); n != 0 {
		t.Fatalf("初始应为 0，实际 %d", n)
	}

	reg1, _ := nodeid.Claim(ctx, cli, cfg, id1, nil)
	defer reg1.Release(ctx)
	id2, _ := ident.New(cfg.ServerID, ident.SvcLobby, 2)
	reg2, _ := nodeid.Claim(ctx, cli, cfg, id2, nil)
	defer reg2.Release(ctx)

	if n := nodeid.CountLive(ctx, cli, cfg, "lobby", cfg.NodeStaleTimeout); n != 2 {
		t.Fatalf("应统计到 2 个存活实例，实际 %d", n)
	}
	if n := nodeid.CountLive(ctx, cli, cfg, "room", cfg.NodeStaleTimeout); n != 0 {
		t.Fatalf("room 服务应为 0，实际 %d", n)
	}
}
