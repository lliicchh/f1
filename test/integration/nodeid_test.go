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

// 撞号是唯一没有兜底的洞：两个进程用同一个 nodeID，etcd 以为是同一个 owner
// 在续租，续租不会失败，两边同时持有同一分片，epoch 还一样，fencing 也拦不住。
// 所以只能在启动时堵
func TestNodeIDConflictRefusesStartup(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// 第一个进程正常抢占
	reg1, err := nodeid.Claim(ctx, cli, cfg, id, nil)
	if err != nil {
		t.Fatalf("首个进程应抢占成功: %v", err)
	}
	defer reg1.Release(ctx)

	// 第二个进程用同一个 NODE_SEQ，典型的「复制 compose 配置忘改数字」
	id2, _ := ident.New(cfg.ServerID, ident.SvcLobby, 1)
	_, err = nodeid.Claim(ctx, cli, cfg, id2, nil)
	if err == nil {
		t.Fatal("撞号必须拒绝启动，绝不能降级用随机数或 hostname 哈希兜底")
	}
	if !errors.Is(err, nodeid.ErrConflict) {
		t.Fatalf("应识别为撞号冲突，实际: %v", err)
	}
	// 错误信息要能直接指出冲突方，否则运维只能靠猜
	if msg := err.Error(); msg == "" {
		t.Fatal("冲突错误必须带上冲突方 host/pid")
	}
	t.Logf("撞号被正确拦下: %v", err)
}

// 不同 NODE_SEQ 的进程互不干扰
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

	// 不同服务用相同序号：注册键按服务名分开，workerID 由位拼接错开
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

// 崩溃留下的僵尸记录得能被接管，不然一个槽位就永久废了
func TestStaleRecordIsReclaimed(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// 直接伪造一条心跳早已过期的注册记录，等价于「进程被 SIGKILL，键还留着」
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

// 本机时间早于这个槽位上一任的最后心跳就拒绝启动
//
// 现在 workerID 绑容器不复用，用不上这条；等改成自动分配就用得上了
func TestClockRegressionRefusesTakeover(t *testing.T) {
	env := harness.Start(t)
	ctx := context.Background()

	cfg, id := env.Config(t, "lobby", 1, nil)
	cli, err := etcdx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// 僵尸记录，但 last_ts 在「未来」：说明上一任所在机器的时钟比本机快
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

// 正常退出要删掉注册键，不然重启得白等十分钟
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

	// 立刻重启应当能直接抢到，不用等僵尸阈值
	reg2, err := nodeid.Claim(ctx, cli, cfg, id, nil)
	if err != nil {
		t.Fatalf("重启应能立即抢占: %v", err)
	}
	_ = reg2.Release(ctx)
}

// CountLive 只该数心跳新鲜的实例，它是分片公平份额的输入
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
