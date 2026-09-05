package config

import (
	"os"
	"testing"
)

func withEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func baseEnv() map[string]string {
	return map[string]string{
		"SERVER_ID": "1",
		"NODE_SEQ":  "2",
	}
}

func TestLoadDerivesIdentity(t *testing.T) {
	withEnv(t, baseEnv())
	cfg, id, err := Load("lobby")
	if err != nil {
		t.Fatal(err)
	}
	if id.NodeID() != "s1-lobby-2" {
		t.Errorf("nodeID = %q", id.NodeID())
	}
	if id.WorkerID() != 258 {
		t.Errorf("workerID = %d，期望 258", id.WorkerID())
	}
	if cfg.EtcdPrefix != "/game/s1" {
		t.Errorf("etcd 前缀默认应按区服推导，实际 %q", cfg.EtcdPrefix)
	}
	if cfg.ShardCount != 1024 {
		t.Errorf("分片数默认应为 1024，实际 %d", cfg.ShardCount)
	}
}

// §3.2 / §3.3：区服 ID 与进程序号是运营/部署概念，程序无法推断，必须显式配置。
func TestMissingRequiredEnvIsFatal(t *testing.T) {
	os.Clearenv()
	t.Setenv("NODE_SEQ", "1")
	if _, _, err := Load("lobby"); err == nil {
		t.Error("缺少 SERVER_ID 应报错")
	}

	os.Clearenv()
	t.Setenv("SERVER_ID", "1")
	if _, _, err := Load("lobby"); err == nil {
		t.Error("缺少 NODE_SEQ 应报错")
	}
}

// §3.4 第一道校验在配置装载阶段就要生效。
func TestNodeSeqBoundsEnforcedAtLoad(t *testing.T) {
	env := baseEnv()
	env["NODE_SEQ"] = "128"
	withEnv(t, env)
	if _, _, err := Load("lobby"); err == nil {
		t.Fatal("NODE_SEQ=128 必须在装载阶段被拒绝")
	}
}

// §4.2：续租间隔必须显著小于 lease TTL，否则一次抖动就丢分片。
func TestKeepAliveMustBeShorterThanTTL(t *testing.T) {
	env := baseEnv()
	env["SHARD_LEASE_TTL"] = "8s"
	env["SHARD_KEEPALIVE"] = "9s"
	withEnv(t, env)
	if _, _, err := Load("lobby"); err == nil {
		t.Fatal("续租间隔大于 TTL 必须被拒绝")
	}
}

// §4.2：nodeID 心跳与分片 lease 的阈值取舍相反，不能配反。
func TestNodeBeatMustBeShorterThanStale(t *testing.T) {
	env := baseEnv()
	env["NODE_BEAT_INTERVAL"] = "20m"
	env["NODE_STALE_TIMEOUT"] = "10m"
	withEnv(t, env)
	if _, _, err := Load("lobby"); err == nil {
		t.Fatal("心跳间隔大于僵尸阈值必须被拒绝")
	}
}

// §4.1：分片数一经确定永不变更；这里至少保证它是 2 的幂，避免取模分布不均。
func TestShardCountMustBePowerOfTwo(t *testing.T) {
	env := baseEnv()
	env["SHARD_COUNT"] = "1000"
	withEnv(t, env)
	if _, _, err := Load("lobby"); err == nil {
		t.Fatal("非 2 的幂的分片数应被拒绝")
	}
}

func TestDefaultsMatchDesign(t *testing.T) {
	withEnv(t, baseEnv())
	cfg, _, err := Load("lobby")
	if err != nil {
		t.Fatal(err)
	}
	// §4.2：lease TTL 8s，续租 2.5s。
	if cfg.ShardLeaseTTL.Seconds() != 8 {
		t.Errorf("lease TTL = %v，设计值 8s", cfg.ShardLeaseTTL)
	}
	if cfg.ShardKeepAlive.Milliseconds() != 2500 {
		t.Errorf("续租间隔 = %v，设计值 2.5s", cfg.ShardKeepAlive)
	}
	// §3.4：僵尸阈值 10min，心跳 30s。
	if cfg.NodeStaleTimeout.Minutes() != 10 {
		t.Errorf("僵尸阈值 = %v，设计值 10min", cfg.NodeStaleTimeout)
	}
	if cfg.NodeBeatInterval.Seconds() != 30 {
		t.Errorf("nodeID 心跳 = %v，设计值 30s", cfg.NodeBeatInterval)
	}
	// §6.2：L1 5s，L2 60s。
	if cfg.FlushL1Interval.Seconds() != 5 {
		t.Errorf("L1 间隔 = %v，设计值 5s", cfg.FlushL1Interval)
	}
	if cfg.FlushL2Interval.Seconds() != 60 {
		t.Errorf("L2 间隔 = %v，设计值 60s", cfg.FlushL2Interval)
	}
	// §10.1：下线后保留 5~10 分钟。
	if cfg.UnloadIdle.Minutes() != 10 {
		t.Errorf("卸载延迟 = %v，设计值 10min", cfg.UnloadIdle)
	}
}

func TestEtcdKeyJoin(t *testing.T) {
	withEnv(t, baseEnv())
	cfg, _, _ := Load("lobby")
	if got := cfg.EtcdKey("shard", "lobby", "0007"); got != "/game/s1/shard/lobby/0007" {
		t.Errorf("EtcdKey = %q", got)
	}
}
