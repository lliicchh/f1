package subject

import "testing"

// 命名规范
func TestSubjectFormats(t *testing.T) {
	cases := []struct{ got, want string }{
		{LobbyReq(7, "login"), "req.lobby.7.login"},
		{LobbyShardWildcard(7), "req.lobby.7.>"},
		{RoomReq(12, "join_room"), "req.room.12.join_room"},
		{MatchReq(2, 5), "req.match.2.5"},
		{GatePush("s1-gateway-1"), "push.gate.s1-gateway-1"},
		{Broadcast, "push.broadcast"},
		{PlayerEvt(1001, "levelup"), "evt.player.1001.levelup"},
		{JobTransfer("tx123"), "job.transfer.tx123"},
		{JobMailSend, "job.mail.send"},
		{CtlHandoff("s1-lobby-2"), "ctl.node.s1-lobby-2.handoff"},
		{CtlShutdown("s1-lobby-2"), "ctl.node.s1-lobby-2.shutdown"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("subject = %q，期望 %q", c.got, c.want)
		}
	}
}

// nodeID 会作为 subject 的一段，不能含点号，否则会被 NATS 当成多级
func TestNodeIDIsSingleToken(t *testing.T) {
	subj := CtlHandoff("s1-lobby-1")
	n := 0
	for _, ch := range subj {
		if ch == '.' {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("ctl.node.{nodeID}.handoff 应恰好有 3 个点，实际 %d（%s）", n, subj)
	}
}

func TestParseShardReq(t *testing.T) {
	kind, shard, cmd, ok := ParseShardReq("req.lobby.7.add_item")
	if !ok || kind != "lobby" || shard != 7 || cmd != "add_item" {
		t.Fatalf("解析结果 kind=%q shard=%d cmd=%q ok=%v", kind, shard, cmd, ok)
	}
	if _, _, _, ok := ParseShardReq("push.gate.x"); ok {
		t.Fatal("非 req. 前缀不应被解析成分片请求")
	}
	if _, _, _, ok := ParseShardReq("req.lobby.abc.cmd"); ok {
		t.Fatal("非数字分片号应解析失败")
	}
}

func TestParseMatchReq(t *testing.T) {
	mode, tier, ok := ParseMatchReq("req.match.3.9")
	if !ok || mode != 3 || tier != 9 {
		t.Fatalf("mode=%d tier=%d ok=%v", mode, tier, ok)
	}
}

// 指标打标只取前两段，避免 label 基数随分片号/uid 爆炸
func TestPrefixBoundsCardinality(t *testing.T) {
	cases := map[string]string{
		"req.lobby.7.login":       "req.lobby",
		"req.room.1023.room_op":   "req.room",
		"push.gate.s1-gateway-1":  "push.gate",
		"push.broadcast":          "push.broadcast",
		"job.transfer.tx1":        "job.transfer",
		"evt.player.1001.levelup": "evt.player",
		"nodots":                  "nodots",
	}
	for in, want := range cases {
		if got := Prefix(in); got != want {
			t.Errorf("Prefix(%q) = %q，期望 %q", in, got, want)
		}
	}
}

// 必达任务必须挂在 job. 前缀下，才能被 JetStream 的 stream 捕获
func TestJobSubjectsUnderJobWildcard(t *testing.T) {
	for _, s := range []string{JobTransfer("tx1"), JobMailSend, JobBattleSettle} {
		if len(s) < 4 || s[:4] != "job." {
			t.Errorf("%q 不在 job. 前缀下，不会进 JetStream", s)
		}
	}
}
