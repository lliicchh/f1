package ident

import "testing"

// §3.3：workerID 必须在所有服务之间唯一。
// 若每个服务都从 1 开始且直接当 workerID，gateway-1 / lobby-1 / room-1 会撞在一起。
func TestWorkerIDUniqueAcrossServices(t *testing.T) {
	seen := map[uint32]string{}
	for _, svc := range []SvcType{SvcGateway, SvcLobby, SvcRoom, SvcMatch, SvcChat, SvcWorld} {
		for seq := 1; seq <= MaxNodeSeq; seq++ {
			id, err := New(1, svc, seq)
			if err != nil {
				t.Fatalf("New(%v,%d): %v", svc, seq, err)
			}
			w := id.WorkerID()
			if prev, dup := seen[w]; dup {
				t.Fatalf("workerID %d 重复：%s 与 %s", w, prev, id.NodeID())
			}
			seen[w] = id.NodeID()
			if w > MaxWorkerID {
				t.Fatalf("workerID %d 超出 10 bit", w)
			}
		}
	}
	if len(seen) != 6*MaxNodeSeq {
		t.Fatalf("期望 %d 个不同 workerID，实际 %d", 6*MaxNodeSeq, len(seen))
	}
}

// §3.3 文档中的具体样例。
func TestWorkerIDExamples(t *testing.T) {
	cases := []struct {
		svc    SvcType
		seq    int
		worker uint32
		node   string
	}{
		{SvcGateway, 1, 129, "s1-gateway-1"},
		{SvcLobby, 1, 257, "s1-lobby-1"},
		{SvcLobby, 2, 258, "s1-lobby-2"},
		{SvcRoom, 1, 385, "s1-room-1"},
	}
	for _, c := range cases {
		id, err := New(1, c.svc, c.seq)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if got := id.WorkerID(); got != c.worker {
			t.Errorf("%s workerID = %d, 期望 %d", c.node, got, c.worker)
		}
		if got := id.NodeID(); got != c.node {
			t.Errorf("nodeID = %q, 期望 %q", got, c.node)
		}
	}
}

// §3.4 第一道：越界校验。
// nodeSeq >= 128 会溢出到相邻服务的号段，静默产生重复 workerID。
func TestNodeSeqBoundsCheck(t *testing.T) {
	for _, seq := range []int{-1, 0, 128, 129, 1000} {
		if _, err := New(1, SvcLobby, seq); err == nil {
			t.Errorf("NODE_SEQ=%d 应该被拒绝", seq)
		}
	}
	for _, seq := range []int{1, 64, 127} {
		if _, err := New(1, SvcLobby, seq); err != nil {
			t.Errorf("NODE_SEQ=%d 应该合法: %v", seq, err)
		}
	}
}

// 越界若不拦，序号会溢出进相邻服务的号段，静默产生重复 workerID。
// 这是 §3.4 第一道校验存在的全部理由，这里把它钉成断言。
func TestOverflowWouldCollide(t *testing.T) {
	// lobby 的号段基址是 2<<7=256，room 是 3<<7=384。
	// nodeSeq=129 的低 7 位是 1，第 8 位溢出加到号段上 → 正好落在 room-1 上。
	lobby129 := uint32(SvcLobby)<<NodeSeqBits | 129
	room1 := uint32(SvcRoom)<<NodeSeqBits | 1
	if lobby129 != room1 {
		t.Fatalf("前提失效：lobby-129 (%d) 本应与 room-1 (%d) 相撞", lobby129, room1)
	}
	// 而校验会在此之前就拦下它。
	if _, err := New(1, SvcLobby, 129); err == nil {
		t.Fatal("NODE_SEQ=129 必须被越界校验拦下")
	}
}

func TestSvcTypeBounds(t *testing.T) {
	if _, err := New(1, SvcType(8), 1); err == nil {
		t.Error("svcType=8 超出 3 bit，应该被拒绝")
	}
	if _, err := New(1, SvcType(0), 1); err == nil {
		t.Error("svcType=0 保留，应该被拒绝")
	}
}

// §3.2：serverID 不进 workerID —— 不同区服同序号的进程 workerID 相同，
// 这是有意为之：uid 只需区服内唯一。
func TestServerIDNotInWorkerID(t *testing.T) {
	a, _ := New(1, SvcLobby, 3)
	b, _ := New(2, SvcLobby, 3)
	if a.WorkerID() != b.WorkerID() {
		t.Error("serverID 不应影响 workerID")
	}
	if a.NodeID() == b.NodeID() {
		t.Error("nodeID 必须带上 serverID 以区分区服")
	}
}
