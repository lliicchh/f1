package store

import "testing"

// hash tag 让同一玩家的 key 落同一个 slot，单玩家的 Lua 和 pipeline 才能用
func TestUIDHashTagKeepsPlayerKeysTogether(t *testing.T) {
	k := NewKeys(TagUID, 1024, "lobby")
	if got := k.Player(1001, ModBase); got != "p:{1001}:base" {
		t.Errorf("Player = %q，期望 p:{1001}:base", got)
	}
	if got := k.Profile(1001); got != "profile:{1001}" {
		t.Errorf("Profile = %q，期望 profile:{1001}", got)
	}
	tag := hashTag(k.Player(1001, ModBase))
	for _, m := range AllModules {
		if got := hashTag(k.Player(1001, m)); got != tag {
			t.Errorf("模块 %s 的 tag = %q，与 base 的 %q 不一致", m, got, tag)
		}
	}
	// profile 与订单键也必须同 tag：写穿要和玩家数据在同一个 Lua 里
	if hashTag(k.Profile(1001)) != tag {
		t.Error("profile 必须与玩家键同 tag")
	}
	if hashTag(k.Order(1001, "o1")) != tag {
		t.Error("订单键必须与玩家键同 tag")
	}
}

// Cluster 下 epoch 键要和该分片的玩家键同 slot，否则 fencing 的 Lua 会 CROSSSLOT
func TestShardHashTagAlignsEpochWithPlayers(t *testing.T) {
	k := NewKeys(TagShard, 1024, "lobby")
	uid := uint64(1001)
	sh := k.Shard(uid)

	epochTag := hashTag(k.Epoch(sh))
	if got := hashTag(k.Player(uid, ModBase)); got != epochTag {
		t.Errorf("玩家键 tag=%q 与 epoch 键 tag=%q 不同，Cluster 下会 CROSSSLOT", got, epochTag)
	}
	if got := hashTag(k.Profile(uid)); got != epochTag {
		t.Errorf("profile tag=%q 与 epoch tag=%q 不同", got, epochTag)
	}
	if got := hashTag(k.Order(uid, "o1")); got != epochTag {
		t.Errorf("订单键 tag=%q 与 epoch tag=%q 不同", got, epochTag)
	}
}

// tx 记录、PENDING 索引必须与发起方分片同 slot（写穿要原子）；
// done 标记必须与接收方分片同 slot（幂等入账要原子）
func TestTxKeysColocateWithTheirSide(t *testing.T) {
	k := NewKeys(TagShard, 1024, "lobby")
	const from, to = uint32(7), uint32(19)

	if hashTag(k.Tx(from, "abc")) != hashTag(k.TxPending(from)) {
		t.Error("tx 记录与 PENDING 索引必须同 slot")
	}
	if hashTag(k.Tx(from, "abc")) != hashTag(k.Epoch(from)) {
		t.Error("tx 记录必须与发起方 epoch 键同 slot")
	}
	if hashTag(k.TxDone(to, "abc")) != hashTag(k.Epoch(to)) {
		t.Error("done 标记必须与接收方 epoch 键同 slot")
	}
}

func TestRoomKeysColocate(t *testing.T) {
	k := NewKeys(TagUID, 1024, "room")
	roomID := uint64(4242)
	sh := k.Shard(roomID)
	if hashTag(k.Room(roomID)) != hashTag(k.Epoch(sh)) {
		t.Error("房间快照必须与 epoch 键同 slot")
	}
	if hashTag(k.RoomIndex(sh)) != hashTag(k.Epoch(sh)) {
		t.Error("房间索引必须与 epoch 键同 slot")
	}
}

func TestShardMapping(t *testing.T) {
	k := NewKeys(TagUID, 1024, "lobby")
	for _, uid := range []uint64{0, 1, 1023, 1024, 999999} {
		if got, want := k.Shard(uid), uint32(uid%1024); got != want {
			t.Errorf("Shard(%d) = %d，期望 %d", uid, got, want)
		}
	}
}

// hashTag 抽取 {} 中的内容；没有大括号则返回整个键
func hashTag(key string) string {
	start := -1
	for i := 0; i < len(key); i++ {
		if key[i] == '{' {
			start = i + 1
			continue
		}
		if key[i] == '}' && start >= 0 {
			return key[start:i]
		}
	}
	return key
}
