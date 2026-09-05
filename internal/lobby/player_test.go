package lobby

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/store"
)

func TestCurrencyNeverGoesNegative(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	if _, ok := p.AddCurrency(uint32(protocol.CurrencyGold), 100); !ok {
		t.Fatal("加钱应成功")
	}
	// 余额不足时必须整体失败，不能扣成负数。
	if bal, ok := p.AddCurrency(uint32(protocol.CurrencyGold), -200); ok {
		t.Fatalf("余额不足应失败，实际扣成 %d", bal)
	}
	if got := p.Currency(uint32(protocol.CurrencyGold)); got != 100 {
		t.Fatalf("失败的扣减不得改动余额，实际 %d", got)
	}
	if _, ok := p.AddCurrency(uint32(protocol.CurrencyGold), -100); !ok {
		t.Fatal("恰好扣完应成功")
	}
}

func TestStackableItemsMerge(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	now := time.Now()
	if _, ok := p.AddItem(1, 100, 5, now); !ok {
		t.Fatal("加道具失败")
	}
	if _, ok := p.AddItem(2, 100, 3, now); !ok {
		t.Fatal("加道具失败")
	}
	if len(p.Bag.Items) != 1 {
		t.Fatalf("可堆叠道具应合并成 1 格，实际 %d", len(p.Bag.Items))
	}
	if got := p.CountItem(100); got != 8 {
		t.Fatalf("总数应为 8，实际 %d", got)
	}
}

func TestNonStackableItemsOccupySlots(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	now := time.Now()
	p.AddItem(1, 20000, 1, now)
	p.AddItem(2, 20000, 1, now)
	if len(p.Bag.Items) != 2 {
		t.Fatalf("不可堆叠道具应各占一格，实际 %d", len(p.Bag.Items))
	}
}

func TestBagFull(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	p.Bag.Capacity = 2
	now := time.Now()
	p.AddItem(1, 20001, 1, now)
	p.AddItem(2, 20002, 1, now)
	if _, ok := p.AddItem(3, 20003, 1, now); ok {
		t.Fatal("背包已满时应拒绝")
	}
}

// 扣道具不足时必须整体失败：不能扣掉一部分再报错，那会凭空销毁资产。
func TestRemoveItemAllOrNothing(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	now := time.Now()
	p.AddItem(1, 100, 3, now)
	p.AddItem(2, 100, 2, now)

	if ok := p.RemoveItemByTpl(100, 10); ok {
		t.Fatal("数量不足应失败")
	}
	if got := p.CountItem(100); got != 5 {
		t.Fatalf("失败的扣减不得改动背包，实际 %d", got)
	}
	if ok := p.RemoveItemByTpl(100, 4); !ok {
		t.Fatal("足量扣减应成功")
	}
	if got := p.CountItem(100); got != 1 {
		t.Fatalf("扣减后应剩 1，实际 %d", got)
	}
}

func TestEmptyStacksAreCompacted(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	p.AddItem(1, 100, 3, time.Now())
	p.RemoveItemByTpl(100, 3)
	if len(p.Bag.Items) != 0 {
		t.Fatalf("空格子应被清理，实际 %d", len(p.Bag.Items))
	}
}

// §6.1：只增字段不改 tag —— 缺失模块的老数据必须能安全加载。
func TestFromBlobsHandlesMissingModules(t *testing.T) {
	base := &pb.PlayerBase{Uid: 1001, Level: 9, Nick: "老玩家"}
	blob, _ := proto.Marshal(base)

	p, err := FromBlobs(1001, map[store.Module][]byte{store.ModBase: blob}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if p.Base.GetLevel() != 9 {
		t.Fatalf("base 未还原，level = %d", p.Base.GetLevel())
	}
	// 缺失的模块必须补成可用的空值，不能是 nil。
	if p.Bag == nil || p.Quest == nil || p.Social == nil || p.Mail == nil {
		t.Fatal("缺失模块必须补齐为空对象，否则后续访问会 panic")
	}
	if p.Bag.GetCapacity() == 0 {
		t.Fatal("背包容量应有默认值")
	}
	if p.Base.GetCurrency() == nil {
		t.Fatal("货币 map 必须非 nil，否则写入会 panic")
	}
}

func TestFromBlobsEmptyMakesNewPlayer(t *testing.T) {
	p, err := FromBlobs(2002, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if p.UID != 2002 || p.Base.GetLevel() != 1 {
		t.Fatalf("新玩家初始化异常: %+v", p.Base)
	}
}

func TestFromBlobsRejectsCorruptData(t *testing.T) {
	// 损坏的数据必须报错，绝不能静默返回空玩家 —— 那等于清档。
	_, err := FromBlobs(1001, map[store.Module][]byte{
		store.ModBase: []byte("这不是 protobuf \xff\xfe"),
	}, time.Now())
	if err == nil {
		t.Fatal("损坏的玩家数据必须报错，不能降级成空对象")
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	p.AddCurrency(uint32(protocol.CurrencyGold), 500)
	p.AddItem(7, 100, 2, time.Now())
	keys := store.NewKeys(store.TagUID, 1024, "lobby")

	kv, err := p.MarshalKeys(keys, []store.Module{store.ModBase, store.ModBag})
	if err != nil {
		t.Fatal(err)
	}
	if len(kv) != 2 {
		t.Fatalf("应序列化 2 个模块，实际 %d", len(kv))
	}

	blobs := map[store.Module][]byte{
		store.ModBase: kv[keys.Player(1001, store.ModBase)],
		store.ModBag:  kv[keys.Player(1001, store.ModBag)],
	}
	back, err := FromBlobs(1001, blobs, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if back.Currency(uint32(protocol.CurrencyGold)) != 500 {
		t.Fatal("货币未往返还原")
	}
	if back.CountItem(100) != 2 {
		t.Fatal("背包未往返还原")
	}
}

func TestProfileDerivedFromBase(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	p.Base.Nick = "阿强"
	p.Base.Level = 42
	p.Base.Power = 9999
	prof := p.Profile()
	if prof.GetNick() != "阿强" || prof.GetLevel() != 42 || prof.GetPower() != 9999 {
		t.Fatalf("摘要未正确派生: %+v", prof)
	}
}

// §10.1：下线后保留 5~10 分钟再卸载。
func TestIdleOnlyCountsAfterOffline(t *testing.T) {
	p := NewPlayer(1001, time.Now())
	p.Online = true
	if d := p.Idle(time.Now()); d != 0 {
		t.Fatalf("在线玩家不应计入空闲，实际 %v", d)
	}
	p.Online = false
	p.OfflineAt = time.Now().Add(-8 * time.Minute)
	if d := p.Idle(time.Now()); d < 7*time.Minute {
		t.Fatalf("空闲时长计算错误: %v", d)
	}
}
