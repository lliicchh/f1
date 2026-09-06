package account

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	m := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStore(rdb, "acct")
}

func TestResolveCreatesOnceThenReturnsSameUID(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	uid, created, err := s.Resolve(ctx, "sandbox", "o-1", 9001)
	if err != nil {
		t.Fatal(err)
	}
	if !created || uid != 9001 {
		t.Fatalf("首登应开号并用候选 uid，得到 uid=%d created=%v", uid, created)
	}

	// 第二次登录换了个候选 uid，必须还是回到同一个号
	uid2, created2, err := s.Resolve(ctx, "sandbox", "o-1", 9002)
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Error("第二次登录不该再开号")
	}
	if uid2 != 9001 {
		t.Fatalf("同一个渠道账号必须回到同一个 uid，得到 %d", uid2)
	}
}

// 并发首登只能开一个号
//
// 同一个玩家在两台设备上同时点登录、或者客户端超时重发，都会走到这里。
// 开出两个号意味着玩家充的钱进了另一个号，事后没法合并
func TestConcurrentFirstLoginOpensOneAccount(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	const n = 20
	uids := make([]uint64, n)
	creates := make([]bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uid, created, err := s.Resolve(ctx, "sandbox", "racer", uint64(10000+i))
			if err != nil {
				t.Error(err)
				return
			}
			uids[i], creates[i] = uid, created
		}(i)
	}
	wg.Wait()

	nCreated := 0
	for _, c := range creates {
		if c {
			nCreated++
		}
	}
	if nCreated != 1 {
		t.Fatalf("并发首登应只开一个号，实际开了 %d 个", nCreated)
	}
	for i, u := range uids {
		if u != uids[0] {
			t.Fatalf("第 %d 个拿到的 uid=%d 与第一个 %d 不同", i, u, uids[0])
		}
	}

	// 开号的同时反表也要写上，不然这个号列不出任何绑定，也解不了绑
	list, err := s.List(ctx, uids[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Channel != "sandbox" || list[0].OpenID != "racer" {
		t.Fatalf("开号时应同时写反表，得到 %+v", list)
	}
}

func TestBindAddsSecondChannel(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	uid, _, err := s.Resolve(ctx, "sandbox", "o-1", 9001)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Bind(ctx, uid, "apple", "apple-1"); err != nil {
		t.Fatalf("绑第二个渠道应成功: %v", err)
	}

	// 从新渠道登录要回到同一个号
	got, created, err := s.Resolve(ctx, "apple", "apple-1", 9999)
	if err != nil {
		t.Fatal(err)
	}
	if created || got != uid {
		t.Fatalf("绑过的渠道登录应回到 uid=%d，得到 %d created=%v", uid, got, created)
	}

	list, err := s.List(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("应有两条绑定，得到 %+v", list)
	}
}

// 重复绑同一个账号当成功，客户端重发不该报错
func TestBindIsIdempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	uid, _, _ := s.Resolve(context.Background(), "sandbox", "o-1", 9001)

	for i := 0; i < 3; i++ {
		if err := s.Bind(ctx, uid, "apple", "apple-1"); err != nil {
			t.Fatalf("第 %d 次重复绑定应成功: %v", i+1, err)
		}
	}
	list, _ := s.List(ctx, uid)
	if len(list) != 2 {
		t.Fatalf("重复绑定不该多出记录，得到 %+v", list)
	}
}

// 一个渠道账号绝不能指向两个 uid
//
// 放过去的话，玩家 A 把自己的微信绑到玩家 B 的号上，之后 A 用微信登录
// 进的是 B 的号，等于把号送人了
func TestBindRejectsAccountOwnedByOthers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	a, _, _ := s.Resolve(ctx, "sandbox", "a", 9001)
	b, _, _ := s.Resolve(ctx, "sandbox", "b", 9002)
	if err := s.Bind(ctx, a, "apple", "apple-x"); err != nil {
		t.Fatal(err)
	}

	if err := s.Bind(ctx, b, "apple", "apple-x"); !errors.Is(err, ErrBindConflict) {
		t.Fatalf("抢别人已绑的渠道账号应被拒，得到 %v", err)
	}
	// 被抢的一方不受影响
	got, _, _ := s.Resolve(ctx, "apple", "apple-x", 0)
	if got != a {
		t.Fatalf("原绑定不该被改动，期望 %d 得到 %d", a, got)
	}
}

// 同一个 uid 在同一渠道只能绑一个账号
func TestBindRejectsSecondAccountOnSameChannel(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	uid, _, _ := s.Resolve(ctx, "sandbox", "o-1", 9001)

	if err := s.Bind(ctx, uid, "apple", "apple-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind(ctx, uid, "apple", "apple-2"); !errors.Is(err, ErrAlreadyBound) {
		t.Fatalf("同渠道再绑一个账号应被拒，得到 %v", err)
	}
}

// 最后一个绑定不能解，解了这个号就再也登不进来
func TestUnbindRefusesLastBinding(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	uid, _, _ := s.Resolve(ctx, "sandbox", "o-1", 9001)

	if err := s.Unbind(ctx, uid, "sandbox"); !errors.Is(err, ErrLastBinding) {
		t.Fatalf("最后一个绑定应拒绝解绑，得到 %v", err)
	}
	if got, _, _ := s.Resolve(ctx, "sandbox", "o-1", 0); got != uid {
		t.Fatal("解绑失败后绑定必须原样保留")
	}
}

// 解绑要把正反两张表一起清掉
//
// 只删反表的话，那个渠道账号会永远占着，谁都绑不上也登不进来
func TestUnbindClearsBothTables(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	uid, _, _ := s.Resolve(ctx, "sandbox", "o-1", 9001)
	if err := s.Bind(ctx, uid, "apple", "apple-1"); err != nil {
		t.Fatal(err)
	}

	if err := s.Unbind(ctx, uid, "apple"); err != nil {
		t.Fatalf("解绑应成功: %v", err)
	}

	list, _ := s.List(ctx, uid)
	if len(list) != 1 || list[0].Channel != "sandbox" {
		t.Fatalf("反表应只剩 sandbox，得到 %+v", list)
	}
	if _, ok, _ := s.UIDOf(ctx, "apple", "apple-1"); ok {
		t.Fatal("正表没清干净，这个渠道账号会被永久占用")
	}
	// 清干净了才能被别人绑走
	other, _, _ := s.Resolve(ctx, "sandbox", "other", 9002)
	if err := s.Bind(ctx, other, "apple", "apple-1"); err != nil {
		t.Fatalf("解绑后该渠道账号应可被重新绑定: %v", err)
	}
}

func TestUnbindUnknownChannel(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	uid, _, _ := s.Resolve(ctx, "sandbox", "o-1", 9001)

	if err := s.Unbind(ctx, uid, "apple"); !errors.Is(err, ErrNotBound) {
		t.Fatalf("解绑没绑过的渠道应返回 ErrNotBound，得到 %v", err)
	}
}

// openid 里带冒号不能把记录解析错，反表存的是 boundAt:openid
func TestOpenIDWithColon(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const openID = "oidc:issuer.example.com:sub-123"

	uid, created, err := s.Resolve(ctx, "oidc", openID, 9001)
	if err != nil || !created {
		t.Fatalf("首登失败: uid=%d created=%v err=%v", uid, created, err)
	}
	list, err := s.List(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].OpenID != openID {
		t.Fatalf("带冒号的 openid 被解析坏了: %+v", list)
	}
	if list[0].BoundAt.IsZero() {
		t.Error("绑定时间应被记录")
	}
}

// 所有账号键共用一个 hash tag，正反表才能进同一个 Lua
func TestKeysShareOneHashTag(t *testing.T) {
	s := newStore(t)
	for _, k := range []string{s.bindKey("sandbox", "o-1"), s.uidKey(9001)} {
		if !contains(k, "{acct}") {
			t.Errorf("键 %q 缺少 {acct} tag，Cluster 下会撞 CROSSSLOT", k)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
