package gameconf

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfigIsValid(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("默认配置应当合法: %v", err)
	}
	if c.Version() == "" {
		t.Fatal("配置必须有版本号")
	}
}

// 版本号取内容哈希：同样的内容必须得到同样的版本，改一个数字就必须变
//
// 这是「历史回合可复算」的前提，人工维护的版本号一定会有人忘了改
func TestVersionIsContentHash(t *testing.T) {
	a := Default()
	b := Default()
	if a.Version() != b.Version() {
		t.Fatalf("相同内容应得到相同版本：%s vs %s", a.Version(), b.Version())
	}

	c := Default()
	m := c.Slots["classic5"]
	m.Paytable[SymAce] = []int64{0, 0, 999, 999, 999}
	if err := c.computeVersion(); err != nil {
		t.Fatal(err)
	}
	if c.Version() == a.Version() {
		t.Fatal("改了赔付表版本号必须变化，否则历史回合无法区分用的哪一版")
	}

	// 改轴带同样要反映到版本号
	d := Default()
	d.Slots["classic5"].Reels[0][0] = SymWild
	if err := d.computeVersion(); err != nil {
		t.Fatal(err)
	}
	if d.Version() == a.Version() {
		t.Fatal("改了轴带版本号必须变化")
	}
}

func TestValidateCatchesBadMachines(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"行数为零", func(c *Config) { c.Slots["classic5"].Rows = 0 }},
		{"没有轴带", func(c *Config) { c.Slots["classic5"].Reels = nil }},
		{"轴带短于行数", func(c *Config) {
			c.Slots["classic5"].Reels[0] = []Symbol{SymTen}
		}},
		{"中奖线长度与轴数不符", func(c *Config) {
			c.Slots["classic5"].Paylines[0] = []int{0, 0}
		}},
		{"中奖线行号越界", func(c *Config) {
			c.Slots["classic5"].Paylines[0] = []int{9, 0, 0, 0, 0}
		}},
		{"下注档位不能被线数整除", func(c *Config) {
			c.Slots["classic5"].BetLevels = []int64{7}
		}},
		{"引用不存在的奖池", func(c *Config) {
			c.Slots["classic5"].JackpotPool = "nope"
		}},
		{"RTP 不合理", func(c *Config) {
			c.Slots["classic5"].TheoreticalRTP = 5
		}},
		{"id 与键不一致", func(c *Config) { c.Slots["classic5"].ID = "other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			tc.mutate(c)
			if err := c.Validate(); err == nil {
				t.Fatalf("%s 应当被校验拦下", tc.name)
			}
		})
	}
}

func TestValidateCatchesBadGachaAndProducts(t *testing.T) {
	t.Run("卡池无保底", func(t *testing.T) {
		c := Default()
		c.Gacha["standard"].PityTop = 0
		if err := c.Validate(); err == nil {
			t.Fatal("没有保底的卡池应被拒绝")
		}
	})
	t.Run("卡池权重全零", func(t *testing.T) {
		c := Default()
		for i := range c.Gacha["standard"].Entries {
			c.Gacha["standard"].Entries[i].Weight = 0
		}
		if err := c.Validate(); err == nil {
			t.Fatal("权重全零的卡池应被拒绝")
		}
	})
	t.Run("商品未配渠道", func(t *testing.T) {
		c := Default()
		c.Products[1].Channels = nil
		if err := c.Validate(); err == nil {
			t.Fatal("未配置支付渠道的商品应被拒绝")
		}
	})
	t.Run("商品价格为零", func(t *testing.T) {
		c := Default()
		c.Products[1].PriceCents = 0
		if err := c.Validate(); err == nil {
			t.Fatal("价格为零的商品应被拒绝")
		}
	})
}

// 下注档位必须能被线数整除，否则线注会产生舍入，钱上不接受舍入
func TestBetLevelsDivisible(t *testing.T) {
	c := Default()
	for id, m := range c.Slots {
		lines := int64(m.LineCount())
		for _, b := range m.BetLevels {
			if b%lines != 0 {
				t.Errorf("%s 的档位 %d 不能被 %d 条线整除", id, b, lines)
			}
		}
	}
}

func TestLoadFromFile(t *testing.T) {
	c := Default()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "game.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	if loaded.Version() != c.Version() {
		t.Fatalf("往返后版本应一致：%s vs %s", loaded.Version(), c.Version())
	}
	if _, ok := loaded.Machine("classic5"); !ok {
		t.Fatal("往返后应仍有 classic5")
	}
}

func TestLoadRejectsInvalidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte(`{"slots":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("空的老虎机配置应被拒绝")
	}
}

// 热替换必须原子：校验不过时旧配置保持不动
func TestStoreRejectsInvalidReplacement(t *testing.T) {
	good := Default()
	s := NewStore(good)

	bad := Default()
	bad.Slots["classic5"].Rows = 0
	if err := s.Replace(bad); err == nil {
		t.Fatal("非法配置不应被接受")
	}
	if s.Get().Version() != good.Version() {
		t.Fatal("替换失败后必须保持原配置")
	}

	next := Default()
	next.Slots["classic5"].TheoreticalRTP = 0.95
	if err := next.computeVersion(); err != nil {
		t.Fatal(err)
	}
	if err := s.Replace(next); err != nil {
		t.Fatalf("合法配置应能替换: %v", err)
	}
	if s.Get().Version() != next.Version() {
		t.Fatal("替换后应生效")
	}
}

func TestStackableFromConfig(t *testing.T) {
	c := Default()
	if !c.Stackable(100) {
		t.Error("金币袋应可堆叠")
	}
	if c.Stackable(4001) {
		t.Error("传说英雄不应可堆叠")
	}
	// 未定义的道具按保守处理（不可堆叠），避免误合并
	if c.Stackable(999999) {
		t.Error("未定义道具应按不可堆叠处理")
	}
}

func TestExpCurveMonotonic(t *testing.T) {
	c := Default()
	var prev uint64
	for lv := uint32(0); lv < 50; lv++ {
		got := c.ExpToLevel(lv)
		if got == 0 {
			t.Fatalf("等级 %d 的升级经验不应为 0", lv)
		}
		if got < prev {
			t.Fatalf("经验曲线应单调不减：等级 %d 的 %d < 上一级的 %d", lv, got, prev)
		}
		prev = got
	}
}
