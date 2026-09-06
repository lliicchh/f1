// Package gameconf 是游戏配置表：加载、校验、内容哈希版本化。
//
// 评审 P1-3 的修复。原来 paytable、商品、经验曲线全是硬编码 switch，
// 两个后果：调个数值要发版；更要命的是**历史回合无法复算** ——
// 没人知道当时用的是哪一版配置，客服查单和监管抽查都无从下手。
//
// 版本号取配置内容的哈希，而不是人工维护的版本字段：
// 人工版本号一定会有人忘了改，内容哈希不会。每个回合落盘时记下这个版本，
// 复算时用同版本配置 + 同种子即可位对位重放。
package gameconf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
)

// Symbol 是 slots 的符号编号。
type Symbol uint32

// Config 是一份完整的游戏配置。加载后只读，热更新走整体替换。
type Config struct {
	Slots    map[string]*SlotMachine `json:"slots"`
	Gacha    map[string]*GachaPool   `json:"gacha"`
	Products map[uint32]*Product     `json:"products"`
	Items    map[uint32]*ItemDef     `json:"items"`
	Jackpots map[string]*Jackpot     `json:"jackpots"`
	LevelExp []uint64                `json:"level_exp"`
	RG       RGLimits                `json:"rg"`
	Bag      BagConf                 `json:"bag"`

	version string
}

// SlotMachine 是一台老虎机的完整数学模型。
//
// RTP 由 Reels（符号分布）与 Paytable（赔付）共同决定，改任何一个都会移动 RTP。
// TheoreticalRTP 是「设计意图」，由蒙特卡洛回归测试守着 ——
// 改错一个 reel 符号可能让 RTP 从 96% 跳到 130%，那必须在 CI 挡住而不是上线后看报表。
type SlotMachine struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Rows int    `json:"rows"`

	// Reels[i] 是第 i 轴的符号带。每次旋转在每轴随机取一个停止位，
	// 向下连取 Rows 个符号（环形）作为该轴可见区域。
	Reels [][]Symbol `json:"reels"`

	// Paylines[i][j] = 第 i 条线在第 j 轴上取第几行。
	Paylines [][]int `json:"paylines"`

	// Paytable[symbol][k] = 连中 k+1 个的赔率，单位是「线注」的倍数。
	Paytable map[Symbol][]int64 `json:"paytable"`

	Wild    Symbol `json:"wild"`
	Scatter Symbol `json:"scatter"`

	// ScatterPays[k] = 出现 k+1 个 scatter 的赔率，单位是「总注」的倍数。
	ScatterPays []int64 `json:"scatter_pays"`
	// FreeSpins[k] = 出现 k+1 个 scatter 授予的免费旋转次数。
	FreeSpins []uint32 `json:"free_spins"`
	// FreeSpinMultiplier 是免费旋转期间的派彩倍数。
	FreeSpinMultiplier uint32 `json:"free_spin_multiplier"`
	// FreeSpinRetrigger 允许免费旋转中再次触发免费旋转。
	FreeSpinRetrigger bool `json:"free_spin_retrigger"`

	// BetLevels 是允许的总投注档位。不在档位里的下注一律拒绝 ——
	// 任由客户端指定金额是另一种形式的造币。
	BetLevels []int64 `json:"bet_levels"`
	Currency  uint32  `json:"currency"`

	TheoreticalRTP float64 `json:"theoretical_rtp"`

	// 奖池联动。
	JackpotPool      string `json:"jackpot_pool"`
	JackpotContribBP int64  `json:"jackpot_contrib_bp"` // 万分比，从每次投注中注入
	JackpotChanceNum int64  `json:"jackpot_chance_num"` // 中奖概率分子
	JackpotChanceDen int64  `json:"jackpot_chance_den"` // 中奖概率分母
}

// LineCount 返回中奖线数量。
func (m *SlotMachine) LineCount() int { return len(m.Paylines) }

// ValidBet 报告下注额是否在允许档位内。
func (m *SlotMachine) ValidBet(bet int64) bool {
	for _, b := range m.BetLevels {
		if b == bet {
			return true
		}
	}
	return false
}

// GachaPool 是一个卡池。
type GachaPool struct {
	ID       string       `json:"id"`
	Cost     int64        `json:"cost"`
	Currency uint32       `json:"currency"`
	Entries  []GachaEntry `json:"entries"`

	// PityTop 抽必出最高稀有度；PityHigh 抽必出高稀有度。
	// 保底是抽卡的合规底线：概率公示了就必须与实现一致，
	// 而没有保底的卡池在多数发行地已经不可过审。
	PityTop    uint32 `json:"pity_top"`
	PityHigh   uint32 `json:"pity_high"`
	TopRarity  uint32 `json:"top_rarity"`
	HighRarity uint32 `json:"high_rarity"`
	// TenPullGuarantee 十连内保证至少一个高稀有度。
	TenPullGuarantee bool `json:"ten_pull_guarantee"`
}

// GachaEntry 是卡池中的一项。
type GachaEntry struct {
	TplID  uint32 `json:"tpl_id"`
	Rarity uint32 `json:"rarity"`
	Weight int64  `json:"weight"`
	Count  int64  `json:"count"`
}

// Product 是一个充值商品。
type Product struct {
	ID         uint32   `json:"id"`
	Currency   uint32   `json:"currency"`
	Amount     int64    `json:"amount"`      // 到账数量
	PriceCents int64    `json:"price_cents"` // 售价（最小货币单位）
	Channels   []string `json:"channels"`    // 允许的支付渠道
}

// AllowsChannel 报告某渠道是否可购买该商品。
func (p *Product) AllowsChannel(ch string) bool {
	if len(p.Channels) == 0 {
		return false
	}
	for _, c := range p.Channels {
		if c == ch {
			return true
		}
	}
	return false
}

// ItemDef 是道具定义。
type ItemDef struct {
	TplID     uint32 `json:"tpl_id"`
	Name      string `json:"name"`
	Stackable bool   `json:"stackable"`
	MaxStack  int64  `json:"max_stack"`
	Rarity    uint32 `json:"rarity"`
}

// Jackpot 是奖池配置。
type Jackpot struct {
	ID       string `json:"id"`
	Currency uint32 `json:"currency"`
	Seed     int64  `json:"seed"` // 派彩后重置到的底注
}

// RGLimits 是责任游戏的默认限额。
//
// 玩家可以把自己的限额调得更严，但不能调得比这里更松。
type RGLimits struct {
	DailyBetLimit  int64 `json:"daily_bet_limit"`
	DailyLossLimit int64 `json:"daily_loss_limit"`
	SessionLimit   int64 `json:"session_limit_seconds"`
	// ResetOffsetMinutes 是结算日的时区偏移（分钟）。
	// 必须显式配置：跨日重置发生在哪一刻，是限额能否被绕过的关键。
	ResetOffsetMinutes int   `json:"reset_offset_minutes"`
	NoticeInterval     int64 `json:"notice_interval_seconds"`
}

// BagConf 是背包配置。
type BagConf struct {
	Capacity uint32 `json:"capacity"`
}

// Version 返回配置内容哈希（前 16 位十六进制）。
func (c *Config) Version() string { return c.version }

// Machine 按 ID 取一台老虎机。
func (c *Config) Machine(id string) (*SlotMachine, bool) {
	m, ok := c.Slots[id]
	return m, ok
}

// Pool 按 ID 取一个卡池。
func (c *Config) Pool(id string) (*GachaPool, bool) {
	p, ok := c.Gacha[id]
	return p, ok
}

// Product 按 ID 取一个商品。
func (c *Config) Product(id uint32) (*Product, bool) {
	p, ok := c.Products[id]
	return p, ok
}

// Item 按模板 ID 取道具定义。
func (c *Config) Item(tpl uint32) (*ItemDef, bool) {
	d, ok := c.Items[tpl]
	return d, ok
}

// Stackable 报告道具是否可堆叠。未定义的道具按不可堆叠处理（保守）。
func (c *Config) Stackable(tpl uint32) bool {
	if d, ok := c.Items[tpl]; ok {
		return d.Stackable
	}
	return false
}

// ExpToLevel 返回升到下一级所需经验。
func (c *Config) ExpToLevel(level uint32) uint64 {
	if len(c.LevelExp) == 0 {
		return 100 * uint64(level+1)
	}
	i := int(level)
	if i >= len(c.LevelExp) {
		i = len(c.LevelExp) - 1
	}
	return c.LevelExp[i]
}

// computeVersion 计算内容哈希。
//
// 用规范化 JSON（map 键有序）保证同样的配置在任何机器上得到同样的版本号。
func (c *Config) computeVersion() error {
	raw, err := json.Marshal(canonical(c))
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	c.version = hex.EncodeToString(sum[:])[:16]
	return nil
}

// canonical 把配置转成键有序的中间结构，保证哈希稳定。
func canonical(c *Config) any {
	slotIDs := make([]string, 0, len(c.Slots))
	for id := range c.Slots {
		slotIDs = append(slotIDs, id)
	}
	sort.Strings(slotIDs)
	slots := make([]any, 0, len(slotIDs))
	for _, id := range slotIDs {
		m := c.Slots[id]
		syms := make([]int, 0, len(m.Paytable))
		for s := range m.Paytable {
			syms = append(syms, int(s))
		}
		sort.Ints(syms)
		pt := make([]any, 0, len(syms))
		for _, s := range syms {
			pt = append(pt, []any{s, m.Paytable[Symbol(s)]})
		}
		slots = append(slots, []any{id, m.Rows, m.Reels, m.Paylines, pt,
			m.Wild, m.Scatter, m.ScatterPays, m.FreeSpins, m.FreeSpinMultiplier,
			m.FreeSpinRetrigger, m.BetLevels, m.Currency, m.JackpotPool,
			m.JackpotContribBP, m.JackpotChanceNum, m.JackpotChanceDen})
	}

	poolIDs := make([]string, 0, len(c.Gacha))
	for id := range c.Gacha {
		poolIDs = append(poolIDs, id)
	}
	sort.Strings(poolIDs)
	pools := make([]any, 0, len(poolIDs))
	for _, id := range poolIDs {
		pools = append(pools, []any{id, c.Gacha[id]})
	}

	prodIDs := make([]int, 0, len(c.Products))
	for id := range c.Products {
		prodIDs = append(prodIDs, int(id))
	}
	sort.Ints(prodIDs)
	prods := make([]any, 0, len(prodIDs))
	for _, id := range prodIDs {
		prods = append(prods, []any{id, c.Products[uint32(id)]})
	}

	itemIDs := make([]int, 0, len(c.Items))
	for id := range c.Items {
		itemIDs = append(itemIDs, int(id))
	}
	sort.Ints(itemIDs)
	items := make([]any, 0, len(itemIDs))
	for _, id := range itemIDs {
		items = append(items, []any{id, c.Items[uint32(id)]})
	}

	jpIDs := make([]string, 0, len(c.Jackpots))
	for id := range c.Jackpots {
		jpIDs = append(jpIDs, id)
	}
	sort.Strings(jpIDs)
	jps := make([]any, 0, len(jpIDs))
	for _, id := range jpIDs {
		jps = append(jps, []any{id, c.Jackpots[id]})
	}

	return []any{slots, pools, prods, items, jps, c.LevelExp, c.RG, c.Bag}
}

// Validate 做上线前的自检。配置错误必须在启动时暴露，而不是在玩家手里暴露。
func (c *Config) Validate() error {
	if len(c.Slots) == 0 {
		return fmt.Errorf("gameconf: 至少要配置一台老虎机")
	}
	for id, m := range c.Slots {
		if m.ID != id {
			return fmt.Errorf("gameconf: 老虎机 %q 的 id 字段是 %q，不一致", id, m.ID)
		}
		if m.Rows <= 0 {
			return fmt.Errorf("gameconf: %s 的 rows 必须为正", id)
		}
		if len(m.Reels) == 0 {
			return fmt.Errorf("gameconf: %s 没有配置轴带", id)
		}
		for i, strip := range m.Reels {
			if len(strip) < m.Rows {
				return fmt.Errorf("gameconf: %s 第 %d 轴的符号数 %d 少于行数 %d", id, i, len(strip), m.Rows)
			}
		}
		if len(m.Paylines) == 0 {
			return fmt.Errorf("gameconf: %s 没有配置中奖线", id)
		}
		for i, line := range m.Paylines {
			if len(line) != len(m.Reels) {
				return fmt.Errorf("gameconf: %s 第 %d 条线的长度 %d 与轴数 %d 不符", id, i, len(line), len(m.Reels))
			}
			for j, row := range line {
				if row < 0 || row >= m.Rows {
					return fmt.Errorf("gameconf: %s 第 %d 条线第 %d 轴的行号 %d 越界", id, i, j, row)
				}
			}
		}
		if len(m.BetLevels) == 0 {
			return fmt.Errorf("gameconf: %s 没有配置下注档位", id)
		}
		lines := int64(len(m.Paylines))
		for _, b := range m.BetLevels {
			if b <= 0 {
				return fmt.Errorf("gameconf: %s 的下注档位必须为正", id)
			}
			if b%lines != 0 {
				// 线注 = 总注 / 线数，除不尽会产生舍入，舍入在钱上是不可接受的。
				return fmt.Errorf("gameconf: %s 的下注档位 %d 不能被线数 %d 整除", id, b, lines)
			}
		}
		if m.JackpotPool != "" {
			if _, ok := c.Jackpots[m.JackpotPool]; !ok {
				return fmt.Errorf("gameconf: %s 引用了不存在的奖池 %q", id, m.JackpotPool)
			}
			if m.JackpotChanceDen <= 0 {
				return fmt.Errorf("gameconf: %s 的奖池中奖分母必须为正", id)
			}
		}
		if m.TheoreticalRTP <= 0 || m.TheoreticalRTP > 2 {
			return fmt.Errorf("gameconf: %s 的理论 RTP %.4f 不合理", id, m.TheoreticalRTP)
		}
	}

	for id, p := range c.Gacha {
		if p.ID != id {
			return fmt.Errorf("gameconf: 卡池 %q 的 id 字段是 %q，不一致", id, p.ID)
		}
		if len(p.Entries) == 0 {
			return fmt.Errorf("gameconf: 卡池 %s 没有条目", id)
		}
		var total int64
		for _, e := range p.Entries {
			if e.Weight < 0 {
				return fmt.Errorf("gameconf: 卡池 %s 存在负权重", id)
			}
			total += e.Weight
		}
		if total <= 0 {
			return fmt.Errorf("gameconf: 卡池 %s 的权重总和为 0", id)
		}
		if p.PityTop == 0 {
			return fmt.Errorf("gameconf: 卡池 %s 必须配置保底次数", id)
		}
	}

	for id, p := range c.Products {
		if p.ID != id {
			return fmt.Errorf("gameconf: 商品 %d 的 id 字段是 %d，不一致", id, p.ID)
		}
		if p.Amount <= 0 || p.PriceCents <= 0 {
			return fmt.Errorf("gameconf: 商品 %d 的数量与价格必须为正", id)
		}
		if len(p.Channels) == 0 {
			return fmt.Errorf("gameconf: 商品 %d 未配置允许的支付渠道", id)
		}
	}

	if c.RG.ResetOffsetMinutes < -12*60 || c.RG.ResetOffsetMinutes > 14*60 {
		return fmt.Errorf("gameconf: 结算时区偏移 %d 分钟不合理", c.RG.ResetOffsetMinutes)
	}
	return nil
}

// Load 从 JSON 文件加载配置；path 为空时返回内置默认配置。
func Load(path string) (*Config, error) {
	c := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("gameconf: 读取配置失败: %w", err)
		}
		c = &Config{}
		if err := json.Unmarshal(raw, c); err != nil {
			return nil, fmt.Errorf("gameconf: 解析配置失败: %w", err)
		}
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if err := c.computeVersion(); err != nil {
		return nil, err
	}
	return c, nil
}

// Store 持有当前生效的配置，支持热替换。
//
// 读路径是无锁的原子读：spin 是热路径，不能每次去抢锁。
type Store struct {
	cur atomic.Pointer[Config]
}

// NewStore 构造配置存储。
func NewStore(c *Config) *Store {
	s := &Store{}
	s.cur.Store(c)
	return s
}

// Get 返回当前配置。
func (s *Store) Get() *Config { return s.cur.Load() }

// Replace 热替换配置。替换前会做完整校验，校验不过则保持原配置不动。
func (s *Store) Replace(c *Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.version == "" {
		if err := c.computeVersion(); err != nil {
			return err
		}
	}
	s.cur.Store(c)
	return nil
}
