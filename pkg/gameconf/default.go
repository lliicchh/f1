package gameconf

// 符号定义。低分符号（牌面）频率高、赔率低；高分符号相反。
const (
	SymTen Symbol = iota
	SymJack
	SymQueen
	SymKing
	SymAce
	SymMinor
	SymMajor
	SymWild
	SymScatter
)

// SymbolName 返回符号名，用于日志与客服排查。
func SymbolName(s Symbol) string {
	switch s {
	case SymTen:
		return "10"
	case SymJack:
		return "J"
	case SymQueen:
		return "Q"
	case SymKing:
		return "K"
	case SymAce:
		return "A"
	case SymMinor:
		return "MINOR"
	case SymMajor:
		return "MAJOR"
	case SymWild:
		return "WILD"
	case SymScatter:
		return "SCATTER"
	}
	return "?"
}

// strip 按 (符号, 个数) 展开成一条轴带。
//
// 轴带的符号构成就是这台机器的概率模型：改一个数字就会移动 RTP，
// 所以任何改动都必须跑一遍蒙特卡洛回归（见 internal/slots 的 RTP 测试）。
func strip(pairs ...any) []Symbol {
	out := make([]Symbol, 0, 32)
	for i := 0; i < len(pairs); i += 2 {
		sym := pairs[i].(Symbol)
		n := pairs[i+1].(int)
		for j := 0; j < n; j++ {
			out = append(out, sym)
		}
	}
	return out
}

// classicStrip 是标准轴带：32 格。
func classicStrip() []Symbol {
	return strip(
		SymTen, 6,
		SymJack, 6,
		SymQueen, 5,
		SymKing, 4,
		SymAce, 4,
		SymMinor, 3,
		SymMajor, 2,
		SymWild, 1,
		SymScatter, 1,
	)
}

// interleave 把轴带打散，避免相同符号连成一片。
//
// 这不改变各符号的出现概率（因此不改变单条线的 RTP），
// 但会显著改变「近失」（near miss）的视觉观感 —— 这是体验调优项，不是数学项。
func interleave(s []Symbol, offset int) []Symbol {
	out := make([]Symbol, len(s))
	n := len(s)
	step := 7 // 与 32 互质，保证遍历所有位置
	idx := offset % n
	for i := 0; i < n; i++ {
		out[i] = s[idx]
		idx = (idx + step) % n
	}
	return out
}

// Default 返回内置默认配置。
//
// 没有配置文件时也能跑起来，这对本地开发和测试很重要；
// 生产必须用配置文件，因为默认值里的 RTP 与商品价格都只是示例。
func Default() *Config {
	base := classicStrip()
	reels := make([][]Symbol, 5)
	for i := range reels {
		reels[i] = interleave(base, i*5)
	}

	c := &Config{
		Slots: map[string]*SlotMachine{
			"classic5": {
				ID:    "classic5",
				Name:  "Classic Five",
				Rows:  3,
				Reels: reels,
				// 10 条标准中奖线。
				Paylines: [][]int{
					{1, 1, 1, 1, 1},
					{0, 0, 0, 0, 0},
					{2, 2, 2, 2, 2},
					{0, 1, 2, 1, 0},
					{2, 1, 0, 1, 2},
					{0, 0, 1, 2, 2},
					{2, 2, 1, 0, 0},
					{1, 0, 1, 2, 1},
					{1, 2, 1, 0, 1},
					{0, 1, 1, 1, 2},
				},
				// 赔率单位是「线注」的倍数，下标 k 对应连中 k+1 个。
				Paytable: map[Symbol][]int64{
					SymTen:   {0, 0, 8, 24, 82},
					SymJack:  {0, 0, 8, 24, 82},
					SymQueen: {0, 0, 13, 41, 132},
					SymKing:  {0, 0, 16, 49, 165},
					SymAce:   {0, 0, 20, 66, 203},
					SymMinor: {0, 0, 33, 121, 406},
					SymMajor: {0, 0, 66, 241, 812},
					SymWild:  {0, 0, 82, 329, 1618},
				},
				Wild:    SymWild,
				Scatter: SymScatter,
				// scatter 赔率单位是「总注」的倍数。
				ScatterPays:        []int64{0, 0, 2, 11, 55},
				FreeSpins:          []uint32{0, 0, 10, 15, 20},
				FreeSpinMultiplier: 2,
				FreeSpinRetrigger:  true,
				// 总注必须能被线数整除，否则线注会产生舍入 —— 钱上不接受舍入。
				BetLevels:      []int64{10, 20, 50, 100, 200, 500, 1000},
				Currency:       1, // CurrencyGold
				TheoreticalRTP: 0.9600,

				JackpotPool:      "grand",
				JackpotContribBP: 100,     // 每次投注的 1% 注入奖池
				JackpotChanceNum: 1,       // 每次旋转 1/200000 的中奖概率
				JackpotChanceDen: 200_000, //（与投注额无关，避免大注刷池）
			},
		},

		Jackpots: map[string]*Jackpot{
			"grand": {ID: "grand", Currency: 1, Seed: 100_000},
		},

		Gacha: map[string]*GachaPool{
			"standard": {
				ID:       "standard",
				Cost:     160,
				Currency: 2, // CurrencyDiamond
				Entries: []GachaEntry{
					{TplID: 1001, Rarity: 1, Weight: 5500, Count: 1},
					{TplID: 1002, Rarity: 1, Weight: 2500, Count: 1},
					{TplID: 2001, Rarity: 2, Weight: 1200, Count: 1},
					{TplID: 2002, Rarity: 2, Weight: 600, Count: 1},
					{TplID: 3001, Rarity: 3, Weight: 150, Count: 1},
					{TplID: 4001, Rarity: 4, Weight: 50, Count: 1},
				},
				TopRarity:        4,
				HighRarity:       3,
				PityTop:          90,
				PityHigh:         10,
				TenPullGuarantee: true,
			},
		},

		Products: map[uint32]*Product{
			1: {ID: 1, Currency: 2, Amount: 60, PriceCents: 600, Channels: []string{"apple", "google", "sandbox"}},
			2: {ID: 2, Currency: 2, Amount: 300, PriceCents: 3000, Channels: []string{"apple", "google", "sandbox"}},
			3: {ID: 3, Currency: 2, Amount: 980, PriceCents: 9800, Channels: []string{"apple", "google", "sandbox"}},
		},

		Items: map[uint32]*ItemDef{
			100:  {TplID: 100, Name: "金币袋", Stackable: true, MaxStack: 9999, Rarity: 1},
			101:  {TplID: 101, Name: "体力药水", Stackable: true, MaxStack: 999, Rarity: 1},
			1001: {TplID: 1001, Name: "普通碎片", Stackable: true, MaxStack: 9999, Rarity: 1},
			1002: {TplID: 1002, Name: "精良碎片", Stackable: true, MaxStack: 9999, Rarity: 1},
			2001: {TplID: 2001, Name: "稀有装备", Stackable: false, MaxStack: 1, Rarity: 2},
			2002: {TplID: 2002, Name: "稀有饰品", Stackable: false, MaxStack: 1, Rarity: 2},
			3001: {TplID: 3001, Name: "史诗武器", Stackable: false, MaxStack: 1, Rarity: 3},
			4001: {TplID: 4001, Name: "传说英雄", Stackable: false, MaxStack: 1, Rarity: 4},
		},

		LevelExp: buildLevelCurve(100),

		RG: RGLimits{
			DailyBetLimit:  1_000_000,
			DailyLossLimit: 200_000,
			SessionLimit:   4 * 3600,
			// 结算日按 UTC+8。必须显式写死：这个值决定「跨日」发生在哪一刻，
			// 配错等于给了玩家一个绕过日限额的窗口。
			ResetOffsetMinutes: 8 * 60,
			NoticeInterval:     1800,
		},

		Bag: BagConf{Capacity: 200},
	}

	_ = c.computeVersion()
	return c
}

// buildLevelCurve 生成经验曲线。
func buildLevelCurve(maxLevel int) []uint64 {
	out := make([]uint64, maxLevel+1)
	for lv := 0; lv <= maxLevel; lv++ {
		n := uint64(lv + 1)
		out[lv] = 100*n + 10*n*n
	}
	return out
}
