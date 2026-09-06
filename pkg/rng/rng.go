// Package rng 提供可审计、可复现的随机数源。
//
// 评审 P0-4 的修复：仓库里原本没有任何 RNG，而 RNG 就是 slots 的全部。
//
// 三条硬要求，决定了这里为什么不能随手用 math/rand：
//
//	一、不可预测。math/rand 的状态可由少量输出反推，等于把 paytable 送人。
//	    这里用 ChaCha8（Go 标准库 math/rand/v2 提供），密码学强度的流密码。
//
//	二、可复现。客服要查「这把为什么没中」，必须能凭 round_id 取出当时的种子，
//	    重放出位对位一致的结果。因此种子必须显式持有并随回合落盘。
//
//	三、种子不可猜。种子由 crypto/rand 产生，不用时间戳 ——
//	    用时间戳播种的 slots 在历史上被薅过很多次。
package rng

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand/v2"
)

// SeedSize 是种子长度（ChaCha8 要求 32 字节）。
const SeedSize = 32

// Seed 是一颗 RNG 种子。
type Seed [SeedSize]byte

// NewSeed 用 crypto/rand 生成一颗种子。
func NewSeed() (Seed, error) {
	var s Seed
	if _, err := crand.Read(s[:]); err != nil {
		return s, fmt.Errorf("rng: 生成种子失败: %w", err)
	}
	return s, nil
}

// Hex 返回种子的十六进制表示，用于落盘与审计。
func (s Seed) Hex() string { return hex.EncodeToString(s[:]) }

// ParseSeed 从十六进制还原种子，用于复算历史回合。
func ParseSeed(h string) (Seed, error) {
	var s Seed
	b, err := hex.DecodeString(h)
	if err != nil {
		return s, fmt.Errorf("rng: 种子解析失败: %w", err)
	}
	if len(b) != SeedSize {
		return s, fmt.Errorf("rng: 种子长度应为 %d 字节，实际 %d", SeedSize, len(b))
	}
	copy(s[:], b)
	return s, nil
}

// Source 是一个确定性随机数源：同一颗种子 + 同一串调用顺序 = 同一串输出。
//
// 非并发安全，且**不应该**并发使用：一个回合的随机数必须是一条确定的序列，
// 并发取数会让复算失效。每个回合创建一个 Source。
type Source struct {
	seed Seed
	r    *mrand.Rand
	n    uint64 // 已消费的随机数个数，便于排查复算不一致
}

// New 用给定种子构造随机源。
func New(seed Seed) *Source {
	chacha := mrand.NewChaCha8(seed)
	return &Source{seed: seed, r: mrand.New(chacha)}
}

// NewRandom 生成一颗新种子并构造随机源。
func NewRandom() (*Source, error) {
	seed, err := NewSeed()
	if err != nil {
		return nil, err
	}
	return New(seed), nil
}

// Seed 返回本随机源的种子。
func (s *Source) Seed() Seed { return s.seed }

// SeedHex 返回种子的十六进制表示。
func (s *Source) SeedHex() string { return s.seed.Hex() }

// Consumed 返回已消费的随机数个数。复算时两边这个值应当一致。
func (s *Source) Consumed() uint64 { return s.n }

// IntN 返回 [0, n) 内的均匀随机整数。
//
// 用标准库的无偏实现，不用 %n —— 取模会让小数值出现得略多，
// 在 slots 里这种偏差会直接体现为 RTP 偏移。
func (s *Source) IntN(n int) int {
	s.n++
	return s.r.IntN(n)
}

// Uint32N 返回 [0, n) 内的均匀随机数。
func (s *Source) Uint32N(n uint32) uint32 {
	s.n++
	return s.r.Uint32N(n)
}

// Int64N 返回 [0, n) 内的均匀随机数。
func (s *Source) Int64N(n int64) int64 {
	s.n++
	return s.r.Int64N(n)
}

// Float64 返回 [0.0, 1.0) 内的随机数。
//
// 概率判定尽量用 WeightedPick / Chance 而不是浮点比较：
// 浮点在不同平台可能有末位差异，会让复算对不上。
func (s *Source) Float64() float64 {
	s.n++
	return s.r.Float64()
}

// Chance 以 num/den 的概率返回 true。用整数运算，跨平台结果一致。
func (s *Source) Chance(num, den int64) bool {
	if num <= 0 || den <= 0 {
		return false
	}
	if num >= den {
		return true
	}
	return s.Int64N(den) < num
}

// WeightedPick 按权重选择一项，返回下标。权重总和为 0 时返回 -1。
func (s *Source) WeightedPick(weights []int64) int {
	var total int64
	for _, w := range weights {
		if w > 0 {
			total += w
		}
	}
	if total <= 0 {
		return -1
	}
	roll := s.Int64N(total)
	for i, w := range weights {
		if w <= 0 {
			continue
		}
		if roll < w {
			return i
		}
		roll -= w
	}
	return len(weights) - 1
}

// Shuffle 原地洗牌。
func (s *Source) Shuffle(n int, swap func(i, j int)) {
	s.n++
	s.r.Shuffle(n, swap)
}
