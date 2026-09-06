// Package rng 提供可复现的随机源
//
// 用 ChaCha8 而不是 math/rand：后者的状态能从少量输出反推出来，
// 在 slots 里玩家能算出下一把开什么。种子由 crypto/rand 生成，不用时间戳。
//
// 种子要随回合落盘。客服查「这把为什么没中」时，拿种子重放一遍就能复现
package rng

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand/v2"
)

// SeedSize 种子长度（ChaCha8 要求 32 字节）
const SeedSize = 32

// Seed 一颗 RNG 种子
type Seed [SeedSize]byte

// NewSeed 用 crypto/rand 生成一颗种子
func NewSeed() (Seed, error) {
	var s Seed
	if _, err := crand.Read(s[:]); err != nil {
		return s, fmt.Errorf("rng: 生成种子失败: %w", err)
	}
	return s, nil
}

// Hex 返回种子的十六进制表示，用于落盘与审计
func (s Seed) Hex() string { return hex.EncodeToString(s[:]) }

// ParseSeed 从十六进制还原种子，用于复算历史回合
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

// Source 确定性随机源：同一颗种子、同一串调用顺序，输出必然相同
//
// 不要并发使用，并发取数会打乱顺序，复算就对不上了。每个回合建一个
type Source struct {
	seed Seed
	r    *mrand.Rand
	n    uint64 // 已消费的随机数个数，便于排查复算不一致
}

func New(seed Seed) *Source {
	chacha := mrand.NewChaCha8(seed)
	return &Source{seed: seed, r: mrand.New(chacha)}
}

// NewRandom 生成一颗新种子并构造随机源
func NewRandom() (*Source, error) {
	seed, err := NewSeed()
	if err != nil {
		return nil, err
	}
	return New(seed), nil
}

func (s *Source) Seed() Seed { return s.seed }

func (s *Source) SeedHex() string { return s.seed.Hex() }

// Consumed 返回已取过的随机数个数，复算时两边应当一致
func (s *Source) Consumed() uint64 { return s.n }

// IntN 返回 [0, n)。用标准库的无偏实现，不用 %n：取模的偏差会体现成 RTP 偏移
func (s *Source) IntN(n int) int {
	s.n++
	return s.r.IntN(n)
}

// Uint32N 返回 [0, n) 内的均匀随机数
func (s *Source) Uint32N(n uint32) uint32 {
	s.n++
	return s.r.Uint32N(n)
}

// Int64N 返回 [0, n) 内的均匀随机数
func (s *Source) Int64N(n int64) int64 {
	s.n++
	return s.r.Int64N(n)
}

// Float64 返回 [0.0, 1.0)
//
// 概率判定优先用 Chance / WeightedPick：浮点在不同平台有末位差异，复算会对不上
func (s *Source) Float64() float64 {
	s.n++
	return s.r.Float64()
}

// Chance 以 num/den 的概率返回 true，整数运算，跨平台一致
func (s *Source) Chance(num, den int64) bool {
	if num <= 0 || den <= 0 {
		return false
	}
	if num >= den {
		return true
	}
	return s.Int64N(den) < num
}

// WeightedPick 按权重挑一项返回下标，权重全零返回 -1
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

// Shuffle 原地洗牌
func (s *Source) Shuffle(n int, swap func(i, j int)) {
	s.n++
	s.r.Shuffle(n, swap)
}
