// Package idgen 实现 §3.5 的雪花 ID 与 §3.6 的时钟回拨处理。
//
//	1 符号 | 41 时间戳(ms) | 10 workerID | 12 序列号
//	           69 年          1024 节点      4096/ms/节点
//
//	workerID 10 bit = svcType(3) | nodeSeq(7)
package idgen

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gamedev/f1/pkg/ident"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
)

const (
	SeqBits       = 12
	WorkerIDBits  = ident.NodeSeqBits + ident.SvcTypeBits // 10
	TimestampBits = 41

	MaxSeq      = (1 << SeqBits) - 1      // 4095
	MaxWorkerID = (1 << WorkerIDBits) - 1 // 1023

	workerShift = SeqBits                // 12
	tsShift     = SeqBits + WorkerIDBits // 22

	// SpinTolerance 小回拨容忍上限：NTP 微调，自旋等待追平（§3.6）。
	SpinTolerance = 10 * time.Millisecond
)

// DefaultEpoch 雪花纪元起点：2024-01-01 00:00:00 UTC。
// 41 bit 可表达约 69 年，即到 2093 年。纪元一经上线不可变更。
var DefaultEpoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// ErrClockBackwards 表示检测到大幅时钟回拨，已停止发号。
//
// 「大回拨绝不容忍继续发号，宁可该进程停止服务」（§3.6）。
var ErrClockBackwards = errors.New("idgen: 时钟大幅回拨，已停止发号")

// ErrHalted 表示发号器处于停止状态，等待时钟修正。
var ErrHalted = errors.New("idgen: 发号器已停止，等待时钟修正")

// Generator 是单进程内的雪花发号器，并发安全。
type Generator struct {
	mu       sync.Mutex
	workerID uint64
	epochMs  int64
	lastTS   int64
	seq      uint64
	halted   bool

	now func() int64 // 可注入，便于测试
}

// New 构造发号器。workerID 必须来自 ident.Identity.WorkerID()。
func New(workerID uint32) (*Generator, error) {
	if workerID > MaxWorkerID {
		return nil, fmt.Errorf("idgen: workerID 越界 %d，必须在 [0,%d]", workerID, MaxWorkerID)
	}
	return &Generator{
		workerID: uint64(workerID),
		epochMs:  DefaultEpoch,
		lastTS:   -1,
		now:      func() int64 { return time.Now().UnixMilli() },
	}, nil
}

// NewFromIdentity 是 New 的便捷封装。
func NewFromIdentity(id *ident.Identity) (*Generator, error) { return New(id.WorkerID()) }

// Next 生成下一个 ID。
//
// 时钟回拨处理（§3.6）：
//   - 回拨 <= 10ms：NTP 微调，自旋等待追平
//   - 回拨  > 10ms：停止发号 + 告警，绝不能继续
//
// 因为序号固定绑定容器、workerID 不会被其他进程复用，跨进程回拨问题不存在，
// 这里只需处理单进程内的回拨。
func (g *Generator) Next() (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()

	if g.halted {
		// 停止状态下，只有时钟追平到上次发号时刻之后才恢复。
		if now <= g.lastTS {
			return 0, ErrHalted
		}
		g.halted = false
		metrics.IDHalted.Set(0)
		logx.Warn("idgen: 时钟已追平，恢复发号", "last_ts", g.lastTS, "now", now)
	}

	if now < g.lastTS {
		diff := g.lastTS - now
		if diff <= SpinTolerance.Milliseconds() {
			// 小回拨：自旋等待追平。
			metrics.IDClockBackwards.WithLabelValues("minor").Inc()
			for now < g.lastTS {
				time.Sleep(time.Duration(g.lastTS-now) * time.Millisecond)
				now = g.now()
			}
		} else {
			// 大回拨：停止发号 + 告警。
			g.halted = true
			metrics.IDClockBackwards.WithLabelValues("major").Inc()
			metrics.IDHalted.Set(1)
			logx.Error("idgen: 检测到时钟大幅回拨，停止发号（告警）",
				"backwards_ms", diff, "last_ts", g.lastTS, "now", now,
				"hint", "NTP 必须用 slew 模式（chrony maxslewrate / ntpd -x），禁止 step")
			return 0, fmt.Errorf("%w: 回拨 %dms", ErrClockBackwards, diff)
		}
	}

	if now == g.lastTS {
		g.seq = (g.seq + 1) & MaxSeq
		if g.seq == 0 {
			// 同毫秒内 4096 个用尽，等待下一毫秒。
			metrics.IDSeqOverflow.Inc()
			now = g.waitNextMilli(now)
		}
	} else {
		g.seq = 0
	}

	g.lastTS = now

	elapsed := now - g.epochMs
	if elapsed < 0 {
		return 0, fmt.Errorf("idgen: 当前时间早于纪元起点，检查系统时钟")
	}
	if elapsed >= (1 << TimestampBits) {
		return 0, fmt.Errorf("idgen: 时间戳位溢出（超过纪元 69 年）")
	}

	id := uint64(elapsed)<<tsShift | g.workerID<<workerShift | g.seq
	metrics.IDGenerated.Inc()
	return id, nil
}

// MustNext 在发号失败时 panic。仅用于「宁可停服也不能发重复 ID」的路径，
// 且调用方已确认无法降级（例如启动期自检）。业务路径请用 Next 并处理错误。
func (g *Generator) MustNext() uint64 {
	id, err := g.Next()
	if err != nil {
		panic(err)
	}
	return id
}

// NextN 批量生成 n 个 ID（开箱、扫荡等批量产出道具 ID，§15）。
func (g *Generator) NextN(n int) ([]uint64, error) {
	out := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		id, err := g.Next()
		if err != nil {
			return out, err
		}
		out = append(out, id)
	}
	return out, nil
}

// Halted 报告是否因大回拨停止发号。
func (g *Generator) Halted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.halted
}

func (g *Generator) waitNextMilli(now int64) int64 {
	for now <= g.lastTS {
		time.Sleep(200 * time.Microsecond)
		now = g.now()
	}
	return now
}

// Parts 是雪花 ID 的分解结果，用于排查问题。
type Parts struct {
	Timestamp time.Time
	WorkerID  uint32
	Seq       uint32
	SvcType   ident.SvcType
	NodeSeq   int
}

// Parse 分解一个雪花 ID。
func Parse(id uint64) Parts {
	seq := uint32(id & MaxSeq)
	worker := uint32((id >> workerShift) & MaxWorkerID)
	ts := int64(id>>tsShift) + DefaultEpoch
	return Parts{
		Timestamp: time.UnixMilli(ts).UTC(),
		WorkerID:  worker,
		Seq:       seq,
		SvcType:   ident.SvcType(worker >> ident.NodeSeqBits),
		NodeSeq:   int(worker & ident.MaxNodeSeq),
	}
}

func (p Parts) String() string {
	return fmt.Sprintf("ts=%s worker=%d(svc=%s seq=%d) seq=%d",
		p.Timestamp.Format(time.RFC3339Nano), p.WorkerID, p.SvcType, p.NodeSeq, p.Seq)
}
