// Package idgen 雪花 ID
//
//	1 符号 | 41 时间戳(ms) | 10 workerID | 12 序列号
//	           69 年          1024 节点      4096/ms/节点
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

	// SpinTolerance 以内的回拨当成 NTP 微调，自旋等它追上来
	SpinTolerance = 10 * time.Millisecond
)

// DefaultEpoch 纪元起点，41 bit 够用到 2093 年。上线后不能改
var DefaultEpoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// ErrClockBackwards 表示时钟大幅回拨，已经停止发号。宁可这个进程不服务，
// 也不能继续发可能重复的 ID
var ErrClockBackwards = errors.New("idgen: 时钟大幅回拨，已停止发号")

// ErrHalted 表示发号器停着呢，等时钟修正
var ErrHalted = errors.New("idgen: 发号器已停止，等待时钟修正")

// Generator 进程内的发号器，并发安全
type Generator struct {
	mu       sync.Mutex
	workerID uint64
	epochMs  int64
	lastTS   int64
	seq      uint64
	halted   bool

	now func() int64 // 可注入，方便测试
}

// New 构造发号器，workerID 来自 ident.Identity.WorkerID()
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

func NewFromIdentity(id *ident.Identity) (*Generator, error) { return New(id.WorkerID()) }

// Next 生成下一个 ID
//
// 时钟回拨 10ms 以内当 NTP 微调，自旋等它追上来；超过就停止发号并告警。
// workerID 绑容器不复用，所以跨进程回拨不用管，这里只处理单进程内的
func (g *Generator) Next() (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()

	if g.halted {
		// 停着的时候，得等时钟追过上次发号时刻才恢复
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
			// 小回拨，自旋等它追上来
			metrics.IDClockBackwards.WithLabelValues("minor").Inc()
			for now < g.lastTS {
				time.Sleep(time.Duration(g.lastTS-now) * time.Millisecond)
				now = g.now()
			}
		} else {
			// 大回拨，停止发号并告警
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
			// 这一毫秒的 4096 个用完了，等下一毫秒
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

// MustNext 失败就 panic，只给启动期检查那种没法降级的地方用。
// 业务路径请用 Next 自己处理错误
func (g *Generator) MustNext() uint64 {
	id, err := g.Next()
	if err != nil {
		panic(err)
	}
	return id
}

// NextN 一次生成 n 个，开箱扫荡这类批量产出用
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

// Halted 报告是不是因为大回拨停了
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

// Parts 拆开的雪花 ID，排查用
type Parts struct {
	Timestamp time.Time
	WorkerID  uint32
	Seq       uint32
	SvcType   ident.SvcType
	NodeSeq   int
}

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
