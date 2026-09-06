// Package logx 全局结构化日志，底层是 zap
//
// 日志都带 node 字段，业务日志尽量再带上 trace_id，出问题时全靠这两个串起来
package logx

import (
	"context"
	"os"
	"strings"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger 带固定字段的日志句柄
//
// 只暴露 key-value 形式，不提供 zap 的 printf / print 风格。混用两种风格
// 会让日志一半能被检索一半不能
type Logger struct{ s *zap.SugaredLogger }

func (l *Logger) Debug(msg string, kv ...any) { l.s.Debugw(msg, kv...) }
func (l *Logger) Info(msg string, kv ...any)  { l.s.Infow(msg, kv...) }
func (l *Logger) Warn(msg string, kv ...any)  { l.s.Warnw(msg, kv...) }
func (l *Logger) Error(msg string, kv ...any) { l.s.Errorw(msg, kv...) }

// With 派生一个带附加字段的句柄
func (l *Logger) With(kv ...any) *Logger { return &Logger{s: l.s.With(kv...)} }

// Zap 返回底层 SugaredLogger，给需要传 zap 的第三方库用
func (l *Logger) Zap() *zap.SugaredLogger { return l.s }

var (
	mu     sync.RWMutex
	base   *Logger
	pkgLvl *Logger // 比 base 多跳一层，给本包的顶层函数用
	nodeID string
	undo   func()
)

func init() { set(newCore(stdout(), zapcore.InfoLevel, "text"), "") }

func stdout() zapcore.WriteSyncer { return zapcore.Lock(os.Stdout) }

// Init 配置全局 logger，level 取 debug/info/warn/error，format 取 text/json
func Init(node, level, format string) {
	var lv zapcore.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = zapcore.DebugLevel
	case "warn", "warning":
		lv = zapcore.WarnLevel
	case "error":
		lv = zapcore.ErrorLevel
	default:
		lv = zapcore.InfoLevel
	}
	set(newCore(stdout(), lv, format), node)
}

func newCore(w zapcore.WriteSyncer, lv zapcore.Level, format string) zapcore.Core {
	enc := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		MessageKey:     "msg",
		CallerKey:      "caller",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}
	var e zapcore.Encoder
	if strings.ToLower(format) == "json" {
		e = zapcore.NewJSONEncoder(enc)
	} else {
		e = zapcore.NewConsoleEncoder(enc)
	}
	return zapcore.NewCore(e, w, lv)
}

// set 换掉全局 logger
//
// 两个句柄的差别只在 caller 跳几层：base 给 Logger 的方法用，pkgLvl 给本包的
// Debug/Info/Warn/Error 用，后者中间多垫了一个函数。搞错了 caller 就会全指到
// logx.go 上，等于没有
func set(core zapcore.Core, node string) {
	z := zap.New(core)
	if node != "" {
		z = z.With(zap.String("node", node))
	}

	mu.Lock()
	defer mu.Unlock()
	if undo != nil {
		undo()
	}
	nodeID = node
	base = &Logger{s: z.WithOptions(zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()}
	pkgLvl = &Logger{s: z.WithOptions(zap.AddCaller(), zap.AddCallerSkip(2)).Sugar()}
	// 第三方库直接用标准库 log 打的东西也收进来
	undo = zap.RedirectStdLog(z)
}

// L 返回全局 logger
func L() *Logger {
	mu.RLock()
	defer mu.RUnlock()
	return base
}

func pkg() *Logger {
	mu.RLock()
	defer mu.RUnlock()
	return pkgLvl
}

// NodeID 返回当前进程 nodeID（Init 之后有效）
func NodeID() string {
	mu.RLock()
	defer mu.RUnlock()
	return nodeID
}

func With(kv ...any) *Logger { return L().With(kv...) }

func Trace(traceID string) *Logger { return L().With("trace_id", traceID) }

func Debug(msg string, kv ...any) { pkg().Debug(msg, kv...) }
func Info(msg string, kv ...any)  { pkg().Info(msg, kv...) }
func Warn(msg string, kv ...any)  { pkg().Warn(msg, kv...) }
func Error(msg string, kv ...any) { pkg().Error(msg, kv...) }

// Fatal 打完就退出，只给启动期那种没救的错误用
func Fatal(msg string, kv ...any) {
	pkg().Error(msg, kv...)
	Sync()
	os.Exit(1)
}

// Sync 冲掉缓冲，进程退出前调一次
func Sync() { _ = L().s.Sync() }

// Ctx 从 context 里取 trace_id 带上
func Ctx(ctx context.Context) *Logger {
	if v, ok := ctx.Value(traceKey{}).(string); ok && v != "" {
		return Trace(v)
	}
	return L()
}

type traceKey struct{}

// WithTrace 把 trace_id 放进 context
func WithTrace(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceKey{}, traceID)
}

// TraceFrom 从 context 取 trace_id
func TraceFrom(ctx context.Context) string {
	v, _ := ctx.Value(traceKey{}).(string)
	return v
}
