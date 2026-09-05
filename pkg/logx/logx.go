// Package logx 提供全局结构化日志。
//
// 所有日志强制携带 node 字段（nodeID），跨进程排查时与 Envelope.from_node 对齐；
// 业务日志应尽量带上 trace_id（Envelope.trace_id），这是全链路追踪的唯一抓手（§5.3）。
package logx

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
)

var (
	mu     sync.RWMutex
	base   *slog.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	nodeID string
)

// Init 配置全局 logger。level 取 debug/info/warn/error，format 取 text/json。
func Init(node, level, format string) {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn", "warning":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lv}
	var h slog.Handler
	if strings.ToLower(format) == "json" {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}

	mu.Lock()
	defer mu.Unlock()
	nodeID = node
	base = slog.New(h).With("node", node)
	slog.SetDefault(base)
}

// L 返回全局 logger。
func L() *slog.Logger {
	mu.RLock()
	defer mu.RUnlock()
	return base
}

// NodeID 返回当前进程 nodeID（Init 之后有效）。
func NodeID() string {
	mu.RLock()
	defer mu.RUnlock()
	return nodeID
}

// With 返回带附加字段的 logger。
func With(args ...any) *slog.Logger { return L().With(args...) }

// Trace 返回带 trace_id 的 logger。
func Trace(traceID string) *slog.Logger { return L().With("trace_id", traceID) }

func Debug(msg string, args ...any) { L().Debug(msg, args...) }
func Info(msg string, args ...any)  { L().Info(msg, args...) }
func Warn(msg string, args ...any)  { L().Warn(msg, args...) }
func Error(msg string, args ...any) { L().Error(msg, args...) }

// Fatal 打印后直接退出。仅用于启动期不可恢复错误（如 NODE_SEQ 越界、nodeID 撞号）。
func Fatal(msg string, args ...any) {
	L().Error(msg, args...)
	os.Exit(1)
}

// Ctx 从 context 中取出 trace_id（若有）并附加。
func Ctx(ctx context.Context) *slog.Logger {
	if v, ok := ctx.Value(traceKey{}).(string); ok && v != "" {
		return Trace(v)
	}
	return L()
}

type traceKey struct{}

// WithTrace 把 trace_id 放进 context。
func WithTrace(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceKey{}, traceID)
}

// TraceFrom 取出 context 中的 trace_id。
func TraceFrom(ctx context.Context) string {
	v, _ := ctx.Value(traceKey{}).(string)
	return v
}
