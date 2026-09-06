package logx

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"go.uber.org/zap/zapcore"
)

// capture 把全局 logger 接到一个 buffer 上，返回读取解析结果的函数
func capture(t *testing.T, lv zapcore.Level) (*bytes.Buffer, func() []map[string]any) {
	t.Helper()
	buf := &bytes.Buffer{}
	set(newCore(zapcore.AddSync(buf), lv, "json"), "s1-lobby-1")
	t.Cleanup(func() { set(newCore(stdout(), zapcore.InfoLevel, "text"), "") })

	return buf, func() []map[string]any {
		var out []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if line == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatalf("日志不是合法 JSON: %q: %v", line, err)
			}
			out = append(out, m)
		}
		return out
	}
}

// 变参必须落成字段，不能被拼进 msg
//
// zap 的 SugaredLogger 同时有 Infow（key-value）和 Info（print 风格），
// 接错一个整套日志就不可检索了，而且不报错
func TestArgsBecomeFields(t *testing.T) {
	_, read := capture(t, zapcore.DebugLevel)

	Info("玩家登录", "uid", 1001, "gate", "s1-gateway-1")
	Trace("tr-7").Warn("刷盘失败", "shard", 7)

	got := read()
	if len(got) != 2 {
		t.Fatalf("应有 2 条日志，实际 %d", len(got))
	}

	if got[0]["msg"] != "玩家登录" {
		t.Errorf("msg = %v，不该把字段拼进来", got[0]["msg"])
	}
	if got[0]["uid"] != float64(1001) || got[0]["gate"] != "s1-gateway-1" {
		t.Errorf("字段没落下来: %v", got[0])
	}
	if got[1]["trace_id"] != "tr-7" || got[1]["shard"] != float64(7) {
		t.Errorf("链式调用的字段没落下来: %v", got[1])
	}
}

// node 字段每条都要有，它和 trace_id 是排查问题的两个抓手
func TestNodeFieldOnEveryLine(t *testing.T) {
	_, read := capture(t, zapcore.DebugLevel)

	Info("一")
	L().Info("二")
	With("k", "v").Info("三")
	Trace("tr").Info("四")

	for _, m := range read() {
		if m["node"] != "s1-lobby-1" {
			t.Errorf("缺 node 字段: %v", m)
		}
	}
}

// caller 必须指向调用方，不能指向 logx.go
//
// 顶层函数比 Logger 的方法多垫一层，两条路径的 skip 不一样，很容易只对一半
func TestCallerPointsAtCallSite(t *testing.T) {
	_, read := capture(t, zapcore.DebugLevel)

	Info("顶层函数")
	L().Info("方法")
	Trace("tr").Info("链式")

	for _, m := range read() {
		caller, _ := m["caller"].(string)
		if !strings.HasPrefix(caller, "logx/logx_test.go:") {
			t.Errorf("caller = %q，应指向测试文件而不是 logx.go：%v", caller, m["msg"])
		}
	}
}

func TestLevelFilter(t *testing.T) {
	_, read := capture(t, zapcore.WarnLevel)

	Debug("debug")
	Info("info")
	Warn("warn")
	Error("error")

	got := read()
	if len(got) != 2 {
		t.Fatalf("Warn 级别下应只剩 2 条，实际 %d", len(got))
	}
	if got[0]["level"] != "WARN" || got[1]["level"] != "ERROR" {
		t.Errorf("级别名不对: %v %v", got[0]["level"], got[1]["level"])
	}
}

func TestTraceRoundTripsThroughContext(t *testing.T) {
	_, read := capture(t, zapcore.DebugLevel)

	ctx := WithTrace(t.Context(), "tr-42")
	if TraceFrom(ctx) != "tr-42" {
		t.Fatalf("TraceFrom = %q", TraceFrom(ctx))
	}
	Ctx(ctx).Info("带 trace")
	Ctx(t.Context()).Info("不带 trace")

	got := read()
	if got[0]["trace_id"] != "tr-42" {
		t.Errorf("ctx 里的 trace_id 没带上: %v", got[0])
	}
	if _, ok := got[1]["trace_id"]; ok {
		t.Errorf("没有 trace_id 时不该凭空造一个: %v", got[1])
	}
}
