package idp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// maxRespBody 上游响应的读取上限
//
// 不封顶的话，上游一旦返回个巨大的错误页，这里就会把内存吃掉
const maxRespBody = 1 << 20

// HTTPDoer 给渠道 Provider 复用的 HTTP 客户端
//
// 存在的意义是把「什么算凭证不对、什么算上游故障」这条线统一定死：
// 4xx（除 429）当凭证不对，不重试；5xx、429、连接层错误当上游故障，可重试。
// 每个 provider 各自判一遍的话迟早会有人把 500 当成凭证不对，
// 那渠道抖一下玩家就会看到「账号无效」
type HTTPDoer struct{ c *http.Client }

// NewHTTPDoer 建客户端。timeout 为 0 取 3s，实际还受 Registry 的 ctx 约束
func NewHTTPDoer(timeout time.Duration) *HTTPDoer {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &HTTPDoer{c: &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}}
}

// GetJSON 发一次 GET 并把 JSON 解进 out
func (d *HTTPDoer) GetJSON(ctx context.Context, url string, hdr map[string]string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("%w: 构造请求失败: %v", ErrUpstream, err)
	}
	return d.do(req, hdr, out)
}

// PostJSON 发一次 JSON POST 并把 JSON 解进 out
func (d *HTTPDoer) PostJSON(ctx context.Context, url string, hdr map[string]string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%w: 序列化请求失败: %v", ErrUpstream, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("%w: 构造请求失败: %v", ErrUpstream, err)
	}
	req.Header.Set("Content-Type", "application/json")
	return d.do(req, hdr, out)
}

func (d *HTTPDoer) do(req *http.Request, hdr map[string]string, out any) error {
	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := d.c.Do(req)
	if err != nil {
		// 连接层错误一律算上游故障，客户端的凭证还没被看过
		return fmt.Errorf("%w: %v", ErrUpstream, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBody))
	if err != nil {
		return fmt.Errorf("%w: 读响应失败: %v", ErrUpstream, err)
	}

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return fmt.Errorf("%w: 被限流 %d", ErrUpstream, resp.StatusCode)
	case resp.StatusCode >= 500:
		return fmt.Errorf("%w: 上游 %d: %s", ErrUpstream, resp.StatusCode, snippet(raw))
	case resp.StatusCode >= 400:
		return fmt.Errorf("%w: 上游 %d: %s", ErrCredentialInvalid, resp.StatusCode, snippet(raw))
	}

	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		// 解不动多半是上游改了协议或者返回了错误页，算故障不算凭证问题
		return fmt.Errorf("%w: 响应不是预期 JSON: %v", ErrUpstream, err)
	}
	return nil
}

func snippet(b []byte) string {
	const n = 200
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}
