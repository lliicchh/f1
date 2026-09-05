// Package bus 封装 NATS 连接、Envelope 编解码与 JetStream。
//
// §5.4 的四条注意事项在此落地：
//   - Slow consumer：必须注册 ErrorHandler 并告警
//   - 回调单 goroutine 串行：回调内只投递入 mailbox，不做业务（由调用方遵守，见 shard 包）
//   - Payload 默认 1MB 上限：发送前校验并计数
//   - 无订阅者直接丢弃：分片交接窗口靠客户端重试兜底
package bus

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/subject"
)

// ErrPayloadTooLarge 表示消息体超过服务端 max_payload。
var ErrPayloadTooLarge = errors.New("bus: payload 超过 max_payload 上限")

// ErrNoResponders 表示无订阅者 —— 通常意味着目标分片正在交接或尚未被认领。
// 客户端应重试（§5.4 / §10.2）。
var ErrNoResponders = errors.New("bus: 无订阅者（分片可能正在交接，请重试）")

// Conn 是 NATS 连接封装。
type Conn struct {
	nc     *nats.Conn
	nodeID string
	seq    atomic.Uint32
}

// Options 是连接参数。
type Options struct {
	URL    string
	NodeID string
	// PendingLimitMsgs/Bytes 控制订阅的 pending 缓冲。超限即 slow consumer，
	// NATS 会静默丢弃，因此必须配合 ErrorHandler 告警。
	PendingLimitMsgs  int
	PendingLimitBytes int
}

// Connect 建立连接并注册必须的错误处理器。
func Connect(opt Options) (*Conn, error) {
	if opt.PendingLimitMsgs == 0 {
		opt.PendingLimitMsgs = 65536
	}
	if opt.PendingLimitBytes == 0 {
		opt.PendingLimitBytes = 64 * 1024 * 1024
	}

	c := &Conn{nodeID: opt.NodeID}

	opts := []nats.Option{
		nats.Name(opt.NodeID),
		nats.MaxReconnects(-1), // 永久重连，NATS 单节点故障靠客户端库自愈（§13）
		nats.ReconnectWait(500 * time.Millisecond),
		nats.ReconnectJitter(200*time.Millisecond, time.Second),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
		nats.Timeout(5 * time.Second),
		nats.FlusherTimeout(5 * time.Second),

		// —— Slow consumer 必须告警（§5.4 / §14）——
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			subj := "-"
			if sub != nil {
				subj = subject.Prefix(sub.Subject)
			}
			if errors.Is(err, nats.ErrSlowConsumer) {
				metrics.NATSSlowConsumer.WithLabelValues(subj).Inc()
				var pending int
				if sub != nil {
					if p, _, e := sub.Pending(); e == nil {
						pending = p
					}
				}
				logx.Error("NATS slow consumer —— 消息正在被静默丢弃（告警）",
					"subject", subj, "pending", pending, "err", err,
					"hint", "回调内只应投递 mailbox，不做业务；或提高 PendingLimits")
				return
			}
			metrics.NATSAsyncErrors.WithLabelValues("async").Inc()
			logx.Error("NATS 异步错误", "subject", subj, "err", err)
		}),

		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			metrics.NATSDisconnects.Inc()
			logx.Warn("NATS 断开", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			metrics.NATSReconnects.Inc()
			logx.Warn("NATS 已重连", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logx.Warn("NATS 连接已关闭")
		}),
	}

	nc, err := nats.Connect(opt.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("连接 NATS 失败 (%s): %w", opt.URL, err)
	}
	c.nc = nc
	logx.Info("NATS 已连接", "url", nc.ConnectedUrl(), "max_payload", nc.MaxPayload())
	return c, nil
}

// NC 返回底层连接（JetStream、Drain 等需要）。
func (c *Conn) NC() *nats.Conn { return c.nc }

// NodeID 返回本进程 nodeID。
func (c *Conn) NodeID() string { return c.nodeID }

// NextSeq 返回下一个应答匹配序号。
func (c *Conn) NextSeq() uint32 { return c.seq.Add(1) }

// NewTraceID 生成 trace_id。
func NewTraceID() string { return nuid.Next() }

// ---------------------------- Envelope ----------------------------

// NewEnvelope 构造信封，自动填充 from_node / ts_ms / seq，trace_id 为空时自动生成。
func (c *Conn) NewEnvelope(cmd protocol.Cmd, uid uint64, traceID string, body proto.Message) (*pb.Envelope, error) {
	var raw []byte
	if body != nil {
		var err error
		raw, err = proto.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("序列化 body 失败: %w", err)
		}
	}
	if traceID == "" {
		traceID = NewTraceID()
	}
	return &pb.Envelope{
		Cmd:      uint32(cmd),
		Uid:      uid,
		TraceId:  traceID,
		FromNode: c.nodeID,
		TsMs:     time.Now().UnixMilli(),
		Seq:      c.NextSeq(),
		Body:     raw,
	}, nil
}

// Unpack 把 Envelope.body 解析成具体消息。
func Unpack(env *pb.Envelope, out proto.Message) error {
	if len(env.GetBody()) == 0 {
		return nil // 空体是合法的（如 GetBagReq）
	}
	return proto.Unmarshal(env.GetBody(), out)
}

// Pack 把消息塞回 Envelope.body。
func Pack(env *pb.Envelope, msg proto.Message) error {
	if msg == nil {
		env.Body = nil
		return nil
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	env.Body = raw
	return nil
}

// Decode 解析一条 NATS 消息为 Envelope。
func Decode(data []byte) (*pb.Envelope, error) {
	env := &pb.Envelope{}
	if err := proto.Unmarshal(data, env); err != nil {
		return nil, fmt.Errorf("Envelope 解析失败: %w", err)
	}
	return env, nil
}

// ---------------------------- 发送 ----------------------------

func (c *Conn) checkPayload(subj string, n int) error {
	if max := c.nc.MaxPayload(); max > 0 && int64(n) > max {
		metrics.NATSPayloadTooLarge.WithLabelValues(subject.Prefix(subj)).Inc()
		return fmt.Errorf("%w: %d > %d，需分片发送或调大服务端 max_payload", ErrPayloadTooLarge, n, max)
	}
	return nil
}

// PublishEnv 发布一个 Envelope（Core NATS，无投递保证）。
func (c *Conn) PublishEnv(subj string, env *pb.Envelope) error {
	data, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	if err := c.checkPayload(subj, len(data)); err != nil {
		return err
	}
	metrics.NATSMessages.WithLabelValues("out", subject.Prefix(subj)).Inc()
	return c.nc.Publish(subj, data)
}

// Publish 构造并发布。
func (c *Conn) Publish(subj string, cmd protocol.Cmd, uid uint64, traceID string, body proto.Message) error {
	env, err := c.NewEnvelope(cmd, uid, traceID, body)
	if err != nil {
		return err
	}
	return c.PublishEnv(subj, env)
}

// RequestEnv 发送请求并等待应答。
//
// 无订阅者时（分片交接窗口）返回 ErrNoResponders，调用方/客户端必须重试（§10.2）。
func (c *Conn) RequestEnv(ctx context.Context, subj string, env *pb.Envelope) (*pb.Envelope, error) {
	data, err := proto.Marshal(env)
	if err != nil {
		return nil, err
	}
	if err := c.checkPayload(subj, len(data)); err != nil {
		return nil, err
	}

	prefix := subject.Prefix(subj)
	metrics.NATSMessages.WithLabelValues("out", prefix).Inc()
	start := time.Now()

	msg, err := c.nc.RequestWithContext(ctx, subj, data)
	metrics.NATSRequestLatency.WithLabelValues(prefix).Observe(time.Since(start).Seconds())
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("%w: subject=%s", ErrNoResponders, subj)
		}
		return nil, err
	}
	metrics.NATSMessages.WithLabelValues("in", prefix).Inc()

	resp, err := Decode(msg.Data)
	if err != nil {
		return nil, err
	}
	if resp.GetSeq() != 0 && env.GetSeq() != 0 && resp.GetSeq() != env.GetSeq() {
		logx.Warn("应答 seq 不匹配", "want", env.GetSeq(), "got", resp.GetSeq(), "subject", subj)
	}
	return resp, nil
}

// Request 构造请求并等待应答，返回的 Envelope 可能带 err_code。
func (c *Conn) Request(ctx context.Context, subj string, cmd protocol.Cmd, uid uint64, traceID string, body proto.Message) (*pb.Envelope, error) {
	env, err := c.NewEnvelope(cmd, uid, traceID, body)
	if err != nil {
		return nil, err
	}
	return c.RequestEnv(ctx, subj, env)
}

// Call 是 Request 的强类型封装：应答带错误码时转成 error，否则解析进 out。
func (c *Conn) Call(ctx context.Context, subj string, cmd protocol.Cmd, uid uint64, traceID string, body, out proto.Message) error {
	resp, err := c.Request(ctx, subj, cmd, uid, traceID, body)
	if err != nil {
		return err
	}
	if resp.GetErrCode() != 0 {
		return &RemoteError{Code: protocol.ErrCode(resp.GetErrCode()), Msg: resp.GetErrMsg()}
	}
	if out != nil {
		return Unpack(resp, out)
	}
	return nil
}

// RemoteError 表示对端返回的业务错误。
type RemoteError struct {
	Code protocol.ErrCode
	Msg  string
}

func (e *RemoteError) Error() string {
	if e.Msg != "" {
		return fmt.Sprintf("远端错误 %d(%s): %s", e.Code, e.Code.String(), e.Msg)
	}
	return fmt.Sprintf("远端错误 %d(%s)", e.Code, e.Code.String())
}

// IsRetryable 报告该错误是否值得客户端重试（分片交接、超时）。
func (e *RemoteError) IsRetryable() bool {
	return e.Code == protocol.ErrUnavailable || e.Code == protocol.ErrNotOwner || e.Code == protocol.ErrTimeout
}

// ---------------------------- 应答 ----------------------------

// Msg 是解码后的入站消息。
type Msg struct {
	Env     *pb.Envelope
	Subject string
	Reply   string
	conn    *Conn
	replied atomic.Bool
}

// NeedReply 报告该消息是否需要应答。
func (m *Msg) NeedReply() bool { return m.Reply != "" }

// Cmd 返回命令号。
func (m *Msg) Cmd() protocol.Cmd { return protocol.Cmd(m.Env.GetCmd()) }

// Reply 回一个成功应答。
func (m *Msg) Respond(body proto.Message) error {
	if m.Reply == "" {
		return nil
	}
	if !m.replied.CompareAndSwap(false, true) {
		return nil // 幂等，避免重复回包
	}
	env := &pb.Envelope{
		Cmd:      m.Env.GetCmd(),
		Uid:      m.Env.GetUid(),
		TraceId:  m.Env.GetTraceId(),
		FromNode: m.conn.nodeID,
		TsMs:     time.Now().UnixMilli(),
		Seq:      m.Env.GetSeq(),
		Shard:    m.Env.GetShard(),
	}
	if err := Pack(env, body); err != nil {
		return err
	}
	data, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	if err := m.conn.checkPayload(m.Reply, len(data)); err != nil {
		return err
	}
	return m.conn.nc.Publish(m.Reply, data)
}

// RespondErr 回一个错误应答。
func (m *Msg) RespondErr(code protocol.ErrCode, format string, args ...any) error {
	if m.Reply == "" {
		return nil
	}
	if !m.replied.CompareAndSwap(false, true) {
		return nil
	}
	msg := code.String()
	if format != "" {
		msg = fmt.Sprintf(format, args...)
	}
	env := &pb.Envelope{
		Cmd:      m.Env.GetCmd(),
		Uid:      m.Env.GetUid(),
		TraceId:  m.Env.GetTraceId(),
		FromNode: m.conn.nodeID,
		TsMs:     time.Now().UnixMilli(),
		Seq:      m.Env.GetSeq(),
		Shard:    m.Env.GetShard(),
		ErrCode:  uint32(code),
		ErrMsg:   msg,
	}
	data, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	return m.conn.nc.Publish(m.Reply, data)
}

// ---------------------------- 订阅 ----------------------------

// Handler 处理一条入站消息。
//
// 重要：NATS 回调是单 goroutine 串行执行的（§5.4）。
// handler 内绝不能做阻塞操作 —— 持有数据的服务应只把消息投递进分片 mailbox 后立即返回。
type Handler func(*Msg)

// Subscribe 订阅 subject，不带 queue group。
//
// 持有玩家数据的服务（Lobby / Room）必须用它，绝不能用 QueueSubscribe：
// queue group 只保证负载均衡，不保证同一玩家路由到同一实例，
// 在内存态数据下会直接导致双副本互相覆盖刷盘（§2.3 / §4.3）。
func (c *Conn) Subscribe(subj string, h Handler) (*nats.Subscription, error) {
	sub, err := c.nc.Subscribe(subj, c.wrap(h))
	if err != nil {
		return nil, err
	}
	c.tune(sub)
	return sub, nil
}

// QueueSubscribe 带 queue group 订阅。
//
// 只允许无状态服务使用（Chat 中继、Gateway 的无状态处理）。
// 持有内存数据的服务用它等于放弃分片独占语义。
func (c *Conn) QueueSubscribe(subj, queue string, h Handler) (*nats.Subscription, error) {
	sub, err := c.nc.QueueSubscribe(subj, queue, c.wrap(h))
	if err != nil {
		return nil, err
	}
	c.tune(sub)
	return sub, nil
}

func (c *Conn) tune(sub *nats.Subscription) {
	// 显式设置 pending 上限，配合 ErrorHandler 让 slow consumer 可见。
	_ = sub.SetPendingLimits(65536, 64*1024*1024)
}

func (c *Conn) wrap(h Handler) nats.MsgHandler {
	return func(m *nats.Msg) {
		metrics.NATSMessages.WithLabelValues("in", subject.Prefix(m.Subject)).Inc()
		env, err := Decode(m.Data)
		if err != nil {
			logx.Error("丢弃无法解析的消息", "subject", m.Subject, "err", err)
			return
		}
		h(&Msg{Env: env, Subject: m.Subject, Reply: m.Reply, conn: c})
	}
}

// Flush 刷出待发消息。
func (c *Conn) Flush(timeout time.Duration) error { return c.nc.FlushTimeout(timeout) }

// Drain 优雅关闭：处理完 pending 后关连接（§10.3 第 2 步）。
func (c *Conn) Drain() error { return c.nc.Drain() }

// Close 立即关闭。
func (c *Conn) Close() { c.nc.Close() }

// IsClosed 报告连接是否已关闭。
func (c *Conn) IsClosed() bool { return c.nc.IsClosed() }
