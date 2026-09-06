// Package gateway WebSocket 连接、编解码、下行推送
//
// 不持有玩家数据，可以轮询负载
// 只订 push.gate.{gateID} 和 push.broadcast 两个 subject
package gateway

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
)

// 帧格式：[4 字节大端长度][Envelope protobuf]
const (
	maxFrameSize   = 1 << 20 // 1MB，与 NATS 默认 max_payload 相同
	writeQueueSize = 256
	readTimeout    = 90 * time.Second
	writeTimeout   = 10 * time.Second
)

// ErrFrameTooLarge 客户端的请求数据帧超限
var ErrFrameTooLarge = errors.New("gateway: 帧长度超限")

// Conn 一条客户端连接
type Conn struct {
	ID   uint64
	UID  atomic.Uint64 // 登录后填充
	raw  net.Conn
	gw   *Service
	out  chan []byte
	once sync.Once
	done chan struct{}

	tokens   atomic.Int64
	lastFill atomic.Int64
}

func newConn(id uint64, raw net.Conn, gw *Service) *Conn {
	c := &Conn{
		ID:   id,
		raw:  raw,
		gw:   gw,
		out:  make(chan []byte, writeQueueSize),
		done: make(chan struct{}),
	}
	c.tokens.Store(int64(gw.rateBurst))
	c.lastFill.Store(time.Now().UnixMilli())
	return c
}

// allow 每秒 rateQPS 个、突发 rateBurst 的令牌桶
func (c *Conn) allow() bool {
	now := time.Now().UnixMilli()
	last := c.lastFill.Load()
	if now > last {
		add := (now - last) * int64(c.gw.rateQPS) / 1000
		if add > 0 && c.lastFill.CompareAndSwap(last, now) {
			n := c.tokens.Add(add)
			if n > int64(c.gw.rateBurst) {
				c.tokens.Store(int64(c.gw.rateBurst))
			}
		}
	}
	return c.tokens.Add(-1) >= 0
}

// Send 把一帧丢进发送队列，不阻塞读循环
func (c *Conn) Send(data []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.out <- data:
		return true
	default:
		// 队列积压说明客户端读不动了，断开比无限缓冲安全
		logx.Warn("客户端发送队列已满，断开连接", "conn", c.ID, "uid", c.UID.Load())
		c.Close()
		return false
	}
}

// SendEnv 编码并发送一个 Envelope
func (c *Conn) SendEnv(env *pb.Envelope) bool {
	data, err := encode(env)
	if err != nil {
		return false
	}
	return c.Send(data)
}

// SendPush 发送一条下行推送
func (c *Conn) SendPush(push protocol.Push, payload []byte, traceID string) bool {
	return c.SendEnv(&pb.Envelope{
		Cmd:      uint32(push),
		Uid:      c.UID.Load(),
		TraceId:  traceID,
		FromNode: c.gw.gateID,
		TsMs:     time.Now().UnixMilli(),
		Body:     payload,
	})
}

func (c *Conn) Close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.raw.Close()
	})
}

func (c *Conn) Closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// readLoop 读取并分发客户端请求
func (c *Conn) readLoop() {
	defer c.gw.onDisconnect(c)
	defer c.Close()

	r := bufio.NewReaderSize(c.raw, 8192)
	var head [4]byte

	for {
		if err := c.raw.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return
		}
		if _, err := io.ReadFull(r, head[:]); err != nil {
			if !errors.Is(err, io.EOF) && !c.Closed() {
				logx.Debug("读取帧头失败", "conn", c.ID, "err", err)
			}
			return
		}
		n := binary.BigEndian.Uint32(head[:])
		if n == 0 || n > maxFrameSize {
			logx.Warn("非法帧长度，断开连接", "conn", c.ID, "len", n)
			return
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return
		}

		env := &pb.Envelope{}
		if err := proto.Unmarshal(buf, env); err != nil {
			logx.Warn("客户端帧无法解析，断开连接", "conn", c.ID, "err", err)
			return
		}
		if !c.allow() {
			c.replyErr(env, protocol.ErrRateLimited, "请求过于频繁")
			continue
		}
		c.gw.dispatch(c, env)
	}
}

// writeLoop 串行写，不要多个 goroutine 抢同一个 socket
func (c *Conn) writeLoop() {
	defer c.Close()
	for {
		select {
		case <-c.done:
			return
		case data := <-c.out:
			if err := c.raw.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				return
			}
			if _, err := c.raw.Write(data); err != nil {
				logx.Debug("写出失败", "conn", c.ID, "err", err)
				return
			}
		}
	}
}

// drainOut 关闭前尽量把队列里的数据发完
func (c *Conn) drainOut(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for len(c.out) > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *Conn) replyErr(req *pb.Envelope, code protocol.ErrCode, format string, args ...any) {
	c.SendEnv(&pb.Envelope{
		Cmd:      req.GetCmd(),
		Uid:      req.GetUid(),
		TraceId:  req.GetTraceId(),
		FromNode: c.gw.gateID,
		TsMs:     time.Now().UnixMilli(),
		Seq:      req.GetSeq(),
		ErrCode:  uint32(code),
		ErrMsg:   fmt.Sprintf(format, args...),
	})
}

func encode(env *pb.Envelope) ([]byte, error) {
	body, err := proto.Marshal(env)
	if err != nil {
		return nil, err
	}
	if len(body) > maxFrameSize {
		return nil, ErrFrameTooLarge
	}
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out[:4], uint32(len(body)))
	copy(out[4:], body)
	return out, nil
}
