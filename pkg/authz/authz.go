// Package authz 实现命令级别的调用鉴权。
//
// 评审 P0-1 的修复：以前「哪些命令客户端能调」写在网关的一个 switch 里，
// 新增内部命令时忘了排除，默认就是放行 —— add_currency 因此被暴露给了客户端，
// 任何人都能给自己造币。
//
// 现在是两道：
//
//	第一道 · 网关按 Cmd.Level() 放行，只有 LevelClient 能进来；
//	第二道 · Lobby 对 Internal / GM 级命令校验 HMAC 签名。
//
// 第二道存在的理由是「不信任网关」：万一网关被改错、或有人直接连上内网 NATS，
// 签名这一关仍然拦得住。网关进程不持有内部密钥（部署时不给它这个环境变量），
// 所以它在物理上就签不出一条合法的内部命令。
package authz

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
)

var (
	// ErrNotClientCallable 表示客户端试图调用非客户端级命令。
	ErrNotClientCallable = errors.New("authz: 该命令不允许客户端调用")
	// ErrMissingSignature 表示内部命令缺少签名。
	ErrMissingSignature = errors.New("authz: 内部命令缺少签名")
	// ErrBadSignature 表示签名校验失败。
	ErrBadSignature = errors.New("authz: 内部命令签名无效")
	// ErrExpired 表示签名已过期（防重放）。
	ErrExpired = errors.New("authz: 内部命令签名已过期")
	// ErrNoSecret 表示本进程没有内部密钥，无法签发内部命令。
	ErrNoSecret = errors.New("authz: 未配置 INTERNAL_SECRET，无法签发内部命令")
)

// MaxSkew 是签名允许的时间偏差，超过即拒绝（防重放）。
const MaxSkew = 5 * time.Minute

// Signer 持有内部密钥，负责签发与校验。
type Signer struct {
	secret []byte
}

// NewSigner 构造签名器。secret 为空时只能校验「不需要签名」的命令，
// 任何签发尝试都会返回 ErrNoSecret —— 默认拒绝，而不是默认放行。
func NewSigner(secret string) *Signer {
	return &Signer{secret: []byte(secret)}
}

// HasSecret 报告本进程是否持有内部密钥。
func (s *Signer) HasSecret() bool { return len(s.secret) > 0 }

// digest 计算签名。绑定 cmd + uid + 时间戳 + trace_id：
//   - 绑 cmd 防止把一条 add_item 的签名挪去当 add_currency 用
//   - 绑 uid 防止把给 A 发奖的签名挪去给 B 发奖
//   - 绑时间戳限制重放窗口
func (s *Signer) digest(cmd uint32, uid uint64, tsMs int64, traceID string) []byte {
	mac := hmac.New(sha256.New, s.secret)
	var buf [20]byte
	binary.BigEndian.PutUint32(buf[0:4], cmd)
	binary.BigEndian.PutUint64(buf[4:12], uid)
	binary.BigEndian.PutUint64(buf[12:20], uint64(tsMs))
	mac.Write(buf[:])
	mac.Write([]byte(traceID))
	return mac.Sum(nil)[:16] // 截断到 128 bit，够用且省带宽
}

// Sign 给内部 / GM 命令签名。客户端级命令不需要签名，直接返回。
func (s *Signer) Sign(env *pb.Envelope) error {
	cmd := protocol.Cmd(env.GetCmd())
	if cmd.Level() == protocol.LevelClient {
		return nil
	}
	if !s.HasSecret() {
		return fmt.Errorf("%w: cmd=%s", ErrNoSecret, cmd)
	}
	if env.GetTsMs() == 0 {
		env.TsMs = time.Now().UnixMilli()
	}
	env.Auth = s.digest(env.GetCmd(), env.GetUid(), env.GetTsMs(), env.GetTraceId())
	return nil
}

// Verify 校验一条入站命令是否有权执行。
//
// fromClient 表示这条消息是网关代客户端转发的。
func (s *Signer) Verify(env *pb.Envelope, fromClient bool) error {
	cmd := protocol.Cmd(env.GetCmd())
	level := cmd.Level()

	if level == protocol.LevelClient {
		return nil
	}
	if fromClient {
		return fmt.Errorf("%w: cmd=%s level=%s", ErrNotClientCallable, cmd, level)
	}
	if len(env.GetAuth()) == 0 {
		return fmt.Errorf("%w: cmd=%s", ErrMissingSignature, cmd)
	}
	if !s.HasSecret() {
		// 自己没密钥却收到需要签名的命令：无法校验就必须拒绝。
		return fmt.Errorf("%w: 无密钥无法校验 cmd=%s", ErrBadSignature, cmd)
	}

	skew := time.Since(time.UnixMilli(env.GetTsMs()))
	if skew < 0 {
		skew = -skew
	}
	if skew > MaxSkew {
		return fmt.Errorf("%w: 偏差 %s，cmd=%s", ErrExpired, skew.Round(time.Second), cmd)
	}

	want := s.digest(env.GetCmd(), env.GetUid(), env.GetTsMs(), env.GetTraceId())
	if !hmac.Equal(want, env.GetAuth()) {
		return fmt.Errorf("%w: cmd=%s from=%s", ErrBadSignature, cmd, env.GetFromNode())
	}
	if level == protocol.LevelGM && env.GetOperator() == "" {
		// GM 操作必须留下操作者，否则事后无法追责。
		return fmt.Errorf("%w: GM 命令缺少 operator 字段", ErrBadSignature)
	}
	return nil
}

// CheckClient 是网关侧的第一道：只放行客户端级命令。
func CheckClient(cmd protocol.Cmd) error {
	if cmd.ClientCallable() {
		return nil
	}
	return fmt.Errorf("%w: cmd=%s level=%s", ErrNotClientCallable, cmd, cmd.Level())
}
