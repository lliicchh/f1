// Package authz 做命令级鉴权
//
// 两道：网关按 Cmd.Level() 只放行 LevelClient；Lobby 再对内部/GM 命令验一次
// HMAC 签名。第二道是防网关的，网关进程拿不到内部密钥，签不出合法签名
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
	// ErrNotClientCallable 表示客户端试图调用非客户端级命令
	ErrNotClientCallable = errors.New("authz: 该命令不允许客户端调用")
	// ErrMissingSignature 表示内部命令缺少签名
	ErrMissingSignature = errors.New("authz: 内部命令缺少签名")
	// ErrBadSignature 表示签名校验失败
	ErrBadSignature = errors.New("authz: 内部命令签名无效")
	// ErrExpired 表示签名已过期（防重放）
	ErrExpired = errors.New("authz: 内部命令签名已过期")
	// ErrNoSecret 表示本进程没有内部密钥，无法签发内部命令
	ErrNoSecret = errors.New("authz: 未配置 INTERNAL_SECRET，无法签发内部命令")
)

// MaxSkew 签名允许的时钟偏差，超过即拒绝，防重放
const MaxSkew = 5 * time.Minute

// Signer 持有内部密钥，负责签发与校验
type Signer struct {
	secret []byte
}

// NewSigner 构造签名器。secret 为空时签发一律失败
func NewSigner(secret string) *Signer {
	return &Signer{secret: []byte(secret)}
}

func (s *Signer) HasSecret() bool { return len(s.secret) > 0 }

// digest 把 cmd、uid、时间戳、trace_id 一起签进去，
// 这样签名不能跨命令、跨玩家挪用，也不能无限重放
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

// Sign 给内部 / GM 命令签名。客户端级命令不需要签名，直接返回
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

// Verify 校验入站命令。fromClient 表示消息由网关代客户端转发
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
		// 验不了就拒，不能放行
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
		// GM 操作必须留下操作者，否则出了事查不到人
		return fmt.Errorf("%w: GM 命令缺少 operator 字段", ErrBadSignature)
	}
	return nil
}

func CheckClient(cmd protocol.Cmd) error {
	if cmd.ClientCallable() {
		return nil
	}
	return fmt.Errorf("%w: cmd=%s level=%s", ErrNotClientCallable, cmd, cmd.Level())
}
