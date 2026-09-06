// Package authn 校验登录票据
//
// 格式 uid.exp.sig，自带签名。生产上通常由账号服务签发、网关只验，格式不变
package authn

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrBadToken 表示token格式错误或签名不符
	ErrBadToken = errors.New("authn: 登录token无效")
	// ErrTokenExpired 表示token已过期
	ErrTokenExpired = errors.New("authn: 登录token已过期")
	// ErrUIDMismatch 表示token里的 uid 与请求声明的不一致
	ErrUIDMismatch = errors.New("authn: token与 uid 不匹配")
	// ErrDevAuthDisabled 表示未配置密钥且未显式开启开发模式
	ErrDevAuthDisabled = errors.New("authn: 未配置 LOGIN_SECRET；如确为本地开发，请显式设置 ALLOW_DEV_AUTH=true")
)

// TokenTTL token默认有效期
const TokenTTL = 30 * time.Minute

// Verifier 校验登录token
type Verifier struct {
	secret []byte
	dev    bool
}

// NewVerifier 构造校验器
//
// secret 为空且 allowDev 为假时所有登录都拒绝。忘了配密钥不该变成谁都能登
func NewVerifier(secret string, allowDev bool) *Verifier {
	return &Verifier{secret: []byte(secret), dev: allowDev}
}

// DevMode 报告是否处于「无密钥放行」的开发模式
func (v *Verifier) DevMode() bool { return len(v.secret) == 0 && v.dev }

// Enabled 报告是否在做真实校验
func (v *Verifier) Enabled() bool { return len(v.secret) > 0 }

// Verify 校验 uid 与token是否匹配
func (v *Verifier) Verify(uid uint64, token string) error {
	if len(v.secret) == 0 {
		if v.dev {
			return nil
		}
		return ErrDevAuthDisabled
	}

	tokUID, exp, sig, err := parse(token)
	if err != nil {
		return err
	}
	if tokUID != uid {
		return fmt.Errorf("%w: token uid=%d 请求 uid=%d", ErrUIDMismatch, tokUID, uid)
	}
	if time.Now().After(time.UnixMilli(exp)) {
		return fmt.Errorf("%w: 过期于 %s", ErrTokenExpired, time.UnixMilli(exp).Format(time.RFC3339))
	}
	if !hmac.Equal(sign(v.secret, tokUID, exp), sig) {
		return ErrBadToken
	}
	return nil
}

// Issue 签发token，供自建登录与测试使用
func (v *Verifier) Issue(uid uint64, ttl time.Duration) (string, error) {
	if len(v.secret) == 0 {
		return "", ErrDevAuthDisabled
	}
	if ttl <= 0 {
		ttl = TokenTTL
	}
	exp := time.Now().Add(ttl).UnixMilli()
	sig := sign(v.secret, uid, exp)
	return fmt.Sprintf("%d.%d.%s", uid, exp, base64.RawURLEncoding.EncodeToString(sig)), nil
}

func sign(secret []byte, uid uint64, exp int64) []byte {
	mac := hmac.New(sha256.New, secret)
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uid)
	binary.BigEndian.PutUint64(buf[8:16], uint64(exp))
	mac.Write(buf[:])
	return mac.Sum(nil)[:16]
}

func parse(token string) (uid uint64, exp int64, sig []byte, err error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0, 0, nil, fmt.Errorf("%w: 格式应为 uid.exp.sig", ErrBadToken)
	}
	if uid, err = strconv.ParseUint(parts[0], 10, 64); err != nil {
		return 0, 0, nil, fmt.Errorf("%w: uid 非法", ErrBadToken)
	}
	if exp, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
		return 0, 0, nil, fmt.Errorf("%w: 过期时间非法", ErrBadToken)
	}
	if sig, err = base64.RawURLEncoding.DecodeString(parts[2]); err != nil {
		return 0, 0, nil, fmt.Errorf("%w: 签名非法", ErrBadToken)
	}
	return uid, exp, sig, nil
}
