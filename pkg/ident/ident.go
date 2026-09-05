// Package ident 实现 §3 的三层 ID 结构与派生标识。
//
//	区服 ID serverID  手动配置（运营概念，程序无法推断）
//	服务类型 svcType  代码枚举（编译期常量）
//	进程序号 nodeSeq  手动配置（每服务从 1 开始）
//
//	nodeID   = s{serverID}-{svcName}-{nodeSeq}      例：s1-lobby-1
//	workerID = svcType<<7 | nodeSeq                 雪花用，10 bit
//
// serverID 不进 workerID：本方案分区分服、数据隔离，uid 只需区服内唯一（§3.2）。
package ident

import (
	"fmt"
	"strings"
)

// SvcType 服务类型枚举。最多 8 种服务（3 bit）。
type SvcType uint32

const (
	SvcUnknown SvcType = 0
	SvcGateway SvcType = 1
	SvcLobby   SvcType = 2
	SvcRoom    SvcType = 3
	SvcMatch   SvcType = 4
	SvcChat    SvcType = 5
	SvcWorld   SvcType = 6
)

// 位宽定义（§3.5）：workerID 10 bit = svcType(3) | nodeSeq(7)。
const (
	NodeSeqBits = 7
	SvcTypeBits = 3

	MaxNodeSeq  = (1 << NodeSeqBits) - 1 // 127，每服务最多 127 实例
	MaxSvcType  = (1 << SvcTypeBits) - 1 // 7，最多 8 种服务（0 保留）
	MaxWorkerID = (1 << (NodeSeqBits + SvcTypeBits)) - 1
)

var svcNames = map[SvcType]string{
	SvcGateway: "gateway",
	SvcLobby:   "lobby",
	SvcRoom:    "room",
	SvcMatch:   "match",
	SvcChat:    "chat",
	SvcWorld:   "world",
}

var svcByName = func() map[string]SvcType {
	m := make(map[string]SvcType, len(svcNames))
	for t, n := range svcNames {
		m[n] = t
	}
	return m
}()

// Name 返回服务名（用于 nodeID、NATS subject、etcd 路径）。
func (s SvcType) Name() string {
	if n, ok := svcNames[s]; ok {
		return n
	}
	return fmt.Sprintf("svc%d", uint32(s))
}

func (s SvcType) String() string { return s.Name() }

// Valid 报告 svcType 是否落在 3 bit 可表达的合法范围内。
func (s SvcType) Valid() bool { return s >= 1 && uint32(s) <= MaxSvcType }

// ParseSvcType 按服务名解析类型。
func ParseSvcType(name string) (SvcType, bool) {
	t, ok := svcByName[strings.ToLower(strings.TrimSpace(name))]
	return t, ok
}

// Identity 是一个进程的完整身份。
type Identity struct {
	ServerID int     // 区服 ID，手动配置
	Svc      SvcType // 服务类型，编译期常量
	NodeSeq  int     // 进程序号，手动配置，[1,127]

	nodeID   string
	workerID uint32
}

// New 构造 Identity 并执行 §3.4 第一道「越界校验」。
//
// nodeSeq >= 128 会溢出到相邻服务的号段，静默产生重复 workerID。
// 这道校验漏了，位拼接的安全性就没了 —— 因此这里返回 error，调用方必须 fatal。
func New(serverID int, svc SvcType, nodeSeq int) (*Identity, error) {
	if serverID < 0 {
		return nil, fmt.Errorf("SERVER_ID 非法: %d（必须 >= 0，且由运维手动配置）", serverID)
	}
	if !svc.Valid() {
		return nil, fmt.Errorf("svcType 越界: %d，必须在 [1,%d]（3 bit）", uint32(svc), MaxSvcType)
	}
	if nodeSeq < 1 || nodeSeq > MaxNodeSeq {
		return nil, fmt.Errorf("NODE_SEQ 越界: %d，必须在 [1,%d]", nodeSeq, MaxNodeSeq)
	}

	id := &Identity{ServerID: serverID, Svc: svc, NodeSeq: nodeSeq}
	id.nodeID = fmt.Sprintf("s%d-%s-%d", serverID, svc.Name(), nodeSeq)
	id.workerID = uint32(svc)<<NodeSeqBits | uint32(nodeSeq)
	return id, nil
}

// NodeID 返回进程唯一标识，例 "s1-lobby-1"。
func (i *Identity) NodeID() string { return i.nodeID }

// WorkerID 返回雪花使用的 10 bit 机器号。
//
// 必须拼接而不能直接用 nodeSeq：若每个服务都从 1 开始且直接当 workerID，
// gateway-1 / lobby-1 / room-1 的 workerID 都是 1，同一毫秒会生成完全相同的雪花 ID（§3.3）。
func (i *Identity) WorkerID() uint32 { return i.workerID }

// SvcName 返回服务名。
func (i *Identity) SvcName() string { return i.Svc.Name() }

func (i *Identity) String() string {
	return fmt.Sprintf("%s(worker=%d)", i.nodeID, i.workerID)
}
