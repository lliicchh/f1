// Package ident 三层 ID：区服 ID、服务类型、进程序号
//
//	nodeID   = s{serverID}-{svcName}-{nodeSeq}    例 s1-lobby-1
//	workerID = svcType<<7 | nodeSeq               雪花用，10 bit
//
// serverID 不进 workerID，分区分服数据隔离，uid 只要区服内唯一就行
package ident

import (
	"fmt"
	"strings"
)

// SvcType 服务类型，3 bit 最多 8 种
type SvcType uint32

const (
	SvcUnknown SvcType = 0
	SvcGateway SvcType = 1
	SvcLobby   SvcType = 2
	SvcRoom    SvcType = 3
	SvcMatch   SvcType = 4
	SvcChat    SvcType = 5
	SvcWorld   SvcType = 6
	SvcAccount SvcType = 7 // 3 bit 的最后一个号，再加服务要先加宽 svcType
)

// workerID 10 bit = svcType 3 bit + nodeSeq 7 bit
const (
	NodeSeqBits = 7
	SvcTypeBits = 3

	MaxNodeSeq  = (1 << NodeSeqBits) - 1 // 每个服务最多 127 个实例
	MaxSvcType  = (1 << SvcTypeBits) - 1 // 最多 8 种服务，0 保留
	MaxWorkerID = (1 << (NodeSeqBits + SvcTypeBits)) - 1
)

var svcNames = map[SvcType]string{
	SvcGateway: "gateway",
	SvcLobby:   "lobby",
	SvcRoom:    "room",
	SvcMatch:   "match",
	SvcChat:    "chat",
	SvcWorld:   "world",
	SvcAccount: "account",
}

var svcByName = func() map[string]SvcType {
	m := make(map[string]SvcType, len(svcNames))
	for t, n := range svcNames {
		m[n] = t
	}
	return m
}()

// Name 返回服务名，nodeID、subject、etcd 路径都用它
func (s SvcType) Name() string {
	if n, ok := svcNames[s]; ok {
		return n
	}
	return fmt.Sprintf("svc%d", uint32(s))
}

func (s SvcType) String() string { return s.Name() }

// Valid 报告 svcType 是否在 3 bit 能表示的范围内
func (s SvcType) Valid() bool { return s >= 1 && uint32(s) <= MaxSvcType }

// ParseSvcType 按服务名解析类型
func ParseSvcType(name string) (SvcType, bool) {
	t, ok := svcByName[strings.ToLower(strings.TrimSpace(name))]
	return t, ok
}

// Identity 一个进程的身份
type Identity struct {
	ServerID int     // 区服 ID，手动配
	Svc      SvcType // 服务类型，编译期常量
	NodeSeq  int     // 进程序号，手动配，取值 1~127

	nodeID   string
	workerID uint32
}

// New 构造 Identity，顺带做第一道越界校验
//
// nodeSeq 到 128 会溢出进相邻服务的号段，静默产生重复 workerID。
// 这道校验漏了，位拼接就白做了，所以这里返回 error，调用方必须直接退出
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

// NodeID 返回进程唯一标识，形如 s1-lobby-1
func (i *Identity) NodeID() string { return i.nodeID }

// WorkerID 返回雪花用的 10 bit 机器号
//
// 必须拼接，不能直接拿 nodeSeq。每个服务都从 1 开始，
// gateway-1、lobby-1、room-1 的 workerID 都会是 1，同一毫秒撞出一样的 ID
func (i *Identity) WorkerID() uint32 { return i.workerID }

func (i *Identity) SvcName() string { return i.Svc.Name() }

func (i *Identity) String() string {
	return fmt.Sprintf("%s(worker=%d)", i.nodeID, i.workerID)
}
