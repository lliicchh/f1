// Package lobby 玩家对象的容器，同时跑玩家的业务逻辑
//
// 背包、任务、邮件、货币、社交的数据和逻辑都在这儿，没有单独的玩家数据服务。
// 逻辑跟数据在一起，直接函数调用，不用过网络也不用锁
package lobby

import (
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/profile"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/store"
)

// Player 常驻内存的玩家对象
//
// 只被所属分片的 Actor goroutine 访问，所以没有锁。
// 同一个玩家的请求天然串行，不会有并发扣道具那类问题
type Player struct {
	UID    uint64
	Base   *pb.PlayerBase
	Bag    *pb.PlayerBag
	Quest  *pb.PlayerQuest
	Social *pb.PlayerSocial
	Mail   *pb.PlayerMail

	// Round 未结算的回合，nil 表示没有。
	// 随投注一起落盘，断线重连时靠懒加载取回
	Round *pb.Round
	// RG 责任游戏的限额累计
	RG *pb.PlayerRG
	// Gacha 抽卡的保底计数
	Gacha *pb.PlayerGacha

	// 运行态，不落盘
	Online     bool
	GateID     string
	ConnID     uint64
	LastActive time.Time
	OfflineAt  time.Time // 下线时刻，延迟卸载用

	// busy 表示有一次 L0 写穿在路上。期间其他改写请求得排队，
	// 这样才能做到落盘成功之后才改内存
	busy bool
}

// NewPlayer 创建一个全新玩家（首次登录）
func NewPlayer(uid uint64, now time.Time) *Player {
	return &Player{
		UID: uid,
		Base: &pb.PlayerBase{
			Uid:       uid,
			Nick:      defaultNick(uid),
			Level:     1,
			Currency:  map[uint32]int64{},
			CreatedAt: now.UnixMilli(),
		},
		Bag:        &pb.PlayerBag{Uid: uid, Capacity: DefaultBagCapacity},
		Quest:      &pb.PlayerQuest{Uid: uid},
		Social:     &pb.PlayerSocial{Uid: uid},
		Mail:       &pb.PlayerMail{Uid: uid},
		RG:         &pb.PlayerRG{Uid: uid},
		Gacha:      &pb.PlayerGacha{Uid: uid},
		LastActive: now,
	}
}

// DefaultBagCapacity 默认背包格数
const DefaultBagCapacity = 200

func defaultNick(uid uint64) string {
	const digits = "0123456789"
	buf := make([]byte, 0, 12)
	buf = append(buf, "Player"...)
	if uid == 0 {
		return string(append(buf, '0'))
	}
	var tmp [20]byte
	i := len(tmp)
	for uid > 0 {
		i--
		tmp[i] = digits[uid%10]
		uid /= 10
	}
	return string(append(buf, tmp[i:]...))
}

// FromBlobs 用 Redis 读回的 blob 还原玩家，缺的模块补空值
//
// 新玩家和加了新模块的老玩家都会走这条路
func FromBlobs(uid uint64, blobs map[store.Module][]byte, now time.Time) (*Player, error) {
	p := NewPlayer(uid, now)
	if len(blobs) == 0 {
		return p, nil
	}

	if b, ok := blobs[store.ModBase]; ok {
		m := &pb.PlayerBase{}
		if err := proto.Unmarshal(b, m); err != nil {
			return nil, err
		}
		if m.Currency == nil {
			m.Currency = map[uint32]int64{}
		}
		m.Uid = uid
		p.Base = m
	}
	if b, ok := blobs[store.ModBag]; ok {
		m := &pb.PlayerBag{}
		if err := proto.Unmarshal(b, m); err != nil {
			return nil, err
		}
		m.Uid = uid
		if m.Capacity == 0 {
			m.Capacity = DefaultBagCapacity
		}
		p.Bag = m
	}
	if b, ok := blobs[store.ModQuest]; ok {
		m := &pb.PlayerQuest{}
		if err := proto.Unmarshal(b, m); err != nil {
			return nil, err
		}
		m.Uid = uid
		p.Quest = m
	}
	if b, ok := blobs[store.ModSocial]; ok {
		m := &pb.PlayerSocial{}
		if err := proto.Unmarshal(b, m); err != nil {
			return nil, err
		}
		m.Uid = uid
		p.Social = m
	}
	if b, ok := blobs[store.ModMail]; ok {
		m := &pb.PlayerMail{}
		if err := proto.Unmarshal(b, m); err != nil {
			return nil, err
		}
		m.Uid = uid
		p.Mail = m
	}
	if b, ok := blobs[store.ModRound]; ok && len(b) > 0 {
		r := &pb.Round{}
		if err := proto.Unmarshal(b, r); err != nil {
			return nil, err
		}
		// 只恢复真正没结算的，已结算的留着只会让客户端犯迷糊
		if r.GetRoundId() != 0 && r.GetState() == protocol.RoundOpen {
			p.Round = r
		}
	}
	if b, ok := blobs[store.ModRG]; ok {
		m := &pb.PlayerRG{}
		if err := proto.Unmarshal(b, m); err != nil {
			return nil, err
		}
		m.Uid = uid
		p.RG = m
	}
	if b, ok := blobs[store.ModGacha]; ok {
		m := &pb.PlayerGacha{}
		if err := proto.Unmarshal(b, m); err != nil {
			return nil, err
		}
		m.Uid = uid
		p.Gacha = m
	}
	return p, nil
}

// Marshal 序列化指定模块，只能在 Actor goroutine 里调
func (p *Player) Marshal(m store.Module) ([]byte, error) {
	switch m {
	case store.ModBase:
		return proto.Marshal(p.Base)
	case store.ModBag:
		return proto.Marshal(p.Bag)
	case store.ModQuest:
		return proto.Marshal(p.Quest)
	case store.ModSocial:
		return proto.Marshal(p.Social)
	case store.ModMail:
		return proto.Marshal(p.Mail)
	case store.ModRound:
		if p.Round == nil {
			// 回合结算了就写个空串把上一局清掉，
			// 否则下次加载会把陈旧回合当成未完成的恢复出来
			return []byte{}, nil
		}
		return proto.Marshal(p.Round)
	case store.ModRG:
		return proto.Marshal(p.RG)
	case store.ModGacha:
		return proto.Marshal(p.Gacha)
	}
	return nil, nil
}

// MarshalKeys 把几个模块序列化成 key→value
func (p *Player) MarshalKeys(keys *store.Keys, mods []store.Module) (map[string][]byte, error) {
	out := make(map[string][]byte, len(mods))
	for _, m := range mods {
		b, err := p.Marshal(m)
		if err != nil {
			return nil, err
		}
		if b == nil {
			continue
		}
		out[keys.Player(p.UID, m)] = b
	}
	return out, nil
}

func (p *Player) Profile() *pb.Profile { return profile.FromBase(p.Base) }

func (p *Player) Currency(t uint32) int64 { return p.Base.GetCurrency()[t] }

// AddCurrency 增减货币，返回新余额。不够扣就什么都不动
func (p *Player) AddCurrency(t uint32, delta int64) (int64, bool) {
	if p.Base.Currency == nil {
		p.Base.Currency = map[uint32]int64{}
	}
	cur := p.Base.Currency[t]
	if delta < 0 && cur+delta < 0 {
		return cur, false
	}
	cur += delta
	p.Base.Currency[t] = cur
	return cur, true
}

// FindItem 按实例 ID 查找道具
func (p *Player) FindItem(instanceID uint64) (*pb.Item, int) {
	for i, it := range p.Bag.GetItems() {
		if it.GetInstanceId() == instanceID {
			return it, i
		}
	}
	return nil, -1
}

// CountItem 统计某模板道具的总数
func (p *Player) CountItem(tplID uint32) int64 {
	var n int64
	for _, it := range p.Bag.GetItems() {
		if it.GetTplId() == tplID {
			n += it.GetCount()
		}
	}
	return n
}

// AddItem 加道具，可堆叠的合并到已有格子。instanceID 用雪花生成
//
// stackable 从配置来而不是写死号段，那是策划数值，写死了加个道具就要发版
func (p *Player) AddItem(instanceID uint64, tplID uint32, count int64, now time.Time, stackable bool) (*pb.Item, bool) {
	if count <= 0 {
		return nil, false
	}
	if stackable {
		for _, it := range p.Bag.GetItems() {
			if it.GetTplId() == tplID {
				it.Count += count
				return it, true
			}
		}
	}
	if int64(len(p.Bag.GetItems())) >= int64(p.Bag.GetCapacity()) {
		return nil, false // 背包已满
	}
	it := &pb.Item{
		InstanceId: instanceID,
		TplId:      tplID,
		Count:      count,
		ObtainedAt: now.UnixMilli(),
	}
	p.Bag.Items = append(p.Bag.Items, it)
	return it, true
}

// RemoveItemByTpl 按模板扣数量，不够就原样不动返回 false
func (p *Player) RemoveItemByTpl(tplID uint32, count int64) bool {
	if count <= 0 || p.CountItem(tplID) < count {
		return false
	}
	remain := count
	items := p.Bag.GetItems()
	for i := 0; i < len(items) && remain > 0; i++ {
		it := items[i]
		if it.GetTplId() != tplID {
			continue
		}
		take := it.GetCount()
		if take > remain {
			take = remain
		}
		it.Count -= take
		remain -= take
	}
	p.compactBag()
	return true
}

// UseItem 按实例 ID 消耗道具
func (p *Player) UseItem(instanceID, count int64) (int64, bool) {
	it, _ := p.FindItem(uint64(instanceID))
	if it == nil || it.GetCount() < count || count <= 0 {
		return 0, false
	}
	it.Count -= count
	remain := it.Count
	p.compactBag()
	return remain, true
}

func (p *Player) compactBag() {
	items := p.Bag.GetItems()
	out := items[:0]
	for _, it := range items {
		if it.GetCount() > 0 {
			out = append(out, it)
		}
	}
	p.Bag.Items = out
}

func (p *Player) FindQuest(id uint32) *pb.Quest {
	for _, q := range p.Quest.GetQuests() {
		if q.GetQuestId() == id {
			return q
		}
	}
	return nil
}

func (p *Player) FindMail(id uint64) *pb.Mail {
	for _, m := range p.Mail.GetMails() {
		if m.GetMailId() == id {
			return m
		}
	}
	return nil
}

// HasFriend 报告是否已是好友
func (p *Player) HasFriend(uid uint64) bool {
	for _, f := range p.Social.GetFriends() {
		if f == uid {
			return true
		}
	}
	return false
}

// Idle 返回下线后过了多久，还在线返回 0
func (p *Player) Idle(now time.Time) time.Duration {
	if p.Online || p.OfflineAt.IsZero() {
		return 0
	}
	return now.Sub(p.OfflineAt)
}
