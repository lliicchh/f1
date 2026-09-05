// Package lobby 实现 Lobby 服务：玩家对象的容器 + 玩家业务逻辑（§2.2）。
//
// 背包、任务、邮件、货币、社交这些模块的数据和逻辑都在这里，
// 不存在独立的「玩家数据服务」—— 内存态设计的全部价值是
// 逻辑在数据所在的地方就地执行，函数调用，纳秒级，无锁。
package lobby

import (
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/profile"
	"github.com/gamedev/f1/pkg/store"
)

// Player 是常驻内存的玩家对象。
//
// 它只被所属分片的 Actor goroutine 访问，因此内部没有任何锁：
// 「同一玩家请求天然串行，不存在并发扣道具类问题」（§4.4）。
type Player struct {
	UID    uint64
	Base   *pb.PlayerBase
	Bag    *pb.PlayerBag
	Quest  *pb.PlayerQuest
	Social *pb.PlayerSocial
	Mail   *pb.PlayerMail

	// —— 运行态，不落盘 ——
	Online     bool
	GateID     string
	ConnID     uint64
	LastActive time.Time
	OfflineAt  time.Time // 下线时刻，用于 §10.1 的延迟卸载

	// busy 为真时该玩家有一次 L0 写穿在途，期间其他改写请求必须排队，
	// 这样才能兑现「同步落 Redis 成功后才改内存回包」（§6.2）。
	busy bool
}

// NewPlayer 创建一个全新玩家（首次登录）。
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
		LastActive: now,
	}
}

// DefaultBagCapacity 是默认背包格数。
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

// FromBlobs 用 Redis 读回的模块 blob 还原玩家对象。缺失的模块用空值补齐 ——
// 新玩家、或新增模块的老玩家都会走到这里。
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
	return p, nil
}

// Marshal 序列化指定模块。
//
// 必须在 Actor goroutine 内调用 —— 读内存必须如此（§6.3）。
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
	}
	return nil, nil
}

// MarshalKeys 把若干模块序列化成 key→value。
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

// Profile 生成只读摘要（§6.5）。
func (p *Player) Profile() *pb.Profile { return profile.FromBase(p.Base) }

// Currency 返回某种货币余额。
func (p *Player) Currency(t uint32) int64 { return p.Base.GetCurrency()[t] }

// AddCurrency 增减货币，返回新余额与是否成功。余额不足时不改动任何状态。
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

// FindItem 按实例 ID 查找道具。
func (p *Player) FindItem(instanceID uint64) (*pb.Item, int) {
	for i, it := range p.Bag.GetItems() {
		if it.GetInstanceId() == instanceID {
			return it, i
		}
	}
	return nil, -1
}

// CountItem 统计某模板道具的总数。
func (p *Player) CountItem(tplID uint32) int64 {
	var n int64
	for _, it := range p.Bag.GetItems() {
		if it.GetTplId() == tplID {
			n += it.GetCount()
		}
	}
	return n
}

// AddItem 加入道具（可堆叠的合并到已有格子）。instanceID 由雪花生成。
func (p *Player) AddItem(instanceID uint64, tplID uint32, count int64, now time.Time) (*pb.Item, bool) {
	if count <= 0 {
		return nil, false
	}
	if Stackable(tplID) {
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

// RemoveItemByTpl 按模板扣除数量，不足时不改动任何状态并返回 false。
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

// UseItem 按实例 ID 消耗道具。
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

// Stackable 报告道具是否可堆叠。真实项目里应查配置表，这里按模板号段约定。
func Stackable(tplID uint32) bool { return tplID < 10000 }

// Quest 查找任务。
func (p *Player) FindQuest(id uint32) *pb.Quest {
	for _, q := range p.Quest.GetQuests() {
		if q.GetQuestId() == id {
			return q
		}
	}
	return nil
}

// FindMail 查找邮件。
func (p *Player) FindMail(id uint64) *pb.Mail {
	for _, m := range p.Mail.GetMails() {
		if m.GetMailId() == id {
			return m
		}
	}
	return nil
}

// HasFriend 报告是否已是好友。
func (p *Player) HasFriend(uid uint64) bool {
	for _, f := range p.Social.GetFriends() {
		if f == uid {
			return true
		}
	}
	return false
}

// Idle 返回下线后经过的时长；仍在线返回 0。
func (p *Player) Idle(now time.Time) time.Duration {
	if p.Online || p.OfflineAt.IsZero() {
		return 0
	}
	return now.Sub(p.OfflineAt)
}
