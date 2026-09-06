// Package ledger 是资金流水账本。
//
// 评审 P1-2 的修复：原来钱的变动只是覆盖式改写玩家 base 模块 ——
// 余额是多少查得到，为什么是这个数查不到。客服无法查单，财务无法对账。
//
// 一条铁律：**流水必须与余额变更在同一个 Lua 里原子写入**。
// 分成两步写，一定会出现「扣了钱没流水」或「有流水没扣钱」，
// 而这两种情况事后无法区分是 bug 还是欺诈 —— 那才是真正致命的。
//
// 存储用 Redis Stream（append-only，且与玩家键同 hash tag，因此能进同一个 Lua）。
// 必须说清楚：**这是过渡方案**。Redis 可被 FLUSH、可被改写，不是审计级存储。
// 真钱上线前，Exporter 必须把流水搬到不可篡改的长期存储（对象存储 / 数仓 / Kafka）。
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/store"
)

// Type 是流水类型。
type Type string

const (
	TypeBet            Type = "BET"        // 下注（出账）
	TypeWin            Type = "WIN"        // 派彩（入账）
	TypeJackpotContrib Type = "JP_CONTRIB" // 奖池注入（会计上属于投注的一部分）
	TypeJackpotWin     Type = "JP_WIN"     // 中奖池（入账）
	TypePurchase       Type = "PURCHASE"   // 充值到账
	TypeGrant          Type = "GRANT"      // GM 补单 / 系统发放
	TypeGachaCost      Type = "GACHA_COST" // 抽卡消耗
	TypeGachaDrop      Type = "GACHA_DROP" // 抽卡产出
	TypeTransferOut    Type = "XFER_OUT"   // 跨分片转出
	TypeTransferIn     Type = "XFER_IN"    // 跨分片转入
	TypeMailClaim      Type = "MAIL_CLAIM" // 邮件附件领取
	TypeBattleReward   Type = "BATTLE"     // 战斗奖励
)

// StreamField 是 Redis Stream 里承载 JSON 的字段名。
const StreamField = "d"

// DefaultMaxLen 是每个玩家流水在 Redis 中保留的近似条数。
//
// Redis 只是热数据窗口，长期留存靠 Exporter 导出；
// 不设上限会让内存随时间无限增长，而 §6.4 要求 Redis 必须 noeviction ——
// 内存打满就是全服写入失败，比丢一段历史流水严重得多。
const DefaultMaxLen = 2000

// Entry 是一条流水。
//
// 字段设计遵循一个原则：**单看一条流水就能复原当时发生了什么**。
// 事后排查往往拿不到上下文，流水里缺什么就永远缺什么。
type Entry struct {
	EntryID  string `json:"id"`
	UID      uint64 `json:"uid"`
	Type     Type   `json:"type"`
	Currency uint32 `json:"cur"`
	Amount   int64  `json:"amt"` // 正 = 入账，负 = 出账
	Balance  int64  `json:"bal"` // 变动后余额，用于逐条核对
	RoundID  uint64 `json:"round,omitempty"`
	GameID   string `json:"game,omitempty"`
	Ref      string `json:"ref,omitempty"`  // 订单号 / txid / 工单号
	Operator string `json:"op,omitempty"`   // GM 操作者，用于追责
	ConfVer  string `json:"conf,omitempty"` // 配置版本，复算用
	TsMs     int64  `json:"ts"`
}

// New 构造一条流水，自动补时间戳。
func New(uid uint64, t Type, currency uint32, amount, balance int64) *Entry {
	return &Entry{
		UID: uid, Type: t, Currency: currency,
		Amount: amount, Balance: balance,
		TsMs: time.Now().UnixMilli(),
	}
}

// WithRound 关联回合。
func (e *Entry) WithRound(roundID uint64, gameID, confVer string) *Entry {
	e.RoundID, e.GameID, e.ConfVer = roundID, gameID, confVer
	return e
}

// WithRef 关联外部单号。
func (e *Entry) WithRef(ref string) *Entry { e.Ref = ref; return e }

// WithOperator 记录 GM 操作者。
func (e *Entry) WithOperator(op string) *Entry { e.Operator = op; return e }

// WithID 指定流水号（通常用雪花，保证跨节点唯一）。
func (e *Entry) WithID(id uint64) *Entry {
	e.EntryID = strconv.FormatUint(id, 10)
	return e
}

// JSON 序列化。
func (e *Entry) JSON() ([]byte, error) { return json.Marshal(e) }

// MustJSON 序列化，失败返回空。
func (e *Entry) MustJSON() []byte {
	b, err := json.Marshal(e)
	if err != nil {
		return nil
	}
	return b
}

// Proto 转成协议消息，供 GM 查询返回。
func (e *Entry) Proto() *pb.LedgerEntry {
	return &pb.LedgerEntry{
		EntryId: e.EntryID, Uid: e.UID, Type: string(e.Type),
		Currency: e.Currency, Amount: e.Amount, Balance: e.Balance,
		RoundId: e.RoundID, GameId: e.GameID, Ref: e.Ref,
		TsMs: e.TsMs, ConfigVersion: e.ConfVer,
	}
}

// Encode 把一批流水序列化成 Lua 需要的形式。
func Encode(entries []*Entry) ([][]byte, error) {
	out := make([][]byte, 0, len(entries))
	for _, e := range entries {
		b, err := e.JSON()
		if err != nil {
			return nil, fmt.Errorf("ledger: 序列化流水失败: %w", err)
		}
		out = append(out, b)
	}
	return out, nil
}

// Reader 读取流水。任何进程都可以读，不经过玩家 owner ——
// 流水是只读投影，与 profile 摘要同理。
type Reader struct {
	rdb  redis.UniversalClient
	keys *store.Keys
}

// NewReader 构造读取器。
func NewReader(rdb redis.UniversalClient, keys *store.Keys) *Reader {
	return &Reader{rdb: rdb, keys: keys}
}

// Recent 返回某玩家最近 n 条流水（时间倒序）。
func (r *Reader) Recent(ctx context.Context, uid uint64, n int64) ([]*Entry, error) {
	if n <= 0 {
		n = 50
	}
	msgs, err := r.rdb.XRevRangeN(ctx, r.keys.Ledger(uid), "+", "-", n).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	return decode(msgs), nil
}

// Since 返回某玩家自某个 stream ID 之后的流水（正序），供导出器增量拉取。
func (r *Reader) Since(ctx context.Context, uid uint64, lastID string, n int64) ([]*Entry, string, error) {
	if lastID == "" {
		lastID = "0-0"
	}
	if n <= 0 {
		n = 500
	}
	msgs, err := r.rdb.XRangeN(ctx, r.keys.Ledger(uid), "("+lastID, "+", n).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, lastID, err
	}
	if len(msgs) == 0 {
		return nil, lastID, nil
	}
	return decode(msgs), msgs[len(msgs)-1].ID, nil
}

// Len 返回某玩家当前保留的流水条数。
func (r *Reader) Len(ctx context.Context, uid uint64) (int64, error) {
	n, err := r.rdb.XLen(ctx, r.keys.Ledger(uid)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
}

// Balance 用流水重算余额，用于对账。
//
// 结果应当与玩家数据里的余额一致；不一致就是资损或 bug，必须立刻查。
// 注意它只能覆盖 Redis 中尚未被裁剪的窗口，全量对账要用导出后的长期存储。
func (r *Reader) Balance(ctx context.Context, uid uint64, currency uint32) (int64, int64, error) {
	entries, err := r.Recent(ctx, uid, DefaultMaxLen)
	if err != nil {
		return 0, 0, err
	}
	var sum int64
	var latest int64
	found := false
	// Recent 是倒序：第一条即最新。
	for i, e := range entries {
		if e.Currency != currency {
			continue
		}
		if i == 0 || !found {
			latest = e.Balance
			found = true
		}
		sum += e.Amount
	}
	return sum, latest, nil
}

func decode(msgs []redis.XMessage) []*Entry {
	out := make([]*Entry, 0, len(msgs))
	for _, m := range msgs {
		raw, ok := m.Values[StreamField]
		if !ok {
			continue
		}
		s, ok := raw.(string)
		if !ok {
			continue
		}
		var e Entry
		if json.Unmarshal([]byte(s), &e) != nil {
			continue
		}
		if e.EntryID == "" {
			e.EntryID = m.ID
		}
		out = append(out, &e)
	}
	return out
}
