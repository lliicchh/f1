// Package ledger 资金流水账本
//
// 流水必须和余额变更在同一个 Lua 里写。分开写迟早会出现「扣了钱没流水」或
// 「有流水没扣钱」，事后没法判断是 bug 还是有人盗刷。
//
// 存储用 Redis Stream，与玩家键同 hash tag 所以能进同一个脚本。
// 这只是过渡：Redis 能被 FLUSH，不是审计级存储，上真钱前得导出到长期存储
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

// Type 流水类型
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

// StreamField Redis Stream 里承载 JSON 的字段名
const StreamField = "d"

// DefaultMaxLen 每个玩家在 Redis 里保留的流水条数上限
//
// Redis 只当热数据窗口。不设上限内存会一直涨，而 Redis 配的是 noeviction，
// 内存打满就是全服写不进去，比丢一段历史流水严重
const DefaultMaxLen = 2000

// Entry 一条流水
//
// 字段要够单看一条就能还原当时发生了什么。事后排查拿不到上下文，
// 流水里缺什么就是永远缺什么
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

// New 构造一条流水，顺手补时间戳
func New(uid uint64, t Type, currency uint32, amount, balance int64) *Entry {
	return &Entry{
		UID: uid, Type: t, Currency: currency,
		Amount: amount, Balance: balance,
		TsMs: time.Now().UnixMilli(),
	}
}

func (e *Entry) WithRound(roundID uint64, gameID, confVer string) *Entry {
	e.RoundID, e.GameID, e.ConfVer = roundID, gameID, confVer
	return e
}

// WithRef 关联外部单号
func (e *Entry) WithRef(ref string) *Entry { e.Ref = ref; return e }

// WithOperator 记录 GM 操作者
func (e *Entry) WithOperator(op string) *Entry { e.Operator = op; return e }

// WithID 指定流水号，一般用雪花
func (e *Entry) WithID(id uint64) *Entry {
	e.EntryID = strconv.FormatUint(id, 10)
	return e
}

func (e *Entry) JSON() ([]byte, error) { return json.Marshal(e) }

// MustJSON 序列化，出错返回 nil
func (e *Entry) MustJSON() []byte {
	b, err := json.Marshal(e)
	if err != nil {
		return nil
	}
	return b
}

// Proto 转成协议消息，给 GM 查询用
func (e *Entry) Proto() *pb.LedgerEntry {
	return &pb.LedgerEntry{
		EntryId: e.EntryID, Uid: e.UID, Type: string(e.Type),
		Currency: e.Currency, Amount: e.Amount, Balance: e.Balance,
		RoundId: e.RoundID, GameId: e.GameID, Ref: e.Ref,
		TsMs: e.TsMs, ConfigVersion: e.ConfVer,
	}
}

// Encode 把一批流水序列化成 Lua 要的形式
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

// Reader 读流水。任何进程都能读，不用经过玩家 owner
type Reader struct {
	rdb  redis.UniversalClient
	keys *store.Keys
}

func NewReader(rdb redis.UniversalClient, keys *store.Keys) *Reader {
	return &Reader{rdb: rdb, keys: keys}
}

// Recent 返回某玩家最近 n 条流水（时间倒序）
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

// Since 返回某个 stream ID 之后的流水，正序，给导出器增量拉
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

// Len 返回某玩家当前保留的流水条数
func (r *Reader) Len(ctx context.Context, uid uint64) (int64, error) {
	n, err := r.rdb.XLen(ctx, r.keys.Ledger(uid)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
}

// Balance 用流水重算余额来对账。对不上就是资损或 bug
//
// 只能覆盖 Redis 里还没被裁掉的那段，全量对账得用导出后的数据
func (r *Reader) Balance(ctx context.Context, uid uint64, currency uint32) (int64, int64, error) {
	entries, err := r.Recent(ctx, uid, DefaultMaxLen)
	if err != nil {
		return 0, 0, err
	}
	var sum int64
	var latest int64
	found := false
	// Recent 倒序：第一条即最新
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
