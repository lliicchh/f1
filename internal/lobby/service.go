package lobby

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/profile"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/session"
	"github.com/gamedev/f1/pkg/shard"
	"github.com/gamedev/f1/pkg/shardsvc"
	"github.com/gamedev/f1/pkg/store"
	"github.com/gamedev/f1/pkg/subject"
	"github.com/gamedev/f1/pkg/xtx"
)

// Service 是 Lobby 服务。
//
// Lobby 是玩家对象的容器，同时跑玩家的业务逻辑，二者在同一进程内（§2.2）。
// 它是分片独占的，绝不使用 queue group（§2.3 / D1）。
type Service struct {
	node     *node.Node
	shards   *shardsvc.Service
	txm      *xtx.Manager
	profiles *profile.Reader
	sessions *session.Store
	js       *bus.JS

	consumers []jetstream.ConsumeContext
	orderTTL  time.Duration

	// jobSem 限制并发转发的 job 数，避免 JetStream 一次性推来的大批任务
	// 把 Lobby 的出向请求打爆。
	jobSem chan struct{}
}

// New 构造 Lobby 服务。
func New() *Service {
	return &Service{
		orderTTL: 30 * 24 * time.Hour,
		jobSem:   make(chan struct{}, 64),
	}
}

// Name 实现 node.Service。
func (s *Service) Name() string { return "lobby" }

// ShardOf 返回 uid 所属分片：lobbyShard(uid) = uid % 1024（§4.1）。
func (s *Service) ShardOf(uid uint64) uint32 { return shard.Of(uid, s.node.Cfg.ShardCount) }

// Owns 报告 uid 所属分片是否由本实例持有并已开始服务。
//
// 供运维接口与测试判断「这个玩家现在归谁」，不参与请求路由 ——
// 路由由 subject 决定，这里只是观察窗口。
func (s *Service) Owns(uid uint64) bool {
	return s.shards != nil && s.shards.Owns(uid)
}

// OwnedShards 返回本实例持有的分片列表。
func (s *Service) OwnedShards() []uint32 {
	if s.shards == nil || s.shards.Claimer() == nil {
		return nil
	}
	return s.shards.Claimer().Owned()
}

// Start 启动服务。
func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.profiles = profile.NewReader(n.Redis, n.Keys)
	s.sessions = session.NewStore(n.Redis, n.Keys, n.Cfg.SessionTTL)
	s.txm = xtx.NewManager(n.Redis, n.Keys, 7*24*time.Hour, 30*24*time.Hour)

	s.shards = shardsvc.New(shardsvc.Options{
		Kind: shard.KindLobby,
		Node: n,
		Wildcard: func(sh uint32) string {
			// 不带 queue group：分片独占性正是靠「同一 subject 只有一个订阅者」保证的（§4.3）。
			return subject.LobbyShardWildcard(sh)
		},
		Factory: func(o shard.Ownership, svc *shardsvc.Service) shardsvc.State {
			return NewShard(o, svc, s)
		},
	})
	if err := s.shards.Start(ctx); err != nil {
		return err
	}

	// job.* 必达任务：跨分片转移与发信（§5.2 / §8）。
	js, err := n.Bus.NewJetStream(ctx)
	if err != nil {
		return fmt.Errorf("初始化 JetStream 失败: %w", err)
	}
	s.js = js

	if err := s.startJobConsumers(ctx); err != nil {
		return err
	}
	return nil
}

// NotifyClients 实现 node.Service。Lobby 不直连客户端，无需通知。
func (s *Service) NotifyClients(ctx context.Context) {}

// StopAccepting 停止接新请求：释放全部分片（各自全量刷盘）。
func (s *Service) StopAccepting(ctx context.Context) {
	for _, cc := range s.consumers {
		cc.Stop()
	}
	s.shards.StopAccepting(ctx)
}

// FlushAll 确认刷盘完成。
func (s *Service) FlushAll(ctx context.Context) error { return s.shards.FlushAll(ctx) }

// Close 释放资源。
func (s *Service) Close(ctx context.Context) { s.shards.Close(ctx) }

// ---------------------------------------------------------------------------
// job 中转层
// ---------------------------------------------------------------------------

func (s *Service) startJobConsumers(ctx context.Context) error {
	// 转移任务。
	cc, err := s.js.Consume(ctx, bus.ConsumerOptions{
		Durable:    "lobby-transfer",
		Filter:     subject.JobTransferWildcard,
		AckWait:    30 * time.Second,
		MaxDeliver: 200,
		MaxAckPend: 256,
	}, s.onTransferJob)
	if err != nil {
		return err
	}
	s.consumers = append(s.consumers, cc)

	// 发信任务。
	cc, err = s.js.Consume(ctx, bus.ConsumerOptions{
		Durable:    "lobby-mail",
		Filter:     subject.JobMailSend,
		AckWait:    30 * time.Second,
		MaxDeliver: 200,
		MaxAckPend: 256,
	}, s.onMailJob)
	if err != nil {
		return err
	}
	s.consumers = append(s.consumers, cc)

	// 战斗发奖任务。
	cc, err = s.js.Consume(ctx, bus.ConsumerOptions{
		Durable:    "lobby-battle",
		Filter:     subject.JobBattleSettle,
		AckWait:    30 * time.Second,
		MaxDeliver: 200,
		MaxAckPend: 256,
	}, s.onBattleJob)
	if err != nil {
		return err
	}
	s.consumers = append(s.consumers, cc)
	return nil
}

func (s *Service) onBattleJob(jm *bus.JobMsg) {
	var res pb.BattleResult
	if err := bus.Unpack(jm.Env, &res); err != nil {
		logx.Error("战斗发奖任务无法解析，终止投递", "subject", jm.Subject, "err", err)
		_ = jm.Term()
		return
	}
	target := res.GetTargetUid()
	if target == 0 {
		target = jm.Env.GetUid()
	}
	s.forward(jm, target, protocol.CmdBattleSettle, &res)
}

// onTransferJob 把必达任务转发给目标 uid 所属分片的 owner。
//
// 多个 Lobby 实例共用同一个 durable，竞争的是「谁来搬运」而不是「谁持有数据」，
// 因此不违反 §2.3 的「持有数据的服务不用 queue group」。
func (s *Service) onTransferJob(jm *bus.JobMsg) {
	var job pb.TransferJob
	if err := bus.Unpack(jm.Env, &job); err != nil {
		logx.Error("转移任务无法解析，终止投递", "subject", jm.Subject, "err", err)
		_ = jm.Term()
		return
	}
	s.forward(jm, job.GetToUid(), protocol.CmdApplyTransfer, &job)
}

func (s *Service) onMailJob(jm *bus.JobMsg) {
	var job pb.MailSendJob
	if err := bus.Unpack(jm.Env, &job); err != nil {
		logx.Error("发信任务无法解析，终止投递", "subject", jm.Subject, "err", err)
		_ = jm.Term()
		return
	}
	s.forward(jm, job.GetToUid(), protocol.CmdApplyMail, &job)
}

// forward 把任务转发给目标分片 owner，成功才 Ack。
func (s *Service) forward(jm *bus.JobMsg, toUID uint64, cmd protocol.Cmd, body proto.Message) {
	if toUID == 0 {
		logx.Error("任务缺少目标 uid，终止投递", "subject", jm.Subject)
		_ = jm.Term()
		return
	}

	select {
	case s.jobSem <- struct{}{}:
	default:
		// 并发已满，稍后重投而不是排队占住消费者。
		_ = jm.Nak(2 * time.Second)
		return
	}

	go func() {
		defer func() { <-s.jobSem }()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		sh := s.ShardOf(toUID)
		subj := subject.LobbyReq(sh, cmd.Name())
		traceID := jm.Env.GetTraceId()

		var ack pb.Ack
		err := s.node.Bus.Call(ctx, subj, cmd, toUID, traceID, body, &ack)
		if err == nil {
			_ = jm.Ack()
			return
		}

		// 分片正在交接、或 owner 暂时不可用：延迟重投。
		// 这正是 job.* 走 JetStream 的意义 —— 丢了会导致资产不一致（§5.2）。
		delay := backoff(jm.Deliveries())
		logx.Trace(traceID).Warn("转发必达任务失败，稍后重投",
			"subject", subj, "to_uid", toUID, "deliveries", jm.Deliveries(),
			"retry_in", delay, "err", err)
		_ = jm.Nak(delay)
	}()
}

func backoff(deliveries uint64) time.Duration {
	switch {
	case deliveries <= 1:
		return time.Second
	case deliveries <= 3:
		return 3 * time.Second
	case deliveries <= 6:
		return 10 * time.Second
	case deliveries <= 10:
		return 30 * time.Second
	default:
		return time.Minute
	}
}

// publishTransfer 投递一笔转移任务。
func (s *Service) publishTransfer(ctx context.Context, rec *xtx.Record, traceID string) error {
	job := &pb.TransferJob{
		Txid:          rec.TxID,
		FromUid:       rec.FromUID,
		ToUid:         rec.ToUID,
		Items:         rec.Items,
		CurrencyDelta: rec.CurrDelta,
		CurrencyType:  rec.CurrType,
		Reason:        rec.Reason,
		CreatedAt:     rec.CreatedAt,
	}
	// msgID 用 txid：JetStream 层面去重，与接收方的 SETNX 幂等形成双保险。
	return s.js.PublishJob(ctx, subject.JobTransfer(rec.TxID),
		protocol.CmdApplyTransfer, rec.ToUID, traceID, rec.TxID, job)
}

// SendMail 通过必达任务给任意玩家发信（可离线）。
func (s *Service) SendMail(ctx context.Context, toUID uint64, mail *pb.Mail, traceID string) error {
	sf, err := s.node.NextID()
	if err != nil {
		return err
	}
	txid := xtx.NewTxID(sf)
	job := &pb.MailSendJob{ToUid: toUID, Mail: mail, Txid: txid}
	return s.js.PublishJob(ctx, subject.JobMailSend, protocol.CmdApplyMail, toUID, traceID, txid, job)
}

// notifyFriendAdded 通知对方「有人加你为好友」。
//
// 对方可能在别的分片甚至不在线，因此走 job 中转层而不是直接改对方内存（§8）。
func (s *Service) notifyFriendAdded(from, to uint64, traceID string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		mail := &pb.Mail{
			Title:   "好友提醒",
			Content: fmt.Sprintf("玩家 %d 把你加为好友", from),
			Sender:  "system",
			SendAt:  time.Now().UnixMilli(),
		}
		if err := s.SendMail(ctx, to, mail, traceID); err != nil {
			logx.Trace(traceID).Warn("发送好友提醒失败", "from", from, "to", to, "err", err)
		}
	}()
}

// fallbackToMailbox 在背包塞不下时把资产写入待领取队列，等玩家上线消费（§6.5）。
//
// 绝不能默默丢弃：那是资产不一致。
func (s *Service) fallbackToMailbox(uid uint64, job *pb.TransferJob, traceID string) {
	mail := &pb.Mail{
		Title:       "背包已满，附件转存",
		Content:     "你的背包已满，物品已转为邮件附件，请清理后领取。",
		Sender:      "system",
		SendAt:      time.Now().UnixMilli(),
		Attachments: job.GetItems(),
	}
	blob, err := proto.Marshal(mail)
	if err != nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.node.Redis.RPush(ctx, s.node.Keys.MailPending(uid), blob).Err(); err != nil {
			logx.Trace(traceID).Error("写入待领取队列失败（告警：资产可能丢失）", "uid", uid, "err", err)
		}
	}()
}

// indexGuildMember 把玩家登记进公会成员索引。
//
// 公会广播需要成员列表才能按 gateID 聚合，而成员关系存在各自玩家的 social 模块里、
// 分散在不同分片，无法在广播时现查 —— 因此维护一份 Redis 索引作为读模型，
// 与 profile 摘要同理（§6.5）。
func (s *Service) indexGuildMember(guildID, uid uint64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.node.Redis.SAdd(ctx, s.node.Keys.GuildMembers(guildID), uid).Err(); err != nil {
			logx.Warn("写入公会成员索引失败", "guild", guildID, "uid", uid, "err", err)
		}
	}()
}

// ---------------------------------------------------------------------------
// 补偿扫描器（§8）
// ---------------------------------------------------------------------------

// scanPendingTx 重投本分片中超时未完成的 PENDING 转移。
//
// 由分片 Tick 触发，因此只有 owner 会扫自己的分片，天然分布、不会重复扫。
// 在 Actor 里只做投递，实际 IO 在独立 goroutine。
func (s *Service) scanPendingTx(sh uint32) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		before := time.Now().Add(-s.node.Cfg.TxTimeout)
		recs, err := s.txm.ScanPending(ctx, sh, before, 100)
		if err != nil {
			logx.Warn("扫描 PENDING 转移失败", "shard", sh, "err", err)
			return
		}
		if n, err := s.txm.PendingCount(ctx, sh); err == nil {
			metrics.TxPending.Set(float64(n))
		}
		if len(recs) == 0 {
			return
		}

		logx.Info("补偿扫描：重投超时的 PENDING 转移", "shard", sh, "count", len(recs))
		for _, rec := range recs {
			if err := s.publishTransfer(ctx, rec, "tx-rescan"); err != nil {
				logx.Warn("重投转移失败", "txid", rec.TxID, "err", err)
				continue
			}
			if err := s.txm.Requeue(ctx, rec); err != nil {
				logx.Warn("更新 PENDING 索引失败", "txid", rec.TxID, "err", err)
			}
		}
	}()
}

// ---------------------------------------------------------------------------
// 下行推送
// ---------------------------------------------------------------------------

// pushToPlayer 给单个在线玩家推送。
//
// 走 push.gate.{gateID} 定向推送：网关订阅数恒定，与在线人数无关（§9.1 / D4）。
func (s *Service) pushToPlayer(p *Player, push protocol.Push, body proto.Message, traceID string) {
	if !p.Online || p.GateID == "" {
		return
	}
	raw, err := proto.Marshal(body)
	if err != nil {
		return
	}
	env, err := s.node.Bus.NewEnvelope(protocol.Cmd(push), p.UID, traceID, &pb.MultiPush{
		Uids:    []uint64{p.UID},
		Cmd:     uint32(push),
		Payload: raw,
	})
	if err != nil {
		return
	}
	if err := s.node.Bus.PublishEnv(subject.GatePush(p.GateID), env); err != nil {
		logx.Trace(traceID).Warn("推送失败", "uid", p.UID, "gate", p.GateID, "err", err)
	}
}

// PublishPlayerEvent 发布玩家事件（evt.player.{uid}.{event}，§5.1）。
//
// 事件是「广播给任何关心的人」，不保证投递、不等应答：
// 活动系统、数据上报、成就统计这类旁路消费者订阅它，
// 不会给玩家主流程增加任何同步开销。丢一条不影响资产，因此走 Core NATS。
func (s *Service) PublishPlayerEvent(uid uint64, event string, body proto.Message, traceID string) {
	if err := s.node.Bus.Publish(subject.PlayerEvt(uid, event),
		protocol.CmdUnknown, uid, traceID, body); err != nil {
		logx.Trace(traceID).Debug("发布玩家事件失败", "uid", uid, "event", event, "err", err)
	}
}

// Broadcast 全服公告走 push.broadcast（§9.3）。
func (s *Service) Broadcast(push protocol.Push, body proto.Message, traceID string) error {
	raw, err := proto.Marshal(body)
	if err != nil {
		return err
	}
	env, err := s.node.Bus.NewEnvelope(protocol.Cmd(push), 0, traceID, &pb.MultiPush{
		Cmd:     uint32(push),
		Payload: raw,
	})
	if err != nil {
		return err
	}
	return s.node.Bus.PublishEnv(subject.Broadcast, env)
}

var (
	_ node.Service = (*Service)(nil)
	_              = errors.Is
	_              = store.L1
)
