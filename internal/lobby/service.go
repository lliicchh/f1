package lobby

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/authz"
	"github.com/gamedev/f1/pkg/bus"
	"github.com/gamedev/f1/pkg/gameconf"
	"github.com/gamedev/f1/pkg/jackpot"
	"github.com/gamedev/f1/pkg/ledger"
	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/node"
	"github.com/gamedev/f1/pkg/payment"
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

// Service Lobby 服务。分片独占，不用 queue group
type Service struct {
	node     *node.Node
	shards   *shardsvc.Service
	txm      *xtx.Manager
	profiles *profile.Reader
	sessions *session.Store
	js       *bus.JS

	// conf 可热替换的游戏配置
	conf *gameconf.Store
	// jackpot 管累积奖池
	jackpot *jackpot.Manager
	// ledger 读流水，GM 查询和对账用
	ledger *ledger.Reader
	// signer 验内部和 GM 命令的签名
	signer *authz.Signer
	// payment 验充值回执
	payment payment.Verifier

	consumers []jetstream.ConsumeContext
	orderTTL  time.Duration

	// jobSem 限制同时转发的 job 数，免得 JetStream 一次推一大批把出向请求打爆
	jobSem chan struct{}
}

func New() *Service {
	return &Service{
		orderTTL: 30 * 24 * time.Hour,
		jobSem:   make(chan struct{}, 64),
	}
}

func (s *Service) Name() string { return "lobby" }

func (s *Service) ShardOf(uid uint64) uint32 { return shard.Of(uid, s.node.Cfg.ShardCount) }

// Owns 报告 uid 所属分片是否由本实例持有并已开始服务
//
// 供运维接口与测试判断「这个玩家现在归谁」，不参与请求路由。
// 路由由 subject 决定，这里只是观察窗口
func (s *Service) Owns(uid uint64) bool {
	return s.shards != nil && s.shards.Owns(uid)
}

// OwnedShards 返回本实例持有的分片列表
func (s *Service) OwnedShards() []uint32 {
	if s.shards == nil || s.shards.Claimer() == nil {
		return nil
	}
	return s.shards.Claimer().Owned()
}

func (s *Service) Start(ctx context.Context, n *node.Node) error {
	s.node = n
	s.profiles = profile.NewReader(n.Redis, n.Keys)
	s.sessions = session.NewStore(n.Redis, n.Keys, n.Cfg.SessionTTL)
	s.txm = xtx.NewManager(n.Redis, n.Keys, 7*24*time.Hour, 30*24*time.Hour)
	s.ledger = ledger.NewReader(n.Redis, n.Keys)
	s.signer = authz.NewSigner(n.Cfg.InternalSecret)

	conf, err := gameconf.Load(n.Cfg.GameConfPath)
	if err != nil {
		return fmt.Errorf("加载游戏配置失败: %w", err)
	}
	s.conf = gameconf.NewStore(conf)
	s.jackpot = jackpot.NewManager(n.Redis, n.Keys, conf.Jackpots)
	metrics.ConfigVersion.WithLabelValues(conf.Version()).Set(1)
	logx.Info("游戏配置已加载",
		"version", conf.Version(), "path", n.Cfg.GameConfPath,
		"machines", len(conf.Slots), "pools", len(conf.Gacha))

	s.payment = payment.New(n.Cfg.PaymentSecret, n.Cfg.PaymentSandbox)
	if !s.payment.Strict() {
		logx.Error("支付校验处于沙箱模式（告警）：仅限本地开发，生产必须配置 PAYMENT_SECRET")
	}
	if !s.signer.HasSecret() {
		logx.Error("未配置 INTERNAL_SECRET（告警）：本进程无法签发内部命令，" +
			"发奖 / 结算 / 跨分片入账都会失败")
	}

	s.shards = shardsvc.New(shardsvc.Options{
		Kind: shard.KindLobby,
		Node: n,
		Wildcard: func(sh uint32) string {
			// 不带 queue group：分片独占靠的就是同一 subject 只有一个订阅者
			return subject.LobbyShardWildcard(sh)
		},
		Factory: func(o shard.Ownership, svc *shardsvc.Service) shardsvc.State {
			return NewShard(o, svc, s)
		},
	})
	if err := s.shards.Start(ctx); err != nil {
		return err
	}

	// job.* 是必达任务，跨分片转移和发信走这里
	js, jerr := n.Bus.NewJetStream(ctx)
	if jerr != nil {
		return fmt.Errorf("初始化 JetStream 失败: %w", jerr)
	}
	s.js = js

	if err := s.startJobConsumers(ctx); err != nil {
		return err
	}
	return nil
}

// NotifyClients 空实现，Lobby 不直连客户端
func (s *Service) NotifyClients(ctx context.Context) {}

// StopAccepting 停接新请求并释放全部分片，各分片会各自刷盘
func (s *Service) StopAccepting(ctx context.Context) {
	for _, cc := range s.consumers {
		cc.Stop()
	}
	s.shards.StopAccepting(ctx)
}

func (s *Service) FlushAll(ctx context.Context) error { return s.shards.FlushAll(ctx) }

func (s *Service) Close(ctx context.Context) { s.shards.Close(ctx) }

// ---------------------------------------------------------------------------
// job 中转层
// ---------------------------------------------------------------------------

func (s *Service) startJobConsumers(ctx context.Context) error {
	// 转移任务
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

	// 发信任务
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

	// 战斗发奖任务
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

// onTransferJob 把任务转给目标 uid 所在分片的 owner
//
// 多个实例共用一个 durable，竞争的是谁来搬运，不是谁持有数据
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

// forward 把任务转给目标分片的 owner，成功才 Ack
func (s *Service) forward(jm *bus.JobMsg, toUID uint64, cmd protocol.Cmd, body proto.Message) {
	if toUID == 0 {
		logx.Error("任务缺少目标 uid，终止投递", "subject", jm.Subject)
		_ = jm.Term()
		return
	}

	select {
	case s.jobSem <- struct{}{}:
	default:
		// 并发满了，稍后重投，别占着消费者排队
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

		// 转的是内部命令，得带签名，否则会被目标分片的第二道鉴权拒掉
		var ack pb.Ack
		err := s.internalCall(ctx, subj, cmd, toUID, traceID, body, &ack)
		if err == nil {
			_ = jm.Ack()
			return
		}

		// 分片在交接或者 owner 暂时不可用，延迟重投。
		// job.* 走 JetStream 就是为了这个，丢了会造成资产不一致
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

// publishTransfer 投递一笔转移任务
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
	// msgID 用 txid，JetStream 先去一次重，加上接收方的 SETNX 是双保险
	return s.js.PublishJob(ctx, subject.JobTransfer(rec.TxID),
		protocol.CmdApplyTransfer, rec.ToUID, traceID, rec.TxID, job)
}

// SendMail 通过必达任务给任意玩家发信，离线也行
func (s *Service) SendMail(ctx context.Context, toUID uint64, mail *pb.Mail, traceID string) error {
	sf, err := s.node.NextID()
	if err != nil {
		return err
	}
	txid := xtx.NewTxID(sf)
	job := &pb.MailSendJob{ToUid: toUID, Mail: mail, Txid: txid}
	return s.js.PublishJob(ctx, subject.JobMailSend, protocol.CmdApplyMail, toUID, traceID, txid, job)
}

// notifyFriendAdded 告诉对方有人加了你。
// 对方可能在别的分片甚至不在线，所以走 job，不直接改人家内存
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

// fallbackToMailbox 背包塞不下时把东西转进待领取队列，等玩家上线再拿。
// 不能默默丢掉，那是资产不一致
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

// indexGuildMember 把玩家登记进公会成员索引
//
// 成员关系散在各玩家的 social 模块里，广播时现查不现实，所以单独维护一份索引
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
// 补偿扫描器
// ---------------------------------------------------------------------------

// scanPendingTx 重投本分片里超时没完成的转移
//
// 由分片 Tick 触发，只有 owner 会扫自己的分片，天然分布不会重复。
// Actor 里只负责发起，IO 在独立 goroutine
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

// pushToPlayer 给单个在线玩家推送，走 push.gate.{gateID}
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

// PublishPlayerEvent 发玩家事件，不保证投递也不等应答
//
// 活动、埋点、成就这类旁路消费者订阅它，丢一条不影响资产，所以走 Core NATS
func (s *Service) PublishPlayerEvent(uid uint64, event string, body proto.Message, traceID string) {
	if err := s.node.Bus.Publish(subject.PlayerEvt(uid, event),
		protocol.CmdUnknown, uid, traceID, body); err != nil {
		logx.Trace(traceID).Debug("发布玩家事件失败", "uid", uid, "event", event, "err", err)
	}
}

// Broadcast 全服公告走 push.broadcast
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

// ---------------------------------------------------------------------------
// 配置 / 奖池 / 内部命令
// ---------------------------------------------------------------------------

func (s *Service) Conf() *gameconf.Config { return s.conf.Get() }

// ReloadConf 热替换配置，校验不过就保持原样
//
// 版本跟着回合走：热更之后开的局记新版本，已经开着的局还是旧版本，复算不会串
func (s *Service) ReloadConf(path string) error {
	c, err := gameconf.Load(path)
	if err != nil {
		return err
	}
	old := s.conf.Get().Version()
	if err := s.conf.Replace(c); err != nil {
		return err
	}
	metrics.ConfigVersion.Reset()
	metrics.ConfigVersion.WithLabelValues(c.Version()).Set(1)
	logx.Info("游戏配置已热替换", "from", old, "to", c.Version())
	return nil
}

func (s *Service) Ledger() *ledger.Reader { return s.ledger }

func (s *Service) Signer() *authz.Signer { return s.signer }

func (s *Service) Jackpot() *jackpot.Manager { return s.jackpot }

// contributeJackpot 往奖池注入，尽力而为
//
// 失败只是池子少涨，玩家那份钱已经扣掉并记了 JP_CONTRIB，差额照流水补得回来
func (s *Service) contributeJackpot(pool string, amount int64) {
	if s.jackpot == nil || amount <= 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		total, err := s.jackpot.Contribute(ctx, pool, amount)
		if err != nil {
			logx.Error("奖池注入失败（告警：需按 JP_CONTRIB 流水对账补回）",
				"pool", pool, "amount", amount, "err", err)
			return
		}
		metrics.JackpotAmount.WithLabelValues(pool).Set(float64(total))
	}()
}

// claimJackpot 处理中奖：清池并落一条 PENDING 记录，再把派彩交给中转层
func (s *Service) claimJackpot(pool string, uid, roundID uint64, traceID string) {
	if s.jackpot == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		sf, err := s.node.NextID()
		if err != nil {
			logx.Trace(traceID).Error("生成奖池 txid 失败", "err", err)
			return
		}
		txid := fmt.Sprintf("jp%d", sf)

		payout, err := s.jackpot.Claim(ctx, pool, uid, roundID, txid)
		if err != nil {
			if errors.Is(err, jackpot.ErrEmpty) {
				logx.Trace(traceID).Info("中奖但奖池已空", "pool", pool, "uid", uid)
				return
			}
			logx.Trace(traceID).Error("奖池清算失败", "pool", pool, "uid", uid, "err", err)
			return
		}

		metrics.JackpotWins.WithLabelValues(pool).Inc()
		metrics.JackpotAmount.WithLabelValues(pool).Set(0)
		logx.Trace(traceID).Info("奖池中奖",
			"pool", pool, "uid", uid, "amount", payout.Amount, "txid", txid)

		if err := s.deliverJackpot(ctx, payout, traceID); err != nil {
			// 没送到不要紧，PENDING 记录还在，扫描会重投
			logx.Trace(traceID).Error("奖池派彩投递失败，等待补偿扫描重投",
				"txid", txid, "err", err)
			return
		}
		if err := s.jackpot.Settle(ctx, payout); err != nil {
			logx.Trace(traceID).Warn("标记奖池派彩完成失败（下轮扫描会重投，幂等安全）",
				"txid", txid, "err", err)
		}
		_ = s.Broadcast(protocol.PushJackpotWon,
			&pb.JackpotInfoResp{PoolId: pool, Amount: payout.Amount}, traceID)
	}()
}

// deliverJackpot 把派彩打给玩家所在分片，幂等靠 txid
func (s *Service) deliverJackpot(ctx context.Context, p *jackpot.Payout, traceID string) error {
	sh := s.ShardOf(p.UID)
	req := &pb.JackpotClaimReq{
		PoolId: p.PoolID, Uid: p.UID, Txid: p.TxID,
		Amount: p.Amount, RoundId: p.RoundID,
	}
	var resp pb.JackpotClaimResp
	return s.internalCall(ctx, subject.LobbyReq(sh, protocol.CmdApplyJackpot.Name()),
		protocol.CmdApplyJackpot, p.UID, traceID, req, &resp)
}

// scanJackpotPayouts 重投超时没完成的奖池派彩，和跨分片补偿是一个套路
func (s *Service) scanJackpotPayouts() {
	if s.jackpot == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		for poolID := range s.conf.Get().Jackpots {
			before := time.Now().Add(-s.node.Cfg.TxTimeout)
			pending, err := s.jackpot.PendingPayouts(ctx, poolID, before, 50)
			if err != nil {
				logx.Warn("扫描奖池待派彩失败", "pool", poolID, "err", err)
				continue
			}
			metrics.JackpotPending.WithLabelValues(poolID).Set(float64(len(pending)))
			for _, p := range pending {
				if err := s.deliverJackpot(ctx, p, "jp-rescan"); err != nil {
					logx.Warn("重投奖池派彩失败", "txid", p.TxID, "err", err)
					_ = s.jackpot.Requeue(ctx, p)
					continue
				}
				if err := s.jackpot.Settle(ctx, p); err != nil {
					logx.Warn("标记奖池派彩完成失败", "txid", p.TxID, "err", err)
				}
			}
		}
	}()
}

// internalCall 发一条带签名的内部命令
//
// 没密钥就直接失败，不发没签名的请求，那样错误会在很远的地方冒出来，难查
func (s *Service) internalCall(ctx context.Context, subj string, cmd protocol.Cmd,
	uid uint64, traceID string, body, out proto.Message) error {

	env, err := s.node.Bus.NewEnvelope(cmd, uid, traceID, body)
	if err != nil {
		return err
	}
	if err := s.signer.Sign(env); err != nil {
		return err
	}
	resp, err := s.node.Bus.RequestEnv(ctx, subj, env)
	if err != nil {
		return err
	}
	if resp.GetErrCode() != 0 {
		return &bus.RemoteError{Code: protocol.ErrCode(resp.GetErrCode()), Msg: resp.GetErrMsg()}
	}
	if out != nil {
		return bus.Unpack(resp, out)
	}
	return nil
}
