package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/gamedev/f1/pkg/logx"
	"github.com/gamedev/f1/pkg/metrics"
	"github.com/gamedev/f1/pkg/pb"
	"github.com/gamedev/f1/pkg/protocol"
	"github.com/gamedev/f1/pkg/subject"
)

// JobStreamName 是承载全部 job.* 必达任务的 Stream 名。
//
// §5.2：默认 Core NATS；任何「丢了会导致资产不一致」的操作
// （跨分片转移、发奖、发信、充值到账）必须走 JetStream，
// 通过 job. 前缀在命名上强制体现。
const JobStreamName = "JOBS"

// JS 是 JetStream 上下文封装。
type JS struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	conn   *Conn
}

// NewJetStream 创建 JetStream 上下文并确保 JOBS stream 存在。
func (c *Conn) NewJetStream(ctx context.Context) (*JS, error) {
	js, err := jetstream.New(c.nc)
	if err != nil {
		return nil, fmt.Errorf("创建 JetStream 上下文失败: %w", err)
	}

	st, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     JobStreamName,
		Subjects: []string{subject.JobWildcard},
		// WorkQueue：消费成功即删除，天然适合「任务」语义。
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		Discard:   jetstream.DiscardNew, // 满了拒绝新消息而不是丢老消息 —— 资产类任务绝不能静默丢
		MaxAge:    7 * 24 * time.Hour,
		Replicas:  1, // 单机 compose；上集群时调整
		// 同一 txid 在 2 分钟内去重，配合接收方的 SETNX 幂等形成双保险。
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("创建/更新 JOBS stream 失败: %w", err)
	}

	logx.Info("JetStream 就绪", "stream", JobStreamName, "subjects", subject.JobWildcard)
	return &JS{js: js, stream: st, conn: c}, nil
}

// Raw 返回底层 jetstream.JetStream。
func (j *JS) Raw() jetstream.JetStream { return j.js }

// PublishJob 发布一个必达任务。msgID 用于 JetStream 层面的去重（通常传 txid）。
func (j *JS) PublishJob(ctx context.Context, subj string, cmd protocol.Cmd, uid uint64, traceID, msgID string, body proto.Message) error {
	env, err := j.conn.NewEnvelope(cmd, uid, traceID, body)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	if err := j.conn.checkPayload(subj, len(data)); err != nil {
		return err
	}

	opts := []jetstream.PublishOpt{jetstream.WithExpectStream(JobStreamName)}
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}
	ack, err := j.js.Publish(ctx, subj, data, opts...)
	if err != nil {
		return fmt.Errorf("发布 job 失败 (%s): %w", subj, err)
	}
	metrics.NATSMessages.WithLabelValues("out", subject.Prefix(subj)).Inc()
	logx.Trace(env.GetTraceId()).Debug("job 已入队",
		"subject", subj, "seq", ack.Sequence, "duplicate", ack.Duplicate)
	return nil
}

// JobMsg 是一条待处理的必达任务。
type JobMsg struct {
	Env     *pb.Envelope
	Subject string
	raw     jetstream.Msg
}

// Ack 确认处理完成，消息从 WorkQueue 中删除。
func (m *JobMsg) Ack() error { return m.raw.Ack() }

// Nak 要求延迟重投。用于「目标分片正在交接」等临时失败。
func (m *JobMsg) Nak(delay time.Duration) error { return m.raw.NakWithDelay(delay) }

// Term 终止投递，不再重试。仅用于永久性失败（消息本身非法）。
func (m *JobMsg) Term() error { return m.raw.Term() }

// Deliveries 返回本条消息已被投递的次数。
func (m *JobMsg) Deliveries() uint64 {
	md, err := m.raw.Metadata()
	if err != nil {
		return 0
	}
	return md.NumDelivered
}

// JobHandler 处理一条必达任务，需自行 Ack/Nak。
type JobHandler func(*JobMsg)

// ConsumerOptions 描述一个持久消费者。
type ConsumerOptions struct {
	Durable    string        // 持久名，多实例共用同一个 durable 即形成竞争消费
	Filter     string        // 过滤 subject，WorkQueue 下多个消费者的 filter 不能重叠
	AckWait    time.Duration // 未 ack 多久后重投
	MaxDeliver int           // 最大投递次数，超过进入 dead letter（这里靠日志 + tx 扫描器兜底）
	MaxAckPend int           // 最大在途未 ack 数，控制并发
}

// Consume 启动一个持久消费者。
//
// 多个 Lobby 实例使用同一 durable 时表现为竞争消费；任务被取到后由中转层
// 转发给目标分片 owner，因此不违反「持有数据的服务不用 queue group」——
// 竞争的是「谁来搬运」，而不是「谁持有数据」。
func (j *JS) Consume(ctx context.Context, opt ConsumerOptions, h JobHandler) (jetstream.ConsumeContext, error) {
	if opt.AckWait == 0 {
		opt.AckWait = 30 * time.Second
	}
	if opt.MaxDeliver == 0 {
		opt.MaxDeliver = 100
	}
	if opt.MaxAckPend == 0 {
		opt.MaxAckPend = 256
	}

	cons, err := j.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       opt.Durable,
		FilterSubject: opt.Filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       opt.AckWait,
		MaxDeliver:    opt.MaxDeliver,
		MaxAckPending: opt.MaxAckPend,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		BackOff: []time.Duration{
			time.Second, 2 * time.Second, 5 * time.Second,
			15 * time.Second, 30 * time.Second, time.Minute,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("创建消费者 %s 失败: %w", opt.Durable, err)
	}

	cc, err := cons.Consume(func(m jetstream.Msg) {
		metrics.NATSMessages.WithLabelValues("in", subject.Prefix(m.Subject())).Inc()
		env, derr := Decode(m.Data())
		if derr != nil {
			logx.Error("job 消息无法解析，终止投递", "subject", m.Subject(), "err", derr)
			_ = m.Term()
			return
		}
		h(&JobMsg{Env: env, Subject: m.Subject(), raw: m})
	})
	if err != nil {
		return nil, fmt.Errorf("启动消费 %s 失败: %w", opt.Durable, err)
	}

	logx.Info("JetStream 消费者已启动", "durable", opt.Durable, "filter", opt.Filter)
	return cc, nil
}

// ErrNoJetStream 表示服务端未开启 JetStream。
var ErrNoJetStream = errors.New("bus: 服务端未开启 JetStream（nats-server 需带 -js）")
