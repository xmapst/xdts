package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/config"
	"github.com/xmapst/xdts/internal/manager/dispatch"
	"github.com/xmapst/xdts/internal/manager/ingest"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/internal/transport"
)

const amqpRetryInterval = 10 * time.Second
const amqpPreconditionFailedRetryInterval = 2 * time.Minute

func amqpRetryPlan(err error) (retryInterval time.Duration, isPrecondition bool) {
	if transport.IsPreconditionFailed(err) {
		return amqpPreconditionFailedRetryInterval, true
	}
	return amqpRetryInterval, false
}

type amqpBootstrap struct {
	conn                          *transport.Conn
	reclaimQueues                 *transport.Declarator
	ingestSvc                     *ingest.Service
	reportConsumer, logConsumer   *transport.Consumer
	rpcClient                     *transport.RPCClient
	registerConsumer, rpcConsumer *transport.Consumer
	dispatchPub                   *transport.Publisher
}

// Stop consumers before their reply publisher and shared connection.
func (b *amqpBootstrap) close(timeout time.Duration) {
	if b.ingestSvc != nil {
		b.ingestSvc.Close(timeout)
	} else {
		if b.reportConsumer != nil {
			b.reportConsumer.CloseWithTimeout(timeout)
		}
		if b.logConsumer != nil {
			b.logConsumer.CloseWithTimeout(timeout)
		}
	}
	if b.registerConsumer != nil {
		b.registerConsumer.CloseWithTimeout(timeout)
	}
	if b.rpcConsumer != nil {
		b.rpcConsumer.CloseWithTimeout(timeout)
	}
	if b.dispatchPub != nil {
		b.dispatchPub.Close()
	}
	if b.rpcClient != nil {
		b.rpcClient.Close()
	}
	if b.reclaimQueues != nil {
		b.reclaimQueues.Close()
	}
	if b.conn != nil {
		if err := b.conn.Close(); err != nil {
			slog.Error("failed to close amqp connection", slog.Any("error", err))
		}
	}
}

func bootstrapAMQP(cfg config.AMQPConfig, db *gorm.DB, instanceID string, dispatcher *dispatch.Dispatcher) (boot *amqpBootstrap, err error) {
	boot = &amqpBootstrap{}
	defer func(resources *amqpBootstrap) {
		if err != nil {
			resources.close(0)
			boot = nil
		}
	}(boot)
	conn, err := transport.NewConn(cfg.URL, 0)
	if err != nil {
		return nil, fmt.Errorf("连接 AMQP 失败: %w", err)
	}
	boot.conn = conn

	topologyDeclarator, err := transport.NewDeclarator(conn)
	if err != nil {
		return nil, fmt.Errorf("创建拓扑声明器失败: %w", err)
	}
	declareErr := topologyDeclarator.DeclareManagerTopology(cfg.LogQueueMaxLength)
	topologyDeclarator.Close()
	if declareErr != nil {
		return nil, fmt.Errorf("声明 Manager 拓扑失败: %w", declareErr)
	}

	reclaimQueues, err := transport.NewDeclarator(conn)
	if err != nil {
		return nil, fmt.Errorf("创建回收循环用声明器失败: %w", err)
	}
	boot.reclaimQueues = reclaimQueues

	reportConsumer, err := transport.NewConsumer(conn, transport.ManagerReportQueue(), transport.WithConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("创建 report 消费者失败: %w", err)
	}
	boot.reportConsumer = reportConsumer
	logConsumer, err := transport.NewConsumer(conn, transport.ManagerLogQueue(), transport.WithConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("创建 log 消费者失败: %w", err)
	}
	boot.logConsumer = logConsumer

	svc, err := ingest.New(ingest.Options{DB: db, ReportConsumer: reportConsumer, LogConsumer: logConsumer})
	if err != nil {
		return nil, fmt.Errorf("构造 ingest 服务失败: %w", err)
	}
	boot.ingestSvc = svc
	if err := svc.Start(); err != nil {
		return nil, fmt.Errorf("启动 ingest 服务失败: %w", err)
	}

	rpcClient, err := transport.NewRPCClient(conn, instanceID,
		transport.WithRPCPerAttemptTimeout(task.DispatchBrokerProbePerAttemptTimeout),
		transport.WithRPCMaxAttempts(task.DispatchBrokerProbeMaxAttempts),
	)
	if err != nil {
		return nil, fmt.Errorf("创建巡检节点 RPC 客户端失败: %w", err)
	}
	boot.rpcClient = rpcClient

	registerConsumer, err := transport.NewConsumer(conn, transport.ManagerRegisterQueue(), transport.WithConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("创建 register 消费者失败: %w", err)
	}
	boot.registerConsumer = registerConsumer
	if err := registerConsumer.Run(dispatcher.HandleRegisterMessage); err != nil {
		return nil, fmt.Errorf("启动 register 消费者失败: %w", err)
	}

	rpcConsumer, err := transport.NewConsumer(conn, transport.ManagerRPCQueue(), transport.WithConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("创建 rpc 消费者失败: %w", err)
	}
	boot.rpcConsumer = rpcConsumer
	if err := rpcConsumer.Run(dispatcher.HandleRPCMessage); err != nil {
		return nil, fmt.Errorf("启动 rpc 消费者失败: %w", err)
	}

	dispatchPub, err := transport.NewPublisher(conn)
	if err != nil {
		return nil, fmt.Errorf("创建 dispatch 发布者失败: %w", err)
	}
	boot.dispatchPub = dispatchPub

	return boot, nil
}

func connectAMQPWithRetry(ctx context.Context, cfg config.AMQPConfig, db *gorm.DB, instanceID string, dispatcher *dispatch.Dispatcher) *amqpBootstrap {
	for {
		if ctx.Err() != nil {
			return nil
		}
		boot, err := bootstrapAMQP(cfg, db, instanceID, dispatcher)
		if err == nil {
			slog.Info("amqp wiring complete")
			return boot
		}

		retryInterval, isPrecondition := amqpRetryPlan(err)
		if isPrecondition {
			slog.Error("amqp wiring failed: queue arguments conflict (406 precondition_failed)",
				slog.Any("error", err),
				slog.String("likely_cause", "amqp.log_queue_max_length 与 broker 上 xdts.manager.log 队列已声明的 x-max-length 不一致（AMQP 队列参数不可变，配置项改过但队列没有跟着重建）"),
				slog.String("remediation", "确认无误后由运维手动删除 xdts.manager.log 队列（会丢弃队列中尚未消费的消息，这是必须由人决定的代价，不能自动执行），删除后 manager 会在下次重试时用当前配置重新声明成功"))
		} else {
			slog.Error("amqp wiring failed, retrying",
				slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryInterval):
		}
	}
}
