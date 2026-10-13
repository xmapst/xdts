package probe

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
)

// probeBroker 实现 PROBE_TYPE_BROKER：查询本中心 RabbitMQ Management API，汇总
// federation 积压与磁盘余量。任何一步失败都回落到 fail(...)，
// 不会不回执（硬约束 4）。
func (m *Module) probeBroker(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	fail := func(code, message string) *iproto.ProbeResult {
		return &iproto.ProbeResult{
			TaskId:  req.TaskId,
			Type:    req.Type,
			Success: false,
			Error:   &iproto.Error{Code: code, Message: message},
		}
	}

	conn := req.GetBrokerMgmt()
	if conn == nil || strings.TrimSpace(conn.GetUrl()) == "" {
		return fail("BROKER_MGMT_MISSING", "请求未提供 broker_mgmt 连接信息")
	}

	client, err := newMgmtClient(m.httpClient, conn, m.opts.Vhost, m.opts.BrokerHost)
	if err != nil {
		// newMgmtClient 的错误文案本身就不含地址/凭据明文（见 internal/node/probe/mgmtclient.go），
		// 可以安全地整体转发。
		return fail("BROKER_MGMT_URL_INVALID", err.Error())
	}

	queues, err := client.listQueues(ctx)
	if err != nil {
		slog.Error("list broker queues failed",
			slog.String("host", client.host()), slog.Any("error", err))
		return fail("BROKER_QUEUES_UNREACHABLE",
			fmt.Sprintf("查询队列列表失败，host=%s", client.host()))
	}
	nodes, err := client.listNodes(ctx)
	if err != nil {
		slog.Error("list broker nodes failed",
			slog.String("host", client.host()), slog.Any("error", err))
		return fail("BROKER_NODES_UNREACHABLE",
			fmt.Sprintf("查询节点信息失败，host=%s", client.host()))
	}
	// Newly declared queues can appear before Management statistics are ready.
	// Wait only for complete observations; missing fields never imply zero.
	statsCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	for !brokerCapacityFieldsReady(queues, nodes) && statsCtx.Err() == nil {
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-timer.C:
		case <-statsCtx.Done():
			timer.Stop()
		}
		if statsCtx.Err() != nil {
			break
		}
		observedQueues, queueErr := client.listQueues(statsCtx)
		observedNodes, nodeErr := client.listNodes(statsCtx)
		if queueErr != nil || nodeErr != nil {
			break
		}
		queues, nodes = observedQueues, observedNodes
	}

	br := &iproto.BrokerResult{FederationOk: true}
	br.CapacityVerified = len(nodes) > 0 && len(nodes) <= 100 && len(queues) <= 10000
	seen := map[string]bool{}
	for _, n := range nodes {
		if !n.CapacityKnown || n.Name == "" || seen[n.Name] || n.DiskFree < 0 || n.DiskFreeLimit <= 0 {
			br.CapacityVerified = false
			continue
		}
		seen[n.Name] = true
		br.Nodes = append(br.Nodes, &iproto.BrokerDiskCapacity{Node: n.Name, DiskFreeBytes: uint64(n.DiskFree), DiskFreeLimitBytes: uint64(n.DiskFreeLimit), DiskFreeAlarm: n.DiskFreeAlarm})
	}
	for _, q := range queues {
		if !q.BytesKnown || q.MessageBytes < 0 || uint64(q.MessageBytes) > math.MaxUint64-br.QueueBytes {
			br.CapacityVerified = false
			continue
		}
		br.QueueBytes += uint64(q.MessageBytes)
	}
	for _, q := range queues {
		if strings.HasPrefix(q.Name, "xdts.pl.") {
			br.DataQueues = append(br.DataQueues, &iproto.BrokerBacklog{Queue: q.Name, Messages: q.Messages, MessagesReady: q.MessagesReady, MessagesUnacked: q.MessagesUnacknowledged, MessageBytes: q.MessageBytes})
		}
		if !strings.HasPrefix(q.Name, federationQueuePrefix) {
			continue
		}
		br.Backlogs = append(br.Backlogs, &iproto.BrokerBacklog{
			Queue:           q.Name,
			Messages:        q.Messages,
			MessagesReady:   q.MessagesReady,
			MessagesUnacked: q.MessagesUnacknowledged,
			MessageBytes:    q.MessageBytes,
		})
		// federation 队列自身的 state 不是 running：可能正在重连/限流，federation
		// 链路不能算健康（当前 backlog 总数判断之外，额外补一层
		// "链路本身还活着"的信号）。
		if q.State != "" && q.State != "running" {
			br.FederationOk = false
			br.Warnings = append(br.Warnings,
				fmt.Sprintf("federation queue %s state=%s", q.Name, q.State))
		}
	}
	br.DataQueuesVerified = len(queues) <= 10000
	for _, q := range queues {
		if strings.HasPrefix(q.Name, "xdts.pl.") && !q.MessagesKnown {
			br.DataQueuesVerified = false
		}
	}

	switch len(nodes) {
	case 0:
		br.Warnings = append(br.Warnings, "management api 未返回任何节点信息")
	default:
		if len(nodes) > 1 {

			br.Warnings = append(br.Warnings,
				fmt.Sprintf("management api 返回 %d 个节点，取第一个", len(nodes)))
		}
		n := nodes[0]
		br.DiskFreeBytes = n.DiskFree
		if w := diskWarning(n); w != "" {
			br.Warnings = append(br.Warnings, w)
		}
	}

	return &iproto.ProbeResult{
		TaskId:  req.TaskId,
		Type:    req.Type,
		Success: true,
		Result:  &iproto.ProbeResult_Broker{Broker: br},
	}
}

func brokerCapacityFieldsReady(queues []mgmtQueue, nodes []mgmtNode) bool {
	if len(nodes) == 0 || len(nodes) > 100 || len(queues) > 10000 {
		return false
	}
	for _, q := range queues {
		if !q.BytesKnown {
			return false
		}
	}
	for _, n := range nodes {
		if !n.CapacityKnown {
			return false
		}
	}
	return true
}

// diskWarning 把 disk_free_alarm / disk_free 低于 disk_free_limit 折叠进一句 warning
// 文案。BrokerResult 的容量字段同时保留原始阈值与告警（见 internal/node/probe/doc.go 容量检查说明），
// 空字符串表示没有需要提醒的情况。
func diskWarning(n mgmtNode) string {
	switch {
	case n.DiskFreeAlarm:
		return fmt.Sprintf("disk free alarm 已触发：disk_free=%d disk_free_limit=%d", n.DiskFree, n.DiskFreeLimit)
	case n.DiskFreeLimit > 0 && n.DiskFree < n.DiskFreeLimit:
		return fmt.Sprintf("disk free 低于 limit：disk_free=%d disk_free_limit=%d", n.DiskFree, n.DiskFreeLimit)
	default:
		return ""
	}
}
