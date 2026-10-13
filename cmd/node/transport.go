package main

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/xmapst/xdts/internal/node/controller"
	"github.com/xmapst/xdts/internal/transport"
)

func bootstrapReportingModules(amqpURL string) (*transport.Conn, *transport.Publisher, error) {
	conn, err := transport.NewConn(amqpURL, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("连接 AMQP 失败: %w", err)
	}

	dc, err := transport.NewDeclarator(conn)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("创建拓扑声明器失败: %w", err)
	}
	declareErr := dc.DeclareExchange(transport.ReportExchangeOptions())
	dc.Close()
	if declareErr != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("声明 %s 交换机失败: %w", transport.ExchangeReport, declareErr)
	}

	publisher, err := transport.NewPublisher(conn)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("创建发布者失败: %w", err)
	}
	return conn, publisher, nil
}

func bootstrapControlPlane(conn *transport.Conn, nodeID, centerCode string) error {
	dc, err := transport.NewDeclarator(conn)
	if err != nil {
		return fmt.Errorf("创建拓扑声明器失败: %w", err)
	}
	defer dc.Close()

	if err := dc.DeclareExchange(transport.ControlExchangeOptions()); err != nil {
		return fmt.Errorf("声明 %s 交换机失败: %w", transport.ExchangeControl, err)
	}
	if err := dc.DeclareExchange(transport.ReplyExchangeOptions()); err != nil {
		return fmt.Errorf("声明 %s 交换机失败: %w", transport.ExchangeReply, err)
	}
	if err := dc.DeclareNodeControlQueue(nodeID, centerCode, controller.DefaultControlQueueTTL); err != nil {
		return fmt.Errorf("声明节点控制队列失败: %w", err)
	}
	return nil
}

func amqpVhost(amqpURL string) (string, error) {
	u, err := url.Parse(amqpURL)
	if err != nil {
		return "", fmt.Errorf("解析 AMQP 连接串失败: %w", err)
	}
	vhost := strings.TrimPrefix(u.Path, "/")
	if vhost == "" {
		vhost = "/"
	}
	return vhost, nil
}

func amqpHost(amqpURL string) (string, error) {
	u, err := url.Parse(amqpURL)
	if err != nil {
		return "", fmt.Errorf("解析 AMQP 连接串失败: %w", err)
	}
	h := u.Hostname()
	if h == "" {
		return "", errors.New("AMQP 连接串缺少 host")
	}
	return h, nil
}
