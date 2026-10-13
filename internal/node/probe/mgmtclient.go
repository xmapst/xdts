package probe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
)

// RabbitMQ Management API 用于获取队列积压和 Broker 磁盘指标。
const federationQueuePrefix = "federation: "

type mgmtQueue struct {
	Name                   string `json:"name"`
	Messages               int64  `json:"messages"`
	MessagesReady          int64  `json:"messages_ready"`
	MessagesUnacknowledged int64  `json:"messages_unacknowledged"`
	MessageBytes           int64  `json:"message_bytes"`
	State                  string `json:"state"`
	BytesKnown             bool   `json:"-"`
	MessagesKnown          bool   `json:"-"`
}

type mgmtNode struct {
	Name          string `json:"name"`
	DiskFree      int64  `json:"disk_free"`
	DiskFreeLimit int64  `json:"disk_free_limit"`
	DiskFreeAlarm bool   `json:"disk_free_alarm"`
	CapacityKnown bool   `json:"-"`
}

func (q *mgmtQueue) UnmarshalJSON(data []byte) error {
	type wire mgmtQueue
	var parsed wire
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	var fields struct {
		Bytes    *int64 `json:"message_bytes"`
		Messages *int64 `json:"messages"`
		Ready    *int64 `json:"messages_ready"`
		Unacked  *int64 `json:"messages_unacknowledged"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*q = mgmtQueue(parsed)
	q.BytesKnown = fields.Bytes != nil
	q.MessagesKnown = fields.Messages != nil && fields.Ready != nil && fields.Unacked != nil && q.Messages >= 0 && q.MessagesReady >= 0 && q.MessagesUnacknowledged >= 0 && q.Messages >= q.MessagesReady && q.Messages-q.MessagesReady == q.MessagesUnacknowledged
	return nil
}

func (n *mgmtNode) UnmarshalJSON(data []byte) error {
	type wire mgmtNode
	var parsed wire
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	var fields struct {
		Free  *int64 `json:"disk_free"`
		Limit *int64 `json:"disk_free_limit"`
		Alarm *bool  `json:"disk_free_alarm"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*n = mgmtNode(parsed)
	n.CapacityKnown = fields.Free != nil && fields.Limit != nil && fields.Alarm != nil
	return nil
}

// mgmtClient 是 RabbitMQ Management API 的最小只读客户端：只实现 probeBroker 需要的
// 两个列表端点，不是通用 SDK，不追求覆盖 Management API 的其它能力。
type mgmtClient struct {
	httpClient *http.Client
	baseURL    *url.URL // 已清空 User，不携带明文口令（硬约束 1）
	username   string
	password   string
	vhost      string
}

// newMgmtClient 解析 conn.Url 并立即清空其中可能携带的 userinfo——即便调用方没有按
// 约定把凭据分别放进 Username/Password 两个字段、而是错误地塞进了 URL 里，也不会让
// 口令经由 baseURL、请求 URL 或后续任何错误文案泄露出去。解析失败时返回一个不包含
// 原始输入的固定文案（不用 %w 包裹 url.Parse 的原始错误）——url.Parse 的错误信息会
// 原样回显传入的字符串，如果调用方确实把凭据错误地塞进了 URL，这里包裹原始错误就是
// 硬约束 1 要防的那个泄露口子本身。
// assertSameBroker 断言 Management API 的主机名与本节点已连的 broker 主机名一致。
//
// 错误文案**不回显任何一方的原始输入**：下发来的 URL 可能被错误地塞进了凭据
// （newMgmtClient 的文档已说明这条口子），而本函数的错误会经 probeBroker 原样
// 转发进回执 ⇒ 回显等于把那条口子重新打开。两个主机名都只以"是否相同"出现。
func assertSameBroker(mgmtURL *url.URL, brokerHost string) error {
	want := strings.ToLower(strings.TrimSpace(brokerHost))
	if want == "" {
		return errors.New("probe: 未提供本节点 broker 主机名，无法校验 Management API 归属 ⇒ 拒绝")
	}
	got := strings.ToLower(mgmtURL.Hostname())
	if got != want {
		return errors.New("probe: Management API 主机与本节点已连的 broker 不是同一台，拒绝访问")
	}
	return nil
}

// 硬约束「node 只能连业务库和 amqp」判在 amqp 侧（Management API 与 AMQP 端口在
//
//	同一台 broker 上、凭据由 Manager 同源下发 ⇒ 没有引入新的对端），而"同一台"
//	这件事在此之前只由"Manager 会正确下发"这条约定保证，约定不是结构。
//	⇒ 在消费侧断言：下发来的 Management API 主机名必须与本节点已连的 broker 相同，
//	  不同即拒绝。这样即使下发逻辑出错或被篡改，node 也不会去连第三个对端。
//
// 只比主机名不比端口：Management API 与 AMQP 本来就是同一台机器上的两个端口
// （15672 / 5672），连端口一起比会把所有正常配置全部拒掉。
// brokerHost 为空时拒绝而不是放行（fail closed）：空值意味着调用方没有提供
// 比对依据，此时"放行"等于这道断言不存在，而它不存在这件事不会出现在任何读数里。
func newMgmtClient(httpClient *http.Client, conn *iproto.ManagementAPIConn, vhost, brokerHost string) (*mgmtClient, error) {
	u, err := url.Parse(strings.TrimSpace(conn.GetUrl()))
	if err != nil {
		return nil, errors.New("probe: 解析 Management API 地址失败")
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New("probe: Management API 地址缺少 scheme 或 host")
	}
	if err := assertSameBroker(u, brokerHost); err != nil {
		return nil, err
	}
	u.User = nil
	return &mgmtClient{
		httpClient: httpClient,
		baseURL:    u,
		username:   conn.GetUsername(),
		password:   conn.GetPassword(),
		vhost:      vhost,
	}, nil
}

// host 只暴露 scheme+host，供日志与失败文案使用（硬约束 1）：baseURL 已经清空了
// User，但习惯上仍然只暴露到这一级，不多给一个字节。
func (c *mgmtClient) host() string {
	return c.baseURL.Host
}

// get 发起一次 GET 请求并把响应体解码进 out。ctx 的超时与 m.httpClient 自身的
// Timeout 字段是两层独立的超时保护（硬约束 3）：前者由调用方按整体请求预算收窄，
// 后者是每个 mgmtClient 共享的兜底上限。
func (c *mgmtClient) get(ctx context.Context, path string, out any) error {
	u := *c.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + path

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("probe: 构造 Management API 请求失败: %w", err)
	}
	httpReq.SetBasicAuth(c.username, c.password)
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		// err 可能是 *url.Error，其 Error() 会带上请求 URL——baseURL 在
		// newMgmtClient 里已经清空 User，因此这里不会泄露口令。
		return fmt.Errorf("probe: 请求 Management API 失败: %w", err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(resp.Body.Close)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("probe: Management API 返回非 200 状态码 %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("probe: 解析 Management API 响应失败: %w", err)
	}
	return nil
}

// listQueues 对应 GET /api/queues/{vhost}（列出该 vhost 下全部队列，调用方自己按
// federationQueuePrefix 过滤出 federation 中转队列，见 internal/node/probe/broker.go）。
func (c *mgmtClient) listQueues(ctx context.Context) ([]mgmtQueue, error) {
	var out []mgmtQueue
	if err := c.get(ctx, "/api/queues/"+url.PathEscape(c.vhost), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// listNodes 对应 GET /api/nodes（列出 Broker 集群的全部节点；本地 dev 栈每个中心都是
// 单节点部署，实测恒返回长度为 1 的数组，见 internal/node/probe/broker.go 对长度 != 1 的防御性处理）。
func (c *mgmtClient) listNodes(ctx context.Context) ([]mgmtNode, error) {
	var out []mgmtNode
	if err := c.get(ctx, "/api/nodes", &out); err != nil {
		return nil, err
	}
	return out, nil
}
