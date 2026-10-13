package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// NodeRuntime defines the persisted nodes.runtime JSON shape for ingest and HTTP.
// It is a snapshot of Node observations, not an independent configuration authority.
type NodeRuntime struct {
	// NodeVersion 是节点二进制版本，形如 "v1.2.3"。
	NodeVersion string `json:"node_version,omitempty"`

	// Resource 是整块资源快照；nil 表示未上报，标量零值表示已上报的实际值。
	Resource *NodeResourceUsage `json:"resource,omitempty"`

	// Pipelines 是该节点当前承载的线路。不要在本列里另存计数，理由见类型
	// 文档注释。
	//
	// 键不存在 / null → nil   节点尚未上报承载镜像（"不知道"）
	// 	[]              → 非 nil 空切片   上报了，承载 0 条
	// 	[{...},{...}]   → len==2          承载 2 条
	//
	// 读取侧必须把三态传下去：nil 渲染成 null，不是 0。视图层的计数字段
	// 因此要是 *int 而不是 int——否则"没有能力表达不知道"这个缺陷只是换了个
	// 位置重建一遍。
	Pipelines []NodeRuntimePipeline `json:"pipelines"`

	// LocalStore 是节点本地状态库的统计。指针 + omitempty，与写入侧一致。
	LocalStore *NodeLocalStoreStats `json:"local_store,omitempty"`

	// PipelinesKnownCount distinguishes a reported empty pipeline list from absent data.
	// When present, it must match the number of entries in Pipelines.
	PipelinesKnownCount *uint32 `json:"pipelines_known_count,omitempty"`

	// UptimeSeconds 是节点进程自身已运行的秒数（proto `NodeHeartbeat.uptime_seconds`，
	// 字段 7，`optional int64`）。指针 + omitempty，presence 必须一路带到磁盘。
	//
	// 单位是秒不是毫秒：下游读取契约已定在秒上，且进程运行时长没有毫秒精度的用途。
	// 语义选"已运行时长"而不是"启动时刻"——后者会让前端对着一个已经掉线的节点
	// 继续把时间累加上去，是个不会报错的错值。
	//
	// Manager 侧算不出它：Manager 只能算"我第一次见到这个节点至今"，那是另一个
	// 语义，且 Manager 一重启就归零。
	UptimeSeconds *int64 `json:"uptime_seconds,omitempty"`
}

// NodeResourceUsage 是节点进程的资源使用快照，对应 iproto 的 ResourceUsage。
//
// 不是风格不统一。CPUPercent/MemBytes/Goroutines 在 proto 里是裸标量、总是与
// 整个 resource 块一起写，缺失粒度是整块——而整块的缺失已经由
// `NodeRuntime.Resource` 这个指针表达了。后三个在 proto 里是 `optional`
// （字段 4/5/6），单个字段可以采不到而其余照常上报，presence 是它们各自携带
// 的信息，必须一路带到磁盘。
type NodeResourceUsage struct {
	CPUPercent float64 `json:"cpu_percent"`
	MemBytes   uint64  `json:"mem_bytes"`
	Goroutines uint32  `json:"goroutines"`

	// MemPercent 是内存占用百分比（0~100），定义为 MemRssBytes / MemTotalBytes × 100。
	// 由节点算，不由 Manager 算——分子分母都只有节点拿得到。
	MemPercent *float64 `json:"mem_percent,omitempty"`

	// MemRssBytes 是进程常驻集，即 MemPercent 的**分子**。
	//
	// **它与 MemBytes 不是同一个数，也不可互推**：MemBytes 是
	// `runtime.MemStats.Sys`——Go 运行时向 OS 取得的**地址空间总量**，含已归还、
	// 已 madvise、从未触碰的部分；MemRssBytes 是**实际驻留物理内存的字节数**。
	// 两者可以相差数倍，方向不固定。
	//
	// 这条必须写在这里而不只写在 proto 里，因为二者会在同一个界面上并排显示：
	// 只看到 "mem_bytes=2GB" 与 "mem_percent=3%" 的运维会去把它们对上，
	// **而它们本来就对不上**——分子根本不是同一个。
	MemRssBytes *uint64 `json:"mem_rss_bytes,omitempty"`

	// MemTotalBytes 是本节点的内存预算，即 MemPercent 的**分母**：有 cgroup 内存
	// 上限时取该上限，否则取宿主机 MemTotal。
	//
	// **顺序不能反**：容器里用宿主机总量会把分母放大、百分比显小，而那恰恰是
	// OOM 之前最危险的方向——界面显示"还很空"，不会报警，只会让人放心。
	//
	// "无上限"与"读不到上限"对消费方是同一件事（都退回宿主机总量），本层不区分；
	// 节点真正采不到时**不设置本字段**，不填 0。
	MemTotalBytes *uint64 `json:"mem_total_bytes,omitempty"`
}

// NodeRuntimePipeline 是节点承载的单条线路在心跳里的摘要。
type NodeRuntimePipeline struct {
	PipelineID    string `json:"pipeline_id"`
	ConfigVersion uint64 `json:"config_version"`

	// Role 存的是 `iproto.PipelineRole` 的 String() 形式，形如
	// "PIPELINE_ROLE_SOURCE"，**不是** model.Side 的 "source"/"target"。
	//
	// 这里刻意保持 string 而不 import iproto：`internal/manager/model` 是存储
	// 模型层，不该依赖传输层的生成代码。代价是**改 proto 里那个枚举的名字会
	// 悄悄改变已落库 JSON 的取值**——枚举名在这里是一条跨边界的已发布契约，
	// 改名要当作数据迁移处理，不是一次重命名。
	Role string `json:"role"`
}

// NodeLocalStoreStats 是节点本地状态库的统计。
type NodeLocalStoreStats struct {
	StoreSizeBytes       uint64 `json:"store_size_bytes"`
	LastCommitDurationMs int64  `json:"last_commit_duration_ms"`
}

func DecodeNodeRuntime(raw JSON) (*NodeRuntime, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var rt NodeRuntime
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&rt); err != nil {
		return nil, fmt.Errorf("model: 解析 node runtime 失败: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("model: 解析 node runtime 失败: trailing data")
		}
		return nil, fmt.Errorf("model: 解析 node runtime 失败: %w", err)
	}
	return &rt, nil
}

// EncodeNodeRuntime 将运行时信息编码为 nodes.runtime JSON。
// 通过副本将 nil Pipelines 规范化为 []，表示节点已上报且当前没有承载线路。
// 读取侧保留缺失、null 与 [] 的区别，以表达未上报和已上报为空。
func EncodeNodeRuntime(rt *NodeRuntime) (JSON, error) {
	if rt == nil {
		return nil, nil
	}
	if rt.Pipelines == nil {
		cp := *rt
		cp.Pipelines = []NodeRuntimePipeline{}
		rt = &cp
	}
	b, err := json.Marshal(rt)
	if err != nil {
		return nil, fmt.Errorf("model: 序列化 node runtime 失败: %w", err)
	}
	return JSON(b), nil
}
