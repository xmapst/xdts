package reporter

import (
	"runtime"

	"github.com/xmapst/xdts/internal/iproto"
)

func (m *Module) sampleResource() *iproto.ResourceUsage {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	cpuPct := m.cpu.sample(m.now())

	// 三个内存项出自同一次采样：mem_rss_bytes 与 mem_total_bytes 是 mem_percent 的
	// 分子与分母，消费方要能**验算**它们（proto/report.proto mem_percent 注释），各自单独
	// 采会让两次读 /proc 之间的 RSS 变化伪装成"节点在说谎"。
	mem := sampleMemory()

	return &iproto.ResourceUsage{
		CpuPercent: cpuPct,
		MemBytes:   memStats.Sys,
		Goroutines: uint32(runtime.NumGoroutine()), //nolint:gosec // NumGoroutine 在正常进程规模下远小于 uint32 上限
		// 这三项都是指针，sampleMemory 采不到的那一项就是 nil，这里**原样赋值、
		// 不做 0 兜底**。proto/report.proto 把它们做成 optional 的全部目的就是让"没采到"
		// 与"真的是 0"可分辨，在这里填 0 会在源头把那个区分销毁掉，而下游每一层
		// 都会正确地把这个 0 传到界面（见 internal/node/reporter/memory_linux.go sampleMemory 文档）。
		MemPercent:    mem.Percent,
		MemRssBytes:   mem.RSSBytes,
		MemTotalBytes: mem.TotalBytes,
	}
}

// 协议侧的两条理由（proto/report.proto uptime_seconds 字段注释）里，第二条决定了本函数
// 的形状：节点掉线后界面显示的时长会停在最后一次心跳，而"启动时刻"会让前端对着
// 一个已经死掉的节点继续把时间累加上去——那是个不会报错的错值。所以上报的是
// "我已经跑了多少秒"，计量发生在节点这一侧。
//
// 字段是 optional（*int64），为的是让"没上报"与"刚启动 0 秒"可分辨——后者是真实
// 存在的状态（进程起来后第一个心跳周期内 uptime 就是 0）。本函数总能给出一个值
// （启动时刻在 New 里已经保证非零，最坏情况退化为模块构造时刻），因此实际不会返回
// nil；保持指针返回是为了让"这个字段有 presence"这件事在类型上一眼可见，调用点
// 不需要知道本函数是否可能失败。
//
// 时钟用 m.now()（与心跳的 ts_ms 同一个），因此注入固定时钟的测试能确定地断言这个值。
// 负数被夹到 0：时钟回拨（NTP 校正）能让 now 早于启动时刻，而"运行了 -3 秒"对下游
// 是个必须处理的畸形值，夹到 0 是此处唯一诚实的答案——它与"刚启动"不可分辨，但那两
// 种情形对运维的处置完全相同。
func (m *Module) uptimeSeconds() *int64 {
	secs := max(int64(m.now().Sub(m.opts.ProcessStartedAt).Seconds()), 0)
	return &secs
}
