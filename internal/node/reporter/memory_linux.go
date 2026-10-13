//go:build linux

package reporter

import (
	"os"
	"strconv"
	"strings"
)

// memorySample 是一次内存采样的三个上报值：分子（RSSBytes）、分母（TotalBytes）
// 与百分比（Percent），对应 proto/report.proto 的 mem_rss_bytes / mem_total_bytes /
// mem_percent。三者都是指针，含义是"这一项采到了没有"，采不到就是 nil。
//
// # 为什么必须是一个结构体、出自一次采样
//
// 字段 5、6 存在的主要理由是让消费方**验算**百分比而不是重新推导（proto/report.proto
// mem_percent 注释）。如果分子分母各自单独采一次，两次读 /proc 之间进程 RSS 会变，
// 消费方算出来的 rss/total*100 与节点上报的 mem_percent 就会对不上——那时它没有
// 办法分辨"节点在说谎"与"两个数不同时刻"，而前者才是它做这项校验的目的。所以
// 三个值由一次采样同时产出，任何时刻它们都自洽。
type memorySample struct {
	// RSSBytes 进程常驻集字节数；nil 表示 /proc/self/statm 读不到或读不懂。
	RSSBytes *uint64
	// TotalBytes 本节点内存预算字节数；nil 表示两个来源都拿不到。
	TotalBytes *uint64
	// Percent 0~100 的百分比；nil 表示分子或分母缺一个。
	Percent *float64
}

// sampleMemory 采集 NodeHeartbeat.Resource 的三个内存项：进程常驻集、本节点
// 的内存预算，以及两者相除得到的百分比。任何一项采不到时该项为 nil。
//
// 心跳里既有的 mem_bytes 是 `MemStats.Sys`——Go 运行时向 OS 取得的地址空间
// 总量，不是进程实际占用。拿它做分子算出来的是"Go 运行时要过多少 ÷ 机器有
// 多少"，回答的是另一个问题；而 mem_bytes 是已发布字段、语义只增不改，事后
// 改不动。所以这里独立取 /proc/self/statm 的 resident 页数（× pagesize）。
//
// 有有限 cgroup 上限时取它，否则取 /proc/meminfo 的 MemTotal。顺序不能反：
// 容器里用宿主机总量会把分母放大、百分比显小，而那恰恰是 OOM 前最危险的方向
// （"看起来还很空"）。
func sampleMemory() memorySample {
	rss, rssOK := processRSSBytes()
	budget, budgetOK := memoryBudgetBytes()
	return memorySampleFrom(rss, rssOK, budget, budgetOK)
}

// memorySampleFrom 是 sampleMemory 的纯函数形态，与读 /proc 分开。
//
// 分母为字面 0 时不上报 TotalBytes：proto mem_total_bytes 明确"真正采不到时不设置
// 本字段，不要填 0"，而一个 0 字节的内存预算不是任何真实节点的状态；它只可能来自
// 解析出一个 0，把它发出去等于把一次解析失败伪装成一个数据点。
func memorySampleFrom(rss uint64, rssOK bool, budget uint64, budgetOK bool) memorySample {
	s := memorySample{Percent: memPercentFrom(rss, rssOK, budget, budgetOK)}
	if rssOK {
		s.RSSBytes = &rss
	}
	if budgetOK && budget > 0 {
		s.TotalBytes = &budget
	}
	return s
}

// memPercentFrom 算百分比：分子或分母缺一个就返回 nil。
//
// budget == 0 单独判：它来自"两个来源都拿不到"之外的另一种可能（解析出一个字面 0），
// 而 0 作分母会算出 +Inf——那是个既不是"没采到"也不是真实百分比的第三种东西。
//
// rss == 0 **不算采不到**：一个真的量到 0 的分子应当算出 0%，而"0% 可表达"正是这个
// 字段要 optional 的原因本身。把 rss==0 当缺失会把 optional 想保住的那一半自己弄丢。
func memPercentFrom(rss uint64, rssOK bool, budget uint64, budgetOK bool) *float64 {
	if !rssOK || !budgetOK || budget == 0 {
		return nil
	}
	pct := float64(rss) / float64(budget) * 100
	return &pct
}

// processRSSBytes 读 /proc/self/statm 的第二个字段（resident 页数）并换算成字节。
func processRSSBytes() (uint64, bool) {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	return rssBytesFrom(string(raw), os.Getpagesize())
}

// rssBytesFrom 把 /proc/self/statm 的内容换算成字节。
//
// statm 的字段单位是**页**，不是 KiB，所以必须乘页大小。pageSize 由调用方传入而不是
// 在这里读 os.Getpagesize()：本机页大小恰好是 4096，写死 4096 在这台机器上与正确实现
// **给出完全相同的结果**，因此任何真机断言都无法分辨两者（与 cgroup 固定路径"在开发机
// 上恰好答对"同一族的伪装）。传参之后可以用 16 KiB 页（ARM64 上常见）把这条钉住。
func rssBytesFrom(statm string, pageSize int) (uint64, bool) {
	if pageSize <= 0 {
		return 0, false
	}
	fields := strings.Fields(statm)
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * uint64(pageSize), true //nolint:gosec // pageSize 已判 >0
}

// memoryBudgetBytes 返回本节点可用内存总量：有有限 cgroup 上限取它，否则取宿主机
// MemTotal。两者都取不到时返回 false（调用方据此上报"未采到"）。
//
// 上限大于宿主机总量时按宿主机总量取（cgroup 允许写一个比物理内存更大的值，那种
// 上限不是真实约束，用它做分母同样会让百分比失真）。
func memoryBudgetBytes() (uint64, bool) {
	total, haveTotal := hostMemTotalBytes()
	limit, haveLimit := cgroupMemoryLimitBytes()
	return pickMemoryBudget(limit, haveLimit, total, haveTotal)
}

// pickMemoryBudget 是上面那个选择的纯函数形态（四种组合 + 取小）。
func pickMemoryBudget(limit uint64, haveLimit bool, total uint64, haveTotal bool) (uint64, bool) {
	switch {
	case haveLimit && haveTotal:
		if limit < total {
			return limit, true
		}
		return total, true
	case haveLimit:
		return limit, true
	case haveTotal:
		return total, true
	default:
		return 0, false
	}
}

// hostMemTotalBytes 解析 /proc/meminfo 的 MemTotal（单位 kB，内核固定用 kB 写，
// 不是 KiB 的笔误——内核源码里这一行就是 "%lu kB"，值本身是 KiB）。
func hostMemTotalBytes() (uint64, bool) {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	return parseMemTotal(string(raw))
}

// parseMemTotal 与读文件分开，是为了让解析本身能被穷举测试（畸形行、缺字段、
// 非数字、MemTotal 不在第一行），不必造一个假的 /proc。
func parseMemTotal(meminfo string) (uint64, bool) {
	for line := range strings.Lines(meminfo) {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

// cgroupMemoryLimitBytes 返回本进程所在 cgroup 的内存上限。**无限制也返回
// false**——"没有上限"与"读不到上限"对调用方是同一件事（都退回宿主机总量），
// 不需要第三种状态。
func cgroupMemoryLimitBytes() (uint64, bool) {
	hostTotal, haveHostTotal := hostMemTotalBytes()
	return cgroupLimitFromPaths(cgroupMemoryLimitPaths(), os.ReadFile, hostTotal, haveHostTotal)
}

// cgroupLimitFromPaths 按 paths 的顺序逐个尝试，返回第一个**能读懂**的结果。
//
// readFile 由调用方传入（生产上就是 os.ReadFile），为的是让"三跳"这件事可测：
// 三个候选路径都是绝对路径，测试无法替换它们的内容，而这个循环恰恰是整段逻辑里
// 最值得钉住的一条——见下面 malformed 的处理。
//
// 两种"读到了但不是有限上限"的结局在这里走**相反**的分支：
//   - 内容是合法的"无限制"（v2 的字面量 max、v1 的哨兵值）：**定案返回 false**，
//     不再试后面的候选。这台机器上的上限就是"没有上限"，继续找会把另一层 cgroup
//     的上限误当成本进程的约束。
//   - 内容读到了但不认识（malformed）：**继续试下一个候选**，而不是直接判定"无限制"。
//     后者会把一次解析失败变成一个看起来完全正常的分母（退回宿主机总量、百分比显小），
//     而那个方向正是 OOM 之前最危险的方向：不报警，只让人放心。
func cgroupLimitFromPaths(
	paths []string,
	readFile func(string) ([]byte, error),
	hostTotal uint64,
	haveHostTotal bool,
) (uint64, bool) {
	for _, path := range paths {
		raw, err := readFile(path)
		if err != nil {
			continue
		}
		limit, ok, malformed := parseCgroupMemoryLimit(string(raw), hostTotal, haveHostTotal)
		if malformed {
			continue
		}
		return limit, ok
	}
	return 0, false
}

// parseCgroupMemoryLimit 解析一份 memory.max / memory.limit_in_bytes 的内容。
//
// 三个返回值分别是：上限、是否存在有限上限、内容是否无法识别。把"无限制"与
// "解析失败"分开返回，是因为调用方对两者的处置不同（见 cgroupLimitFromPaths）。
//
// 与读文件分开的理由同 parseMemTotal：v1 的哨兵值、v2 的字面量 "max"、以及"上限
// 大于宿主机总量"这三条判断都值得穷举，而它们都不需要真的 cgroup 文件系统。
func parseCgroupMemoryLimit(content string, hostTotal uint64, haveHostTotal bool) (limit uint64, finite bool, malformed bool) {
	v := strings.TrimSpace(content)
	// v2 无限制写的是字面量 "max"。
	if v == "max" {
		return 0, false, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false, true
	}
	// v1 无限制写的是一个接近 uint64 上限的哨兵值（常见
	// 9223372036854771712 = PAGE_COUNTER_MAX << PAGE_SHIFT）。用"不小于宿主机
	// 总量"判定，而不是比对某个具体常量——那个常量随内核页大小变化。
	if haveHostTotal && n >= hostTotal {
		return 0, false, false
	}
	return n, true, false
}

// cgroupMemoryLimitPaths 给出三个候选路径，顺序即优先级（理由见 sampleMemory 文档）：
//  1. cgroup v2，按 /proc/self/cgroup 的相对路径定位；
//  2. cgroup v2 的固定路径（容器内 /proc/self/cgroup 常被 namespace 化成 "0::/"，
//     这一跳就是那种环境下的正解）；
//  3. cgroup v1 的 memory.limit_in_bytes。
func cgroupMemoryLimitPaths() []string {
	rel, ok := selfCgroupPath()
	return cgroupMemoryLimitPathsFrom(rel, ok)
}

// cgroupMemoryLimitPathsFrom 是路径构造的纯函数形态，让"namespace 化成 0::/ 时
// 第一跳消失、固定路径顶上来"这条能被直接断言，不必依赖本机恰好是哪种形态。
func cgroupMemoryLimitPathsFrom(selfRel string, haveSelfRel bool) []string {
	paths := make([]string, 0, 3)
	if haveSelfRel && selfRel != "/" {
		paths = append(paths, "/sys/fs/cgroup"+selfRel+"/memory.max")
	}
	paths = append(paths, "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes")
	return paths
}

// selfCgroupPath 从 /proc/self/cgroup 取 v2（"0::<path>"）那一行的路径部分。
func selfCgroupPath() (string, bool) {
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", false
	}
	return parseSelfCgroup(string(raw))
}

// parseSelfCgroup 解析 /proc/self/cgroup：v2 那一行形如 "0::<path>"，v1 的行形如
// "<id>:<controllers>:<path>"（本函数只认 v2 行，v1 的上限走固定路径那一跳）。
func parseSelfCgroup(content string) (string, bool) {
	for line := range strings.Lines(content) {
		line = strings.TrimSpace(line)
		if rel, ok := strings.CutPrefix(line, "0::"); ok && rel != "" {
			return rel, true
		}
	}
	return "", false
}
