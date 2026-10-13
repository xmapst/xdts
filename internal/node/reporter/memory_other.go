//go:build !linux

package reporter

// memorySample 见 memory_linux.go；非 Linux 上三项恒为 nil，本文件只需要这个类型
// 存在，字段含义与 Linux 版完全一致。
type memorySample struct {
	RSSBytes   *uint64
	TotalBytes *uint64
	Percent    *float64
}

// sampleMemory 在非 Linux 上恒返回三项全 nil 的空采样：分子（/proc/self/statm 的
// resident 页数）与分母（/proc/meminfo、cgroup）都是 Linux 特有接口，
// macOS/BSD/Windows 上都不存在。
//
// 与 internal/node/reporter/resource_other.go 的 cpuSampler.sample 是同一种平台分界（那边是 getrusage），
// 因此非 Linux 上心跳仍然正常发送，只是这三个字段缺省。
func sampleMemory() memorySample { return memorySample{} }
