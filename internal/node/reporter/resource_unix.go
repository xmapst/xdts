//go:build unix

package reporter

import (
	"syscall"
	"time"
)

// sample 返回自上次采样以来的平均 CPU 占用率（百分比）：用两次 getrusage 之间的
// 进程 CPU 时间增量除以墙钟时间增量。首次调用（没有上一次采样基准）返回 0——
// 瞬时 CPU 占用率没有意义，必须跨两个采样点求差，这与 NODE-RPT-01 默认 5s 心跳周期
// 天然匹配（两次心跳之间的平均占用率）。只在心跳定时器（事件循环）里调用，
// 不需要加锁。
func (c *cpuSampler) sample(now time.Time) float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	cpu := time.Duration(ru.Utime.Nano()) + time.Duration(ru.Stime.Nano())

	prevHave, prevWall, prevCPU := c.have, c.lastWall, c.lastCPU
	c.have, c.lastWall, c.lastCPU = true, now, cpu

	if !prevHave {
		return 0
	}
	wallDelta := now.Sub(prevWall)
	if wallDelta <= 0 {
		return 0
	}
	cpuDelta := cpu - prevCPU
	if cpuDelta < 0 {
		return 0
	}
	return float64(cpuDelta) / float64(wallDelta) * 100
}
