//go:build !unix

package reporter

import "time"

// sample 在非 Unix 平台（本项目部署目标是 Linux，此分支只是保证编译期可移植性，
// 未做过验证）上没有零依赖的 getrusage 等价物，固定返回 0。goroutine 数与内存两项
// 不受影响，仍然是真实值。
func (c *cpuSampler) sample(now time.Time) float64 {
	c.have, c.lastWall = true, now
	return 0
}
