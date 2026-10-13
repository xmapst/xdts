// Package position 实现节点侧的起始位点换算：
// 将最新、显式 GTID、文件偏移、时间点换算或校验为 binlog_in 可用的 GTID 集合。
// 时间点按文件首事件时间近似定位，再扫描 immediate commit 时间；非单调文件时间
// 回退到保留范围首文件。扫描固定截止位置，限 256 文件/512 MiB 且要求 deadline。
// 返回符合时间点的第一个事务之前的集合、文件偏移和提交时间，由 Manager 保存确认。
//
// binlogin 消费本包的产出：resolveStartGTIDSet 的既有注释"NODE-POS-04：调用方已
// 完成文件+偏移/时间点换算"正是描述这层分工——binlogin.Options.StartGTIDSet 应该
// 是本包某个 Resolve* 函数的输出，本包不反过来依赖 binlogin。
package position
