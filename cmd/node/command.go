package main

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/xmapst/xdts/internal/buildinfo"
	"github.com/xmapst/xdts/internal/node/config"
	"github.com/xmapst/xdts/internal/node/resourcequota"
)

func newRootCommand() *cobra.Command {
	startedAt := time.Now()
	var configFile string

	cmd := &cobra.Command{
		Use:          "xdts-node",
		Short:        "xdts Node：多中心 MySQL 同步平台的分布式同步节点进程",
		SilenceUsage: true,
		Version:      buildinfo.String(),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return newApplication(config.LoadOptions{ConfigFile: configFile, Flags: cmd.Flags()}, startedAt).Run(cmd.Context())
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&configFile, "config", "", "配置文件路径（未指定时按当前目录 / /etc/xdts 查找 node.yaml）")
	flags.String("amqp.url", "", "本中心 RabbitMQ 地址")
	flags.String("node.data_dir", "", "节点状态目录")
	flags.String("node.register_code", "", "注册码及长期状态库口令（推荐环境变量注入）")
	flags.String("log.level", "info", "本地日志级别")
	flags.String("log.format", "json", "本地日志格式")
	flags.String("log.file", "", "本地日志文件")
	resources := resourcequota.DefaultLimits()
	flags.Uint64("resources.max_pipelines", resources.MaxPipelines, "节点最大运行流水线数（0不限）")
	flags.Uint64("resources.max_buffer_bytes", resources.MaxBufferBytes, "节点保留业务缓冲总上限（字节）")
	flags.Uint64("resources.pipeline_buffer_bytes", resources.PipelineBufferBytes, "每条流水线保留业务缓冲上限（字节）")
	flags.Uint64("resources.snapshot_concurrency", resources.SnapshotConcurrency, "节点全量读写SQL总并发上限")

	return cmd
}
