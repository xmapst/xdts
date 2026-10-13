package main

import (
	"github.com/spf13/cobra"

	"github.com/xmapst/xdts/internal/buildinfo"
	"github.com/xmapst/xdts/internal/manager/config"
)

func newRootCommand() *cobra.Command {
	var configFile string

	cmd := &cobra.Command{
		Use:          "xdts-manager",
		Short:        "xdts Manager：多中心 MySQL 同步平台的集中管理进程",
		SilenceUsage: true,
		Version:      buildinfo.String(),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return newApplication(config.LoadOptions{ConfigFile: configFile, Flags: cmd.Flags()}).Run(cmd.Context())
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&configFile, "config", "", "配置文件路径（未指定时按当前目录 / /etc/xdts 查找 manager.yaml）")
	cmd.AddCommand(newMigrateCommand())

	return cmd
}

func newMigrateCommand() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use: "migrate", Short: "迁移管理库后退出，不启动 HTTP 或 AMQP", SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return newApplication(config.LoadOptions{ConfigFile: file, Flags: cmd.Flags()}).Migrate(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&file, "config", "", "Manager 配置文件路径")
	return cmd
}
