package cmd

import (
	_ "github.com/solodba/binlog_parser/apps/all"
	"github.com/solodba/binlog_parser/cmd/start"
	"github.com/solodba/mcube/apps"
	"github.com/solodba/mcube/logger"
	"github.com/solodba/mcube/version"
	"github.com/spf13/cobra"
)

// 全局参数
var (
	showVersion bool
)

// 根命令
var RootCmd = &cobra.Command{
	Use:     "binlog-parser [init|start]",
	Short:   "binlog-parser service",
	Long:    "binlog-parser service",
	Example: "binlog-parser -v",
	RunE: func(cmd *cobra.Command, args []string) error {
		if showVersion {
			logger.L().Info().Msgf(version.ShortVersion())
			return nil
		}
		return cmd.Help()
	},
}

// 初始化函数
func Initial() {
	err := apps.InitInternalApps()
	cobra.CheckErr(err)
}

// 执行函数
func Execute() {
	cobra.OnInitialize(Initial)
	RootCmd.AddCommand(start.Cmd)
	err := RootCmd.Execute()
	cobra.CheckErr(err)
}

// 初始化函数
func init() {
	RootCmd.PersistentFlags().BoolVarP(&showVersion, "version", "v", false, "show project mcenter version")
}
