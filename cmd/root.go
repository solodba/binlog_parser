package cmd

import (
	_ "github.com/solodba/binlog_parser/apps/all"
	"github.com/solodba/binlog_parser/cmd/start"
	"github.com/solodba/binlog_parser/conf"
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

// 加载全局配置
func LoadConfigFromCmd() error {
	conf.Conf = conf.NewDefaultConfig()
	conf.Conf.MySQL.Username = start.Username
	conf.Conf.MySQL.Password = start.Password
	conf.Conf.MySQL.Host = start.Host
	conf.Conf.MySQL.Port = start.Port
	conf.Conf.MySQL.DB = "mysql"
	conf.Conf.MySQL.MaxOpenConn = 50
	conf.Conf.MySQL.MaxIdleConn = 10
	conf.Conf.MySQL.MaxLifeTime = 600
	conf.Conf.MySQL.MaxIdleTime = 300
	return nil
}

// 初始化函数
func Initial() {
	err := LoadConfigFromCmd()
	cobra.CheckErr(err)
	err = apps.InitInternalApps()
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
