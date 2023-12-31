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
	Username    string
	Password    string
	Host        string
	Port        int32
	StartTime   string
	BinLogName  string
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
func LoadConfigFromCmd() {
	conf.Conf = conf.NewDefaultConfig()
	conf.Conf.MySQL.Username = Username
	conf.Conf.MySQL.Password = Password
	conf.Conf.MySQL.Host = Host
	conf.Conf.MySQL.Port = Port
	conf.Conf.MySQL.DB = "mysql"
	conf.Conf.MySQL.MaxOpenConn = 50
	conf.Conf.MySQL.MaxIdleConn = 10
	conf.Conf.MySQL.MaxLifeTime = 600
	conf.Conf.MySQL.MaxIdleTime = 300
	conf.Conf.CmdConf.Username = Username
	conf.Conf.CmdConf.Password = Password
	conf.Conf.CmdConf.Host = Host
	conf.Conf.CmdConf.Port = Port
	conf.Conf.CmdConf.StartTime = StartTime
	conf.Conf.CmdConf.BinLogName = BinLogName
}

// 初始化函数
func Initial() {
	LoadConfigFromCmd()
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
	RootCmd.PersistentFlags().BoolVarP(&showVersion, "version", "v", false, "show project binlog parse version")
	RootCmd.PersistentFlags().StringVarP(&Username, "username", "u", "test", "connect mysql username")
	RootCmd.PersistentFlags().StringVarP(&Password, "password", "p", "test", "connect mysql password")
	RootCmd.PersistentFlags().StringVarP(&Host, "host", "m", "127.0.0.1", "mysql host ip")
	RootCmd.PersistentFlags().Int32VarP(&Port, "port", "P", 3306, "mysql port")
	RootCmd.PersistentFlags().StringVarP(&StartTime, "starttime", "s", "1970-01-01 00:00:00", "mysql binlog parse start time")
	RootCmd.PersistentFlags().StringVarP(&BinLogName, "binlogname", "f", "xxx", "mysql binlog name")
}
