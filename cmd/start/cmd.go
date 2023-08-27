package start

import (
	"github.com/spf13/cobra"
)

// 全局参数
var (
	Username  string
	Password  string
	Host      string
	Port      int32
	StartTime string
	EndTime   string
)

// 项目启动子命令
var Cmd = &cobra.Command{
	Use:     "start",
	Short:   "binlog-parser start service",
	Long:    "binlog-parser service",
	Example: `binlog-parser start -s "2023-08-27 10:00:00"-e "2023-08-27 10:30:00"`,
	RunE: func(cmd *cobra.Command, args []string) error {
		srv := NewServer()
		if err := srv.Start(); err != nil {
			return err
		}
		return nil
	},
}

// 服务结构体
type Server struct {
}

// 服务结构体初始化函数
func NewServer() *Server {
	return &Server{}
}

// Server服务启动方法
func (s *Server) Start() error {
	return nil
}

// Server服务停止方法
func (s *Server) Stop() error {
	return nil
}

// 初始化函数
func init() {
	Cmd.PersistentFlags().StringVarP(&Username, "username", "u", "test", "connect mysql username")
	Cmd.PersistentFlags().StringVarP(&Password, "password", "p", "test", "connect mysql password")
	Cmd.PersistentFlags().StringVarP(&Host, "host", "m", "127.0.0.1", "mysql host ip")
	Cmd.PersistentFlags().Int32VarP(&Port, "port", "P", 3306, "mysql port")
	Cmd.PersistentFlags().StringVarP(&StartTime, "starttime", "s", "1970-01-01 00:00:00", "mysql binlog parse start time")
	Cmd.PersistentFlags().StringVarP(&EndTime, "endtime", "e", "1970-01-01 00:00:00", "mysql binlog parse end time")
}
