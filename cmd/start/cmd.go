package start

import (
	"github.com/solodba/binlog_parser/protocol"
	"github.com/spf13/cobra"
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
	ParseService *protocol.ParseService
}

// 服务结构体初始化函数
func NewServer() *Server {
	return &Server{
		ParseService: protocol.NewParseService(),
	}
}

// Server服务启动方法
func (s *Server) Start() error {
	if err := s.ParseService.Start(); err != nil {
		return err
	}
	return nil
}

// Server服务停止方法
func (s *Server) Stop() error {
	return nil
}
