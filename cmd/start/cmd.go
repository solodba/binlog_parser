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
	Example: `./binlog_parser start  -u root -p Root@123 -m 192.168.1.140 -P 3306 -s "2023-08-30 23:50:00" -f mysql-bin.000009`,
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
