package parse

import (
	"github.com/solodba/binlog_parser/protocol"
	"github.com/spf13/cobra"
)

// 项目启动子命令
var Cmd = &cobra.Command{
	Use:     "parse",
	Short:   "binlog-parser service",
	Long:    "binlog-parser parse service",
	Example: `./binlog_parser parse -u root -p Root@123 -m 192.168.1.140 -P 3306 -s "2023-08-30 23:50:00" -f mysql-bin.000009`,
	RunE: func(cmd *cobra.Command, args []string) error {
		srv := NewServer()
		if err := srv.Parse(); err != nil {
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
func (s *Server) Parse() error {
	if err := s.ParseService.Start(); err != nil {
		return err
	}
	return nil
}
