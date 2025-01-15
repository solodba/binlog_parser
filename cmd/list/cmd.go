package list

import (
	"github.com/solodba/binlog_parser/protocol"
	"github.com/spf13/cobra"
)

// 项目启动子命令
var Cmd = &cobra.Command{
	Use:     "list",
	Short:   "binlog_parser service",
	Long:    "binlog_parser list service",
	Example: `./binlog_parser list -u root -p Root@123 -m 192.168.1.140 -P 3306`,
	RunE: func(cmd *cobra.Command, args []string) error {
		srv := NewServer()
		if err := srv.List(); err != nil {
			return err
		}
		return nil
	},
}

// 服务结构体
type Server struct {
	ListService *protocol.ListService
}

// 服务结构体初始化函数
func NewServer() *Server {
	return &Server{
		ListService: protocol.NewListService(),
	}
}

// 列出mysql所有binlog方法
func (s *Server) List() error {
	if err := s.ListService.List(); err != nil {
		return err
	}
	return nil
}
