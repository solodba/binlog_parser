package protocol

import (
	"fmt"

	"github.com/solodba/binlog_parser/apps/parse"
	"github.com/solodba/mcube/apps"
)

// list服务结构体
type ListService struct {
	listsvc parse.Service
}

// ListService服务结构体构造函数
func NewListService() *ListService {
	return &ListService{
		listsvc: apps.GetInternalApp(parse.AppName).(parse.Service),
	}
}

// 列出mysql数据库所有binlog
func (m *ListService) List() error {
	allBinLogResp, err := m.listsvc.GetAllBinLogPath(ctx)
	if err != nil {
		return err
	}
	if allBinLogResp.Total == 0 {
		return fmt.Errorf("%s", "mysql数据库没有找到binlog!")
	}
	fmt.Println("目前数据库所有binlog路径如下:")
	for _, item := range allBinLogResp.Items {
		fmt.Println(item.BinLogPath)
	}
	return nil
}
