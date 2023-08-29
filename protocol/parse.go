package protocol

import (
	"context"
	"fmt"

	"github.com/solodba/binlog_parser/apps/parse"
	"github.com/solodba/mcube/apps"
)

var (
	ctx = context.Background()
)

// binlog parse服务结构体
type ParseService struct {
	svc parse.Service
}

// binlog parse服务结构体构造函数
func NewParseService() *ParseService {
	return &ParseService{
		svc: apps.GetInternalApp(parse.AppName).(parse.Service),
	}
}

// binlog parse服务启动方法
func (s *ParseService) Start() error {
	binLogPos, err := s.svc.GetBinLogPosition(ctx)
	if err != nil {
		return err
	}
	fmt.Println(binLogPos)
	return nil
}

// GRPC服务停止方法
func (s *ParseService) Stop() error {
	return nil
}
