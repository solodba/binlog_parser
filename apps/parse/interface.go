package parse

import "context"

// 模块名称
const (
	AppName = "parse"
)

type Service interface {
	// 查询binlog mode
	QueryBinLogMode(context.Context) (*BinLogModeResponse, error)
	// 判断binlog是否开启
	IsBinLog(context.Context) (*IsBinLogResponse, error)
	// 查询当前
}
