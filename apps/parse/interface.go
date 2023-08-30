package parse

import "context"

// 模块名称
const (
	AppName = "parse"
)

type Service interface {
	// 查询binlog mode
	QueryBinLogMode(context.Context) (*BinLogResponse, error)
	// 查询mysql server id
	QueryMysqlServerId(context.Context) (*BinLogResponse, error)
	// 判断binlog是否开启
	IsBinLog(context.Context) (*IsBinLogResponse, error)
	// 查询当前binlog记录模式
	QueryBinLogFormat(context.Context) (*BinLogResponse, error)
	// 获取需要解析的binlog路径
	GetBinLogPath(context.Context) (*BinLogPathResponse, error)
	// 获取所有binglog路径
	GetAllBinLogPath(context.Context) (*AllBinLogPathResponse, error)
	// 通过时间获取binlog position
	GetBinLogPosition(context.Context) (*BinLogPositionResponse, error)
	// 解析binlog日志
	ParseBinLog(context.Context)
}
