package parse

import (
	"context"

	"github.com/go-mysql-org/go-mysql/replication"
)

// 模块名称
const (
	AppName = "parse"
)

type Service interface {
	// 查询mysql版本
	QueryMysqlVersion(context.Context) (string, error)
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
	// 生成列切片
	GenColList(string, string) ([]string, error)
	// 生成插入语句字符串
	GenInsertSqlString(string, string, []byte) (string, error)
	// 生成删除语句字符串
	GenDeleteSqlString(string, string, []byte) (string, error)
	// 生成更新语句字符串
	GenUpdateSqlString1(string, string, []byte) (string, error)
	GenUpdateSqlString2(string, string, []byte) (string, error)
	// binlog statement事件处理函数
	BinlogStatementEventHandler(*replication.BinlogEvent) error
	// binlog row事件处理函数
	BinlogRowEventHandler(*replication.BinlogEvent) error
	// 解析binlog日志
	ParseBinLog(context.Context) error
}
