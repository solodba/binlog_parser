package impl

import (
	"context"
	"strings"

	"github.com/solodba/binlog_parser/apps/parse"
)

// 查询binlog mode
func (i *impl) QueryBinLogMode(ctx context.Context) (*parse.BinLogModeResponse, error) {
	sql := `show global variables like 'log_bin'`
	row := i.db.QueryRowContext(ctx, sql)
	res := parse.NewBinLogModeResponse()
	err := row.Scan(&res.VariableName, &res.Value)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// 判断binlog是否开启
func (i *impl) IsBinLog(ctx context.Context) (*parse.IsBinLogResponse, error) {
	binLogModeRes, err := i.QueryBinLogMode(ctx)
	if err != nil {
		return nil, err
	}
	isBinLogRes := parse.NewIsBinLogResponse()
	if binLogModeRes.VariableName == "log_bin" && strings.ToUpper(binLogModeRes.Value) == "ON" {
		isBinLogRes.On = true
	}
	return isBinLogRes, nil
}
