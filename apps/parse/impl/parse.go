package impl

import (
	"context"
	"fmt"
	"strings"

	"github.com/solodba/binlog_parser/apps/parse"
)

// 查询binlog mode
func (i *impl) QueryBinLogMode(ctx context.Context) (*parse.BinLogResponse, error) {
	sql := `show global variables like 'log_bin'`
	row := i.db.QueryRowContext(ctx, sql)
	res := parse.NewBinLogResponse()
	err := row.Scan(&res.VariableName, &res.Value)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// 查询mysql server id
func (i *impl) QueryMysqlServerId(ctx context.Context) (*parse.BinLogResponse, error) {
	sql := `show global variables like 'server_id'`
	row := i.db.QueryRowContext(ctx, sql)
	res := parse.NewBinLogResponse()
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

// 查询当前binlog记录模式
func (i *impl) QueryBinLogFormat(ctx context.Context) (*parse.BinLogResponse, error) {
	sql := `show global variables like 'binlog_format'`
	row := i.db.QueryRowContext(ctx, sql)
	res := parse.NewBinLogResponse()
	err := row.Scan(&res.VariableName, &res.Value)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// 获取需要解析的binlog路径
func (i *impl) GetBinLogPath(ctx context.Context) (*parse.BinLogPathResponse, error) {
	isBinlogRes, err := i.IsBinLog(ctx)
	if err != nil {
		return nil, err
	}
	if !isBinlogRes.On {
		return nil, fmt.Errorf("%s", "mysql数据库没有开启binlog!")
	}
	sql := `show global variables like 'log_bin_basename'`
	row := i.db.QueryRowContext(ctx, sql)
	binLogRes := parse.NewBinLogResponse()
	err = row.Scan(&binLogRes.VariableName, &binLogRes.Value)
	if err != nil {
		return nil, err
	}
	pathList := strings.Split(binLogRes.Value, `/`)
	baseDir := strings.Join(pathList[0:len(pathList)-1], `/`)
	binLogPathRes := parse.NewBinLogPathResponse(baseDir)
	return binLogPathRes, nil
}

// 获取所有binglog路径
func (i *impl) GetAllBinLogPath(ctx context.Context) (*parse.AllBinLogPathResponse, error) {
	binLogPathRes, err := i.GetBinLogPath(ctx)
	if err != nil {
		return nil, err
	}
	sql := `show binary logs`
	allBinLogPath := parse.NewAllBinLogPathResponse()
	rows, err := i.db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var logName, fileSize, encrypted string
	for rows.Next() {
		err = rows.Scan(&logName, &fileSize, &encrypted)
		if err != nil {
			return nil, err
		}
		binLogPath := parse.NewBinLogPathResponse(binLogPathRes.BinLogPath + `/` + logName)
		allBinLogPath.AddItems(binLogPath)
	}
	allBinLogPath.Total = len(allBinLogPath.Items)
	return allBinLogPath, nil
}

// 解析binlog日志
func (i *impl) ParseBinLog(ctx context.Context) error {
	isBinlogRes, err := i.IsBinLog(ctx)
	if err != nil {
		return err
	}
	if !isBinlogRes.On {
		return fmt.Errorf("%s", "数据库没有开启binlog!")
	}

	return nil
}
