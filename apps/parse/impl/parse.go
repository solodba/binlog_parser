package impl

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/solodba/binlog_parser/apps/parse"
	"github.com/solodba/mcube/logger"
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
	sql := `show global variables like 'log_bin_basename'`
	row := i.db.QueryRowContext(ctx, sql)
	binLogRes := parse.NewBinLogResponse()
	err := row.Scan(&binLogRes.VariableName, &binLogRes.Value)
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
	defer rows.Close()
	if err != nil {
		return nil, err
	}
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

// 通过时间获取binlog position
func (i *impl) GetBinLogPosition(ctx context.Context) (*parse.BinLogPositionResponse, error) {
	// binLogPath, err := i.GetBinLogPath(ctx)
	// if err != nil {
	// 	return nil, err
	// }
	// binLogName := binLogPath.BinLogPath + `/` + i.c.CmdConf.BinLogName
	cmd := fmt.Sprintf(`mysqlbinlog --read-from-remote-server -u%s -h%s -p"%s" -P%d --start-datetime="%s" --stop-datetime="%s" "%s" -vv`,
		i.c.CmdConf.Username, i.c.CmdConf.Host, i.c.CmdConf.Password, i.c.CmdConf.Port, i.c.CmdConf.StartTime, i.c.CmdConf.EndTime, i.c.CmdConf.BinLogName)
	logger.L().Info().Msgf(cmd)
	outPutByte, err := exec.CommandContext(ctx, "/bin/sh", "-c", cmd).Output()
	if err != nil {
		return nil, err
	}
	binLogPosDateSet := parse.NewBinLogPosDateSet()
	binLogPosDateList := strings.Split(string(outPutByte), "#")
	for i, item := range binLogPosDateList {
		isMatch, err := regexp.MatchString(`at \d+`, item)
		if err != nil {
			return nil, err
		}
		if isMatch {
			r, err := regexp.Compile(`\d{6} \d{2}:\d{2}:\d{2}`)
			if err != nil {
				return nil, err
			}
			result := r.FindString(binLogPosDateList[i+1])
			binLogPosDate := parse.NewBinLogPosDate()
			binLogPosDate.Pos = strings.Trim(item, "\n")
			binLogPosDate.Date = result
			binLogPosDateSet.AddItems(binLogPosDate)
		}
	}
	binLogPosDateSet.Total = len(binLogPosDateSet.Items)
	return binLogPosDateSet.GetStartAndEndPos()
}
