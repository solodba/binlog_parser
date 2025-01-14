package impl

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
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

// 通过时间获取binlog position
func (i *impl) GetBinLogPosition(ctx context.Context) (*parse.BinLogPositionResponse, error) {
	cmd := fmt.Sprintf(`mysqlbinlog --read-from-remote-server -u%s -h%s -p"%s" -P%d --start-datetime="%s" "%s" -vv`,
		i.c.CmdConf.Username, i.c.CmdConf.Host, i.c.CmdConf.Password, i.c.CmdConf.Port, i.c.CmdConf.StartTime, i.c.CmdConf.BinLogName)
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

// 解析binlog日志
func (i *impl) ParseBinLog(ctx context.Context) {
	pos, err := i.GetBinLogPosition(ctx)
	if err != nil {
		logger.L().Error().Msgf(err.Error())
		return
	}
	res, err := i.QueryMysqlServerId(ctx)
	if err != nil {
		logger.L().Error().Msgf(err.Error())
		return
	}
	serverId, err := strconv.Atoi(res.Value)
	if err != nil {
		logger.L().Error().Msgf(err.Error())
		return
	}
	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(serverId),
		Flavor:   "mysql",
		User:     i.c.CmdConf.Username,
		Password: i.c.CmdConf.Password,
		Host:     i.c.CmdConf.Host,
		Port:     uint16(i.c.CmdConf.Port),
	}
	// binLogPath, err := i.GetBinLogPath(ctx)
	// if err != nil {
	// 	logger.L().Error().Msgf(err.Error())
	// 	return
	// }
	isBinLogRes, err := i.IsBinLog(ctx)
	if err != nil {
		logger.L().Error().Msgf(err.Error())
		return
	}
	if !isBinLogRes.On {
		logger.L().Error().Msgf("mysql没有开启binlog模式")
		return
	}
	binLogFormatRes, err := i.QueryBinLogFormat(ctx)
	if err != nil {
		logger.L().Error().Msgf(err.Error())
		return
	}
	logger.L().Info().Msgf("当前binlog记录模式为[%s]", binLogFormatRes.Value)
	allBinLogPathRes, err := i.GetAllBinLogPath(ctx)
	if err != nil {
		logger.L().Error().Msgf(err.Error())
		return
	}
	binlogPathSet := make([]string, 0)
	for _, item := range allBinLogPathRes.Items {
		binlogPathSet = append(binlogPathSet, item.BinLogPath)
	}
	logger.L().Info().Msgf("所有binLog路径:[%s]", strings.Join(binlogPathSet, `,`))
	logger.L().Info().Msgf("开始解析指定binlog")
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, err := syncer.StartSync(mysql.Position{
		Name: i.c.CmdConf.BinLogName,
		Pos:  uint32(pos.StartPos),
	})
	if err != nil {
		logger.L().Error().Msgf(err.Error())
		return
	}
	for {
		ev, err := streamer.GetEvent(ctx)
		if err == context.DeadlineExceeded {
			continue
		}
		ev.Dump(os.Stdout)
	}
}
