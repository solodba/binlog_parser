package impl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"database/sql"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/solodba/binlog_parser/apps/parse"
)

var (
	StartTime string
	EndTime   string
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
		return fmt.Errorf("%s", "mysql数据库没有开启binlog!")
	}
	binlogRes, err := i.QueryBinLogFormat(ctx)
	if err != nil {
		return err
	}
	StartTime = i.c.CmdConf.StartTime
	EndTime = i.c.CmdConf.EndTime
	binlogParser := replication.NewBinlogParser()
	if binlogRes.Value == "STATEMENT" {
		err = binlogParser.ParseFile(i.c.CmdConf.BinLogName, 0, i.BinlogStatementEventHandler)
		if err != nil {
			return err
		}
	}
	if binlogRes.Value == "ROW" {
		err = binlogParser.ParseFile(i.c.CmdConf.BinLogName, 0, i.BinlogRowEventHandler)
		if err != nil {
			return err
		}
	}
	return nil
}

// binlog statement事件处理函数
func (i *impl) BinlogStatementEventHandler(be *replication.BinlogEvent) error {
	if be.Header.EventType == replication.QUERY_EVENT {
		ev, ok := be.Event.(*replication.QueryEvent)
		if !ok {
			return fmt.Errorf("%s", "数据类型断言失败!")
		}
		if StartTime != "" && EndTime == "" {
			startTime, err := StringToTime(StartTime)
			if err != nil {
				return err
			}
			if TimestampToTime(be.Header.Timestamp).After(startTime) {
				fmt.Println("========================================================")
				fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
				fmt.Printf("schema: %s\n", ev.Schema)
				fmt.Printf("sql: %s\n", string(ev.Query))
				fmt.Printf("execute_time: %d\n", ev.ExecutionTime)
			}
		}
		if StartTime == "" && EndTime != "" {
			endTime, err := StringToTime(EndTime)
			if err != nil {
				return err
			}
			if TimestampToTime(be.Header.Timestamp).Before(endTime) {
				fmt.Println("========================================================")
				fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
				fmt.Printf("schema: %s\n", ev.Schema)
				fmt.Printf("sql: %s\n", string(ev.Query))
				fmt.Printf("execute_time: %d\n", ev.ExecutionTime)
			}
		}
		if StartTime == "" && EndTime == "" {
			fmt.Println("========================================================")
			fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
			fmt.Printf("schema: %s\n", ev.Schema)
			fmt.Printf("sql: %s\n", string(ev.Query))
			fmt.Printf("execute_time: %d\n", ev.ExecutionTime)
		}
		if StartTime != "" && EndTime != "" {
			startTime, err := StringToTime(StartTime)
			if err != nil {
				return err
			}
			endTime, err := StringToTime(EndTime)
			if err != nil {
				return err
			}
			if TimestampToTime(be.Header.Timestamp).After(startTime) && TimestampToTime(be.Header.Timestamp).Before(endTime) {
				fmt.Println("========================================================")
				fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
				fmt.Printf("schema: %s\n", ev.Schema)
				fmt.Printf("sql: %s\n", string(ev.Query))
				fmt.Printf("execute_time: %d\n", ev.ExecutionTime)
			}
		}
	}
	return nil
}

// binlog row事件处理函数
func (i *impl) BinlogRowEventHandler(be *replication.BinlogEvent) error {
	switch ev := be.Event.(type) {
	case *replication.RowsEvent:
		if be.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
			fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
			insertSqlString, err := i.GenInsertSqlString(string(ev.Table.Schema), string(ev.Table.Table))
			if err != nil {
				return err
			}
			for _, row := range ev.Rows {
				fmt.Printf("sql: %s\n", fmt.Sprintf(insertSqlString, row))
			}
		}
		return nil
	default:
		return nil
	}
}

// 生成列拼接字符串
func (i *impl) GenColString(schemaName string, tableName string) (string, error) {
	colList := make([]string, 0)
	sqlText := fmt.Sprintf(`show columns from %s from %s`, tableName, schemaName)
	rows, err := i.db.QueryContext(context.Background(), sqlText)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var field, colType, isNull, key, isDefault, extra sql.NullString
		err = rows.Scan(&field, &colType, &isNull, &key, &isDefault, &extra)
		if err != nil {
			return "", err
		}
		if field.Valid {
			colList = append(colList, field.String)
		}
	}
	return strings.Join(colList, ","), nil
}

// 生成插入语句字符串
func (i *impl) GenInsertSqlString(schemaName string, tableName string) (string, error) {
	colString, err := i.GenColString(schemaName, tableName)
	if err != nil {
		return "", err
	}
	insertSqlString := fmt.Sprintf(`insert into %s.%s(%s) values(%v)`, schemaName, tableName, colString, "%s")
	return insertSqlString, nil
}

func TimestampToTime(timestamp uint32) time.Time {
	ts := int64(timestamp)
	return time.Unix(ts, 0)
}

func TimestampToString(timestamp uint32) string {
	ts := int64(timestamp)
	t := time.Unix(ts, 0)
	return t.Format("2006-01-02 15:04:05")
}

func StringToTime(t string) (time.Time, error) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.Time{}, err
	}
	return time.ParseInLocation("2006-01-02 15:04:05", t, location)
}
