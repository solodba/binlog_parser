package impl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"database/sql"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/solodba/binlog_parser/apps/parse"
)

var (
	StartTime string
	EndTime   string
)

// 查询mysql版本
func (i *impl) QueryMysqlVersion(ctx context.Context) (string, error) {
	sql := `select version()`
	row := i.db.QueryRowContext(ctx, sql)
	version := ""
	err := row.Scan(&version)
	if err != nil {
		return "", err
	}
	return version, nil
}

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
	version, err := i.QueryMysqlVersion(ctx)
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
	if strings.Split(version, ".")[0] == "8" {
		for rows.Next() {
			err = rows.Scan(&logName, &fileSize, &encrypted)
			if err != nil {
				return nil, err
			}
			binLogPath := parse.NewBinLogPathResponse(binLogPathRes.BinLogPath + `/` + logName)
			allBinLogPath.AddItems(binLogPath)
		}
	}
	if strings.Split(version, ".")[0] == "5" {
		for rows.Next() {
			err = rows.Scan(&logName, &fileSize)
			if err != nil {
				return nil, err
			}
			binLogPath := parse.NewBinLogPathResponse(binLogPathRes.BinLogPath + `/` + logName)
			allBinLogPath.AddItems(binLogPath)
		}
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
			if StartTime != "" && EndTime == "" {
				startTime, err := StringToTime(StartTime)
				if err != nil {
					return err
				}
				if TimestampToTime(be.Header.Timestamp).After(startTime) {
					fmt.Println("========================================================")
					fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
					insertSqlString, err := i.GenInsertSqlString(string(ev.Table.Schema), string(ev.Table.Table), ev.Table.ColumnType)
					if err != nil {
						return err
					}
					for _, row := range ev.Rows {
						sql := fmt.Sprintf(insertSqlString, row...)
						if strings.Split(sql, " ")[0] == "insert" {
							sql = strings.ReplaceAll(sql, "'%!s(<nil>)'", "null")
							sql = strings.ReplaceAll(sql, "%!d(<nil>)", "null")
						}
						fmt.Println(sql)
					}
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
					insertSqlString, err := i.GenInsertSqlString(string(ev.Table.Schema), string(ev.Table.Table), ev.Table.ColumnType)
					if err != nil {
						return err
					}
					for _, row := range ev.Rows {
						sql := fmt.Sprintf(insertSqlString, row...)
						if strings.Split(sql, " ")[0] == "insert" {
							sql = strings.ReplaceAll(sql, "'%!s(<nil>)'", "null")
							sql = strings.ReplaceAll(sql, "%!d(<nil>)", "null")
						}
						fmt.Println(sql)
					}
				}
			}
			if StartTime == "" && EndTime == "" {
				fmt.Println("========================================================")
				fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
				insertSqlString, err := i.GenInsertSqlString(string(ev.Table.Schema), string(ev.Table.Table), ev.Table.ColumnType)
				if err != nil {
					return err
				}
				for _, row := range ev.Rows {
					sql := fmt.Sprintf(insertSqlString, row...)
					if strings.Split(sql, " ")[0] == "insert" {
						sql = strings.ReplaceAll(sql, "'%!s(<nil>)'", "null")
						sql = strings.ReplaceAll(sql, "%!d(<nil>)", "null")
					}
					fmt.Println(sql)
				}
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
					insertSqlString, err := i.GenInsertSqlString(string(ev.Table.Schema), string(ev.Table.Table), ev.Table.ColumnType)
					if err != nil {
						return err
					}
					for _, row := range ev.Rows {
						sql := fmt.Sprintf(insertSqlString, row...)
						if strings.Split(sql, " ")[0] == "insert" {
							sql = strings.ReplaceAll(sql, "'%!s(<nil>)'", "null")
							sql = strings.ReplaceAll(sql, "%!d(<nil>)", "null")
						}
						fmt.Println(sql)
					}
				}
			}
		}
		if be.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
			if StartTime != "" && EndTime == "" {
				startTime, err := StringToTime(StartTime)
				if err != nil {
					return err
				}
				if TimestampToTime(be.Header.Timestamp).After(startTime) {
					fmt.Println("========================================================")
					fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
					deleteSqlString, err := i.GenDeleteSqlString(string(ev.Table.Schema), string(ev.Table.Table), ev.Table.ColumnType)
					if err != nil {
						return err
					}
					for _, row := range ev.Rows {
						sql := fmt.Sprintf(deleteSqlString, row...)
						if strings.Split(sql, " ")[0] == "delete" {
							sql = strings.ReplaceAll(sql, "='%!s(<nil>)'", " is null")
							sql = strings.ReplaceAll(sql, "=%!d(<nil>)", " is null")
						}
						fmt.Println(sql)
					}
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
					deleteSqlString, err := i.GenDeleteSqlString(string(ev.Table.Schema), string(ev.Table.Table), ev.Table.ColumnType)
					if err != nil {
						return err
					}
					for _, row := range ev.Rows {
						sql := fmt.Sprintf(deleteSqlString, row...)
						if strings.Split(sql, " ")[0] == "delete" {
							sql = strings.ReplaceAll(sql, "='%!s(<nil>)'", " is null")
							sql = strings.ReplaceAll(sql, "=%!d(<nil>)", " is null")
						}
						fmt.Println(sql)
					}
				}
			}
			if StartTime == "" && EndTime == "" {
				fmt.Println("========================================================")
				fmt.Printf("timestamp: %s\n", TimestampToString(be.Header.Timestamp))
				deleteSqlString, err := i.GenDeleteSqlString(string(ev.Table.Schema), string(ev.Table.Table), ev.Table.ColumnType)
				if err != nil {
					return err
				}
				for _, row := range ev.Rows {
					sql := fmt.Sprintf(deleteSqlString, row...)
					if strings.Split(sql, " ")[0] == "delete" {
						sql = strings.ReplaceAll(sql, "='%!s(<nil>)'", " is null")
						sql = strings.ReplaceAll(sql, "=%!d(<nil>)", " is null")
					}
					fmt.Println(sql)
				}
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
					deleteSqlString, err := i.GenDeleteSqlString(string(ev.Table.Schema), string(ev.Table.Table), ev.Table.ColumnType)
					if err != nil {
						return err
					}
					for _, row := range ev.Rows {
						sql := fmt.Sprintf(deleteSqlString, row...)
						if strings.Split(sql, " ")[0] == "delete" {
							sql = strings.ReplaceAll(sql, "='%!s(<nil>)'", " is null")
							sql = strings.ReplaceAll(sql, "=%!d(<nil>)", " is null")
						}
						fmt.Println(sql)
					}
				}
			}
		}
		return nil
	default:
		return nil
	}
}

// 生成列拼接字符串
func (i *impl) GenColList(schemaName string, tableName string) ([]string, error) {
	colList := make([]string, 0)
	sqlText := fmt.Sprintf(`show columns from %s from %s`, tableName, schemaName)
	rows, err := i.db.QueryContext(context.Background(), sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var field, colType, isNull, key, isDefault, extra sql.NullString
		err = rows.Scan(&field, &colType, &isNull, &key, &isDefault, &extra)
		if err != nil {
			return nil, err
		}
		if field.Valid {
			colList = append(colList, field.String)
		}
	}
	return colList, nil
}

// 生成插入语句字符串
func (i *impl) GenInsertSqlString(schemaName string, tableName string, colTypeList []byte) (string, error) {
	colList, err := i.GenColList(schemaName, tableName)
	if err != nil {
		return "", err
	}
	if len(colList) == 0 {
		return "", fmt.Errorf("%s.%s表的列数为0", schemaName, tableName)
	}
	if len(colList) != len(colTypeList) {
		return "", fmt.Errorf("%s.%s表的列数和值的个数不匹配", schemaName, tableName)
	}
	insertSqlString := fmt.Sprintf(`insert into %s.%s(%s) values(`, schemaName, tableName, strings.Join(colList, ","))
	for _, item := range colTypeList {
		switch item {
		case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_LONG:
			insertSqlString = insertSqlString + "%d,"
		case mysql.MYSQL_TYPE_VARCHAR:
			insertSqlString = insertSqlString + "'%s',"
		case mysql.MYSQL_TYPE_NULL:
			insertSqlString = insertSqlString + "%s,"
		default:
			return "", fmt.Errorf("mysql不支持该数据类型")
		}
	}
	insertSqlString = strings.TrimSuffix(insertSqlString, ",") + ");"
	return insertSqlString, nil
}

// 生成删除语句字符串
func (i *impl) GenDeleteSqlString(schemaName string, tableName string, colTypeList []byte) (string, error) {
	colList, err := i.GenColList(schemaName, tableName)
	if err != nil {
		return "", err
	}
	if len(colList) == 0 {
		return "", fmt.Errorf("%s.%s表的列数为0", schemaName, tableName)
	}
	if len(colList) != len(colTypeList) {
		return "", fmt.Errorf("%s.%s表的列数和值的个数不匹配", schemaName, tableName)
	}
	deleteSqlString := fmt.Sprintf(`delete from %s.%s where `, schemaName, tableName)
	for i := 0; i < len(colList); i++ {
		switch colTypeList[i] {
		case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_LONG:
			deleteSqlString = deleteSqlString + colList[i] + "=%d and "
		case mysql.MYSQL_TYPE_VARCHAR:
			deleteSqlString = deleteSqlString + colList[i] + "='%s' and "
		case mysql.MYSQL_TYPE_NULL:
			deleteSqlString = deleteSqlString + colList[i] + "=%s and "
		default:
			return "", fmt.Errorf("mysql不支持该数据类型")
		}
	}
	deleteSqlString = strings.TrimSuffix(deleteSqlString, " and ") + ";"
	return deleteSqlString, nil
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
