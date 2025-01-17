package impl_test

import (
	"testing"

	"github.com/solodba/binlog_parser/test/tools"
)

func TestIsBinLogMode(t *testing.T) {
	binLogRes, err := svc.QueryBinLogMode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tools.MustToJson(binLogRes))
}

func TestQueryMysqlServerId(t *testing.T) {
	binLogRes, err := svc.QueryMysqlServerId(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tools.MustToJson(binLogRes))
}

func TestIsBinLog(t *testing.T) {
	isBinLogRes, err := svc.IsBinLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tools.MustToJson(isBinLogRes))
}

func TestQueryBinLogFormat(t *testing.T) {
	binlogRes, err := svc.QueryBinLogFormat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tools.MustToJson(binlogRes))
}

func TestGetBinLogPath(t *testing.T) {
	binLogPathRes, err := svc.GetBinLogPath(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tools.MustToJson(binLogPathRes))
}

func TestGetAllBinLogPath(t *testing.T) {
	allBinLogRes, err := svc.GetAllBinLogPath(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tools.MustToJson(allBinLogRes))
}

func TestGenColList(t *testing.T) {
	colList, err := svc.GenColList("test", "hu")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(colList)
}

func TestGenInsertSqlString(t *testing.T) {
	insertSqlString, err := svc.GenInsertSqlString("test", "hu", []byte{3, 15, 15, 3, 15})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(insertSqlString)
}
