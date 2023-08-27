package impl_test

import (
	"testing"

	"github.com/solodba/binlog_parser/test/tools"
)

func TestIsBinLogMode(t *testing.T) {
	binLogModeRes, err := svc.QueryBinLogMode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tools.MustToJson(binLogModeRes))
}

func TestIsBinLog(t *testing.T) {
	isBinLogRes, err := svc.IsBinLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tools.MustToJson(isBinLogRes))
}
