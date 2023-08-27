package impl_test

import (
	"context"

	"github.com/solodba/binlog_parser/apps/parse"
	"github.com/solodba/binlog_parser/test/tools"
	"github.com/solodba/mcube/apps"
)

var (
	svc parse.Service
	ctx = context.Background()
)

func init() {
	tools.DevelopmentSet()
	svc = apps.GetInternalApp(parse.AppName).(parse.Service)
}
