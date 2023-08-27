package tools

import (
	_ "github.com/solodba/binlog_parser/apps/all"
	"github.com/solodba/mcube/apps"
	"github.com/solodba/mcube/logger"
)

func DevelopmentSet() {
	err := apps.InitInternalApps()
	if err != nil {
		logger.L().Panic().Msgf("initial object config error, err: %s", err.Error())
	}
}
