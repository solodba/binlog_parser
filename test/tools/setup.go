package tools

import (
	_ "github.com/solodba/binlog_parser/apps/all"
	"github.com/solodba/binlog_parser/conf"
	"github.com/solodba/mcube/apps"
	"github.com/solodba/mcube/logger"
)

func LoadConfig() {
	conf.Conf = conf.NewDefaultConfig()
	conf.Conf.MySQL.Username = "root"
	conf.Conf.MySQL.Password = "Root@123"
	conf.Conf.MySQL.Host = "192.168.1.100"
	conf.Conf.MySQL.Port = 3306
	conf.Conf.CmdConf.BinLogName = "mysql-bin.000013"
	// conf.Conf.CmdConf.StartTime = "2025-01-21 21:00:00"
	// conf.Conf.CmdConf.EndTime = "2025-01-21 22:00:00"
	conf.Conf.MySQL.MaxOpenConn = 50
	conf.Conf.MySQL.MaxIdleConn = 10
	conf.Conf.MySQL.MaxLifeTime = 600
	conf.Conf.MySQL.MaxIdleTime = 300
}

func DevelopmentSet() {
	LoadConfig()
	err := apps.InitInternalApps()
	if err != nil {
		logger.L().Panic().Msgf("initial object config error, err: %s", err.Error())
	}
}
