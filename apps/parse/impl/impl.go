package impl

import (
	"database/sql"

	"github.com/solodba/binlog_parser/apps/parse"
	"github.com/solodba/binlog_parser/conf"
	"github.com/solodba/mcube/apps"
)

var (
	svc = &impl{}
)

// 业务实现类
type impl struct {
	c  *conf.Config
	db *sql.DB
}

// 实现Ioc中心Name方法
func (i *impl) Name() string {
	return parse.AppName
}

// 实现Ioc中心Conf方法
func (i *impl) Conf() error {
	i.c = conf.Conf
	db, err := conf.Conf.MySQL.GetDbConn()
	if err != nil {
		return err
	}
	i.db = db
	return nil
}

// 注册实例类
func init() {
	apps.RegistryInternalApp(svc)
}
