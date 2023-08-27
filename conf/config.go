package conf

import (
	"database/sql"
	"sync"
)

// 全局配置参数
var (
	Conf *Config
)

// Config结构体
type Config struct {
	MySQL *MySQL
}

// MySQL结构体
type MySQL struct {
	Username    string
	Password    string
	Host        string
	Port        int32
	DB          string
	MaxOpenConn int64
	MaxIdleConn int64
	MaxLifeTime int64
	MaxIdleTime int64
	lock        sync.Mutex
	db          *sql.DB
}

// MySQL结构体构造函数
func NewDefaultMySQL() *MySQL {
	return &MySQL{
		Username:    "root",
		Password:    "123456",
		Host:        "127.0.0.1",
		Port:        3306,
		DB:          "test",
		MaxOpenConn: 50,
		MaxIdleConn: 10,
	}
}

// Config结构体构造函数
func NewDefaultConfig() *Config {
	return &Config{
		MySQL: NewDefaultMySQL(),
	}
}
