package protocol

// binlog parse服务结构体
type ParseService struct {
}

// binlog parse服务结构体构造函数
func NewParseService() *ParseService {
	return &ParseService{}
}

// binlog parse服务启动方法
func (s *ParseService) Start() error {
	return nil
}

// GRPC服务停止方法
func (s *ParseService) Stop() error {
	return nil
}
