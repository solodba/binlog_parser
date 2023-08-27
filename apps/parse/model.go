package parse

// BinLogModeResponse结构体
type BinLogModeResponse struct {
	VariableName string
	Value        string
}

// BinLogModeResponse结构体构造函数
func NewBinLogModeResponse() *BinLogModeResponse {
	return &BinLogModeResponse{}
}

// IsBinLogResponse结构体
type IsBinLogResponse struct {
	On bool
}

// IsBinLogResponse构造函数
func NewIsBinLogResponse() *IsBinLogResponse {
	return &IsBinLogResponse{}
}
