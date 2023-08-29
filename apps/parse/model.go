package parse

// BinLogResponse结构体
type BinLogResponse struct {
	VariableName string
	Value        string
}

// BinLogResponse结构体构造函数
func NewBinLogResponse() *BinLogResponse {
	return &BinLogResponse{}
}

// IsBinLogResponse结构体
type IsBinLogResponse struct {
	On bool
}

// IsBinLogResponse构造函数
func NewIsBinLogResponse() *IsBinLogResponse {
	return &IsBinLogResponse{}
}
