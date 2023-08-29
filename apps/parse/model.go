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

// BinLogPathResponse结构体
type BinLogPathResponse struct {
	BinLogPath string
}

// BinLogPathResponse构造函数
func NewBinLogPathResponse(path string) *BinLogPathResponse {
	return &BinLogPathResponse{
		BinLogPath: path,
	}
}

// AllBinLogPathResponse结构体
type AllBinLogPathResponse struct {
	Total int
	Items []*BinLogPathResponse
}

// AllBinLogPathResponse构造函数
func NewAllBinLogPathResponse() *AllBinLogPathResponse {
	return &AllBinLogPathResponse{
		Items: make([]*BinLogPathResponse, 0),
	}
}

// AllBinLogPathResponse结构体添加方法
func (a *AllBinLogPathResponse) AddItems(items ...*BinLogPathResponse) {
	a.Items = append(a.Items, items...)
}
