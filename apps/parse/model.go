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

// BinLogPositionResponse结构体
type BinLogPositionResponse struct {
	BinLogPos string
}

// 存放Binlog Position和日期结构体
type BinLogPosDate struct {
	Pos  string
	Date string
}

// BinLogPosDateSet结构体
type BinLogPosDateSet struct {
	Total int
	Items []*BinLogPosDate
}

// BinLogPosDate初始化函数
func NewBinLogPosDate() *BinLogPosDate {
	return &BinLogPosDate{}
}

// BinLogPosDateSet初始化函数
func NewBinLogPosDateSet() *BinLogPosDateSet {
	return &BinLogPosDateSet{
		Items: make([]*BinLogPosDate, 0),
	}
}

// BinLogPosDateSet添加方法
func (b *BinLogPosDateSet) AddItems(items ...*BinLogPosDate) {
	b.Items = append(b.Items, items...)
}
