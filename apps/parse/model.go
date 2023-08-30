package parse

import (
	"fmt"
	"strconv"
	"strings"
)

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
	StartPos int64
	EndPos   int64
}

// BinLogPositionResponse构造函数
func NewBinLogPositionResponse() *BinLogPositionResponse {
	return &BinLogPositionResponse{}
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

// 获取起始时间对应的position
func (b *BinLogPosDateSet) GetStartAndEndPos() (*BinLogPositionResponse, error) {
	if len(b.Items) == 0 || len(b.Items) == 1 {
		return nil, fmt.Errorf("未找到起始时间对应的Position")
	}
	binLogPosRes := NewBinLogPositionResponse()
	posStartList := strings.Split(b.Items[1].Pos, " ")
	binlogStartPos, err := strconv.Atoi(posStartList[len(posStartList)-1])
	if err != nil {
		return nil, err
	}
	binLogPosRes.StartPos = int64(binlogStartPos)
	posEndList := strings.Split(b.Items[len(b.Items)-1].Pos, " ")
	binLogEndPos, err := strconv.Atoi(posEndList[len(posEndList)-1])
	if err != nil {
		return nil, err
	}
	binLogPosRes.EndPos = int64(binLogEndPos)
	return binLogPosRes, nil
}
