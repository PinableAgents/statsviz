package sqlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/PinableAgents/statsviz/history"
)

const aggregatePayloadVersion = 1

// aggregateWireCells 是内部持久格式的 cell 列表。v1 使用固定九项 tuple：
// [kind, hasValue, weightedSum, weightNS, sum, min, max, last, count]。
// 数字 kind 及固定位置用于压低 SQLite payload；版本只承诺新程序读取旧格式，
// 旧程序不能读取 v1 数组格式。
type aggregateWireCells []aggregateCell

type aggregateWireCell struct {
	cell aggregateCell
}

func encodeAggregatePayload(payload aggregatePayload) ([]byte, error) {
	plots := make(map[string]aggregateWireCells, len(payload.Plots))
	for name, cells := range payload.Plots {
		plots[name] = aggregateWireCells(cells)
	}
	encoded, err := json.Marshal([2]any{aggregatePayloadVersion, plots})
	if err != nil {
		return nil, fmt.Errorf("编码 v1 聚合 payload: %w", err)
	}
	return encoded, nil
}

func decodeAggregatePayload(raw []byte) (aggregatePayload, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return aggregatePayload{}, fmt.Errorf("聚合 payload 为空")
	}
	if trimmed[0] == '{' {
		// 旧数据库使用 {"plots": ...} object。此分支只负责向后读取，
		// 新写入统一使用下面带显式版本的紧凑数组格式。
		var legacy aggregatePayload
		if err := json.Unmarshal(trimmed, &legacy); err != nil {
			return aggregatePayload{}, fmt.Errorf("解码旧聚合 payload: %w", err)
		}
		if legacy.Plots == nil {
			return aggregatePayload{}, fmt.Errorf("旧聚合 payload 缺少 plots")
		}
		return legacy, nil
	}
	if trimmed[0] != '[' {
		return aggregatePayload{}, fmt.Errorf("聚合 payload 顶层必须是旧 object 或版本数组")
	}

	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	if err := expectJSONDelimiter(decoder, '['); err != nil {
		return aggregatePayload{}, fmt.Errorf("解码聚合 payload: %w", err)
	}
	if !decoder.More() {
		return aggregatePayload{}, fmt.Errorf("聚合 payload 缺少格式版本")
	}
	var version int
	if err := decoder.Decode(&version); err != nil {
		return aggregatePayload{}, fmt.Errorf("解码聚合 payload 版本: %w", err)
	}
	if version != aggregatePayloadVersion {
		return aggregatePayload{}, fmt.Errorf("未知聚合 payload 版本 %d", version)
	}
	if !decoder.More() {
		return aggregatePayload{}, fmt.Errorf("v1 聚合 payload 缺少 plots")
	}
	var wirePlots map[string]aggregateWireCells
	if err := decoder.Decode(&wirePlots); err != nil {
		return aggregatePayload{}, fmt.Errorf("解码 v1 聚合 plots: %w", err)
	}
	if wirePlots == nil {
		return aggregatePayload{}, fmt.Errorf("v1 聚合 plots 必须是 object")
	}
	if decoder.More() {
		return aggregatePayload{}, fmt.Errorf("v1 聚合 payload 顶层必须恰好包含两个元素")
	}
	if err := expectJSONDelimiter(decoder, ']'); err != nil {
		return aggregatePayload{}, fmt.Errorf("结束 v1 聚合 payload: %w", err)
	}
	if err := expectJSONEOF(decoder); err != nil {
		return aggregatePayload{}, fmt.Errorf("结束 v1 聚合 payload: %w", err)
	}

	plots := make(map[string][]aggregateCell, len(wirePlots))
	for name, cells := range wirePlots {
		// 命名 slice 转换复用底层数组，不复制整份 cell 矩阵。
		plots[name] = []aggregateCell(cells)
	}
	return aggregatePayload{Plots: plots}, nil
}

func (cells aggregateWireCells) MarshalJSON() ([]byte, error) {
	var buffer bytes.Buffer
	buffer.WriteByte('[')
	for index, cell := range cells {
		if index != 0 {
			buffer.WriteByte(',')
		}
		encoded, err := (aggregateWireCell{cell: cell}).MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("编码第 %d 个聚合 cell: %w", index, err)
		}
		buffer.Write(encoded)
	}
	buffer.WriteByte(']')
	return buffer.Bytes(), nil
}

func (cells *aggregateWireCells) UnmarshalJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := expectJSONDelimiter(decoder, '['); err != nil {
		return err
	}
	// v1 实际 tuple 至少约 22 字节；预估容量可避免 60/80 单元序列在
	// 解码时反复扩容并复制 72 字节的 aggregateCell。上限避免损坏的
	// 超大空白 payload 仅凭字节数触发不受控预分配；超过上限仍可正常扩容。
	capacity := len(raw) / 22
	if capacity > 4096 {
		capacity = 4096
	}
	result := make(aggregateWireCells, 0, capacity)
	for decoder.More() {
		var wire aggregateWireCell
		if err := decoder.Decode(&wire); err != nil {
			return fmt.Errorf("解码第 %d 个聚合 cell: %w", len(result), err)
		}
		result = append(result, wire.cell)
	}
	if err := expectJSONDelimiter(decoder, ']'); err != nil {
		return err
	}
	if err := expectJSONEOF(decoder); err != nil {
		return err
	}
	*cells = result
	return nil
}

func (wire aggregateWireCell) MarshalJSON() ([]byte, error) {
	kind, err := aggregateKindCode(wire.cell.Kind)
	if err != nil {
		return nil, err
	}
	return json.Marshal([9]any{
		kind,
		wire.cell.HasValue,
		wire.cell.WeightedSum,
		wire.cell.WeightNS,
		wire.cell.Sum,
		wire.cell.Min,
		wire.cell.Max,
		wire.cell.Last,
		wire.cell.Count,
	})
}

func (wire *aggregateWireCell) UnmarshalJSON(raw []byte) error {
	fields, err := splitAggregateTuple(raw)
	if err != nil {
		return err
	}
	kindCode, err := parseAggregateTupleInt(fields[0], "kind")
	if err != nil {
		return err
	}
	kind, err := aggregateKindFromCode(kindCode)
	if err != nil {
		return err
	}
	var cell aggregateCell
	cell.Kind = kind
	switch {
	case bytes.Equal(fields[1], []byte("true")):
		cell.HasValue = true
	case bytes.Equal(fields[1], []byte("false")):
	default:
		return fmt.Errorf("聚合 cell tuple 的 hasValue 无效")
	}
	if cell.WeightedSum, err = parseAggregateTupleFloat(fields[2], "weightedSum"); err != nil {
		return err
	}
	if cell.WeightNS, err = parseAggregateTupleInt(fields[3], "weightNS"); err != nil {
		return err
	}
	if cell.Sum, err = parseAggregateTupleFloat(fields[4], "sum"); err != nil {
		return err
	}
	if cell.Min, err = parseAggregateTupleFloat(fields[5], "min"); err != nil {
		return err
	}
	if cell.Max, err = parseAggregateTupleFloat(fields[6], "max"); err != nil {
		return err
	}
	if cell.Last, err = parseAggregateTupleFloat(fields[7], "last"); err != nil {
		return err
	}
	if cell.Count, err = parseAggregateTupleInt(fields[8], "count"); err != nil {
		return err
	}
	wire.cell = cell
	return nil
}

// splitAggregateTuple 只处理已经由 encoding/json 验证过的 v1 cell 值。
// tuple 中只允许数字和 bool，因此按逗号切分即可，无需在 aggregateCell 上
// 维护一套通用 JSON 解析器，也不会为九个字段创建临时 interface 矩阵。
func splitAggregateTuple(raw []byte) ([9][]byte, error) {
	var fields [9][]byte
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return fields, fmt.Errorf("聚合 cell tuple 必须是数组")
	}
	body := trimmed[1 : len(trimmed)-1]
	start, count := 0, 0
	for index := 0; index <= len(body); index++ {
		if index != len(body) && body[index] != ',' {
			continue
		}
		if count >= len(fields) {
			return fields, fmt.Errorf("聚合 cell tuple 必须恰好包含九项")
		}
		field := bytes.TrimSpace(body[start:index])
		if len(field) == 0 {
			return fields, fmt.Errorf("聚合 cell tuple 的第 %d 项为空", count)
		}
		fields[count] = field
		count++
		start = index + 1
	}
	if count != len(fields) {
		return fields, fmt.Errorf("聚合 cell tuple 必须恰好包含九项")
	}
	return fields, nil
}

func parseAggregateTupleInt(raw []byte, name string) (int64, error) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("聚合 cell tuple 的 %s 无效: %w", name, err)
	}
	return value, nil
}

func parseAggregateTupleFloat(raw []byte, name string) (float64, error) {
	value, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("聚合 cell tuple 的 %s 无效", name)
	}
	return value, nil
}

func aggregateKindCode(kind history.Kind) (int, error) {
	switch kind {
	case history.Gauge:
		return 0, nil
	case history.Rate:
		return 1, nil
	case history.Delta:
		return 2, nil
	case history.Histogram:
		return 3, nil
	case history.Event:
		return 4, nil
	default:
		return 0, fmt.Errorf("未知聚合 kind %q", kind)
	}
}

func aggregateKindFromCode(code int64) (history.Kind, error) {
	switch code {
	case 0:
		return history.Gauge, nil
	case 1:
		return history.Rate, nil
	case 2:
		return history.Delta, nil
	case 3:
		return history.Histogram, nil
	case 4:
		return history.Event, nil
	default:
		return "", fmt.Errorf("未知聚合 kind 编号 %d", code)
	}
}

func expectJSONDelimiter(decoder *json.Decoder, want json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	got, ok := token.(json.Delim)
	if !ok || got != want {
		return fmt.Errorf("需要 %q，实际为 %v", want, token)
	}
	return nil
}

func expectJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("JSON 末尾包含额外值")
	}
	return err
}
