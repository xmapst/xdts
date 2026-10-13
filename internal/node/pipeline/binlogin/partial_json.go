package binlogin

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

var errPartialJSON = errors.New("binlogin: invalid partial JSON row image")

// decodeSourceRows keeps the library's complete-image decoder. Partial events
// reconstruct every after value from the same event's FULL before image; no
// current source row, mutable parser state or dependency cache is consulted.
func decodeSourceRows(event *replication.RowsEvent, raw []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = errPartialJSON
		}
	}()
	if event.Type() != replication.EnumRowsEventTypeUnknown {
		return event.Decode(raw)
	}
	pos, err := event.DecodeHeader(raw)
	if err != nil {
		return errPartialJSON
	}
	columns := int(event.ColumnCount)
	if columns == 0 || event.Table == nil || len(event.Table.ColumnType) != columns || len(event.Table.ColumnMeta) != columns || len(event.ColumnBitmap2) == 0 {
		return errPartialJSON
	}
	event.Rows = nil
	event.SkippedColumns = nil
	skipped := make([][]int, 2)
	for image, bitmap := range [][]byte{event.ColumnBitmap1, event.ColumnBitmap2} {
		for column := range columns {
			if !partialBit(bitmap, column) {
				skipped[image] = append(skipped[image], column)
			}
		}
	}
	for pos < len(raw) {
		before, next, err := decodePartialImage(event, raw, pos, nil)
		if err != nil {
			return err
		}
		pos = next
		after, next, err := decodePartialImage(event, raw, pos, before)
		if err != nil {
			return err
		}
		pos = next
		event.Rows = append(event.Rows, before, after)
		event.SkippedColumns = append(event.SkippedColumns, append([]int(nil), skipped[0]...), append([]int(nil), skipped[1]...))
	}
	if len(event.Rows) == 0 {
		return errPartialJSON
	}
	return nil
}

func partialBit(bitmap []byte, index int) bool {
	return index >= 0 && index/8 < len(bitmap) && bitmap[index/8]&(1<<uint(index%8)) != 0
}

func decodePartialImage(event *replication.RowsEvent, raw []byte, pos int, before []any) ([]any, int, error) {
	partial := []byte(nil)
	if before != nil {
		options, n, err := partialLength(raw[pos:])
		if err != nil || options > 1 {
			return nil, pos, errPartialJSON
		}
		pos += n
		if options == 1 {
			n := int((event.Table.JsonColumnCount() + 7) / 8)
			if n > len(raw)-pos {
				return nil, pos, errPartialJSON
			}
			partial = raw[pos : pos+n]
			pos += n
		}
	}
	bitmap := event.ColumnBitmap1
	if before != nil {
		bitmap = event.ColumnBitmap2
	}
	included := 0
	for i := 0; i < int(event.ColumnCount); i++ {
		if partialBit(bitmap, i) {
			included++
		}
	}
	nullBytes := (included + 7) / 8
	if nullBytes > len(raw)-pos {
		return nil, pos, errPartialJSON
	}
	nulls := raw[pos : pos+nullBytes]
	pos += nullBytes
	values := make([]any, event.ColumnCount)
	jsonIndex := 0
	nullIndex := 0
	for i, tp := range event.Table.ColumnType {
		isPartial := false
		if tp == gmysql.MYSQL_TYPE_JSON {
			isPartial = partialBit(partial, jsonIndex)
			jsonIndex++
		}
		if !partialBit(bitmap, i) {
			continue
		}
		isNull := partialBit(nulls, nullIndex)
		nullIndex++
		if isNull {
			continue
		}
		n, err := partialColumnBytes(raw[pos:], tp, event.Table.ColumnMeta[i])
		if err != nil || n > len(raw)-pos {
			return nil, pos, errPartialJSON
		}
		cell := raw[pos : pos+n]
		pos += n
		if isPartial {
			if before == nil || before[i] == nil {
				return nil, pos, errPartialJSON
			}
			text, ok := before[i].(string)
			if !ok {
				return nil, pos, errPartialJSON
			}
			prefix := int(event.Table.ColumnMeta[i])
			if prefix < 1 || prefix > 4 || prefix > len(cell) {
				return nil, pos, errPartialJSON
			}
			values[i], err = applyPartialVector(event, text, cell[prefix:])
			if err != nil {
				return nil, pos, err
			}
		} else {
			values[i], err = decodePartialCell(event, i, cell)
			if err != nil {
				return nil, pos, err
			}
		}
	}
	return values, pos, nil
}

// Public DecodeData on a private one-column event retains the library's exact
// temporal/decimal/unsigned/binary decoding without copying its private parser.
func decodePartialCell(event *replication.RowsEvent, column int, cell []byte) (any, error) {
	copyEvent := *event
	table := *event.Table
	table.ColumnCount = 1
	table.ColumnType = []byte{event.Table.ColumnType[column]}
	table.ColumnMeta = []uint16{event.Table.ColumnMeta[column]}
	// SignednessMap indexes numeric columns, so preserve the decoded value's
	// original sign explicitly when the producer supplies optional metadata.
	unsigned := event.Table.UnsignedMap()[column]
	table.SignednessBitmap = nil
	if unsigned {
		table.SignednessBitmap = []byte{0x80}
	}
	copyEvent.Table = &table
	copyEvent.ColumnCount = 1
	copyEvent.ColumnBitmap1 = []byte{1}
	copyEvent.ColumnBitmap2 = []byte{1}
	data := make([]byte, 0, len(cell)*2+3)
	data = append(data, 0)
	data = append(data, cell...)
	data = append(data, 0, 0)
	data = append(data, cell...)
	if err := copyEvent.DecodeData(0, data); err != nil || len(copyEvent.Rows) != 2 || len(copyEvent.Rows[0]) != 1 {
		return nil, errPartialJSON
	}
	return copyEvent.Rows[0][0], nil
}

func partialLength(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, errPartialJSON
	}
	switch data[0] {
	case 251, 255:
		return 0, 0, errPartialJSON
	case 252:
		if len(data) < 3 {
			return 0, 0, errPartialJSON
		}
		return uint64(binary.LittleEndian.Uint16(data[1:])), 3, nil
	case 253:
		if len(data) < 4 {
			return 0, 0, errPartialJSON
		}
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16, 4, nil
	case 254:
		if len(data) < 9 {
			return 0, 0, errPartialJSON
		}
		return binary.LittleEndian.Uint64(data[1:]), 9, nil
	default:
		return uint64(data[0]), 1, nil
	}
}

func partialColumnBytes(data []byte, tp byte, meta uint16) (int, error) {
	size := 0
	switch tp {
	case gmysql.MYSQL_TYPE_NULL:
		return 0, nil
	case gmysql.MYSQL_TYPE_TINY, gmysql.MYSQL_TYPE_YEAR:
		size = 1
	case gmysql.MYSQL_TYPE_SHORT:
		size = 2
	case gmysql.MYSQL_TYPE_INT24, gmysql.MYSQL_TYPE_DATE, gmysql.MYSQL_TYPE_NEWDATE, gmysql.MYSQL_TYPE_TIME:
		size = 3
	case gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_FLOAT, gmysql.MYSQL_TYPE_TIMESTAMP:
		size = 4
	case gmysql.MYSQL_TYPE_LONGLONG, gmysql.MYSQL_TYPE_DOUBLE, gmysql.MYSQL_TYPE_DATETIME:
		size = 8
	case gmysql.MYSQL_TYPE_TIMESTAMP2:
		if meta > 6 {
			return 0, errPartialJSON
		}
		size = 4 + int((meta+1)/2)
	case gmysql.MYSQL_TYPE_DATETIME2:
		if meta > 6 {
			return 0, errPartialJSON
		}
		size = 5 + int((meta+1)/2)
	case gmysql.MYSQL_TYPE_TIME2:
		if meta > 6 {
			return 0, errPartialJSON
		}
		size = 3 + int((meta+1)/2)
	case gmysql.MYSQL_TYPE_BIT:
		size = int(((meta>>8)*8 + (meta & 255) + 7) / 8)
	case gmysql.MYSQL_TYPE_NEWDECIMAL:
		precision, scale := int(meta>>8), int(meta&255)
		if precision == 0 || precision < scale || precision > 65 || scale > 30 {
			return 0, errPartialJSON
		}
		digits := []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
		integral := precision - scale
		size = integral/9*4 + digits[integral%9] + scale/9*4 + digits[scale%9]
	case gmysql.MYSQL_TYPE_ENUM, gmysql.MYSQL_TYPE_SET:
		size = int(meta & 255)
	case gmysql.MYSQL_TYPE_BLOB, gmysql.MYSQL_TYPE_JSON, gmysql.MYSQL_TYPE_GEOMETRY, gmysql.MYSQL_TYPE_VECTOR:
		prefix := int(meta)
		if prefix < 1 || prefix > 4 || len(data) < prefix {
			return 0, errPartialJSON
		}
		length := uint64(0)
		for i := range prefix {
			length |= uint64(data[i]) << uint(i*8)
		}
		if length > uint64(len(data)-prefix) {
			return 0, errPartialJSON
		}
		size = prefix + int(length)
	case gmysql.MYSQL_TYPE_VARCHAR, gmysql.MYSQL_TYPE_VAR_STRING, gmysql.MYSQL_TYPE_STRING:
		maximum := int(meta)
		if tp == gmysql.MYSQL_TYPE_STRING && meta >= 256 {
			high := byte(meta >> 8)
			if high&0x30 != 0x30 {
				maximum = int(meta&255) | int((high&0x30)^0x30)<<4
			} else {
				real := high
				if real == gmysql.MYSQL_TYPE_ENUM || real == gmysql.MYSQL_TYPE_SET {
					return partialColumnBytes(data, real, meta)
				}
				maximum = int(meta & 255)
			}
		}
		prefix := 1
		if maximum >= 256 {
			prefix = 2
		}
		if len(data) < prefix {
			return 0, errPartialJSON
		}
		length := int(data[0])
		if prefix == 2 {
			length = int(binary.LittleEndian.Uint16(data))
		}
		size = prefix + length
	default:
		return 0, errPartialJSON
	}
	if size < 0 || size > len(data) {
		return 0, errPartialJSON
	}
	return size, nil
}

func applyPartialVector(event *replication.RowsEvent, before string, vector []byte) (string, error) {
	var root any
	decoder := json.NewDecoder(strings.NewReader(before))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return "", errPartialJSON
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return "", errPartialJSON
	}
	for len(vector) > 0 {
		op := vector[0]
		vector = vector[1:]
		if op > 2 {
			return "", errPartialJSON
		}
		length, n, err := partialLength(vector)
		if err != nil || length > uint64(len(vector)-n) {
			return "", errPartialJSON
		}
		path := string(vector[n : n+int(length)])
		vector = vector[n+int(length):]
		var value any
		if op != 2 {
			length, n, err = partialLength(vector)
			if err != nil || length == 0 || length > uint64(len(vector)-n) {
				return "", errPartialJSON
			}
			binaryJSON := vector[n : n+int(length)]
			vector = vector[n+int(length):]
			// MySQL JSON value decoding via a private JSON column image; binary length
			// is independent of the original JSON column framing.
			copyEvent := *event
			table := *event.Table
			table.ColumnType = []byte{gmysql.MYSQL_TYPE_JSON}
			table.ColumnMeta = []uint16{4}
			table.ColumnCount = 1
			table.SignednessBitmap = nil
			copyEvent.Table = &table
			copyEvent.ColumnCount = 1
			copyEvent.ColumnBitmap1 = []byte{1}
			copyEvent.ColumnBitmap2 = []byte{1}
			cell := make([]byte, 4, len(binaryJSON)+4)
			binary.LittleEndian.PutUint32(cell, uint32(len(binaryJSON)))
			cell = append(cell, binaryJSON...)
			data := append([]byte{0}, cell...)
			data = append(data, 0, 0)
			data = append(data, cell...)
			if err := copyEvent.DecodeData(0, data); err != nil || len(copyEvent.Rows) != 2 {
				return "", errPartialJSON
			}
			text, ok := copyEvent.Rows[0][0].(string)
			if !ok {
				return "", errPartialJSON
			}
			dec := json.NewDecoder(strings.NewReader(text))
			dec.UseNumber()
			if err := dec.Decode(&value); err != nil {
				return "", errPartialJSON
			}
		}
		legs, err := partialJSONPath(path)
		if err != nil {
			return "", err
		}
		root, err = partialJSONApply(root, legs, op, value)
		if err != nil {
			return "", err
		}
	}
	encoded, err := json.Marshal(root)
	if err != nil {
		return "", errPartialJSON
	}
	return string(encoded), nil
}

type partialPathLeg struct {
	member  string
	index   int
	array   bool
	fromEnd bool
}

func partialJSONPath(path string) ([]partialPathLeg, error) {
	if path == "" || path[0] != '$' {
		return nil, errPartialJSON
	}
	path = path[1:]
	legs := []partialPathLeg{}
	for len(path) > 0 {
		switch path[0] {
		case '.':
			path = path[1:]
			if path == "" {
				return nil, errPartialJSON
			}
			name := ""
			if path[0] == '"' {
				end := 1
				escaped := false
				for end < len(path) {
					c := path[end]
					if c == '"' && !escaped {
						break
					}
					if c == '\\' && !escaped {
						escaped = true
					} else {
						escaped = false
					}
					end++
				}
				if end >= len(path) || json.Unmarshal([]byte(path[:end+1]), &name) != nil {
					return nil, errPartialJSON
				}
				path = path[end+1:]
			} else {
				end := strings.IndexAny(path, ".[")
				if end < 0 {
					end = len(path)
				}
				name = path[:end]
				path = path[end:]
				if name == "" || strings.ContainsAny(name, "* \t\r\n\\\"") {
					return nil, errPartialJSON
				}
			}
			legs = append(legs, partialPathLeg{member: name})
		case '[':
			end := strings.IndexByte(path, ']')
			if end < 2 {
				return nil, errPartialJSON
			}
			raw := path[1:end]
			fromEnd := raw == "last" || strings.HasPrefix(raw, "last-")
			if fromEnd {
				raw = strings.TrimPrefix(raw, "last")
				if raw == "" {
					raw = "0"
				} else {
					raw = strings.TrimPrefix(raw, "-")
				}
			}
			index, err := strconv.Atoi(raw)
			if err != nil || index < 0 || strconv.Itoa(index) != raw {
				return nil, errPartialJSON
			}
			legs = append(legs, partialPathLeg{array: true, index: index, fromEnd: fromEnd})
			path = path[end+1:]
		default:
			return nil, errPartialJSON
		}
		if len(legs) > 1000 {
			return nil, errPartialJSON
		}
	}
	if len(legs) == 0 {
		return nil, errPartialJSON
	}
	return legs, nil
}

func partialJSONApply(root any, legs []partialPathLeg, op byte, value any) (any, error) {
	if len(legs) == 0 {
		return nil, errPartialJSON
	}
	leg := legs[0]
	last := len(legs) == 1
	if leg.array {
		array, ok := root.([]any)
		if !ok {
			return nil, errPartialJSON
		}
		if leg.fromEnd {
			if leg.index >= len(array) {
				return nil, errPartialJSON
			}
			leg.index = len(array) - 1 - leg.index
		}
		if last && op == 1 {
			if leg.index > len(array) {
				return nil, errPartialJSON
			}
			array = append(array, nil)
			copy(array[leg.index+1:], array[leg.index:])
			array[leg.index] = value
			return array, nil
		}
		if leg.index >= len(array) {
			return nil, errPartialJSON
		}
		if last {
			if op == 0 {
				array[leg.index] = value
				return array, nil
			}
			if op == 2 {
				return append(array[:leg.index], array[leg.index+1:]...), nil
			}
			return nil, errPartialJSON
		}
		child, err := partialJSONApply(array[leg.index], legs[1:], op, value)
		if err != nil {
			return nil, err
		}
		array[leg.index] = child
		return array, nil
	}
	object, ok := root.(map[string]any)
	if !ok {
		return nil, errPartialJSON
	}
	old, exists := object[leg.member]
	if last {
		switch op {
		case 0:
			if !exists {
				return nil, errPartialJSON
			}
			object[leg.member] = value
		case 1:
			if exists {
				return nil, errPartialJSON
			}
			object[leg.member] = value
		case 2:
			if !exists {
				return nil, errPartialJSON
			}
			delete(object, leg.member)
		default:
			return nil, errPartialJSON
		}
		return object, nil
	}
	if !exists {
		return nil, errPartialJSON
	}
	child, err := partialJSONApply(old, legs[1:], op, value)
	if err != nil {
		return nil, err
	}
	object[leg.member] = child
	return object, nil
}
