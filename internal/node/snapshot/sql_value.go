package snapshot

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/mysqlx"
)

// DecodeSQLValue encodes the MySQL text protocol value. nil is SQL NULL;
// a non-nil empty byte slice is an empty string/blob. Database errors and values
// are not included in failure messages, avoiding row contents in control logs.
func DecodeSQLValue(raw []byte, column Column) (*iproto.ColumnValue, error) {
	if !supportedSQLType(column.DataType) {
		return nil, fmt.Errorf("snapshot: unsupported SQL column type")
	}
	if raw == nil {
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_IsNull{IsNull: true}}, nil
	}
	text := string(raw)
	typeName := strings.ToLower(column.DataType)
	if integerBits(typeName) != 0 || typeName == "year" {
		if column.Unsigned {
			value, err := strconv.ParseUint(text, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("snapshot: invalid unsigned SQL value")
			}
			return &iproto.ColumnValue{Value: &iproto.ColumnValue_U64{U64: value}}, nil
		}
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("snapshot: invalid integer SQL value")
		}
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_I64{I64: value}}, nil
	}
	switch typeName {
	case "float", "double", "real":
		value, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("snapshot: invalid floating SQL value")
		}
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_F64{F64: value}}, nil
	case "decimal", "numeric":
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_Decimal{Decimal: text}}, nil
	case "bit":
		if len(raw) > 8 {
			return nil, fmt.Errorf("snapshot: invalid BIT width")
		}
		var padded [8]byte
		copy(padded[8-len(raw):], raw)
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_U64{U64: binary.BigEndian.Uint64(padded[:])}}, nil
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_RawBytes{RawBytes: append([]byte{}, raw...)}}, nil
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "json", "enum", "set":
		if !utf8.Valid(raw) {
			return nil, fmt.Errorf("snapshot: invalid UTF-8 SQL text")
		}
		return &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: text}}, nil
	case "date", "time", "datetime", "timestamp":
		meta := mysqlx.ColumnMeta{Name: column.Name}
		switch typeName {
		case "date":
			meta.MySQLType = gmysql.MYSQL_TYPE_DATE
		case "time":
			meta.MySQLType = gmysql.MYSQL_TYPE_TIME
		case "datetime":
			meta.MySQLType = gmysql.MYSQL_TYPE_DATETIME
		case "timestamp":
			meta.MySQLType = gmysql.MYSQL_TYPE_TIMESTAMP
		}
		var value any = text
		if (typeName == "datetime" || typeName == "timestamp") && !strings.HasPrefix(text, "0000-00-00") {
			parsed, err := time.ParseInLocation("2006-01-02 15:04:05.999999", text, time.UTC)
			if err != nil {
				return nil, fmt.Errorf("snapshot: invalid temporal SQL value")
			}
			value = parsed
		}
		encoded, err := mysqlx.DecodeColumnValue(value, meta)
		if err != nil {
			return nil, fmt.Errorf("snapshot: invalid temporal SQL value")
		}
		return encoded, nil
	default:
		return nil, fmt.Errorf("snapshot: unsupported SQL column type")
	}
}

func supportedSQLType(typeName string) bool {
	typeName = strings.ToLower(typeName)
	if integerBits(typeName) != 0 {
		return true
	}
	switch typeName {
	case "year", "float", "double", "real", "decimal", "numeric", "bit",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
		"char", "varchar", "tinytext", "text", "mediumtext", "longtext", "json", "enum", "set",
		"date", "time", "datetime", "timestamp":
		return true
	default:
		return false
	}
}
