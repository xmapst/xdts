// Package uuidx 提供管理库主键所需的 UUIDv7 类型封装。
//
// 背景：Go 1.27 标准库 uuid 包提供了 NewV7 与文本编解码（MarshalText /
// UnmarshalText），但没有实现 database/sql 的 Scanner 与 driver.Valuer，
// 因此不能直接用作 gorm 字段。本包补齐这两个接口，并把列类型固定为
// BINARY(16)，使 UUIDv7 的时间递增特性作用于 InnoDB 聚簇索引。
//
// 对外（HTTP API、前端、日志）一律使用标准字符串形式，只有落库时才是二进制。
//
// 对应需求 C-10 与。
package uuidx

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"uuid"
)

// Size 是 UUID 的二进制长度，也是管理库中 BINARY 列的宽度。
const Size = 16

// UUID 是可直接用于 gorm 的 UUIDv7。零值为 Nil。
type UUID [Size]byte

// Nil 是全零 UUID，用于表示「未设置」。
var Nil UUID

var (
	// ErrInvalidLength 表示扫描到的字节长度不是 16。
	ErrInvalidLength = errors.New("uuidx: 二进制 UUID 长度必须为 16 字节")
	// ErrUnsupportedScanType 表示数据库返回了无法解析为 UUID 的类型。
	ErrUnsupportedScanType = errors.New("uuidx: 不支持的扫描类型")
)

// New 生成一个新的 UUIDv7。同一进程内按时间递增（系统时钟回拨时除外）。
func New() UUID { return UUID(uuid.NewV7()) }

// Parse 解析标准字符串形式的 UUID。
func Parse(s string) (UUID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return Nil, fmt.Errorf("uuidx: 解析 %q 失败: %w", s, err)
	}
	return UUID(u), nil
}

// MustParse 解析字符串形式的 UUID，失败即 panic。只用于常量与测试。
func MustParse(s string) UUID {
	u, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

// FromBytes 由 16 字节构造 UUID。
func FromBytes(b []byte) (UUID, error) {
	if len(b) != Size {
		return Nil, fmt.Errorf("%w: 实际 %d", ErrInvalidLength, len(b))
	}
	var u UUID
	copy(u[:], b)
	return u, nil
}

// String 返回标准的 8-4-4-4-12 小写形式。
func (u UUID) String() string { return uuid.UUID(u).String() }

// IsZero 报告是否为 Nil。
func (u UUID) IsZero() bool { return u == Nil }

// Bytes 返回底层 16 字节的副本。
func (u UUID) Bytes() []byte {
	b := make([]byte, Size)
	copy(b, u[:])
	return b
}

// Compare 按字节序比较。UUIDv7 的前 48 位是时间戳，因此字节序等价于生成顺序。
func (u UUID) Compare(v UUID) int { return uuid.UUID(u).Compare(uuid.UUID(v)) }

// Value 实现 driver.Valuer，以 16 字节二进制落库。Nil 落为 NULL。
func (u UUID) Value() (driver.Value, error) {
	if u.IsZero() {
		return nil, nil
	}
	return u.Bytes(), nil
}

// Scan 实现 sql.Scanner。兼容驱动返回的二进制、字符串与 NULL。
func (u *UUID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*u = Nil
		return nil
	case []byte:
		// BINARY(16) 列返回原始字节；某些场景（如 CAST 为文本）会返回字符串形式的字节。
		if len(v) == Size {
			copy(u[:], v)
			return nil
		}
		parsed, err := Parse(string(v))
		if err != nil {
			return err
		}
		*u = parsed
		return nil
	case string:
		parsed, err := Parse(v)
		if err != nil {
			return err
		}
		*u = parsed
		return nil
	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedScanType, src)
	}
}

// MarshalText 实现 encoding.TextMarshaler，JSON 中即为标准字符串形式。
func (u UUID) MarshalText() ([]byte, error) { return uuid.UUID(u).MarshalText() }

// UnmarshalText 实现 encoding.TextUnmarshaler。空串解析为 Nil。
func (u *UUID) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		*u = Nil
		return nil
	}
	var raw uuid.UUID
	if err := raw.UnmarshalText(b); err != nil {
		return fmt.Errorf("uuidx: 解析 %q 失败: %w", string(b), err)
	}
	*u = UUID(raw)
	return nil
}

// GormDataType 供 gorm 推导列类型，固定为 binary(16)。
func (UUID) GormDataType() string { return "binary(16)" }
