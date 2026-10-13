package mysqlx

import (
	"fmt"
	"strconv"
	"strings"
)

// Version 是一个 MySQL 版本号（major.minor.patch），DetectCaps 用它计算能力集合。
type Version struct {
	Major, Minor, Patch int
}

// ParseVersion 解析 `SELECT VERSION()` / `@@version` 返回的版本字符串。
//
// MySQL 版本字符串形如 "8.0.46" 或 "8.0.46-log"、"8.0.46-0ubuntu0.22.04.1"（发行版附加
// 后缀）；本函数只取前导的 major.minor.patch 三段数字，第一个非数字/非点字符起的内容
// 一律丢弃。只支持 MySQL——需求 O-02 的支持范围不含 MariaDB。MariaDB 的版本字符串形如
// "5.5.5-10.11.2-MariaDB-..."：前导的 "5.5.5" 是协议兼容占位符，真实版本在第二段，
// 如果只按"取前导三段数字"解析会把 MariaDB 服务器误判成"MySQL 5.5.5"——因此显式检测
// 大小写不敏感的 "mariadb" 子串并拒绝，而不是依赖前导段恰好不是三段式这种间接信号
// （"边界输入必须校验"）。
func ParseVersion(s string) (Version, error) {
	if strings.Contains(strings.ToLower(s), "mariadb") {
		return Version{}, fmt.Errorf("mysqlx: 不支持 MariaDB 版本字符串 %q（需求 O-02 仅覆盖 MySQL）", s)
	}
	core := s
	for i, r := range s {
		if r != '.' && (r < '0' || r > '9') {
			core = s[:i]
			break
		}
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("mysqlx: 无法解析版本号 %q（需要 major.minor.patch 三段式）", s)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		if p == "" {
			return Version{}, fmt.Errorf("mysqlx: 无法解析版本号 %q：第 %d 段为空", s, i+1)
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return Version{}, fmt.Errorf("mysqlx: 无法解析版本号 %q 的第 %d 段 %q", s, i+1, p)
		}
		nums[i] = n
	}
	return Version{Major: nums[0], Minor: nums[1], Patch: nums[2]}, nil
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Compare 返回 v 与 other 的大小关系：-1 表示 v<other，0 表示相等，1 表示 v>other。
func (v Version) Compare(other Version) int {
	switch {
	case v.Major != other.Major:
		return v.cmpInt(v.Major, other.Major)
	case v.Minor != other.Minor:
		return v.cmpInt(v.Minor, other.Minor)
	default:
		return v.cmpInt(v.Patch, other.Patch)
	}
}

// AtLeast 返回 v >= other，是 DetectCaps 判定每条能力是否具备的唯一比较方式。
func (v Version) AtLeast(other Version) bool {
	return v.Compare(other) >= 0
}

func (v Version) cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
