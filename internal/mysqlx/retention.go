package mysqlx

import "strconv"

// BinlogRetentionSeconds reads the server's actual retention settings. Unknown
// or malformed settings remain unverified; zero means expiration is disabled.
func BinlogRetentionSeconds(params map[string]string) (uint64, bool) {
	if value, exists := params["binlog_expire_logs_seconds"]; exists {
		seconds, err := strconv.ParseUint(value, 10, 64)
		return seconds, err == nil
	}
	value, exists := params["expire_logs_days"]
	if !exists {
		return 0, false
	}
	days, err := strconv.ParseUint(value, 10, 64)
	const secondsPerDay = 24 * 60 * 60
	if err != nil || days > ^uint64(0)/secondsPerDay {
		return 0, false
	}
	return days * secondsPerDay, true
}
