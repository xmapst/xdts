package http

import (
	"errors"
	"fmt"
	"time"
)

// - 都未提供：取最近 defaultWindow。
//   - 只提供 since：向后补 defaultWindow，不超过 now。
//   - 只提供 until：向前补 defaultWindow。
//   - 都提供：原样使用（仍然校验跨度上限）。
//
// maxWindowReason 只用于超限时的错误文案：不同端点"为什么限制这个跨度"
// 的原因不一样，错误措辞不该混为一谈，所以由调用方传入具体理由，不在
// 这里写死。
func resolveTimeRangeQuery(rawSince, rawUntil string, now time.Time, defaultWindow, maxWindow time.Duration, maxWindowReason string) (since, until time.Time, err error) {
	var sinceGiven, untilGiven bool
	if rawSince != "" {
		since, err = time.Parse(time.RFC3339, rawSince)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("since 格式不合法，需为 RFC3339: %w", err)
		}
		sinceGiven = true
	}
	if rawUntil != "" {
		until, err = time.Parse(time.RFC3339, rawUntil)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("until 格式不合法，需为 RFC3339: %w", err)
		}
		untilGiven = true
	}

	switch {
	case !sinceGiven && !untilGiven:
		until = now
		since = now.Add(-defaultWindow)
	case sinceGiven && !untilGiven:
		until = since.Add(defaultWindow)
		if until.After(now) {
			until = now
		}
	case !sinceGiven && untilGiven:
		since = until.Add(-defaultWindow)
	}

	if until.Before(since) {
		return time.Time{}, time.Time{}, errors.New("until 不能早于 since")
	}
	if maxWindow > 0 && until.Sub(since) > maxWindow {
		return time.Time{}, time.Time{}, fmt.Errorf("时间范围不能超过 %d 天（%s）", int(maxWindow/(24*time.Hour)), maxWindowReason)
	}
	return since, until, nil
}
