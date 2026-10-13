package config

import "time"

const (
	DefaultRetentionLogs      = 15 * 24 * time.Hour
	DefaultRetentionMetrics   = 15 * 24 * time.Hour
	DefaultRetentionEvents    = 30 * 24 * time.Hour
	DefaultRetentionConflicts = 30 * 24 * time.Hour
)
