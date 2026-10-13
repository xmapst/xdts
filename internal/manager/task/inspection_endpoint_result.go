package task

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type endpointProbeDetail struct {
	TaskID                       string   `json:"taskId"`
	Reachable                    bool     `json:"reachable"`
	Error                        *string  `json:"error"`
	Version                      *string  `json:"version"`
	ServerUUID                   *string  `json:"serverUuid"`
	GroupUUID                    *string  `json:"groupUuid"`
	LowerCaseTableNames          *string  `json:"lowerCaseTableNames"`
	GTIDMode                     *string  `json:"gtidMode"`
	EnforceGTIDConsistency       *string  `json:"enforceGtidConsistency"`
	LogBin                       *string  `json:"logBin"`
	BinlogFormat                 *string  `json:"binlogFormat"`
	BinlogRowImage               *string  `json:"binlogRowImage"`
	BinlogRowMetadata            *string  `json:"binlogRowMetadata"`
	BinlogExpireLogsSeconds      *int64   `json:"binlogExpireLogsSeconds"`
	BinlogRowValueOptions        *string  `json:"binlogRowValueOptions"`
	BinlogTransactionCompression *string  `json:"binlogTransactionCompression"`
	Grants                       []string `json:"grants"`
}

func endpointProbeSuccess(taskID string, result *iproto.ConnTestResult, lctn mysqlx.LowerCaseTableNames) endpointProbeDetail {
	params := result.GetKeyParams()
	var retention *int64
	if seconds, known := mysqlx.BinlogRetentionSeconds(params); known && seconds <= uint64(1<<63-1) {
		value := int64(seconds)
		retention = &value
	}
	grants := append([]string{}, result.GetGrants()...)
	return endpointProbeDetail{
		TaskID: taskID, Reachable: true, Version: optionalProbeValue(result.GetVersion()),
		ServerUUID:                   optionalProbeValue(result.GetServerUuid()),
		GroupUUID:                    optionalProbeValue(params["group_replication_group_name"]),
		LowerCaseTableNames:          optionalProbeValue(lctn.String()),
		GTIDMode:                     optionalProbeValue(params["gtid_mode"]),
		EnforceGTIDConsistency:       optionalProbeValue(params["enforce_gtid_consistency"]),
		LogBin:                       optionalProbeValue(params["log_bin"]),
		BinlogFormat:                 optionalProbeValue(params["binlog_format"]),
		BinlogRowImage:               optionalProbeValue(params["binlog_row_image"]),
		BinlogRowMetadata:            optionalProbeValue(params["binlog_row_metadata"]),
		BinlogExpireLogsSeconds:      retention,
		BinlogRowValueOptions:        optionalProbeValue(params["binlog_row_value_options"]),
		BinlogTransactionCompression: optionalProbeValue(params["binlog_transaction_compression"]),
		Grants:                       grants,
	}
}

func optionalProbeValue(raw string) *string {
	if raw == "" {
		return nil
	}
	return &raw
}

func (l *InspectionLoop) persistEndpointFailure(ctx context.Context, endpointID, nodeID uuidx.UUID, now time.Time, taskID, code, message string) error {
	if code == "" {
		code = "PROBE_FAILED"
	}
	message = redact.PotentialCredentials(strings.TrimSpace(message))
	if message == "" {
		message = "端点探测失败"
	}
	message = fmt.Sprintf("%s: %s", code, message)
	detail, err := json.Marshal(endpointProbeDetail{
		TaskID: taskID, Reachable: false, Error: &message, Grants: []string{},
	})
	if err != nil {
		return err
	}
	return repo.UpdateEndpointProbeFailure(ctx, l.db, endpointID, nodeID, now, model.JSON(detail))
}
