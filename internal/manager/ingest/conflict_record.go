package ingest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func conflictRecord(event *iproto.Event) (model.Conflict, error) {
	d := event.GetConflict()
	rc := d.GetChange()
	targetRow := d.GetTargetRow()
	if d.GetRedactedChange() != nil {
		if rc != nil || targetRow != nil {
			return model.Conflict{}, fmt.Errorf("ingest: conflict mixes original and redacted images")
		}
		rc, targetRow = d.GetRedactedChange(), d.GetRedactedTargetRow()
	} else if d.GetRedactedTargetRow() != nil {
		return model.Conflict{}, fmt.Errorf("ingest: redacted target requires a redacted change")
	}
	if rc == nil || rc.GetSchema() == "" || rc.GetTable() == "" || len(rc.GetColumnNames()) == 0 || len(rc.GetPrimaryKeyColumns()) == 0 {
		return model.Conflict{}, fmt.Errorf("ingest: conflict detail requires a table and positioning key")
	}
	id, err := uuidx.Parse(event.GetEventId())
	if err != nil || id.IsZero() {
		return model.Conflict{}, fmt.Errorf("ingest: conflict event requires a valid event UUID")
	}
	reason := model.ConflictType(d.GetReason())
	if !reason.Valid() {
		return model.Conflict{}, fmt.Errorf("ingest: invalid conflict reason")
	}
	// Changed-key UPDATE may collide with the new INSERT key, retaining its
	// original change type while explicitly reporting insert_duplicate.
	compatible := rc.GetType() == iproto.ChangeType_CHANGE_TYPE_INSERT && reason == model.ConflictTypeInsertDuplicate || rc.GetType() == iproto.ChangeType_CHANGE_TYPE_UPDATE && (reason == model.ConflictTypeInsertDuplicate || reason == model.ConflictTypeUpdateMissing || reason == model.ConflictTypeUpdateMismatch) || rc.GetType() == iproto.ChangeType_CHANGE_TYPE_DELETE && (reason == model.ConflictTypeDeleteMissing || reason == model.ConflictTypeDeleteMismatch)
	if reason == model.ConflictTypeLWWRejected && d.GetPolicy() == iproto.ConflictMode_CONFLICT_MODE_LWW && (rc.GetType() == iproto.ChangeType_CHANGE_TYPE_INSERT || rc.GetType() == iproto.ChangeType_CHANGE_TYPE_UPDATE || rc.GetType() == iproto.ChangeType_CHANGE_TYPE_DELETE) {
		compatible = true
	}
	if !compatible {
		return model.Conflict{}, fmt.Errorf("ingest: conflict reason does not match row change")
	}
	resolution := model.ConflictResolution("")
	switch d.GetPolicy() {
	case iproto.ConflictMode_CONFLICT_MODE_UNSPECIFIED, iproto.ConflictMode_CONFLICT_MODE_SOURCE_WINS:
		if d.GetResolution() == iproto.ConflictResolution_CONFLICT_RESOLUTION_APPLIED {
			resolution = model.ConflictResolutionSourceWins
		}
	case iproto.ConflictMode_CONFLICT_MODE_LWW:
		switch d.GetResolution() {
		case iproto.ConflictResolution_CONFLICT_RESOLUTION_APPLIED:
			resolution = model.ConflictResolutionSourceWins
		case iproto.ConflictResolution_CONFLICT_RESOLUTION_DISCARDED:
			resolution = model.ConflictResolutionTargetWins
		case iproto.ConflictResolution_CONFLICT_RESOLUTION_TXN_ROLLED_BACK:
			resolution = model.ConflictResolutionSkipped
		}
	case iproto.ConflictMode_CONFLICT_MODE_STOP:
		if d.GetResolution() == iproto.ConflictResolution_CONFLICT_RESOLUTION_TXN_ROLLED_BACK {
			resolution = model.ConflictResolutionStopped
		}
	}
	if !resolution.Valid() {
		return model.Conflict{}, fmt.Errorf("ingest: invalid conflict policy or resolution")
	}
	before, err := conflictRowJSON(rc.GetColumnNames(), rc.GetBefore())
	if err != nil {
		return model.Conflict{}, err
	}
	after, err := conflictRowJSON(rc.GetColumnNames(), rc.GetAfter())
	if err != nil {
		return model.Conflict{}, err
	}
	target, err := conflictRowJSON(rc.GetColumnNames(), targetRow)
	if err != nil {
		return model.Conflict{}, err
	}
	if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_INSERT && after == nil || rc.GetType() == iproto.ChangeType_CHANGE_TYPE_UPDATE && (before == nil || after == nil) || rc.GetType() == iproto.ChangeType_CHANGE_TYPE_DELETE && before == nil {
		return model.Conflict{}, fmt.Errorf("ingest: conflict detail is missing a row image")
	}
	keyRow := before
	if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_INSERT || reason == model.ConflictTypeInsertDuplicate {
		keyRow = after
	}
	var values map[string]any
	if err := json.Unmarshal(keyRow, &values); err != nil {
		return model.Conflict{}, err
	}
	key := make(map[string]any, len(rc.GetPrimaryKeyColumns()))
	for _, name := range rc.GetPrimaryKeyColumns() {
		v, ok := values[name]
		if !ok || v == nil {
			return model.Conflict{}, fmt.Errorf("ingest: conflict key is not in row image")
		}
		if _, exists := key[name]; exists {
			return model.Conflict{}, fmt.Errorf("ingest: duplicate conflict key column")
		}
		key[name] = v
	}
	pk, err := json.Marshal(key)
	if err != nil {
		return model.Conflict{}, err
	}
	source := after
	if source == nil {
		source = before
	}
	return model.Conflict{ID: id, SchemaName: rc.GetSchema(), Table: rc.GetTable(), PK: pk, ConflictType: reason, Resolution: resolution, SourceRow: source, TargetRow: target}, nil
}

func conflictRowJSON(columns []string, row *iproto.Row) (model.JSON, error) {
	if row == nil {
		return nil, nil
	}
	if len(row.GetValues()) != len(columns) {
		return nil, fmt.Errorf("ingest: conflict row width does not match columns")
	}
	values := make(map[string]any, len(columns))
	for i, name := range columns {
		if name == "" {
			return nil, fmt.Errorf("ingest: conflict column name is empty")
		}
		if _, exists := values[name]; exists {
			return nil, fmt.Errorf("ingest: duplicate conflict column")
		}
		v := row.Values[i]
		if v == nil || v.GetValue() == nil {
			return nil, fmt.Errorf("ingest: missing conflict column value")
		}
		var value any
		switch x := v.GetValue().(type) {
		case *iproto.ColumnValue_IsNull:
			if !x.IsNull {
				return nil, fmt.Errorf("ingest: invalid conflict NULL value")
			}
		case *iproto.ColumnValue_I64:
			value = strconv.FormatInt(x.I64, 10)
		case *iproto.ColumnValue_U64:
			value = strconv.FormatUint(x.U64, 10)
		case *iproto.ColumnValue_F64:
			value = x.F64
		case *iproto.ColumnValue_Decimal:
			value = x.Decimal
		case *iproto.ColumnValue_RawBytes:
			value = base64.StdEncoding.EncodeToString(x.RawBytes)
		case *iproto.ColumnValue_Text:
			value = x.Text
		case *iproto.ColumnValue_TemporalUsec:
			value = strconv.FormatInt(x.TemporalUsec, 10)
			if v.GetZeroDate() {
				value = "0000-00-00"
			}
		default:
			return nil, fmt.Errorf("ingest: unsupported conflict column value")
		}
		values[name] = value
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("ingest: encode conflict row failed")
	}
	return model.JSON(redact.PotentialCredentials(string(raw))), nil
}
