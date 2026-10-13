package iproto

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"uuid"

	"github.com/xmapst/xdoc"
	"google.golang.org/protobuf/proto"
)

const MaxStateBundleBytes = 8 << 20
const MaxStateBundleRecords = 16384

// SnapshotStateChunkLimit reads the original authenticated plan and its
// append-only extensions. It never derives chunk ranges from today's tables.
func SnapshotStateChunkLimit(bundle *PipelineStateBundle) (uint64, error) {
	if err := ValidateStateBundle(bundle); err != nil {
		return 0, err
	}
	if bundle.Role != PipelineRole_PIPELINE_ROLE_SOURCE {
		return 0, errors.New("iproto: snapshot chunk range requires source state")
	}
	base := fmt.Sprintf("%d:%s:%d", len(bundle.PipelineId), bundle.PipelineId, bundle.RunId)
	var limit uint64
	for _, record := range bundle.Records {
		if record.Collection != "snapshot_chunks" || record.Id != base && !strings.HasPrefix(record.Id, base+".extension.") {
			continue
		}
		doc, err := xdoc.DecodeDocument(record.Bson)
		if err != nil {
			return 0, err
		}
		total, valid := doc.Get("total").AsInt64()
		if !valid || total <= 0 || total > 1<<63-2 {
			return 0, errors.New("iproto: invalid saved snapshot chunk range")
		}
		limit = max(limit, uint64(total))
	}
	if limit == 0 {
		return 0, errors.New("iproto: saved snapshot chunk range missing")
	}
	return limit, nil
}

func canonicalStateUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil() && id.String() == value
}

// The digest binds provenance and destination as well as exact BSON bytes.
// Callers own the input; no field is changed while computing its identity.
func StateBundleDigest(bundle *PipelineStateBundle) (string, error) {
	if bundle == nil || len(bundle.GetRecords()) > MaxStateBundleRecords || proto.Size(bundle) > MaxStateBundleBytes {
		return "", errors.New("iproto: state bundle exceeds limits")
	}
	owned := proto.Clone(bundle).(*PipelineStateBundle)
	owned.Digest = ""
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(owned)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func stateRecordInScope(bundle *PipelineStateBundle, record *PipelineStateRecord) bool {
	base := fmt.Sprintf("%d:%s:%d", len(bundle.PipelineId), bundle.PipelineId, bundle.RunId)
	switch bundle.Role {
	case PipelineRole_PIPELINE_ROLE_SOURCE:
		switch record.Collection {
		case "checkpoints":
			return record.Id == bundle.PipelineId+".source"
		case "snapshot_runs":
			return record.Id == base+".start" || record.Id == base+".end" || strings.HasPrefix(record.Id, base+".creation.") || strings.HasPrefix(record.Id, base+".recovery.")
		case "snapshot_chunks":
			return record.Id == base || strings.HasPrefix(record.Id, base+".extension.") || strings.HasPrefix(record.Id, base+".published.") || strings.HasPrefix(record.Id, base+".recovery.")
		case "schema_history":
			return record.Id == fmt.Sprintf("%s.source.history.%d", bundle.PipelineId, bundle.RunId) || strings.HasPrefix(record.Id, fmt.Sprintf("%s.source.history-version.%d.", bundle.PipelineId, bundle.RunId)) || bundle.RunId == 0 && record.Id == bundle.PipelineId+".source"
		}
	case PipelineRole_PIPELINE_ROLE_TARGET:
		switch record.Collection {
		case "checkpoints":
			return record.Id == bundle.PipelineId+".target"
		case "snapshot_targets":
			return record.Id == base || strings.HasPrefix(record.Id, base+".recovery.") || strings.HasPrefix(record.Id, base+".recovery_deferred.")
		case "skipped_transaction_sets":
			return record.Id == base
		case "schema_migrations":
			return strings.HasPrefix(record.Id, base+".")
		case "publication_window_cursors", "skipped_transactions":
			return strings.HasPrefix(record.Id, base+":")
		}
	}
	return false
}

func ValidateStateBundle(bundle *PipelineStateBundle) error {
	if bundle == nil || !canonicalStateUUID(bundle.PipelineId) || !canonicalStateUUID(bundle.SnapshotUuid) || !canonicalStateUUID(bundle.SourceNodeId) || !canonicalStateUUID(bundle.DestinationNodeId) || bundle.SourceNodeId == bundle.DestinationNodeId || bundle.RunId > math.MaxInt64 || bundle.ConfigVersion == 0 || bundle.ConfigVersion > math.MaxInt64 || bundle.Role != PipelineRole_PIPELINE_ROLE_SOURCE && bundle.Role != PipelineRole_PIPELINE_ROLE_TARGET {
		return errors.New("iproto: invalid state bundle identity")
	}
	digest, err := StateBundleDigest(bundle)
	if err != nil || digest != bundle.Digest {
		return errors.New("iproto: invalid state bundle digest or limits")
	}
	base := fmt.Sprintf("%d:%s:%d", len(bundle.PipelineId), bundle.PipelineId, bundle.RunId)
	start, plan := false, false
	previous := ""
	for _, record := range bundle.Records {
		if record == nil || len(record.Id) > 512 || !stateRecordInScope(bundle, record) || len(record.Bson) == 0 {
			return errors.New("iproto: state bundle record outside role scope")
		}
		key := record.Collection + "\x00" + record.Id
		if key <= previous {
			return errors.New("iproto: state bundle records must be unique and sorted")
		}
		previous = key
		document, err := xdoc.DecodeDocument(record.Bson)
		if err != nil {
			return errors.New("iproto: invalid state bundle BSON")
		}
		id, valid := document.Get("_id").AsString()
		encoded, err := document.Encode()
		if !valid || id != record.Id || err != nil || !bytes.Equal(encoded, record.Bson) {
			return errors.New("iproto: state bundle record encoding or identity mismatch")
		}
		// Old schema-history records deliberately retain capitalized disk keys.
		for _, field := range []string{"pipeline_id", "PipelineID"} {
			value := document.Get(field)
			if !document.Has(field) {
				continue
			}
			valueID, ok := value.AsString()
			if !ok || valueID != bundle.PipelineId {
				return errors.New("iproto: foreign pipeline in state bundle document")
			}
		}
		for _, field := range []string{"run_id", "RunID"} {
			value := document.Get(field)
			if !document.Has(field) {
				continue
			}
			if number, ok := value.AsInt64(); ok && number >= 0 && uint64(number) == bundle.RunId {
				continue
			}
			if text, ok := value.AsString(); ok {
				expected := strconv.FormatUint(bundle.RunId, 10)
				if suffix, recovery := strings.CutPrefix(record.Id, base+".recovery."); recovery && record.Collection == "snapshot_chunks" {
					sequence, _, _ := strings.Cut(suffix, ".")
					number, err := strconv.ParseUint(sequence, 10, 64)
					if err != nil || number == 0 || number > 4096 || sequence != strconv.FormatUint(number, 10) {
						return errors.New("iproto: invalid recovery plan identity")
					}
					expected += ".recovery." + sequence
				}
				if text == expected {
					continue
				}
			}
			return errors.New("iproto: foreign run in state bundle document")
		}
		if record.Collection == "snapshot_runs" && record.Id == base+".start" {
			identity, ok := document.Get("snapshot_uuid").AsString()
			if !ok || identity != bundle.SnapshotUuid {
				return errors.New("iproto: snapshot identity differs in state bundle")
			}
			start = true
		}
		if record.Collection == "snapshot_chunks" && record.Id == base {
			plan = true
		}
		if record.Collection == "snapshot_runs" && strings.HasPrefix(record.Id, base+".creation.") {
			var creation struct {
				PipelineID  string `bson:"pipeline_id"`
				RunID       uint64 `bson:"run_id"`
				UUID        string `bson:"uuid"`
				Schema      string `bson:"schema"`
				Table       string `bson:"table"`
				Transaction []byte `bson:"transaction"`
			}
			if err := xdoc.UnmarshalValue(document.Value(), &creation); err != nil || creation.PipelineID != bundle.PipelineId || creation.RunID != bundle.RunId || creation.UUID != bundle.SnapshotUuid || record.Id != fmt.Sprintf("%s.creation.%d:%s%s", base, len(creation.Schema), creation.Schema, creation.Table) || len(creation.Transaction) == 0 || len(creation.Transaction) > 1<<20 {
				return errors.New("iproto: invalid snapshot creation record identity")
			}
			txn := new(Txn)
			if err := proto.Unmarshal(creation.Transaction, txn); err != nil || ValidateSnapshotCreation(txn, creation.Schema, creation.Table) != nil {
				return errors.New("iproto: invalid snapshot creation record transaction")
			}
		}
		if record.Collection == "snapshot_targets" && record.Id == base || record.Collection == "snapshot_chunks" && strings.HasPrefix(record.Id, base+".published.") {
			identity, ok := document.Get("uuid").AsString()
			if !ok || identity != bundle.SnapshotUuid {
				return errors.New("iproto: snapshot record UUID differs in state bundle")
			}
		}
	}
	if bundle.Role == PipelineRole_PIPELINE_ROLE_SOURCE && (!start || !plan) {
		return errors.New("iproto: source snapshot migration requires original start and plan")
	}
	return nil
}
