package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xhive/chanrpc"
)

const (
	MaxTransferBytes   = 8 << 20
	MaxTransferRecords = 16384
	transferTimeout    = 30 * time.Second
)

// Selection is supplied by the stopped role's owner, never by a wire client.
// Exactly one of ID and Prefix is present. Identity/configuration are excluded.
type StateSelection struct {
	Collection string
	ID         string
	Prefix     string
}

// StateRecord preserves the published BSON field names and numeric types.
type StateRecord struct {
	Collection string
	ID         string
	Data       []byte
}

type StateExportRequest struct {
	Context    context.Context
	Selections []StateSelection
}
type StateExportAck struct{ Records []StateRecord }

type StateImportRequest struct {
	Context context.Context
	Marker  string
	Version uint64
	Digest  string
	Records []StateRecord
}

type transferMarker struct {
	Version uint64 `bson:"version"`
	Digest  string `bson:"digest"`
}

func transferCollection(name string) bool {
	switch name {
	case "snapshot_runs", "snapshot_chunks", "snapshot_targets", "checkpoints", "schema_history", "schema_migrations", "publication_window_cursors", "skipped_transactions", "skipped_transaction_sets":
		return true
	}
	return false
}

func immutableTransferCollection(name string) bool {
	switch name {
	case "snapshot_runs", "snapshot_chunks", "snapshot_targets", "schema_migrations":
		return true
	}
	return false
}

func cloneStateRecords(records []StateRecord) []StateRecord {
	copy := slices.Clone(records)
	for i := range copy {
		copy[i].Data = bytes.Clone(copy[i].Data)
	}
	return copy
}

// StateDigest includes lengths and identifiers to avoid ambiguous concatenation.
// Input order is irrelevant; the actual document bytes remain authoritative.
func StateDigest(records []StateRecord) (string, error) {
	if len(records) > MaxTransferRecords {
		return "", errors.New("store: transfer record count outside limit")
	}
	ordered := slices.Clone(records)
	slices.SortFunc(ordered, func(a, b StateRecord) int {
		if order := strings.Compare(a.Collection, b.Collection); order != 0 {
			return order
		}
		return strings.Compare(a.ID, b.ID)
	})
	hash := sha256.New()
	var length [8]byte
	total := 0
	for i, record := range ordered {
		if !transferCollection(record.Collection) || record.ID == "" || len(record.ID) > 512 || len(record.Data) == 0 {
			return "", errors.New("store: invalid transfer record identity")
		}
		if i > 0 && record.Collection == ordered[i-1].Collection && record.ID == ordered[i-1].ID {
			return "", errors.New("store: duplicate transfer record")
		}
		if len(record.Data)+len(record.ID)+len(record.Collection) > MaxTransferBytes-total {
			return "", errors.New("store: transfer byte limit exceeded")
		}
		total += len(record.Data) + len(record.ID) + len(record.Collection)
		for _, part := range [][]byte{[]byte(record.Collection), []byte(record.ID), record.Data} {
			binary.BigEndian.PutUint64(length[:], uint64(len(part)))
			hash.Write(length[:])
			hash.Write(part)
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validateStateSelections(selections []StateSelection) error {
	if len(selections) == 0 || len(selections) > 16 {
		return errors.New("store: transfer selection count outside limit")
	}
	for _, selection := range selections {
		if !transferCollection(selection.Collection) || (selection.ID == "") == (selection.Prefix == "") || len(selection.ID)+len(selection.Prefix) > 512 {
			return errors.New("store: invalid transfer selection")
		}
	}
	return nil
}

func (m *Module) onStateExport(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.RequestAs[StateExportRequest]()
	if !ok || req.Context == nil {
		return &chanrpc.RetInfo{Err: errors.New("store: invalid transfer export request")}
	}
	if err := validateStateSelections(req.Selections); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}
	selections := slices.Clone(req.Selections)
	reply := ci.Hold()
	var records []StateRecord
	var err error
	m.flush()
	m.linear.Go(func() { records, err = m.exportState(req.Context, selections) }, func() {
		_ = reply.Ret(&chanrpc.RetInfo{Ack: StateExportAck{Records: records}, Err: err})
	})
	return nil
}

func (m *Module) exportState(ctx context.Context, selections []StateSelection) ([]StateRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, transferTimeout)
	defer cancel()
	var result []StateRecord
	err := m.stateDB.Transaction(ctx, func(tx *xdoc.Tx) error {
		result = nil // Transaction may retry a deadlock; no partial result escapes.
		seen := map[[2]string]bool{}
		total := 0
		for _, selection := range selections {
			query := tx.Collection(selection.Collection).Query().OrderBy("_id").Limit(MaxTransferRecords + 1)
			if selection.ID != "" {
				query = query.Where("_id = @id").Param("id", selection.ID)
			} else {
				query = query.Where("_id >= @lower AND _id < @upper").Param("lower", selection.Prefix).Param("upper", selection.Prefix+"\uffff")
			}
			for document, err := range query.All(ctx) {
				if err != nil {
					return err
				}
				id, ok := document.Get("_id").AsString()
				if !ok || selection.ID != "" && id != selection.ID || selection.Prefix != "" && !strings.HasPrefix(id, selection.Prefix) {
					return errors.New("store: transfer query returned invalid identity")
				}
				key := [2]string{selection.Collection, id}
				if seen[key] {
					continue
				}
				seen[key] = true
				if len(result) == MaxTransferRecords {
					return errors.New("store: transfer record count outside limit")
				}
				data, err := document.Encode()
				if err != nil {
					return err
				}
				if len(data)+len(id)+len(selection.Collection) > MaxTransferBytes-total {
					return errors.New("store: transfer byte limit exceeded")
				}
				total += len(data) + len(id) + len(selection.Collection)
				result = append(result, StateRecord{Collection: selection.Collection, ID: id, Data: data})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *Module) onStateImport(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.RequestAs[StateImportRequest]()
	if !ok || req.Context == nil || !strings.HasPrefix(req.Marker, "state_migration:") || len(req.Marker) > 512 || req.Version == 0 {
		return &chanrpc.RetInfo{Err: errors.New("store: invalid transfer import request")}
	}
	digest, err := StateDigest(req.Records)
	if err != nil || req.Digest != digest {
		return &chanrpc.RetInfo{Err: errors.New("store: invalid transfer import digest or records")}
	}
	req.Records = cloneStateRecords(req.Records)
	reply := ci.Hold()
	m.flush()
	m.linear.Go(func() { err = m.importState(req) }, func() {
		_ = reply.Ret(&chanrpc.RetInfo{Ack: OpAck{Committed: err == nil}, Err: err})
	})
	return nil
}

func (m *Module) importState(req StateImportRequest) error {
	ctx, cancel := context.WithTimeout(req.Context, transferTimeout)
	defer cancel()
	return m.stateDB.Transaction(ctx, func(tx *xdoc.Tx) error {
		markers := tx.Collection("runtime")
		existing, err := markers.FindByID(ctx, xdoc.String(req.Marker))
		if err == nil {
			var prior transferMarker
			if xdoc.UnmarshalValue(existing.Value(), &prior) != nil || prior.Version == 0 || len(prior.Digest) != 64 {
				return errors.New("store: invalid stored transfer marker")
			}
			if prior.Version > req.Version || prior.Version == req.Version && prior.Digest != req.Digest {
				return errors.New("store: transfer marker conflict or stale version")
			}
			if prior.Version == req.Version {
				return nil // Keep all progress written after the first import.
			}
		} else if !errors.Is(err, xdoc.ErrNotFound) {
			return err
		}
		for _, record := range req.Records {
			document, err := xdoc.DecodeDocument(record.Data)
			if err != nil {
				return errors.New("store: invalid transfer document encoding")
			}
			id, ok := document.Get("_id").AsString()
			encoded, err := document.Encode()
			if !ok || id != record.ID || err != nil || !bytes.Equal(encoded, record.Data) {
				return errors.New("store: noncanonical transfer document or mismatched identity")
			}
			collection := tx.Collection(record.Collection)
			if immutableTransferCollection(record.Collection) && (record.Collection != "schema_migrations" || !strings.HasSuffix(record.ID, ".reinit")) {
				previous, err := collection.FindByID(ctx, xdoc.String(record.ID))
				if err == nil {
					data, err := previous.Encode()
					if err != nil || !bytes.Equal(data, record.Data) {
						return fmt.Errorf("store: immutable transfer document conflict in %s", record.Collection)
					}
					continue
				}
				if !errors.Is(err, xdoc.ErrNotFound) {
					return err
				}
			}
			if _, err := collection.Upsert(ctx, document); err != nil {
				return err
			}
		}
		marker, err := buildDoc(Op{Collection: "runtime", ID: req.Marker, Doc: transferMarker{Version: req.Version, Digest: req.Digest}})
		if err != nil {
			return err
		}
		_, err = markers.Upsert(ctx, marker)
		return err
	})
}

// These calls belong to background workflows. Each call owns a response client;
// no caller shares a mutable RPC response queue with another goroutine.
func (c *Client) ExportState(ctx context.Context, selections []StateSelection) ([]StateRecord, error) {
	if ctx == nil {
		return nil, errors.New("store: transfer context required")
	}
	if err := validateStateSelections(selections); err != nil {
		return nil, err
	}
	client := chanrpc.NewClient()
	defer client.Close()
	ret := client.CallWithContext(ctx, c.server, StateExportRequest{Context: ctx, Selections: slices.Clone(selections)})
	if ret == nil {
		return nil, errors.New("store: transfer export response missing")
	}
	if ret.Err != nil {
		return nil, ret.Err
	}
	ack, ok := ret.AckAs[StateExportAck]()
	if !ok {
		return nil, errors.New("store: transfer export response type invalid")
	}
	return ack.Records, nil
}

func (c *Client) ImportState(ctx context.Context, marker string, version uint64, digest string, records []StateRecord) error {
	if ctx == nil {
		return errors.New("store: transfer context required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	actual, err := StateDigest(records)
	if err != nil || actual != digest {
		return errors.New("store: invalid transfer import digest or records")
	}
	client := chanrpc.NewClient()
	defer client.Close()
	ret := client.CallWithContext(ctx, c.server, StateImportRequest{Context: ctx, Marker: marker, Version: version, Digest: digest, Records: cloneStateRecords(records)})
	if ret == nil {
		return errors.New("store: transfer import response missing")
	}
	if ret.Err != nil {
		return ret.Err
	}
	ack, ok := ret.AckAs[OpAck]()
	if !ok || !ack.Committed {
		return errors.New("store: transfer import not committed")
	}
	return nil
}
