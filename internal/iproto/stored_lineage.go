package iproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"uuid"
)

const StoredLineageVersion uint32 = 1
const MaxStoredLineageBytes = 64 << 10
const ReservedLineageSchema = "xdtsmeta"

// StoredDatabaseLineage is the independently owned v1 database record. Paths
// describe known visits; Complete=false records that the source did not provide
// a complete route proof.
type StoredDatabaseLineage struct {
	Version      uint32   `json:"version"`
	GTID         string   `json:"gtid"`
	OriginCenter string   `json:"origin_center"`
	Path         []string `json:"path"`
	EndpointPath []string `json:"endpoint_path"`
	Hops         uint32   `json:"hops"`
	Complete     bool     `json:"complete"`
}

func canonicalLineageGTID(text string) (string, error) {
	sid, sequence, ok := strings.Cut(text, ":")
	if !ok || len(sid) != 36 || strings.Contains(sequence, ":") {
		return "", errors.New("iproto: lineage requires a single GTID")
	}
	id, err := uuid.Parse(sid)
	if err != nil || id == uuid.Nil() {
		return "", errors.New("iproto: invalid lineage GTID UUID")
	}
	n, err := strconv.ParseUint(sequence, 10, 64)
	if err != nil || n == 0 || n > math.MaxInt64 || strconv.FormatUint(n, 10) != sequence {
		return "", errors.New("iproto: invalid lineage GTID sequence")
	}
	return id.String() + ":" + sequence, nil
}

func (record StoredDatabaseLineage) Validate() error {
	gtid, err := canonicalLineageGTID(record.GTID)
	if err != nil || gtid != record.GTID || record.Version != StoredLineageVersion {
		return errors.New("iproto: invalid stored lineage identity or version")
	}
	if len(record.Path) == 0 || len(record.Path) > 256 || record.Path[0] != record.OriginCenter {
		return errors.New("iproto: invalid stored center lineage")
	}
	seen := make(map[string]bool, len(record.Path))
	for _, center := range record.Path {
		if center == "" || strings.TrimSpace(center) != center || len(center) > 64 || seen[center] {
			return errors.New("iproto: invalid or repeated stored center")
		}
		seen[center] = true
	}
	if len(record.EndpointPath) == 0 || ValidateTxnLineage(&Txn{EndpointPath: record.EndpointPath}) != nil {
		return errors.New("iproto: invalid stored endpoint lineage")
	}
	if record.Hops == 0 || record.Hops > 256 || record.Hops < uint32(max(len(record.Path), len(record.EndpointPath))) {
		return errors.New("iproto: invalid stored lineage hops")
	}
	return nil
}

// CaptureTargetLineage appends a visit only after the existing route guard has
// authorized it. The caller persists the returned value with the target write.
func CaptureTargetLineage(txn *Txn, route DatabaseLineageRoute) (StoredDatabaseLineage, error) {
	if !route.Persistent || route.Validate() != nil || txn == nil || !route.AllowsTxn(txn) {
		return StoredDatabaseLineage{}, errors.New("iproto: target lineage route denied")
	}
	gtid, err := canonicalLineageGTID(txn.GetGtid())
	if err != nil {
		return StoredDatabaseLineage{}, err
	}
	record := StoredDatabaseLineage{Version: StoredLineageVersion, GTID: gtid,
		OriginCenter: txn.GetOriginCenter(), Path: slices.Clone(txn.GetPath()),
		EndpointPath: slices.Clone(txn.GetEndpointPath()), Hops: txn.GetHops(), Complete: txn.GetLineageVersion() == StoredLineageVersion}
	if len(record.Path) == 0 || record.Path[len(record.Path)-1] != route.SourceCenter {
		return StoredDatabaseLineage{}, errors.New("iproto: lineage does not end at source center")
	}
	if len(record.EndpointPath) == 0 {
		return StoredDatabaseLineage{}, errors.New("iproto: lineage lacks source database history")
	} else if record.EndpointPath[len(record.EndpointPath)-1] != route.SourceServerUUID {
		return StoredDatabaseLineage{}, errors.New("iproto: lineage does not end at source database")
	}
	record.Hops = max(record.Hops, uint32(max(len(record.Path), len(record.EndpointPath)))) + 1
	record.EndpointPath = append(record.EndpointPath, route.TargetServerUUID)
	if route.SourceCenter != route.TargetCenter {
		record.Path = append(record.Path, route.TargetCenter)
	}
	return record, record.Validate()
}

// SourceTxn returns a fresh value for a source session. Restoring metadata to a
// different physical database adds that visit; a previously visited database or
// center cannot be silently reintroduced through a restore.
func (record StoredDatabaseLineage) SourceTxn(center, endpoint string) (*Txn, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(center) != center || center == "" || !canonicalDatabaseUUID(endpoint) {
		return nil, errors.New("iproto: invalid source lineage identity")
	}
	path, endpoints := slices.Clone(record.Path), slices.Clone(record.EndpointPath)
	hops := record.Hops
	if endpoints[len(endpoints)-1] != endpoint {
		if slices.Contains(endpoints, endpoint) {
			return nil, errors.New("iproto: restored database was already visited")
		}
		endpoints = append(endpoints, endpoint)
		hops++
	}
	if path[len(path)-1] != center {
		if slices.Contains(path, center) {
			return nil, errors.New("iproto: restored center was already visited")
		}
		path = append(path, center)
		hops = max(hops, uint32(len(path)))
	}
	copy := record
	copy.Path, copy.EndpointPath, copy.Hops = path, endpoints, hops
	if err := copy.Validate(); err != nil {
		return nil, err
	}
	version := uint32(0)
	if record.Complete {
		version = StoredLineageVersion
	}
	return &Txn{Gtid: record.GTID, OriginCenter: record.OriginCenter, Path: path, EndpointPath: endpoints, Hops: hops, LineageVersion: version}, nil
}

func (record StoredDatabaseLineage) Encode() ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(record)
	if err != nil || len(body) > MaxStoredLineageBytes {
		return nil, errors.New("iproto: lineage record encoding exceeds its bound")
	}
	return body, nil
}

func DecodeStoredDatabaseLineage(body []byte, expectedGTID string) (StoredDatabaseLineage, error) {
	if len(body) == 0 || len(body) > MaxStoredLineageBytes {
		return StoredDatabaseLineage{}, errors.New("iproto: invalid lineage record size")
	}
	// encoding/json accepts repeated keys and case-insensitive field names.
	// Check the exact wire keys before decoding this durable authority record.
	keys := json.NewDecoder(bytes.NewReader(body))
	start, err := keys.Token()
	if err != nil || start != json.Delim('{') {
		return StoredDatabaseLineage{}, errors.New("iproto: invalid lineage record object")
	}
	seen := make(map[string]bool, 8)
	for keys.More() {
		key, err := keys.Token()
		name, ok := key.(string)
		if err != nil || !ok || seen[name] || !slices.Contains([]string{"version", "gtid", "origin_center", "path", "endpoint_path", "hops", "complete"}, name) {
			return StoredDatabaseLineage{}, errors.New("iproto: ambiguous lineage record fields")
		}
		seen[name] = true
		var value json.RawMessage
		if keys.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return StoredDatabaseLineage{}, errors.New("iproto: invalid lineage record value")
		}
	}
	if len(seen) != 7 {
		return StoredDatabaseLineage{}, errors.New("iproto: incomplete lineage record fields")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var record StoredDatabaseLineage
	if err := decoder.Decode(&record); err != nil {
		return StoredDatabaseLineage{}, errors.New("iproto: invalid lineage record encoding")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return StoredDatabaseLineage{}, errors.New("iproto: trailing lineage record data")
	}
	gtid, err := canonicalLineageGTID(expectedGTID)
	if err != nil || gtid != record.GTID {
		return StoredDatabaseLineage{}, errors.New("iproto: lineage record belongs to a different GTID")
	}
	return record, record.Validate()
}
