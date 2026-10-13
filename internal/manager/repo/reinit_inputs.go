package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm/clause"
)

// ReinitInputsDigest binds the preview to the actual pipeline inputs. Only the
// hash is persisted or exposed; endpoint secrets never enter the document.
func ReinitInputsDigest(p *model.Pipeline, source, target *model.Endpoint, rules model.JSON, readEndpoint ...*model.Endpoint) (string, error) {
	if p == nil || source == nil || target == nil || p.SourceEndpointID != source.ID || p.TargetEndpointID != target.ID {
		return "", ErrReinitConflict
	}
	decode := func(input []byte, out *any) error {
		if !json.Valid(input) {
			return ErrReinitConflict
		}
		decoder := json.NewDecoder(bytes.NewReader(input))
		decoder.UseNumber()
		if err := decoder.Decode(out); err != nil {
			return ErrReinitConflict
		}
		return nil
	}
	type endpointInput struct {
		ID, CenterID                   uuidx.UUID
		Host, User, Password           string
		Port                           uint16
		TLS, Params                    any
		Version, ServerUUID, CaseNames string
	}
	endpoint := func(e *model.Endpoint) (endpointInput, error) {
		out := endpointInput{ID: e.ID, CenterID: e.CenterID, Host: e.Host, User: e.Username, Password: e.Password, Port: e.Port, Version: e.Version, ServerUUID: e.ServerUUID, CaseNames: e.LowerCaseTableNames}
		for _, value := range []struct {
			input model.JSON
			out   *any
		}{{e.TLS, &out.TLS}, {e.Params, &out.Params}} {
			if len(value.input) != 0 && decode(value.input, value.out) != nil {
				return endpointInput{}, ErrReinitConflict
			}
		}
		return out, nil
	}
	a, err := endpoint(source)
	if err != nil {
		return "", err
	}
	b, err := endpoint(target)
	if err != nil {
		return "", err
	}
	var replicaInput *endpointInput
	if id, enabled, err := SnapshotReplicaEndpointID(rules); err != nil {
		return "", ErrReinitConflict
	} else if enabled {
		if len(readEndpoint) != 1 || readEndpoint[0] == nil || readEndpoint[0].ID != id {
			return "", ErrReinitConflict
		}
		value, err := endpoint(readEndpoint[0])
		if err != nil {
			return "", err
		}
		replicaInput = &value
	}
	var ruleDoc any
	if decode(rules, &ruleDoc) != nil {
		return "", ErrReinitConflict
	}
	blob, err := json.Marshal(struct {
		Pipeline, Topology, SourceNode, TargetNode, Strategy uuidx.UUID
		StrategyVersion                                      uint32
		BinlogServerID                                       *uint32
		Source, Target                                       endpointInput
		Rules                                                any
		Replica                                              *endpointInput `json:",omitempty"`
	}{p.ID, p.TopologyID, p.SourceNodeID, p.TargetNodeID, p.StrategyID, p.StrategyVersion, p.BinlogServerID, a, b, ruleDoc, replicaInput})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(blob)
	return hex.EncodeToString(hash[:]), nil
}

// Called with topology/pipeline locks held. Endpoint shared locks prevent a
// concurrent connection edit from committing between validation and confirm.
func (r *Reinitializations) validateInputs(ctx context.Context, p *model.Pipeline, expected string) error {
	if expected == "" {
		return ErrReinitConflict
	}
	ids := []uuidx.UUID{p.SourceEndpointID, p.TargetEndpointID}
	var version model.StrategyVersion
	if err := r.db.WithContext(ctx).Clauses(clause.Locking{Strength: "SHARE"}).Where("strategy_id=? AND version=?", p.StrategyID, p.StrategyVersion).First(&version).Error; err != nil {
		return ErrReinitConflict
	}
	replicaID, replicaEnabled, err := SnapshotReplicaEndpointID(version.Rules)
	if err != nil {
		return ErrReinitConflict
	}
	if replicaEnabled {
		ids = append(ids, replicaID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	endpoints := map[uuidx.UUID]*model.Endpoint{}
	for _, id := range ids {
		if endpoints[id] != nil {
			continue
		}
		var ep model.Endpoint
		if err := r.db.WithContext(ctx).Clauses(clause.Locking{Strength: "SHARE"}).First(&ep, "id=?", id).Error; err != nil {
			return ErrReinitConflict
		}
		endpoints[id] = &ep
	}
	actual, err := ReinitInputsDigest(p, endpoints[p.SourceEndpointID], endpoints[p.TargetEndpointID], version.Rules, endpoints[replicaID])
	if err != nil || actual != expected {
		return ErrReinitConflict
	}
	return nil
}
