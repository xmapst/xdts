package http

import (
	"bytes"
	"encoding/json"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
)

func canonicalPipelineRules(raw json.RawMessage) ([]byte, error) {
	if err := validateStrategyRules(raw); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	doc, err := pipelinespec.ParseStrategyRules(raw)
	if err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := iproto.ValidateConflictPolicy(conflictPolicyFromRules(doc)); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidateObjectFilterRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidateColumnFilterRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidateRowFilterRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidateTransformRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidateMappingRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidateDDLPolicyRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidateWritePolicyRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidatePublicationWindowRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	if err := pipelinespec.ValidateSnapshotReplicaRules(doc); err != nil {
		return nil, &pipelineValidationError{"invalid_rules", err.Error()}
	}
	return pipelinespec.MarshalStrategyRules(doc)
}

func hasPipelineRules(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
