package pipelinespec

import "github.com/xmapst/xdts/internal/iproto"

func ValidateWritePolicyRules(doc *StrategyRulesDoc) error {
	if doc == nil || doc.Target == nil {
		return nil
	}
	return iproto.ValidateWritePolicy(doc.Target.WritePolicy)
}
