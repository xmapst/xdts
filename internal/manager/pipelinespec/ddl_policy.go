package pipelinespec

import "github.com/xmapst/xdts/internal/iproto"

func ValidateDDLPolicyRules(doc *StrategyRulesDoc) error {
	if doc == nil || doc.Target == nil {
		return nil
	}
	return iproto.ValidateDDLPolicy(doc.Target.DDLPolicy)
}
