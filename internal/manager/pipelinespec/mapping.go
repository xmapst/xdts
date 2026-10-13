package pipelinespec

import "github.com/xmapst/xdts/internal/iproto"

func ValidateMappingRules(doc *StrategyRulesDoc) error {
	if doc == nil || doc.Target == nil {
		return nil
	}
	_, err := iproto.CompileMapping(doc.Target.Mappings, false)
	return err
}
