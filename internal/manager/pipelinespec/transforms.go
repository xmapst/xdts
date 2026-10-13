package pipelinespec

import (
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
)

func ValidateTransformRules(doc *StrategyRulesDoc) error {
	if doc == nil || doc.Target == nil {
		return nil
	}
	if doc.Target.ConflictPolicy.GetMode() == iproto.ConflictMode_CONFLICT_MODE_LWW {
		for _, transform := range doc.Target.Transforms {
			for _, table := range doc.Target.ConflictPolicy.GetLwwTables() {
				if strings.EqualFold(transform.GetSchema(), table.GetSchema()) && strings.EqualFold(transform.GetTable(), table.GetTable()) && strings.EqualFold(transform.GetColumn(), table.GetTimestampColumn()) {
					return rule.ErrTransformConfig
				}
			}
		}
	}
	_, err := iproto.CompileTransforms(doc.Target.Transforms, false)
	return err
}
