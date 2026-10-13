package controller

import (
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

func targetMapping(rules *iproto.MappingRules, sensitive bool) (schemamig.Mapping, error) {
	return iproto.CompileMapping(rules, sensitive)
}
