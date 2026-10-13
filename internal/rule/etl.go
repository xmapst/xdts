package rule

import (
	"maps"
	"strings"
)

// Programs must be deterministic, concurrent-safe, bounded and perform no IO.
// Each invocation owns its input; failures must not expose business values.
type IETLProgram interface {
	Apply(value any, row map[string]any) (any, error)
}

type ETLFactory func(params map[string]string) (IETLProgram, error)

type ETLRegistry struct{ factories map[string]ETLFactory }

func NewETLRegistry(extra map[string]ETLFactory) (*ETLRegistry, error) {
	factories := map[string]ETLFactory{"trim": compileTrim, "replace": compileReplace, "json_extract": compileJSONExtract}
	for name, factory := range extra {
		if name == "" || len(name) > 64 || strings.TrimSpace(name) != name || factory == nil || factories[name] != nil {
			return nil, ErrTransformConfig
		}
		factories[name] = factory
	}
	return &ETLRegistry{factories: factories}, nil
}

func (r *ETLRegistry) Compile(name string, params map[string]string) (IETLProgram, error) {
	if r == nil || r.factories[name] == nil {
		return nil, ErrTransformConfig
	}
	return r.factories[name](maps.Clone(params))
}

type etlFunction func(any, map[string]any) (any, error)

func (f etlFunction) Apply(value any, row map[string]any) (any, error) { return f(value, row) }
