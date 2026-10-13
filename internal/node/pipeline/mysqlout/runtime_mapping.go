package mysqlout

import (
	"errors"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
	"google.golang.org/protobuf/proto"
)

// UpdateRuntimeMapping is pure computation called exclusively under the same
// serial ownership as ApplyBatch. It never opens or changes the SQL session.
// All dictionaries are validated and copied before the active mapping changes.
func (w *Writer) UpdateRuntimeMapping(config iproto.TargetRuntimeConfig) error {
	if err := iproto.ValidateWritePolicy(config.WritePolicy); err != nil {
		return err
	}
	if iproto.ReplayDegree(config.WritePolicy) != len(w.workers)+1 {
		return errors.New("mysqlout: replay degree change requires prepared connections")
	}
	writers := append([]*Writer{w}, w.workers...)
	prepared := make([]*Writer, len(writers))
	for i, writer := range writers {
		candidate, err := writer.prepareRuntimeMapping(config)
		if err != nil {
			return err
		}
		if i > 0 && candidate.opts.WritePolicy != nil {
			candidate.opts.WritePolicy.ParallelReplayDegree = 1
		}
		prepared[i] = candidate
	}
	for i, writer := range writers {
		writer.opts, writer.transforms, writer.conflictMasks = prepared[i].opts, prepared[i].transforms, prepared[i].conflictMasks
	}
	return nil
}

func (w *Writer) prepareRuntimeMapping(config iproto.TargetRuntimeConfig) (*Writer, error) {
	if err := config.DatabaseRoute.Validate(); err != nil {
		return nil, err
	}
	if config.DatabaseRoute.TargetServerUUID != w.opts.ExpectedServerUUID {
		return nil, errors.New("mysqlout: target database identity change requires rebuild")
	}
	if config.DatabaseRoute.Persistent != w.opts.DatabaseRoute.Persistent {
		return nil, errors.New("mysqlout: lineage storage change requires rebuild")
	}
	next, err := runtimeMapping(config)
	if err != nil {
		return nil, err
	}
	opts := w.opts
	opts.DatabaseRoute = config.DatabaseRoute
	opts.WritePolicy = nil
	if config.WritePolicy != nil {
		opts.WritePolicy = proto.Clone(config.WritePolicy).(*iproto.WritePolicy)
	}
	opts.Mapping, opts.Transforms = next, cloneTransformRules(config.Transforms)
	opts.ShadowTablePolicy = iproto.CloneShadowTablePolicy(nil)
	if len(config.ShadowTables) != 0 {
		opts.ShadowTablePolicy = &iproto.ShadowTablePolicy{Rules: iproto.CloneShadowTables(config.ShadowTables), SourceCaseSensitive: config.SourceCaseSensitive}
	}
	transforms, err := compileWriterTransforms(opts)
	if err != nil {
		return nil, err
	}
	candidate := &Writer{opts: opts, transforms: transforms}
	candidate.compileConflictMasks()
	return candidate, nil
}

// ValidateRuntimeMapping performs the same checks without changing a writer.
func ValidateRuntimeMapping(config iproto.TargetRuntimeConfig) error {
	if err := iproto.ValidateWritePolicy(config.WritePolicy); err != nil {
		return err
	}
	mapping, err := runtimeMapping(config)
	if err != nil {
		return err
	}
	_, err = compileWriterTransforms(Options{Mapping: mapping, Transforms: config.Transforms, SchemaOptions: schemamig.Options{SourceCaseSensitive: config.SourceCaseSensitive}})
	return err
}

func runtimeMapping(config iproto.TargetRuntimeConfig) (schemamig.Mapping, error) {
	valid := func(name string) bool { return name != "" && !strings.ContainsRune(name, 0) }
	invalid := errors.New("mysqlout: invalid runtime mapping")
	next := schemamig.Mapping{Schemas: make(map[string]string), Tables: make(map[schemamig.SchemaTable]string), Columns: make(map[schemamig.SchemaTable]map[string]string)}
	for from, to := range config.Schemas {
		if !valid(from) || !valid(to) {
			return schemamig.Mapping{}, invalid
		}
		next.Schemas[from] = to
	}
	for from, to := range config.Tables {
		if !valid(from.Schema) || !valid(from.Table) || !valid(to) {
			return schemamig.Mapping{}, invalid
		}
		next.Tables[schemamig.SchemaTable{Schema: from.Schema, Table: from.Table}] = to
	}
	for from, columns := range config.Columns {
		if !valid(from.Schema) || !valid(from.Table) {
			return schemamig.Mapping{}, invalid
		}
		copy := make(map[string]string, len(columns))
		seen := make(map[string]bool, len(columns))
		for column, to := range columns {
			if !valid(column) || !valid(to) || seen[strings.ToLower(to)] {
				return schemamig.Mapping{}, invalid
			}
			key := strings.ToLower(column)
			if _, exists := copy[key]; exists {
				return schemamig.Mapping{}, invalid
			}
			copy[key], seen[strings.ToLower(to)] = to, true
		}
		next.Columns[schemamig.SchemaTable{Schema: from.Schema, Table: from.Table}] = copy
	}
	if len(config.ShadowTables) != 0 {
		compiled, err := iproto.CompileShadowMapping(config.ShadowTables, next, config.SourceCaseSensitive, config.TargetCaseSensitive)
		if err != nil {
			return schemamig.Mapping{}, err
		}
		next = compiled
	}
	return next, nil
}
