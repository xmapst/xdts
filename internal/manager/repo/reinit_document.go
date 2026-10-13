package repo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var ErrReinitConflict = errors.New("reinitialization preview or current run changed")
var ErrReinitBlocked = errors.New("reinitialization plan is blocked")
var ErrReinitNotFound = errors.New("reinitialization preview not found")

type ReinitTable struct {
	SourceSchema string `json:"sourceSchema"`
	SourceTable  string `json:"sourceTable"`
	SourceDDL    string `json:"sourceDdl,omitempty"`
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	// Empty means the target did not exist during the server-side preview.
	ExpectedDDL string `json:"expectedDdl"`
}

type ReinitDocument struct {
	InputsDigest        string                     `json:"inputsDigest,omitempty"`
	Schemas             []string                   `json:"schemas"`
	TargetHandling      model.ReinitTargetHandling `json:"targetHandling"`
	TargetCaseSensitive bool                       `json:"targetCaseSensitive"`
	Tables              []ReinitTable              `json:"tables"`
	SchemaPlan          *schemaplan.Plan           `json:"schemaPlan,omitempty"`
}

func reinitName(name string) bool {
	return name != "" && utf8.ValidString(name) && utf8.RuneCountInString(name) <= 64 && !strings.ContainsRune(name, 0)
}

// reinitDocument validates the persisted authority and produces a canonical
// encoding independent of preview object order. Target definitions are CREATE
// statements; data removal is expressed only through the target handling mode.
func reinitDocument(id uuidx.UUID, run, version uint64, doc ReinitDocument) ([]byte, string, error) {
	if id.IsZero() || run >= math.MaxInt64 || version >= math.MaxInt64 || (doc.TargetHandling != model.ReinitTargetKeep && doc.TargetHandling != model.ReinitTargetClear) || len(doc.Schemas) == 0 || len(doc.Schemas) > 100 || len(doc.Tables) == 0 || len(doc.Tables) > 4000 {
		return nil, "", ErrReinitConflict
	}
	if len(doc.InputsDigest) != 64 {
		return nil, "", ErrReinitConflict
	}
	if _, err := hex.DecodeString(doc.InputsDigest); err != nil {
		return nil, "", ErrReinitConflict
	}
	doc.Schemas = append([]string(nil), doc.Schemas...)
	doc.Tables = append([]ReinitTable(nil), doc.Tables...)
	sort.Strings(doc.Schemas)
	scope := map[string]bool{}
	for _, schema := range doc.Schemas {
		if !reinitName(schema) || scope[schema] {
			return nil, "", ErrReinitConflict
		}
		scope[schema] = true
	}
	sort.Slice(doc.Tables, func(i, j int) bool {
		a, b := doc.Tables[i], doc.Tables[j]
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		if a.SourceSchema != b.SourceSchema {
			return a.SourceSchema < b.SourceSchema
		}
		return a.SourceTable < b.SourceTable
	})
	targets, sources := map[string]bool{}, map[string]bool{}
	for _, table := range doc.Tables {
		if !reinitName(table.SourceSchema) || !reinitName(table.SourceTable) || !reinitName(table.Schema) || !reinitName(table.Table) || !scope[table.SourceSchema] {
			return nil, "", ErrReinitConflict
		}
		sourceKey := fmt.Sprintf("%d:%s%s", len(table.SourceSchema), table.SourceSchema, table.SourceTable)
		schema, name := table.Schema, table.Table
		if !doc.TargetCaseSensitive {
			schema, name = strings.ToLower(schema), strings.ToLower(name)
		}
		targetKey := fmt.Sprintf("%d:%s%s", len(schema), schema, name)
		if sources[sourceKey] || targets[targetKey] {
			return nil, "", ErrReinitConflict
		}
		if table.SourceDDL == "" {
			return nil, "", ErrReinitConflict
		}
		kind, sourceSchema, sourceName, err := schemamig.CreationIdentity(table.SourceSchema, table.SourceDDL)
		if err != nil || kind != schemamig.ObjectTable || sourceSchema != table.SourceSchema || sourceName != table.SourceTable {
			return nil, "", ErrReinitConflict
		}
		sources[sourceKey], targets[targetKey] = true, true
		if table.ExpectedDDL != "" {
			kind, schema, name, err := schemamig.CreationIdentity(table.Schema, table.ExpectedDDL)
			matches := schema == table.Schema && name == table.Table
			if !doc.TargetCaseSensitive {
				matches = strings.EqualFold(schema, table.Schema) && strings.EqualFold(name, table.Table)
			}
			if err != nil || kind != schemamig.ObjectTable || !matches {
				return nil, "", ErrReinitConflict
			}
		} else if doc.SchemaPlan == nil {
			return nil, "", ErrReinitBlocked
		}
		if doc.SchemaPlan != nil {
			found := false
			for _, step := range doc.SchemaPlan.Steps {
				if step.Kind == model.ObjectTypeTable && step.SourceSchema == table.SourceSchema && step.SourceName == table.SourceTable && step.Schema == table.Schema && step.Name == table.Table {
					found = true
					if table.ExpectedDDL == "" && step.Action != model.MigrationActionCreate {
						return nil, "", ErrReinitBlocked
					}
				}
			}
			if !found {
				return nil, "", ErrReinitConflict
			}
		}
	}
	if doc.SchemaPlan != nil {
		if _, _, err := schemaPlanDocument(id, run+1, version, *doc.SchemaPlan); err != nil {
			return nil, "", err
		}
		for _, step := range doc.SchemaPlan.Steps {
			if !reinitName(step.SourceSchema) || !scope[step.SourceSchema] {
				return nil, "", ErrReinitConflict
			}
			if step.Kind != model.ObjectTypeTable {
				continue
			}
			selected := false
			for _, table := range doc.Tables {
				if step.SourceSchema == table.SourceSchema && step.SourceName == table.SourceTable && step.Schema == table.Schema && step.Name == table.Table {
					selected = true
					break
				}
			}
			if !selected {
				return nil, "", ErrReinitConflict
			}
			kind, schema, name, err := schemamig.CreationIdentity(step.SourceSchema, step.SourceDDL)
			if err != nil || kind != schemamig.ObjectTable || schema != step.SourceSchema || name != step.SourceName {
				return nil, "", ErrReinitConflict
			}
			if step.Action != model.MigrationActionBlock {
				kind, schema, name, err = schemamig.CreationIdentity(step.Schema, step.DDL)
				if err != nil || kind != schemamig.ObjectTable || schema != step.Schema || name != step.Name {
					return nil, "", ErrReinitConflict
				}
			}
		}
	}
	blob, err := json.Marshal(doc)
	if err != nil {
		return nil, "", err
	}
	if len(blob) > 16<<20 {
		return nil, "", ErrReinitConflict
	}
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%s:%d:%d:", id, run, version)
	hash.Write(blob)
	return blob, hex.EncodeToString(hash.Sum(nil)), nil
}
