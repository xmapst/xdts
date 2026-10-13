package rule

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strings"
)

func compileTrim(params map[string]string) (IETLProgram, error) {
	if len(params) > 1 || len(params) == 1 && !hasETLParam(params, "cutset") {
		return nil, ErrTransformConfig
	}
	cutset := params["cutset"]
	return etlFunction(func(value any, _ map[string]any) (any, error) {
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, ErrTransformEvaluation
		}
		if cutset == "" {
			return strings.TrimSpace(text), nil
		}
		return strings.Trim(text, cutset), nil
	}), nil
}

func compileReplace(params map[string]string) (IETLProgram, error) {
	old, oldOK := params["old"]
	replacement, newOK := params["new"]
	if len(params) != 2 || !oldOK || old == "" || !newOK {
		return nil, ErrTransformConfig
	}
	return etlFunction(func(value any, _ map[string]any) (any, error) {
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok || len(text) > 1<<20 {
			return nil, ErrTransformEvaluation
		}
		count := strings.Count(text, old)
		if len(text)+count*(len(replacement)-len(old)) > 1<<20 {
			return nil, ErrTransformEvaluation
		}
		return strings.ReplaceAll(text, old, replacement), nil
	}), nil
}

func compileJSONExtract(params map[string]string) (IETLProgram, error) {
	path := params["path"]
	if len(params) != 1 || !strings.HasPrefix(path, "$.") || len(path) > 256 {
		return nil, ErrTransformConfig
	}
	parts := strings.Split(path[2:], ".")
	if slices.Contains(parts, "") {
		return nil, ErrTransformConfig
	}
	return etlFunction(func(value any, _ map[string]any) (any, error) {
		if value == nil {
			return nil, nil
		}
		var raw []byte
		switch value := value.(type) {
		case string:
			raw = []byte(value)
		case []byte:
			raw = value
		default:
			return nil, ErrTransformEvaluation
		}
		if len(raw) > 1<<20 {
			return nil, ErrTransformEvaluation
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var decoded any
		if decoder.Decode(&decoded) != nil || decoder.Decode(new(any)) != io.EOF {
			return nil, ErrTransformEvaluation
		}
		for _, part := range parts {
			object, ok := decoded.(map[string]any)
			if !ok {
				return nil, ErrTransformEvaluation
			}
			decoded = object[part]
			if decoded == nil {
				return nil, nil
			}
		}
		switch value := decoded.(type) {
		case nil, string, bool:
			return value, nil
		case json.Number:
			return string(value), nil
		default:
			return nil, ErrTransformEvaluation
		}
	}), nil
}

func hasETLParam(params map[string]string, name string) bool { _, ok := params[name]; return ok }
