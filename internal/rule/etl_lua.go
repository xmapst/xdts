package rule

import (
	"io"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/arnodel/golua/code"
	"github.com/arnodel/golua/lib/stringlib"
	rt "github.com/arnodel/golua/runtime"
)

type luaETL struct{ unit *code.Unit }

func CompileLuaETL(source string) (program IETLProgram, compileErr error) {
	defer func() {
		if recover() != nil {
			program = nil
			compileErr = ErrTransformConfig
		}
	}()
	if len(source) > 4096 || strings.TrimSpace(source) == "" || !utf8.ValidString(source) || strings.ContainsRune(source, 0) {
		return nil, ErrTransformConfig
	}
	r := rt.New(io.Discard)
	var closeErr error
	defer r.Close(&closeErr)
	var unit *code.Unit
	state, err := r.MainThread().CallContext(rt.RuntimeContextDef{HardLimits: rt.RuntimeResources{Cpu: 1000000, Memory: 8 << 20, Millis: 100}}, func() error {
		if r.HardLimits().Cpu == 0 || r.HardLimits().Memory == 0 {
			return ErrTransformConfig
		}
		var compileErr error
		unit, _, compileErr = r.CompileLuaChunk("etl", []byte(source))
		return compileErr
	})
	if err != nil || state.Status() != rt.StatusDone || unit == nil {
		return nil, ErrTransformConfig
	}
	return &luaETL{unit: unit}, nil
}

func (p *luaETL) Apply(value any, row map[string]any) (output any, evalErr error) {
	defer func() {
		if recover() != nil {
			output = nil
			evalErr = ErrTransformEvaluation
		}
	}()
	r := rt.New(io.Discard)
	var closeErr error
	defer r.Close(&closeErr)
	stringValue, _ := stringlib.LibLoader.Load(r)
	env := rt.NewTable()
	for name, function := range map[string]func(*rt.Thread, *rt.GoCont) (rt.Cont, error){"tonumber": luaToNumber, "tostring": luaToString, "type": luaType} {
		r.SetEnvGoFunc(env, name, function, 1, false).SolemnlyDeclareCompliance(rt.ComplyMemSafe | rt.ComplyCpuSafe | rt.ComplyIoSafe | rt.ComplyTimeSafe)
	}
	mathTable := rt.NewTable()
	for name, operation := range map[string]func(float64) float64{"abs": math.Abs, "floor": math.Floor, "ceil": math.Ceil, "sqrt": math.Sqrt} {
		r.SetEnvGoFunc(mathTable, name, luaMath(operation), 1, false).SolemnlyDeclareCompliance(rt.ComplyMemSafe | rt.ComplyCpuSafe | rt.ComplyIoSafe | rt.ComplyTimeSafe)
	}
	r.SetEnv(stringValue.AsTable(), "dump", rt.NilValue)
	r.SetEnv(env, "string", stringValue)
	r.SetEnv(env, "math", rt.TableValue(mathTable))
	var result rt.Value
	state, err := r.MainThread().CallContext(rt.RuntimeContextDef{
		HardLimits:    rt.RuntimeResources{Cpu: 100000, Memory: 8 << 20, Millis: 25},
		RequiredFlags: rt.ComplyMemSafe | rt.ComplyCpuSafe | rt.ComplyIoSafe | rt.ComplyTimeSafe,
	}, func() error {
		input, err := luaScalar(value)
		if err != nil {
			return err
		}
		r.SetEnv(env, "value", input)
		inputRow := rt.NewTable()
		size := 0
		for name, value := range row {
			value = luaInputScalar(value)
			count, err := transformScalarSize(value)
			if err != nil || count > 1<<20 {
				return ErrTransformEvaluation
			}
			size += len(name) + count
			if size > 4<<20 {
				return ErrTransformEvaluation
			}
			scalar, err := luaScalar(value)
			if err != nil {
				return err
			}
			r.SetEnv(inputRow, name, scalar)
		}
		r.RequireBytes(size)
		r.SetEnv(env, "row", rt.TableValue(inputRow))
		chunk := r.LoadLuaUnit(p.unit, rt.TableValue(env))
		result, err = rt.Call1(r.MainThread(), rt.FunctionValue(chunk))
		return err
	})
	if err != nil || state.Status() != rt.StatusDone {
		return nil, ErrTransformEvaluation
	}
	switch result.Type() {
	case rt.NilType:
		return nil, nil
	case rt.BoolType:
		return result.AsBool(), nil
	case rt.IntType:
		return result.AsInt(), nil
	case rt.FloatType:
		f := result.AsFloat()
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, ErrTransformEvaluation
		}
		return f, nil
	case rt.StringType:
		text := result.AsString()
		if len(text) > 1<<20 {
			return nil, ErrTransformEvaluation
		}
		return text, nil
	default:
		return nil, ErrTransformEvaluation
	}
}

func luaInputScalar(value any) any {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case float32:
		return float64(v)
	default:
		return value
	}
}

func luaScalar(value any) (rt.Value, error) {
	switch value := luaInputScalar(value).(type) {
	case nil:
		return rt.NilValue, nil
	case string:
		return rt.StringValue(value), nil
	case []byte:
		return rt.StringValue(string(value)), nil
	case bool:
		return rt.BoolValue(value), nil
	case int64:
		return rt.IntValue(value), nil
	case uint64:
		if value > math.MaxInt64 {
			return rt.StringValue(strconv.FormatUint(value, 10)), nil
		}
		return rt.IntValue(int64(value)), nil
	case float64:
		if !math.IsInf(value, 0) && !math.IsNaN(value) {
			return rt.FloatValue(value), nil
		}
	}
	return rt.NilValue, ErrTransformEvaluation
}

func luaToNumber(t *rt.Thread, c *rt.GoCont) (rt.Cont, error) {
	if err := c.Check1Arg(); err != nil {
		return nil, err
	}
	value, kind := rt.ToNumberValue(c.Arg(0))
	if kind == rt.NaN {
		value = rt.NilValue
	}
	return c.PushingNext1(t.Runtime, value), nil
}
func luaToString(t *rt.Thread, c *rt.GoCont) (rt.Cont, error) {
	if err := c.Check1Arg(); err != nil {
		return nil, err
	}
	value := c.Arg(0)
	var text string
	switch value.Type() {
	case rt.NilType:
		text = "nil"
	case rt.StringType:
		text = value.AsString()
	case rt.IntType:
		text = strconv.FormatInt(value.AsInt(), 10)
	case rt.FloatType:
		text = strconv.FormatFloat(value.AsFloat(), 'g', -1, 64)
	case rt.BoolType:
		text = strconv.FormatBool(value.AsBool())
	default:
		return nil, ErrTransformEvaluation
	}
	t.RequireBytes(len(text))
	return c.PushingNext1(t.Runtime, rt.StringValue(text)), nil
}
func luaType(t *rt.Thread, c *rt.GoCont) (rt.Cont, error) {
	if err := c.Check1Arg(); err != nil {
		return nil, err
	}
	return c.PushingNext1(t.Runtime, rt.StringValue(c.Arg(0).TypeName())), nil
}
func luaMath(operation func(float64) float64) func(*rt.Thread, *rt.GoCont) (rt.Cont, error) {
	return func(t *rt.Thread, c *rt.GoCont) (rt.Cont, error) {
		value, err := c.FloatArg(0)
		if err != nil {
			return nil, err
		}
		return c.PushingNext1(t.Runtime, rt.FloatValue(operation(value))), nil
	}
}
