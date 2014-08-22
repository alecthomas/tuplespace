package tuplespace

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
)

type TupleMatcher struct {
	ast  *ast.Expr
	Expr string
}

func MustMatch(expr string, args ...interface{}) *TupleMatcher {
	m, err := Match(expr, args...)
	if err != nil {
		panic(expr + ": " + err.Error())
	}
	return m
}

// Match creates a new TupleMatcher for evaluating an expression against a
// tuple. An empty expression always evaluates to true.
func Match(expr string, args ...interface{}) (*TupleMatcher, error) {
	t := &TupleMatcher{
		Expr: fmt.Sprintf(expr, args...),
	}
	err := t.compile()
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *TupleMatcher) String() string {
	return t.Expr
}

func (t *TupleMatcher) compile() error {
	if t.Expr == "" {
		return nil
	}
	ast, err := parser.ParseExpr(t.Expr)
	if err != nil {
		return err
	}
	t.ast = &ast
	return nil
}

func (t *TupleMatcher) Match(tuple Tuple) bool {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Evaluation of %#v against %#v failed: %s", t.Expr, tuple, err)
		}
	}()
	if t.Expr == "" {
		return true
	}
	return toBool(eval(tuple, *t.ast))
}

func (t *TupleMatcher) Eval(tuple Tuple) interface{} {
	return eval(tuple, *t.ast)
}

func normalize(n string, v interface{}) interface{} {
	switch rv := v.(type) {
	case int:
		return int64(rv)
	case int8:
		return int64(rv)
	case int16:
		return int64(rv)
	case int32:
		return int64(rv)
	case int64:
		return rv

	// These types can be safely converted to signed integers.
	case uint8:
		return uint64(rv)
	case uint16:
		return uint64(rv)
	case uint32:
		return uint64(rv)
	case uint64:
		return v

	case float32:
		return float64(rv)
	case float64:
		return rv

	case string:
		return v
	}
	if v == nil {
		return false
	}
	return v
}

func index(expr ast.Node, out []string) []string {
	switch n := expr.(type) {
	case *ast.BinaryExpr:
		out = index(n.X, out)
		out = index(n.Y, out)
	case *ast.ParenExpr:
		out = index(n.X, out)
	case *ast.Ident:
		if n.Name != "nil" && n.Name != "true" && n.Name != "false" {
			out = append(out, n.Name)
		}
	}
	return out
}

func eval(tuple Tuple, expr ast.Node) interface{} {
	switch n := expr.(type) {
	case *ast.BinaryExpr:
		ll := eval(tuple, n.X)

		// Bool is first, to support short-circuit evaluation.
		if l, ok := ll.(bool); ok {
			switch n.Op {
			case token.LAND:
				return l && toBool(eval(tuple, n.Y))
			case token.LOR:
				return l || toBool(eval(tuple, n.Y))
			case token.EQL:
				r := eval(tuple, n.Y)
				if r == nil {
					return false
				}
				return l == r
			case token.NEQ:
				r := eval(tuple, n.Y)
				if r == nil {
					return true
				}
				return l != r
			}
			panic("unsupported boolean operation")
		}

		rr := eval(tuple, n.Y)

		if ll == nil {
			switch n.Op {
			case token.EQL:
				return ll == rr
			case token.NEQ:
				return ll != rr
			}
		}

		if rr == nil {
			switch n.Op {
			case token.EQL:
				return ll == nil
			case token.NEQ:
				return ll != nil
			}
		}

		switch l := ll.(type) {
		case int64:
			r, ok := rr.(int64)
			if !ok {
				r = int64(rr.(uint64))
			}
			switch n.Op {
			case token.EQL:
				return l == r
			case token.NEQ:
				return l != r
			case token.LSS:
				return l < r
			case token.GTR:
				return l > r
			case token.LEQ:
				return l <= r
			case token.GEQ:
				return l >= r

			case token.ADD:
				return l + r
			case token.SUB:
				return l - r
			case token.MUL:
				return l * r
			case token.QUO:
				return l / r
			case token.REM:
				return l % r
			case token.SHL:
				if r < 0 {
					panic("negative shift count")
				}
				return l << uint64(r)
			case token.SHR:
				if r < 0 {
					panic("negative shift count")
				}
				return l >> uint64(r)

			case token.AND:
				return l & r
			case token.OR:
				return l | r
			case token.XOR:
				return l ^ r
			case token.AND_NOT:
				return l &^ r
			}

		case uint64:
			r, ok := rr.(uint64)
			if !ok {
				r = uint64(rr.(int64))
			}
			switch n.Op {
			case token.EQL:
				return l == r
			case token.NEQ:
				return l != r
			case token.LSS:
				return l < r
			case token.GTR:
				return l > r
			case token.LEQ:
				return l <= r
			case token.GEQ:
				return l >= r

			case token.ADD:
				return l + r
			case token.SUB:
				return l - r
			case token.MUL:
				return l * r
			case token.QUO:
				return l / r
			case token.REM:
				return l % r
			case token.SHL:
				return l << r
			case token.SHR:
				return l >> r

			case token.AND:
				return l & r
			case token.OR:
				return l | r
			case token.XOR:
				return l ^ r
			case token.AND_NOT:
				return l &^ r
			}

		case string:
			r := rr.(string)
			switch n.Op {
			case token.ADD:
				return l + r

			case token.EQL:
				return l == r
			case token.NEQ:
				return l != r
			case token.LSS:
				return l < r
			case token.GTR:
				return l > r
			case token.LEQ:
				return l <= r
			case token.GEQ:
				return l >= r
			}
		case float64:
			r := rr.(float64)
			switch n.Op {
			case token.ADD:
				return l + r
			case token.SUB:
				return l - r
			case token.MUL:
				return l * r
			case token.QUO:
				return l / r

			case token.EQL:
				return l == r
			case token.NEQ:
				return l != r
			case token.LSS:
				return l < r
			case token.GTR:
				return l > r
			case token.LEQ:
				return l <= r
			case token.GEQ:
				return l >= r
			}
		default:
			kind := reflect.TypeOf(ll).Kind()
			if kind == reflect.Map {
				return ll
			}
			panic(fmt.Sprintf("unsupported type %#v", ll))
		}
		panic(fmt.Sprintf("unsupported expression %v %s %v", ll, n.Op, rr))
	case *ast.BasicLit:
		switch n.Kind {
		case token.STRING:
			s, err := strconv.Unquote(n.Value)
			if err != nil {
				panic(err.Error())
			}
			return s
		case token.INT:
			nu, err := strconv.ParseUint(n.Value, 10, 64)
			if err != nil {
				ni, err := strconv.ParseInt(n.Value, 10, 64)
				if err != nil {
					panic(err.Error())
				}
				return ni
			}
			return nu
		case token.FLOAT:
			n, err := strconv.ParseFloat(n.Value, 64)
			if err != nil {
				panic(err.Error())
			}
			return n
		}
		panic("unsupported type")
	case *ast.ParenExpr:
		return eval(tuple, n.X)
	case *ast.UnaryExpr:
		if n.Op == token.NOT {
			return !toBool(eval(tuple, n.X))
		}
		panic(fmt.Sprintf("unsupported unary operator %s", n.Op))
	case *ast.Ident:
		if v, ok := tuple[n.Name]; ok {
			return normalize(n.Name, v)
		}
		if n.Name == "true" {
			return true
		} else if n.Name == "false" {
			return false
		}
		return nil
	case *ast.SelectorExpr:
		v := eval(tuple, n.X)
		if m, ok := v.(Tuple); ok {
			if v, ok := m[n.Sel.Name]; ok {
				return v
			}
		}
		panic(fmt.Sprintf("unknown attribute \"%s\" on %#v", n.Sel.Name, v))
	}
	panic(fmt.Sprintf("unsupported expression node %#v", expr))
}

func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch rv := v.(type) {
	case bool:
		return rv
	case string:
		return rv != ""
	case int, int16, int32, int64, uint, uint16, uint32, uint64:
		return rv != 0
	case float32, float64:
		return rv != 0.0
	}
	return true
}
