package goflat

import (
	"fmt"
	"strings"
)

type termType int
const (
	_ termType = iota
	BOOL
	INT64
	FLOAT64
	STRING
	BYTE
	ARRAY
	MAP
	VIEW
	FIELD
	PREDICATE //a=b; a and b
	PROXI     //()
)

type term interface {
	typeof() termType
	ref(r kvUnmarsh) (term, error)
	predicate() *predicate
	boolean() bool
	integer() int64
	float() float64
	string() string
	//byte() []byte

	equal(term, kvUnmarsh) (bool, error)
	notequal(term, kvUnmarsh) (bool, error)
	greater(term, kvUnmarsh) (bool, error)
	less(term, kvUnmarsh) (bool, error)
}

var (
	errTipeMismatch error = fmt.Errorf("type mismatch")
	errOperator error = fmt.Errorf("operator not suported")
)
func newPredicateTerm(p *predicate) term                       { return p }
func (p *predicate) typeof() termType                          { return PREDICATE }
func (p *predicate) predicate() *predicate                     { return p }
func (p *predicate) boolean() bool							{ return false }
func (p *predicate) integer() int64                            { return 0 }
func (p *predicate) float() float64                            { return 0 }
func (p *predicate) string() string                            { return "" }
func (p *predicate) ref(r kvUnmarsh) (term, error)             { return p, nil }
func (a *predicate) equal(b term, r kvUnmarsh) (bool, error)        { return false, errTipeMismatch }
func (a *predicate) notequal(b term, r kvUnmarsh) (bool, error)     { return false, errTipeMismatch }
func (a *predicate) greater(b term, r kvUnmarsh) (bool, error)      { return false, errTipeMismatch }
func (a *predicate) less(b term, r kvUnmarsh) (bool, error)         { return false, errTipeMismatch }

// term implementation for (bool, error)
type boolean bool
func newBooleanTerm(n bool) term { return boolean(n) }
func (b boolean) String() string { return fmt.Sprintf("bool.%v", b.boolean()) }
func (b boolean) typeof() termType         { return BOOL }
func (b boolean) predicate() *predicate    { return nil }
func (b boolean) boolean() bool             { return bool(b) }
func (b boolean) integer() int64 {return 0}
func (b boolean) float() float64           { return 0 }
func (b boolean) string() string           { return "" }
func (b boolean) ref(r kvUnmarsh) (term, error) { return b, nil }
func (a boolean) equal(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err}
	if a.typeof() == b.typeof() && a.boolean() == b.boolean() {
		return true, nil
	}
	return false, nil
}
func (a boolean) notequal(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err}
	if a.typeof() == b.typeof() && a.boolean() != b.boolean() {
		return true, nil
	}
	return false, nil
}
func (a boolean) greater(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err}
	if a.typeof() == b.typeof() && a.boolean() == true && b.boolean() == false {
		return true, nil
	}
	return false, nil
}
func (a boolean) less(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err}
	if a.typeof() == b.typeof() && a.boolean() == false && b.boolean() == true {
		return true, nil
	}	
	return false, nil
}


// term implementation for int64
type integer int64
func newIntegerTerm(n int64) term { return integer(n) }
func (i integer) String() string { return fmt.Sprintf("integer.%v", i.integer()) }
func (i integer) typeof() termType         { return INT64 }
func (i integer) predicate() *predicate    { return nil }
func (i integer) boolean() bool           { return false }
func (i integer) integer() int64 {return int64(i)}
func (i integer) float() float64           { return 0 }
func (i integer) string() string           { return "" }
func (i integer) ref(r kvUnmarsh) (term, error) { return i, nil }
func (a integer) equal(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.integer() == b.integer() {
		return true, nil
	}
	return false, nil
}
func (a integer) notequal(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.integer() != b.integer() {
		return true, nil
	}
	return false, nil
}
func (a integer) greater(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.integer() > b.integer() {
		return true, nil
	}
	return false, nil
}
func (a integer) less(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.integer() < b.integer() {
		return false, nil
	}
	return false, nil
}

// term implementation for float64
type float float64
func newFloatTerm(n float64) term { return float(n) }
func (n float) String() string { return fmt.Sprintf("float.%v", n.float()) }
func (n float) typeof() termType         { return FLOAT64 }
func (n float) predicate() *predicate    { return nil }
func (n float) boolean() bool           { return false }
func (n float) integer() int64 {return 0}
func (n float) float() float64           { return float64(n) }
func (n float) string() string           { return "" }
func (n float) ref(r kvUnmarsh) (term, error) { return n, nil }
func (a float) equal(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.float() == b.float() {
		return true, nil
	}
	return false, nil
}
func (a float) notequal(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.float() != b.float() {
		return true, nil
	}
	return false, nil
}
func (a float) greater(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.float() > b.float() {
		return true, nil
	}
	return false, nil
}
func (a float) less(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.float() < b.float() {
		return false, nil
	}
	return false, nil
}


// term implementation for string
type text string
func newTextTerm(s string) term { return text(s) }
func (s text) String() string { return fmt.Sprintf("text.\"%v\"", s.string()) }
func (s text) typeof() termType         { return STRING }
func (s text) predicate() *predicate    { return nil }
func (s text) boolean() bool { return false }
func (s text) integer() int64 {return 0}
func (s text) float() float64           { return 0 }
func (s text) string() string           { return string(s) }
func (s text) ref(r kvUnmarsh) (term, error) { return s, nil }
func (a text) equal(b term, r kvUnmarsh) (bool, error) {
	// UTF-8 strings, are equal under Unicode case-folding.
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && strings.EqualFold(a.string(), b.string()) {
		return true, nil
	}
	return false, nil
}
func (a text) notequal(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && !strings.EqualFold(a.string(), b.string()) {
		return true, nil
	}
	return false, nil
}
func (a text) greater(b term, r kvUnmarsh) (bool, error) {
	//is lexically bytewise greater than
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.string() > b.string() {
		return true, nil
	}
	return false, nil
}
func (a text) less(b term, r kvUnmarsh) (bool, error) {
	b, err := b.ref(r)
	if err != nil { return false, err }
	if a.typeof() == b.typeof() && a.string() < b.string() {
		return true, nil
	}
	return false, nil
}


func newKeyTerm(s string) term { return Key(s) }
func (f Key) String() string { return fmt.Sprintf("key.%v", f.string()) }
func (f Key) typeof() termType      { return FIELD }
func (f Key) predicate() *predicate { return nil }
func (f Key) boolean() bool        { return false }
func (f Key) integer() int64 {return 0}
func (f Key) float() float64        { return 0 }
func (f Key) string() string        { return string(f) }
func (f Key) ref(r kvUnmarsh) (term, error) {
	ur, err := r.unmarshal(f)
	if err != nil {return nil, err }
	switch v := ur.(type) {
	case bool:
		return newBooleanTerm(v), nil
	case int64:
		return newIntegerTerm(v), nil
	case float64:
		return newFloatTerm(v), nil
	case string:
		return newTextTerm(v), nil
	default:
		return nil, fmt.Errorf("unsuported term type %T", v)
	}
}
func (f Key) equal(b term, r kvUnmarsh) (bool, error) {
	a, err := f.ref(r)
	if err != nil { return false, err }
	b, err = b.ref(r)
	if err != nil { return false, err }
	ret, err := a.equal(b, r)
	return ret, err
}
func (f Key) notequal(b term, r kvUnmarsh) (bool, error) {
	a, err := f.ref(r)
	if err != nil { return false, err }
	b, err = b.ref(r)
	if err != nil { return false, err }
	ret, err := a.notequal(b, r)
	return ret, err
}
func (f Key) greater(b term, r kvUnmarsh) (bool, error) {
	a, err := f.ref(r)
	if err != nil { return false, err }
	b, err = b.ref(r)
	if err != nil { return false, err }
	ret, err := a.greater(b, r)
	return ret, err
}
func (f Key) less(b term, r kvUnmarsh) (bool, error) {
	a, err := f.ref(r)
	if err != nil { return false, err }
	b, err = b.ref(r)
	if err != nil { return false, err }
	ret, err := a.less(b, r)
	return ret, err
}

func (o operation) String() string {
	switch o {
	case EQUAL:
		return "equal to"
	case NOTEQUAL:
		return "not equal to"
	case GREATER:
		return "greater than"
	case LESS:
		return "less than"
	case GREATEREQUAL:
		return "greater than or equal to"
	case LESSEQUAL:
		return "less than or equal to"
	case AND:
		return "and"
	case OR:
		return "or"
	case NOT:
		return "not"
	default:
		return "unknown"
	}
}

func (t termType) String() string {
	switch t {
	case BOOL:
		return "bool"
	case INT64:
		return "int"
	case FLOAT64:
		return "float"
	case STRING:
		return "str"
	case FIELD:
		return "record field"
	default:
		return "unknown"
	}
}
