package goflat

import (
	//"fmt"
	//"strings"
	"time"
	"bytes"
)

// Term represents a single term in a predicate
type Term struct {
	val interface{}
}

func boolTerm(i bool) *Term {
	return &Term{val: i}
}
func intTerm(i int64) *Term {
	return &Term{val: i}
}
func floatTerm(i float64) *Term {
	return &Term{val: i}
}
func stringTerm(i string) *Term {
	return &Term{val: i}
}
func timeTerm(i time.Time) *Term {
	return &Term{val: i}
}
func byteTerm(i []byte) *Term {
	return &Term{val: i}
}
// KeyTerm returns a new key term
func KeyTerm(k interface{}) *Term {
	switch vk := k.(type) {
	case string:
		return &Term{val: Key(vk)}
	case Key:
		return &Term{val: vk}
	}
	return &Term{}
}
// ValueTerm returns a new value term
func ValueTerm(v interface{}) *Term {
	switch vv := v.(type) {
	case Key:
		return &Term{val: vv}
	case bool:
		return boolTerm(vv)
	case int:
		return intTerm(int64(vv))
	case int8:
		return intTerm(int64(vv))
	case int16:
		return intTerm(int64(vv))
	case int32:
		return intTerm(int64(vv))
	case int64:
		return intTerm(vv)
	case float32:
		return floatTerm(float64(vv))
	case float64:
		return floatTerm(vv)
	case string:
		return stringTerm(vv)
	case time.Time:
		return timeTerm(vv)
	case []uint8:
		return byteTerm(vv)
	default:
		return &Term{}
	}
	return nil
}


// Equals is a rational perator: a == b
func (a *Term) Equals(b *Term) *Predicate {
	return &Predicate{a: a, b: b, f: f_eq}
}
// NotEquals is a rational perator: a != b
func (a *Term) NotEquals(b *Term) *Predicate {
	return &Predicate{a: a, b: b, f: f_neq}
}
// Greater is a rational perator: a > b
func (a *Term) Greater(b *Term) *Predicate {
	return &Predicate{a: a, b: b, f: f_gr}
}
// GreaterEqual is a rational perator: a >= b
func (a *Term) GreaterEqual(b *Term) *Predicate {
	return &Predicate{a: a, b: b, f: f_greq}
}
// Less is a rational perator: a < b
func (a *Term) Less(b *Term) *Predicate {
	return &Predicate{a: a, b: b, f: f_ls}
}
// LessEqual is a rational perator: a <= b
func (a *Term) LessEqual(b *Term) *Predicate {
	return &Predicate{a: a, b: b, f: f_lseq}
}
// Null is a rational perator: a == nil
func (a *Term) Null() *Predicate {
	return &Predicate{a: a, b: nil, f: f_nil}
}
// NotNull is a rational perator: a != nil
func (a *Term) NotNull() *Predicate {
	return &Predicate{a: a, b: nil, f: f_nnil}
}


// StringEval is a perator that runs user defined func(string, string) bool on operator a and b
func (a *Term) StringEval(b *Term, fe func(string, string) bool) *Predicate {
	var f_fe func(a *Term, b *Term, d kvUnmarsh) interface{}
	f_fe = func(a *Term, b *Term, d kvUnmarsh) interface{} {
		if a.val != nil && b.val != nil || fe == nil {		
			va, ka := a.val.(Key)
			if ka {
				ra, err := d.unmarshal(va)
				if err != nil {
					return nil
				}
				c := &Term{val: ra}
				return f_fe(c, b, d)

			}
			vb, kb := b.val.(Key)
			if kb {
				rb, err := d.unmarshal(vb)
				if err != nil {
					return nil
				}
				c := &Term{val: rb}
				return f_fe(a, c, d)
			}

			sa, ea := a.val.(string)
			sb, eb := b.val.(string)
			if !ea || !eb {
				return nil
			}
			return fe(sa, sb)
		}
		return nil
	}

	return &Predicate{a: a, b: b, f: f_fe}
}

// Predicate represents a single predicate in where clause 
type Predicate struct {
	a *Term
	b *Term
	f func(*Term, *Term, kvUnmarsh) interface{}

}
// And is a logical operator: a && b
func (a *Predicate) And(b *Predicate) *Predicate {
	return &Predicate{a: &Term{val: a}, b: &Term{val: b}, f: f_a}
}
// Or is a logical operator: a || b
func (a *Predicate) Or(b *Predicate) *Predicate {
	return &Predicate{a: &Term{val: a}, b: &Term{val: b}, f: f_o}
}
// Not is a logical operator: !a 
func Not(b *Predicate) *Predicate {
	return &Predicate{a: nil, b: &Term{val: b}, f: f_n}
}

func f_eq(a *Term, b *Term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &Term{val: ra}
			return f_eq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &Term{val: rb}
			return f_eq(a, c, d)
		}

		switch va := a.val.(type) {
		case bool:
			switch vb := b.val.(type) {
			case bool:
				return va == vb
			default:
				return nil
			}
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va == vb
			case float64:
				return float64(va) == vb
			default:
				return nil
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va == float64(vb)
			case float64:
				return va == vb
			default:
				return nil
			}
		case string:
			switch vb := b.val.(type) {
			case string:
				return va == vb 
			default:
				return nil
			}
		case time.Time:
			switch vb := b.val.(type) {
			case time.Time:
				return va.Equal(vb)
			default:
				return nil
			}
		case []uint8:
			switch vb := b.val.(type) {
			case []uint8:
				return bytes.Equal(va, vb)
			default:
				return nil
			}			
		default:
			return nil
		}
	}
	return nil
}

func f_neq(a *Term, b *Term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &Term{val: ra}
			return f_neq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &Term{val: rb}
			return f_neq(a, c, d)
		}

		switch va := a.val.(type) {
		case bool:
			switch vb := b.val.(type) {
			case bool:
				return va != vb
			default:
				return nil
			}
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va != vb
			case float64:
				return float64(va) != vb
			default:
				return nil
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va != float64(vb)
			case float64:
				return va != vb
			default:
				return nil
			}
		case string:
			switch vb := b.val.(type) {
			case string:
				return va != vb
			default:
				return nil
			}
		case time.Time:
			switch vb := b.val.(type) {
			case time.Time:
				return !va.Equal(vb)
			default:
				return nil
			}
		case []uint8:
			switch vb := b.val.(type) {
			case []uint8:
				return !bytes.Equal(va, vb)
			default:
				return nil
			}			
		default:
			return nil
		}
	}
	return nil
}

func f_gr(a *Term, b *Term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &Term{val: ra}
			return f_gr(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &Term{val: rb}
			return f_gr(a, c, d)
		}

		switch va := a.val.(type) {
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va > vb
			case float64:
				return float64(va) > vb
			default:
				return nil
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va > float64(vb)
			case float64:
				return va > vb
			default:
				return nil
			}
		case string:
			switch vb := b.val.(type) {
			case string:
				//lexically bytewise greater than
				return va > vb
			default:
				return nil
			}
		case time.Time:
			switch vb := b.val.(type) {
			case time.Time:
				return va.After(vb)
			default:
				return nil
			}
		case []uint8:
			switch vb := b.val.(type) {
			case []uint8:
				return string(va) > string(vb)
			default:
				return nil
			}			
		default:
			return nil
		}
	}
	return nil
}
func f_greq(a *Term, b *Term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &Term{val: ra}
			return f_greq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &Term{val: rb}
			return f_greq(a, c, d)
		}

		switch va := a.val.(type) {
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va >= vb
			case float64:
				return float64(va) >= vb
			default:
				return nil
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va >= float64(vb)
			case float64:
				return va >= vb
			default:
				return nil
			}
		case string:
			switch vb := b.val.(type) {
			case string:
				//lexically bytewise greater than
				return va >= vb
			default:
				return nil
			}
		case time.Time:
			switch vb := b.val.(type) {
			case time.Time:
				return va.After(vb) || va.Equal(vb)
			default:
				return nil
			}
		case []uint8:
			switch vb := b.val.(type) {
			case []uint8:
				return bytes.Equal(va, vb) || string(va) > string(vb)
			default:
				return nil
			}			
		default:
			return nil
		}
	}
	return nil
}

func f_ls(a *Term, b *Term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &Term{val: ra}
			return f_ls(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &Term{val: rb}
			return f_ls(a, c, d)
		}

		switch va := a.val.(type) {
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va < vb
			case float64:
				return float64(va) < vb
			default:
				return nil
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va < float64(vb)
			case float64:
				return va < vb
			default:
				return nil
			}
		case string:
			switch vb := b.val.(type) {
			case string:
				//lexically bytewise greater than
				return va < vb
			default:
				return nil
			}
		case time.Time:
			switch vb := b.val.(type) {
			case time.Time:
				return va.Before(vb)
			default:
				return nil
			}
		case []uint8:
			switch vb := b.val.(type) {
			case []uint8:
				return string(va) < string(vb)
			default:
				return nil
			}			
		default:
			return nil
		}
	}
	return nil
}

func f_lseq(a *Term, b *Term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &Term{val: ra}
			return f_lseq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &Term{val: rb}
			return f_lseq(a, c, d)
		}

		switch va := a.val.(type) {
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va <= vb
			case float64:
				return float64(va) <= vb
			default:
				return nil
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va <= float64(vb)
			case float64:
				return va <= vb
			default:
				return nil
			}
		case string:
			switch vb := b.val.(type) {
			case string:
				//lexically bytewise greater than
				return va <= vb
			default:
				return nil
			}
		case time.Time:
			switch vb := b.val.(type) {
			case time.Time:
				return va.Before(vb) || va.Equal(vb)
			default:
				return nil
			}
		case []uint8:
			switch vb := b.val.(type) {
			case []uint8:
				return bytes.Equal(va, vb) || string(va) < string(vb)
			default:
				return nil
			}			
		default:
			return nil
		}
	}
	return nil
}

func f_nil(a *Term, b *Term, d kvUnmarsh) interface{} {
	if a.val == nil {
		return true
	}

	va, ka := a.val.(Key)
	if ka {
		ra, err := d.unmarshal(va)
		if err != nil {
			return nil
		}
		if ra == nil {
			return true
		}
	}
	return false
}
func f_nnil(a *Term, b *Term, d kvUnmarsh) interface{} {
	if a.val == nil {
		return false
	}
	va, ka := a.val.(Key)
	if ka {
		ra, err := d.unmarshal(va)
		if err != nil {
			return nil
		}
		if ra == nil {
			return false
		}
	}
	return true
}

func f_a(a *Term, b *Term, d kvUnmarsh) interface{} {
	if a.val != nil && b.val != nil {
		va, e := a.val.(*Predicate)
		if !e {
			return nil
		}
		vb, e := b.val.(*Predicate)
		if !e {
			return nil
		}
		eva := va.eval(d)
		evb := vb.eval(d)
		//if eva != nil && evb != nil {
		veva, e := eva.(bool)
		if !e {
			return nil
		}
		if !veva {
			return false
		}

		vevb, e := evb.(bool)
		if !e {
			return nil
		}
		return veva && vevb
		//}
	}
	return nil
}
func f_o(a *Term, b *Term, d kvUnmarsh) interface{} {
	if a.val != nil && b.val != nil {
		va, e := a.val.(*Predicate)
		if !e {
			return nil
		}
		vb, e := b.val.(*Predicate)
		if !e {
			return nil
		}
		eva := va.eval(d)
		evb := vb.eval(d)
		//if eva != nil && evb != nil {
		veva, e := eva.(bool)
		if !e {
			return nil
		}
		vevb, e := evb.(bool)
		if !e {
			return nil
		}
		return veva || vevb
		//}
	}
	return nil
}

func f_n(a *Term, b *Term, d kvUnmarsh) interface{} {
	if b.val != nil {
		vb, e := b.val.(*Predicate)
		if !e {
			return nil
		}
		evb := vb.eval(d)
		//if eva != nil && evb != nil {
		vevb, e := evb.(bool)
		if !e {
			return nil
		}
		return !vevb
		//}
	}
	return nil
}

func (p *Predicate) eval(d kvUnmarsh) interface{} {
	return p.f(p.a, p.b, d)
}
