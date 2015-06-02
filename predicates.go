package goflat

import (
	//"fmt"
	"strings"
	"time"
)

type term struct {
	val interface{}
}

func BoolTerm(i bool) *term {
	return &term{val: i}
}
func IntTerm(i int64) *term {
	return &term{val: i}
}
func FloatTerm(i float64) *term {
	return &term{val: i}
}
func StringTerm(i string) *term {
	return &term{val: i}
}
func TimeTerm(i time.Time) *term {
	return &term{val: i}
}
func KeyTerm(k interface{}) *term {
	switch vk := k.(type) {
	case string:
		return &term{val: Key(vk)}
	case Key:
		return &term{val: vk}
	}
	return nil
}

func (a *term) Equals(b *term) *predicate {
	return &predicate{a: a, b: b, f: f_eq}
}
func (a *term) NotEquals(b *term) *predicate {
	return &predicate{a: a, b: b, f: f_neq}
}
func (a *term) Greater(b *term) *predicate {
	return &predicate{a: a, b: b, f: f_gr}
}
func (a *term) GreaterEqual(b *term) *predicate {
	return &predicate{a: a, b: b, f: f_greq}
}
func (a *term) Less(b *term) *predicate {
	return &predicate{a: a, b: b, f: f_ls}
}
func (a *term) LessEqual(b *term) *predicate {
	return &predicate{a: a, b: b, f: f_lseq}
}
func (a *term) Null() *predicate {
	return &predicate{a: a, b: nil, f: f_nil}
}
func (a *term) NotNull() *predicate {
	return &predicate{a: a, b: nil, f: f_nnil}
}

type predicate struct {
	a *term
	b *term
	f func(*term, *term, kvUnmarsh) interface{}
}

func (a *predicate) And(b *predicate) *predicate {
	return &predicate{a: &term{val: a}, b: &term{val: b}, f: f_a}
}
func (a *predicate) Or(b *predicate) *predicate {
	return &predicate{a: &term{val: a}, b: &term{val: b}, f: f_o}
}

func Not(b *predicate) *predicate {
	return &predicate{a: nil, b: &term{val: b}, f: f_n}
}

func f_eq(a *term, b *term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &term{val: ra}
			return f_eq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &term{val: rb}
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
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va == float64(vb)
			case float64:
				return va == vb
			}
		case string:
			switch vb := b.val.(type) {
			case string:
				//UTF-8 strings, are equal under Unicode case-folding.
				return strings.EqualFold(va, vb)
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
		default:
			return nil
		}
	}
	return nil
}

func f_neq(a *term, b *term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &term{val: ra}
			return f_neq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &term{val: rb}
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
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va != float64(vb)
			case float64:
				return va != vb
			}
		case string:
			switch vb := b.val.(type) {
			case string:
				return !strings.EqualFold(va, vb)
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
		default:
			return nil
		}
	}
	return nil
}

func f_gr(a *term, b *term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &term{val: ra}
			return f_eq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &term{val: rb}
			return f_eq(a, c, d)
		}

		switch va := a.val.(type) {
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va > vb
			case float64:
				return float64(va) > vb
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va > float64(vb)
			case float64:
				return va > vb
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
		default:
			return nil
		}
	}
	return nil
}
func f_greq(a *term, b *term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &term{val: ra}
			return f_eq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &term{val: rb}
			return f_eq(a, c, d)
		}

		switch va := a.val.(type) {
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va >= vb
			case float64:
				return float64(va) >= vb
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va >= float64(vb)
			case float64:
				return va >= vb
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
		default:
			return nil
		}
	}
	return nil
}

func f_ls(a *term, b *term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &term{val: ra}
			return f_eq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &term{val: rb}
			return f_eq(a, c, d)
		}

		switch va := a.val.(type) {
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va < vb
			case float64:
				return float64(va) < vb
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va < float64(vb)
			case float64:
				return va < vb
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
		default:
			return nil
		}
	}
	return nil
}

func f_lseq(a *term, b *term, d kvUnmarsh) interface{} {

	if a.val != nil && b.val != nil {

		va, ka := a.val.(Key)
		if ka {
			ra, err := d.unmarshal(va)
			if err != nil {
				return nil
			}
			c := &term{val: ra}
			return f_eq(c, b, d)
		}
		vb, kb := b.val.(Key)
		if kb {
			rb, err := d.unmarshal(vb)
			if err != nil {
				return nil
			}
			c := &term{val: rb}
			return f_eq(a, c, d)
		}

		switch va := a.val.(type) {
		case int64:
			switch vb := b.val.(type) {
			case int64:
				return va <= vb
			case float64:
				return float64(va) <= vb
			}
		case float64:
			switch vb := b.val.(type) {
			case int64:
				return va <= float64(vb)
			case float64:
				return va <= vb
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
				return va.After(vb) || va.Equal(vb)
			default:
				return nil
			}
		default:
			return nil
		}
	}
	return nil
}

func f_nil(a *term, b *term, d kvUnmarsh) interface{} {
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
func f_nnil(a *term, b *term, d kvUnmarsh) interface{} {
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

func f_a(a *term, b *term, d kvUnmarsh) interface{} {
	if a.val != nil && b.val != nil {
		va, e := a.val.(*predicate)
		if !e {
			return nil
		}
		vb, e := b.val.(*predicate)
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
func f_o(a *term, b *term, d kvUnmarsh) interface{} {
	if a.val != nil && b.val != nil {
		va, e := a.val.(*predicate)
		if !e {
			return nil
		}
		vb, e := b.val.(*predicate)
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

func f_n(a *term, b *term, d kvUnmarsh) interface{} {
	if b.val != nil {
		vb, e := b.val.(*predicate)
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

func (p *predicate) eval(d kvUnmarsh) interface{} {
	return p.f(p.a, p.b, d)
}
