package goflat

import (
 	"testing"
 	//"reflect"
 	"strings"
)

var termTests = []struct {
	a *Term
	b *Term
	data Set
	result map[string]interface{}
}{
	{
		a: IntTerm(1),
		b: IntTerm(1),
		data: nil,
		result: map[string]interface{}{"Equals": true, "NotEquals": false, "Greater": false, "GreaterEqual": true, "Less": false, "LessEqual": true, "Null": false, "NotNull": true, "StringEval_EqualFold": nil },
	},
	{
		a: IntTerm(1),
		b: IntTerm(2),
		data: nil,
		result: map[string]interface{}{"Equals": false, "NotEquals": true, "Greater": false, "GreaterEqual": false, "Less": true, "LessEqual": true, "Null": false, "NotNull": true, "StringEval_EqualFold": nil },
	},
	{
		a: IntTerm(1),
		b: FloatTerm(1.0),
		data: nil,
		result: map[string]interface{}{"Equals": true, "NotEquals": false, "Greater": false, "GreaterEqual": true, "Less": false, "LessEqual": true, "Null": false, "NotNull": true, "StringEval_EqualFold": nil },
	},
	{
		a: FloatTerm(1.0),
		b: IntTerm(1),
		data: nil,
		result: map[string]interface{}{"Equals": true, "NotEquals": false, "Greater": false, "GreaterEqual": true, "Less": false, "LessEqual": true, "Null": false, "NotNull": true, "StringEval_EqualFold": nil },
	},
	{
		a: IntTerm(1),
		b: StringTerm("1"),
		data: nil,
		result: map[string]interface{}{"Equals": nil, "NotEquals": nil, "Greater": nil, "GreaterEqual": nil, "Less": nil, "LessEqual": nil, "Null": false, "NotNull": true, "StringEval_EqualFold": nil },
	},
	{
		a: IntTerm(1),
		b: KeyTerm("id"),
		data: Set{"id": int64(1)},
		result: map[string]interface{}{"Equals": true, "NotEquals": false, "Greater": false, "GreaterEqual": true, "Less": false, "LessEqual": true, "Null": false, "NotNull": true, "StringEval_EqualFold": nil },
	},
	{
		a: KeyTerm("id"),
		b: IntTerm(1),
		data: Set{"notid": int64(1)},
		result: map[string]interface{}{"Equals": nil, "NotEquals": nil, "Greater": nil, "GreaterEqual": nil, "Less": nil, "LessEqual": nil, "Null": true, "NotNull": false, "StringEval_EqualFold": nil },
	},	
	{
		a: StringTerm("text"),
		b: StringTerm("text"),
		data: nil,
		result: map[string]interface{}{"Equals": true, "NotEquals": false, "Greater": false, "GreaterEqual": true, "Less": false, "LessEqual": true, "Null": false, "NotNull": true, "StringEval_EqualFold": true },
	},		
	{
		a: StringTerm("text"),
		b: StringTerm("Text"),
		data: nil,
		result: map[string]interface{}{"Equals": false, "NotEquals": true, "Greater": true, "GreaterEqual": true, "Less": false, "LessEqual": false, "Null": false, "NotNull": true, "StringEval_EqualFold": true },
	},	
	{
		a: KeyTerm("name"),
		b: StringTerm("john"),
		data: Set{"name": "JOHN"},
		result: map[string]interface{}{"Equals": false, "NotEquals": true, "Greater": false, "GreaterEqual": false, "Less": true, "LessEqual": true, "Null": false, "NotNull": true, "StringEval_EqualFold": true },
	},	
	{
		a: &Term{},
		b: IntTerm(1),
		data: nil,
		result: map[string]interface{}{"Equals": nil, "NotEquals": nil, "Greater": nil, "GreaterEqual": nil, "Less": nil, "LessEqual": nil, "Null": true, "NotNull": false, "StringEval_EqualFold": nil },
	},

}

func TestEquals(t *testing.T){
	op := "Equals"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.Equals(test.b).eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
func TestNotEquals(t *testing.T){
	op := "NotEquals"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.NotEquals(test.b).eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
func TestGreater(t *testing.T){
	op := "Greater"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.Greater(test.b).eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
func TestGreaterEqual(t *testing.T){
	op := "GreaterEqual"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.GreaterEqual(test.b).eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
func TestLess(t *testing.T){
	op := "Less"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.Less(test.b).eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
func TestLessEqual(t *testing.T){
	op := "LessEqual"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.LessEqual(test.b).eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
func TestNull(t *testing.T){
	op := "Null"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.Null().eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
func TestNotNull(t *testing.T){
	op := "NotNull"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.NotNull().eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
func TestStringEval_EqualFold(t *testing.T){
	op := "StringEval_EqualFold"
	for i, test := range termTests {
    	e, g := test.result[op], test.a.StringEval(test.b, strings.EqualFold).eval(test.data)
		if e != g {
			t.Errorf("test%v: %T=%v %v %T=%v - expected %v and got %v",i, test.a.val, test.a.val, op, test.b.val, test.b.val, e, g )
		}	
  	}
}
