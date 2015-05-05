package goflat

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	//"time"
)

var (
	nothingToParse = errors.New("nothing to parse")
)

const ILL_CHARS string = "'\"()[]{},;?!*"

type operation int

const (
	UNSUPORTED operation = iota
	EQUAL
	NOTEQUAL
	GREATER
	LESS
	GREATEREQUAL
	LESSEQUAL
	IN
	AND
	OR
	NOT
	EXISTS
)


type predicate struct {
	op operation
	A  term
	B  term
}



func (p *predicate) evaluate(r kvUnmarsh) (bool, error) {
	var (
		a, b, result bool
		err error
	)
	//fmt.Printf("A:%v B:%v => %v\n", p.A, p.B, r.UnmarshalAll())
	switch p.op {
	case AND:
		a, err = p.A.predicate().evaluate(r)
		if err != nil {return false, err}
		if a {
			b, err = p.B.predicate().evaluate(r)
			if err != nil {return false, err}
			result = a && b
		} else {
			result = false
		}
	case OR:
		a, err = p.A.predicate().evaluate(r)
		if err != nil {return false, err}
		if !a {
			b, err = p.B.predicate().evaluate(r)
			if err != nil {return false, err}
			result = b
		} else {
			result = true
		}
		//result = p.A.predicate().evaluate(r) || p.B.predicate().evaluate(r)
	case NOT:
		b, err = p.B.predicate().evaluate(r)
		if err != nil {return false, err}		
		result = !b
		//result = !p.B.predicate().evaluate(r)
	case EXISTS:

	case EQUAL:
		result, err = p.A.equal(p.B, r)
		if err != nil {return false, err}
	case NOTEQUAL:
		result, err = p.A.notequal(p.B, r)
		if err != nil {return false, err}
	case GREATER:
		result, err = p.A.greater(p.B, r)
		if err != nil {return false, err}
	case LESS:
		result, err = p.A.less(p.B, r)
		if err != nil {return false, err}
	case GREATEREQUAL:
		a, err = p.A.greater(p.B, r)
		if err != nil {return false, err}
		if !a { 
			b, err = p.A.equal(p.B, r)
			if err != nil {return false, err}		
			result = b
		} else {
			result = false
		}
		//result = p.A.greaterequal(p.B, r)
	case LESSEQUAL:
		a, err = p.A.less(p.B, r)
		if err != nil {return false, err}
		if !a { 
			b, err = p.A.equal(p.B, r)
			if err != nil {return false, err}		
			result = b
		} else {
			result = false
		}		
		//result = p.A.lessequal(p.B, r)
	default:
		result = false
	}
	return result, nil
}

func parsePredicate(s string) (*predicate, error) {
	//fmt.Printf("parsePredicate: %s\n", s)
	s = strings.Trim(s, " ")
	if s == "" {
		return nil, nothingToParse
	}
	p := &predicate{}
	var err error

	a, b := splitPredicate2(s, "or")
	if a != "" {
		p.op = OR
		tp, err := parsePredicate(a)
		if err != nil {
			return nil, feedErr(err, 1)
		}
		p.A = newPredicateTerm(tp)

		tp, err = parsePredicate(b)
		if err != nil {
			return nil, feedErr(err, 2)
		}
		p.B = newPredicateTerm(tp)
		return p, nil

	}
	a, b = splitPredicate2(s, "and")
	if a != "" {
		p.op = AND
		tp, err := parsePredicate(a)
		if err != nil {
			return nil, feedErr(err, 3)
		}
		p.A = newPredicateTerm(tp)

		tp, err = parsePredicate(b)
		if err != nil {
			return nil, feedErr(err, 4)
		}
		p.B = newPredicateTerm(tp)
		return p, nil
	}

	a, b = splitPredicate2(s, "not")
	if b != "" {
		p.op = NOT
		p.A = nil
		tp, err := parsePredicate(b)
		if err != nil {
			return nil, feedErr(err, 5)
		}
		p.B = newPredicateTerm(tp)
		return p, nil
	}

	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") && strings.Count(s, "(") == strings.Count(s, ")") {
		p, err = parsePredicate(strings.Trim(s, "()"))
		if err != nil {
			return nil, feedErr(err, 6)
		}
		return p, nil
	}
	//fmt.Printf("= [%s]\n", s)
	var spl []string
	if strings.Contains(s, "exists") {
	
	} else if strings.Contains(s, "=") {
		if strings.Contains(s, "!=") {
			spl = strings.Split(s, "!=")
			p.op = NOTEQUAL
		} else if strings.Contains(s, ">=") {
			spl = strings.Split(s, ">=")
			p.op = GREATEREQUAL
		} else {
			spl = strings.Split(s, "=")
			p.op = EQUAL
		}
	} else if strings.Contains(s, ">") {
		spl = strings.Split(s, ">")
		p.op = GREATER

	} else if strings.Contains(s, "<") {
		spl = strings.Split(s, "<")
		p.op = LESS
	}

	if len(spl) != 2 {
		return nil, fmt.Errorf("Syntax error in: %s", s)
	}

	p.A, err = convertTermType(spl[0])
	if err != nil {
		return nil, feedErr(err, 7)
	}
	p.B, err = convertTermType(spl[1])
	if err != nil {
		return nil, feedErr(err, 8)
	}
	//fmt.Printf("%v\n", p)
	return p, nil
}

func convertTermType(a string) (term, error) {
	a = strings.TrimSpace(a)
	if (strings.HasPrefix(a, "\"") || strings.HasPrefix(a, "'")) && (strings.HasSuffix(a, "\"") || strings.HasSuffix(a, "'")) { //strings.Contains(a, "\"") || strings.Contains(a, "'") {
		//string
		return newTextTerm(strings.Trim(a, "\"'")), nil
	}

	//boolean
	if a == "true" {
		return newBooleanTerm(true), nil
	}
	if a == "false" {
		return newBooleanTerm(false), nil
	}
	//number?
	i, err := strconv.ParseInt(a, 10, 64)
	if err == nil {
		//f := float64(i)
		return newIntegerTerm(i), nil
	}
	f, err := strconv.ParseFloat(a, 64)
	if err == nil {
		return newFloatTerm(f), nil
	}
	
	if len(a) == 0 {
		return nil, fmt.Errorf("zero length of parameter: %s", a)
	}	
	if strings.ContainsAny(a, ILL_CHARS) {
		return nil, fmt.Errorf("illagal character in field name or sytax error in: %s", a)
	}
	return newKeyTerm(strings.ToLower(a)), nil
}

func splitPredicate2(b string, s string) (string, string) {
	//var nest = make(map[string]string)
	//var output string
	var nlp, nrp int = 0, 0
	//var ops, ope int = -1, -1
	var ops, ope int = -1, -1
	//var numnest int = 0
	/*if !utf8.Valid(b) {
		return nil, fmt.Errorf("Not valid utf-8 text")
	}*/
	var parenthesis bool = false
	var text bool = false
	var word bool = false

	for i, r := range b {

		if !parenthesis && !text && r == '(' {
			parenthesis = true
			//nlp++
		}

		if !parenthesis {
			if r == '\'' || r == '\u0022' {
				if !text {
					text = true
				} else {
					text = false
				}
			}
		}

		if parenthesis {
			if r == '(' {
				nlp++
			}
			if r == ')' {
				nrp++
			}
			if nlp == nrp {
				parenthesis = false
				nlp, nrp = 0, 0
			}
		}

		if !parenthesis && !text {
			if r != ' ' && r != '(' && r != ')' && r != '\'' && r != '\u0022' {
				if !word {
					word = true
					ops = i
				}
			} else {
				if word {
					word = false
					ope = i
				}
			}
		} else {
			if word {
				word = false
				ope = i
			}
		}

		//fmt.Printf("%v %c - %v %v %v (%v:%v)\n",i, r, parenthesis, text, word, ops, ope)

		if ops >= 0 && ope > 0 {
			//fmt.Printf("%v - (%v:%v)(%s) [%s][%s]\n", i, ops, ope, b[ops:ope] , b[:ops], b[ope:])
			if strings.Contains(strings.ToLower(b[ops:ope]), strings.ToLower(s)) {
				return b[:ops], b[ope:]
			}
			ops, ope = -1, -1
			word = false
		}
	}
	return "", ""
}

/*func (p *predicate) explain(i int) string {
	var a, b string
	oddel := strings.Repeat(" ", i)
	i = i + 2
	if p.op != NOT {
		if p.A.typeof() == PREDICATE {
			a = oddel + "[\n" + p.A.predicate().explain(i) + "\n" + oddel + "]\n"
		} else {
			//a = oddel + "" + fmt.Sprintf("%T:%v", p.A, p.A)+ "\n"
			a = fmt.Sprintf("%s%v\n", oddel, p.A)
		}
	}
	if p.B.typeof() == PREDICATE {
		b = oddel + "[\n" + p.B.predicate().explain(i) + "\n" + oddel + "]"
	} else {
		//b = oddel + "[\n" + oddel2 + fmt.Sprintf("%T: %v", p.B, p.B) + "\n" + oddel + "]"
		//b = oddel + "" + fmt.Sprintf("%T:%v", p.B, p.B)+ ""
		b = fmt.Sprintf("%s%v", oddel, p.B)
	}
	return fmt.Sprintf("%s%s%s [pca:%.2f(%v/%v) cos:%v]\n%s", a, oddel, p.op, (p.call)/(p.ctrue), p.ctrue, p.call, statNumber(p.cost.Nanoseconds()/1e6), b)
	//(predicate cardinality:
}

func (p *predicate) len() (r int) {
	r = 1

	if p.A.typeof() == PREDICATE {
		r = r + p.A.predicate().len()
	}
	if p.B.typeof() == PREDICATE {
		r = r + p.B.predicate().len()
	}
	return
}*/