package goflat

import (
	"strings"
	"fmt"
	"sync"
)

type trigType int
const (
	BEFORE trigType = iota
	AFTER
	INSTEAD
) 

type Trigger struct {
	name            string
	def 	string
	f TriggerFunc
	when trigType 
	statement StatementType 
	on string
	forEachRow bool
	enabled bool

	sync.Mutex

}
type TriggerFunc func(Trx, Set, Set) error

//before update emp each row
func newTrigger(name, def string, f TriggerFunc) (*Trigger, error) {
	name = strings.ToLower(name)
	def = strings.ToLower(def)
	t := &Trigger{name:name, def: def, f:f}

	

	s := strings.SplitAfter(def, " ")
	if len(s) < 2 {
		return nil, fmt.Errorf("sintax error in %v", def)
	}


	switch strings.TrimSpace(s[0]) {
	case "before":
		t.when = BEFORE
	case "after":
		t.when = AFTER
	case "instead":
		t.when = INSTEAD
	default:
		return nil, fmt.Errorf("unknown command \"%v\" in \"%v\"", s[0], def)
	}

	switch strings.TrimSpace(s[1]) {
	case "insert":
		t.statement = INSERT
	case "update":
		t.statement = UPDATE
	case "delete":
		t.statement = DELETE
	default:
		return nil, fmt.Errorf("unknown command \"%v\" in \"%v\"", s[0], def)
	}

	t.forEachRow = false
		
	if t.statement == INSERT || len(s) == 2 {
		t.on = "*"
		if len(s) > 2 {
			if strings.TrimSpace(s[2]) == "each" {
				t.forEachRow = true
			} else {
				return nil, fmt.Errorf("unknown command \"%v\" in \"%v\"", s[2], def)			
			}
		}
	} else {

		if strings.TrimSpace(s[2]) == "*" {
			t.on = "*"
		} else {
			if strings.ContainsAny(s[2], ILL_CHARS) {
				return nil, fmt.Errorf("illagal character in field name or sytax error in: %s", s[2])
			}
			t.on = strings.TrimSpace(s[2])
		}
		if len(s) > 3 {
			if strings.TrimSpace(s[3]) == "each" {
				t.forEachRow = true
			} else {
				return nil, fmt.Errorf("unknown command \"%v\" in \"%v\"", s[3], def)			
			}
		}
	}
	t.enabled = true
	return t, nil

}
