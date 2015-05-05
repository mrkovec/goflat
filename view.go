package goflat

import (
	"time"
	"strings"
	"fmt"
	"strconv"
	"sort"
)

type View struct {
	name            string
	query 	string
	filterKeys      *keylist
	from            *keylist
	where           *predicate
	orderby 	*keylist
	hasParams	bool
}
 
type keylist struct {
	keys    []Key
	alias   []Key
}

// create view from parameters
func newView(name, query string) *View {
	return &View{name:name, query:query}
}

func parseKeylist(f string) (*keylist, error) {
	if f == "" {
		return nil, nothingToParse
	}
	spl := strings.Split(f, ",")

	fld := make([]Key, len(spl))
	fldals := make([]Key, len(spl))

	for i, s := range spl {
		s = strings.TrimSpace(s)

		var als []string
		if strings.ContainsAny(s, " ") {
			als = strings.Split(s, " ")
		}

		if len(als) > 0 {
			if strings.ContainsAny(als[0], ILL_CHARS) {
				return nil, feedErr(fmt.Errorf("illegal charakter in %v", als[0]), 1)
			}
			if strings.ContainsAny(als[1], ILL_CHARS) {
				return nil, feedErr(fmt.Errorf("illegal charakter in %v", als[1]), 2)
			}
			fld[i] = Key(als[0])
			//fmt.Printf("%v\n", als[0])

			fldals[i] = Key(als[1])

		} else {
			if strings.ContainsAny(s, ILL_CHARS) {
				return nil, feedErr(fmt.Errorf("illegal charakter in %v", s), 3)
			}
			fld[i] = Key(s)
			fldals[i] = Key(s)
		}

	}
	//fmt.Printf("%s %s\n", fld, fldals )
	return &keylist{keys: fld, alias: fldals}, nil

}

func getFromQuery(s, from, to string) (string, error) {
	st := strings.Index(strings.ToLower(s), from)
	if st < 0 {
		return "", fmt.Errorf("syntax error: no %s statement in %s",from, s)
	}
	st = st + len(from)
	var en int
	if to != "eof" {
		en = strings.Index(strings.ToLower(s), to)
		if en < 0 {
			return "", fmt.Errorf("syntax error: no %s statement in %s", to, s)
		}
	} else {
		en = len(s)
	}
	return strings.TrimSpace(s[st:en]), nil
}

func (v *View) parse(args ...Value) (*View, error) {
	var (
		fields, from, rep, where, orderby string
		err error
	)


	rquery := v.query
	for _, a := range args {
		if  !strings.Contains(rquery, "?") {
			return nil, feedErr(fmt.Errorf("cannot use argument \"%v\" in query \"%s\"",a, rquery),1)
		}
		
		switch t := a.(type) {
		case bool:
			if t {rep = "true"} else {rep = "false"}
		case int64:
			rep = strconv.FormatInt(t,10) 
		case float64:
			rep = strconv.FormatFloat(t,'g',-1,64)
		case string:
			rep = "\""+t+"\""
		default:
			return nil, feedErr(fmt.Errorf("not suported type \"%T\" of parameter \"%v\" in query \"%s\"", a, a, rquery),1)
		}
		rquery = strings.Replace(rquery, "?", rep, 1)
	}
	
	fmt.Println(rquery)

	fields, err = getFromQuery(rquery, "select", "from")
	if err != nil {
		return nil, feedErr(err,1)
	}
	if fields == "*" {
		fields = ""
	}
	from, err = getFromQuery(rquery, "from", "where")
	if err != nil {
		return nil, feedErr(err,2)
	}
	if from == "*" {
		from = ""
	}
	
	if strings.Contains(rquery, "order by") {
		where, err = getFromQuery(rquery, "where", "order by")
		if err != nil {
			return nil, feedErr(err,3)
		}

		orderby, err = getFromQuery(rquery, "order by", "eof")
		if err != nil {
			return nil, feedErr(err,3)
		}

	} else {
		where, err = getFromQuery(rquery, "where", "eof")
		if err != nil {
			return nil, feedErr(err,3)
		}
	}
	


	f, err := parseKeylist(fields)
	if err != nil && err != nothingToParse {
		return nil, feedErr(err, 4)
	}

	fr, err := parseKeylist(from)
	if err != nil && err != nothingToParse {
		return nil, feedErr(err, 5)
	}
	p, err := parsePredicate(where)
	if err != nil && err != nothingToParse {
		return nil, feedErr(err, 6)
	}
	
	o, err := parseKeylist(orderby)
	if err != nil && err != nothingToParse {
		return nil, feedErr(err, 7)
	}	

	//kf := f != nil && p.len() > len(f.keys)

	var nv = &View{name: v.name, query:v.query, where: p, filterKeys: f, from: fr, orderby:o}
	return nv, nil
}



func (v *View) evaulatePredicates(r kvUnmarsh) (bool, error) {
	if v.where != nil {
		return v.where.evaluate(r) 
	}
	return true, nil
}
func (v *View) filter(r kvUnmarsh) (Set, bool) {
	if v.filterKeys != nil {
		nr := NewRecordSet()
		for i, k := range v.filterKeys.keys {
			if val, err := r.unmarshal(k); val != nil && err == nil {
				nr[v.filterKeys.alias[i]] = val
			} else {
				return nil, false
			}
		}
		return nr, true
	}
	return nil, true
}

func sanitizeQuery(i string) (string, error) {
	if strings.ContainsAny(i, ILL_CHARS) {
		return "", fmt.Errorf("illagal character in field name or sytax error in: %s", i)
	}
	return strings.ToLower(i), nil
}

func (v *View) sort(c *[]Set,  nr Set) error {
	var (
		sort_t, sort_f bool
	)

	if v.orderby == nil { 
		*c = append(*c, nr)
		return nil
	}
	//for _, by := range v.orderby.alias {
		by:= v.orderby.keys[0]
		k, e := nr[by]
		if !e {
			return fmt.Errorf("canot order by nonexisting key \"%v\"", by)
		}
		
		sort_t, sort_f = true, false
		if v.orderby.alias[0] == "desc" {
			sort_t, sort_f = false, true
		}



		rc := *c
		i := sort.Search(len(*c), func(i int) bool {
			//fmt.Printf("hee %v\n", len(*c))	
			
			//fmt.Printf("%v (%T)\n", rc[i][by], rc[i][by])		
			switch kt := k.(type) {
			case bool:
				b, e := rc[i][by].(bool)
				if !e { 
					return true
				}
				if kt && !b {
					return sort_t
				}
				return sort_f
			case int64:
				b, e := rc[i][by].(int64)
				if !e { 
					return true
				}
				if kt < b {
					return sort_t
				}
				return sort_f
			case float64:
				b, e := rc[i][by].(float64)
				if !e { 
					return true
				}
				if kt < b {
					return sort_t
				}
				return sort_f
			case time.Time:
				b, e := rc[i][by].(time.Time)
				if !e { 
					return true
				}
				if kt.Before(b) {
					return sort_t
				}
				return sort_f

			}
			//return k < rc[i][by]
			return false
		})
		//fmt.Printf("	%v - %v %v\n", k, i, len(*c))
		if i > len(*c) {
			*c = append(*c, nr)
		} else {
			*c = append(rc[:i], append([]Set{nr}, rc[i:]...)...)
		}
	//}
	return nil
}

/*

func searchTerm(key Key) (int, bool) {
	i := sort.Search(len(k), func(i int) bool {
		return key.Less(k[i])
	})
	if (i > 0  && !k[i-1].Less(key)) {
		return i , true
	}
	return i, false
}

*/