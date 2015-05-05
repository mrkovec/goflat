package goflat

import (
	//"fmt"
	//"unsafe"
	//"reflect"
	//"sync"
	//"bytes"
	//"encoding/binary"
	//"time"
)





func (b *basicFlatFile) Insert(r ...Set) (err error) {
	/*if err = b.dbConn.runTriggers("*", BEFORE, INSERT, r...); err != nil {
		return feedErr(err, 1)
	}*/

	if err = b.encodeData(r...); err != nil {
		return feedErr(err, 2)
	}
	b.stats.Inserted = b.stats.Inserted + len(r)
	b.needStore = true
	return nil
}




func (b *basicFlatFile) read(v *View) ([]Set, error) {
	var (
		//i int
		d kvUnmarsh
		err error
	)
	c := make([]Set, 0, len(b.data))
	for _, s := range b.data {
		d = &recDec{data:s}
		err = b.eval(v, d, &c)
		if err != nil { 
			return nil, err	
		}
	}
	return c, nil
}


func (b *basicFlatFile) eval(v *View, d kvUnmarsh, c *[]Set) error {
	var (
		nr Set
		ex bool
		err error
	)
	nr, ex = v.filter(d)
	if ex {
		ex, err = v.evaulatePredicates(d)
		if err!= nil { 
			return err 
		}
		if ex {
			if nr != nil {
				//*c = append(*c, nr)
				v.sort(c, nr)
			} else {
				//unmarshal all fields
				nr, err = d.unmarshalAll()
				if err!= nil { 
					return err 
				}
				//*c = append(*c, nr)
				v.sort(c, nr)
			}
		}
	}
	return nil
}

/*func (b *basicFlatFile) join(v *View) []Record {
	var (
		end  bool = true
		a, j int
		all  bool = true
		key  Key
		val  interface{}
		//nkey string
		//sep string = "."
	)

	tRecs := make([][]Record, len(v.from.keys))
	var mxln int
	for j = 0; j < len(v.from.keys); j++ {
		tRecs[j] = b.read(uberViewIndex[string(v.from.keys[j])])

		for a = 0; a < len(tRecs[j]); a++ {
			//rename keys according to view name
			nkey := v.from.alias[j] + "."
			for key, val = range tRecs[j][a] {
				delete(tRecs[j][a], key)
				key = nkey + key
				tRecs[j][a][key] = val
			}
		}

		if len(tRecs[j]) > mxln {
			mxln = len(tRecs[j])
		}
	}

	is := make([]int, len(v.from.keys))
	c := make([]Record, 0, mxln)

	//000...
	trec := NewRecord()

	for j = 0; j < len(is); j++ {
		for key, val = range tRecs[j][0] {
			//key = v.from.alias[j] + "." + key
			trec[key] = val
		}
	}
	b.eval(v, trec, &c)

	for {
		for j = 0; j < len(is); j++ {
			all = true
			for a = j + 1; a < len(is); a++ {
				all = all && is[a] == (len(tRecs[a])-1)
			}
			if all {
				is[j]++
				for a = j + 1; a < len(is); a++ {
					is[a] = 0
				}
				break
			}
		}

		for key = range trec {
			delete(trec, key)
		}

		for j = 0; j < len(is); j++ {
			for key, val = range tRecs[j][is[j]] {
				trec[key] = val
			}

		}
		b.eval(v, trec, &c)

		end = true
		for j = 0; j < len(is); j++ {
			end = end && is[j] == len(tRecs[j])-1
		}
		if end {
			break
		}
	}
	return c
}
*/
func (b *basicFlatFile) Select(viewName string, args ...Value) ([]Set, error) {
	v, err := b.dbConn.view(viewName)
	if err != nil {
		return nil, feedErr(err, 1)
	}

	v, err = v.parse(args...)
	if err != nil {
		return nil, feedErr(err, 2)
	}
	/*if v.from != nil {
		return b.join(v), nil
	}*/
	return b.read(v)
}



/*func (b *basicFlatFile) Update(viewName string, args ...Value) error {

}*/