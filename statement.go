package goflat

import (
	"fmt"
	//"unsafe"
	//"reflect"
	//"sync"
	//"bytes"
	//"encoding/binary"
	//"time"
)

func (b *basicFlatFile) Insert() *insStatement {
	return &insStatement{b:b, bif:nil, aft:nil}
}
func (b *basicFlatFile) Select() *selStatement {
	return &selStatement{b:b, from:nil, where:nil}
}
func (b *basicFlatFile) Update() *updStatement {
	return &updStatement{b:b, bif:nil, aft:nil}
}
func (b *basicFlatFile) Delete() *delStatement {
	return &delStatement{b:b, bif:nil}
}
/*func (b *basicFlatFile) Update(viewName string, args ...Value, set Set) (int, error) {
	d, err :=  b.Select(viewName, args...)
	if err != nil {
		return 0, feedErr(err, 1)
	}
	
	return 0, nil
}*/
type insStatement struct {
	b *basicFlatFile
	bif func(Trx, Set) error
	aft func(Trx, Set) error
}
func (i *insStatement) Values(r ...Set) error {
	var err error
	
	if i.bif != nil {
		for _, or := range r {
			err = i.bif(i.b, or)
			if err != nil {
				return feedErr(err, 1)
			}
		}
	}
	
	if err = i.b.encodeData(r...); err != nil {
		return feedErr(err, 2)
	}

	if i.aft != nil {
		for _, or := range r {
			err = i.aft(i.b, or)
			if err != nil {
				return feedErr(err, 3)
			}
		}
	}	

	i.b.stats.Inserted = i.b.stats.Inserted + len(r)
	i.b.needStore = true
	return nil	
}
func (i *insStatement) BeforeTrigger(f func(Trx, Set) error) *insStatement {
	i.bif = f
	return i
}	
func (i *insStatement) AfterTrigger(f func(Trx, Set) error) *insStatement {
	i.aft = f
	return i
}

type selStatement struct {
	b *basicFlatFile
	from interface{}
	where *predicate
}
func (s *selStatement) From(f interface{}) *selStatement {
	s.from = f
	return s
}
func (s *selStatement) Where(p *predicate) *selStatement {
	s.where = p
	return s
}
func (s *selStatement) AllRows() ([]Set, error) {
	var (
		err error
 		d kvUnmarsh
 		nr Set
 		ve bool
 		//ex bool
 	)
 	c := make([]Set, 0, len(s.b.data))
 	if s.from != nil {
 		var data []Set

 		switch vsfrom := s.from.(type) {
 		case *selStatement:
 			data, err = vsfrom.AllRows()
 			if err != nil {
 				return nil, feedErr(err,1)
 			}
 		case []Set:
 			data = vsfrom
 		default:
 			return nil, feedErr(fmt.Errorf("invalid from parameter"),2)
 		}
 		
 		for _, os := range data {
 			ve, err = evalPredic(s.where, os)
 			if err != nil {
 				return nil, feedErr(err, 3)
 			}
            if ve {
 				nr, err = os.unmarshalAll()
				if err != nil { 
					return nil, feedErr(err,4)
				}
				c = append(c, nr)
	        }
 		}

 	} else {

	 	for _, os := range s.b.data {
	 		d = &recDec{data:os}
	 		ve, err = evalPredic(s.where, d)
			if err != nil {
 				return nil, feedErr(err, 5)
 			}	        
	        if ve {
				nr, err = d.unmarshalAll()
				if err != nil { 
					return nil, feedErr(err , 6)
				}
				c = append(c, nr)
	        }

	 	}
	 }
 	return c, nil
}

func evalPredic(p *predicate, d kvUnmarsh) (bool, error) {
	if p == nil {
		return true, nil
	}

	e := p.eval(d)
	if e == nil {
		return false, nil
	}
	ve, ex := e.(bool)
   	if !ex { 
		return false, fmt.Errorf("invalid predicate result: %v", e)
	}
	return ve, nil
}

type updStatement struct {
	b *basicFlatFile
	where *predicate
	bif func(Trx, Set, Set) error
	aft func(Trx, Set) error
}

func (u *updStatement) Where(p *predicate) *updStatement {
	u.where = p
	return u
}
func (u *updStatement) BeforeTrigger(f func(Trx, Set, Set) error) *updStatement {
	u.bif = f
	return u
}	
func (u *updStatement) AfterTrigger(f func(Trx, Set) error) *updStatement {
	u.aft = f
	return u
}

func (u *updStatement) Set(s Set) (int, error) {
	var (
		err error
 		d kvUnmarsh
 		//nr Set
 		ve bool
 		num int
  	)
	num = 0
 	for _, os := range u.b.data {
 		d = &recDec{data:os}
 		ve, err = evalPredic(u.where, d)
 		if err != nil {
 			return num, feedErr(err, 1)
 		}

		if ve {
			ve = false
			for key, val := range s {
				
				for i := 0; i < len(os); i = i + 2 {
					if Key(os[i]) == key {
						if u.bif != nil {
							oldv, err := decodeValue(os[i+1])
							if err != nil {
								return num, feedErr(err, 2)
							}
							err = u.bif(u.b, map[Key]Value{key:oldv}, map[Key]Value{key:val})
							if err != nil {
								return num, feedErr(err, 3)
							}
						}
						
						nval, err := encodeValue(val)
						if err != nil {
							return num, feedErr(err, 4)
						}

						os[i+1] = nval
						ve = true
						u.b.needStore = true

						if u.aft != nil {
							err = u.aft(u.b, map[Key]Value{key:val})
							if err != nil {
								return num, feedErr(err, 5)
							}
						}

					}
				}
			}
			if ve {
				num++
			}
		}
 	}
	
	u.b.stats.Updated = u.b.stats.Updated + num
 	return num, nil
}

func (u *updStatement) Add(s Set) (int, error) {
	var (
		err error
 		d kvUnmarsh
 		//nr Set
 		ve bool
 		num int
 		ex bool
  	)
	num = 0
 	for osi, os := range u.b.data {
 		d = &recDec{data:os}
 		ve, err = evalPredic(u.where, d)
 		if err != nil {
 			return num, feedErr(err, 1)
 		}

		if ve {
			ve = false
			for key, val := range s {
				ex = false
				for i := 0; i < len(os); i = i + 2 {
					if Key(os[i]) == key {
						ex = true
						break
					}
				}
				if !ex {

						if u.bif != nil {
							err = u.bif(u.b, nil, map[Key]Value{key:val})
							if err != nil {
								return num, feedErr(err, 3)
							}
						}
						
						nval, err := encodeValue(val)
						if err != nil {
							return num, feedErr(err, 4)
						}

						s := make([][]byte, 2)
						s[0] = []byte(key)
						s[1] = nval
						u.b.data[osi] = append(u.b.data[osi], s[0], s[1])

						ve = true
						u.b.needStore = true

						if u.aft != nil {
							err = u.aft(u.b, map[Key]Value{key:val})
							if err != nil {
								return num, feedErr(err, 5)
							}
						}
				}
			}
			if ve {
				num++
			}
		}
 	}
	
	u.b.stats.Updated = u.b.stats.Updated + num
 	return num, nil
}

type delStatement struct {
	b *basicFlatFile
	where *predicate
	bif func(Trx, Set) error
}
func (u *delStatement) Where(p *predicate) *delStatement {
	u.where = p
	return u
}
func (u *delStatement) BeforeTrigger(f func(Trx, Set) error) *delStatement {
	u.bif = f
	return u
}	
func (u *delStatement) Row() (int, error) {
		var (
		err error
 		d kvUnmarsh
 		ve bool
 		num int
 		//ex bool
  	)
	num = 0
 	for i, os := range u.b.data {
 		d = &recDec{data:os}
 		ve, err = evalPredic(u.where, d)
 		if err != nil {
 			return num, feedErr(err, 1)
 		}
 		if ve {

			if u.bif != nil {
				s, err := d.unmarshalAll()
				if err != nil {
					return num, feedErr(err, 2)
				}				
				err = u.bif(u.b, s)
				if err != nil {
					return num, feedErr(err, 2)
				}
			}

 			u.b.data[i], u.b.data[len(u.b.data)-1], u.b.data = u.b.data[len(u.b.data)-1], nil, u.b.data[:len(u.b.data)-1]
 			u.b.needStore = true
 			num++
 		}
 	}
 	u.b.stats.Deleted = u.b.stats.Deleted + num
 	return num, nil
}
func (u *delStatement) Key(k ...Key) (int, error) {
		var (
		err error
 		d kvUnmarsh
 		ve bool
 		num int
 		ex bool
 		di int
  	)
	num = 0	
	START:


 	for i, os := range u.b.data {
 		d = &recDec{data:os}
 		ve, err = evalPredic(u.where, d)
 		if err != nil {
 			return num, feedErr(err, 1)
 		}
 		ex = false
 		if ve {
 			ve = false
			for _, key := range k {
				//fmt.Println(key)
				ex = false
				for i := 0; i < len(os); i = i + 2 {
						if Key(os[i]) == key {
							ex = true
							di = i
							break
						}
				}
				if ex {

					if u.bif != nil {
						v, err := d.unmarshal(key)
						if err != nil {
							return num, feedErr(err, 2)
						}
						err = u.bif(u.b, map[Key]Value{key:v})
						if err != nil {
							return num, feedErr(err, 2)
						}
					}

					//delete(os, key)
					//fmt.Printf("%v %v\n", di, len(u.b.data[i]))
					/*u.b.data[i][di], u.b.data[i][len(u.b.data[i])-1], u.b.data[i] = u.b.data[i][len(u.b.data[i])-1], nil, u.b.data[i][:len(u.b.data[i])-1]
					fmt.Println("a")
					u.b.data[i][di], u.b.data[i][len(u.b.data[i])-1], u.b.data[i] = u.b.data[i][len(u.b.data[i])-1], nil, u.b.data[i][:len(u.b.data[i])-1]
					//u.b.data[i][di+1], u.b.data[i][len(u.b.data[i])-1], u.b.data[i] = u.b.data[i][len(u.b.data[i])-1], nil, u.b.data[i][:len(u.b.data[i])-1]
					fmt.Println("b")*/
					copy(u.b.data[i][di:], u.b.data[i][di+2:])
					u.b.data[i][len(u.b.data[i])-2] = nil // or the zero value of T
					u.b.data[i][len(u.b.data[i])-1] = nil // or the zero value of T
					u.b.data[i] = u.b.data[i][:len(u.b.data[i])-2]				

					//fmt.Printf("%v\n", len(u.b.data[i]))
		 			u.b.needStore = true
					ve = true

					if len(u.b.data[i]) == 0 {
						u.b.data[i], u.b.data[len(u.b.data)-1], u.b.data = u.b.data[len(u.b.data)-1], nil, u.b.data[:len(u.b.data)-1]
					}
				}
			}
			if ve { 			
 				num++
 				goto START
 			}
 		}
 	}

 	u.b.stats.Deleted = u.b.stats.Deleted + num
 	return num, nil
}
/*type statement struct {
	b *basicFlatFile
	t StatementType
	bif func(Trx, Set) error
	aft func(Trx, Set) error
}*/



//updStatement interface






// func (b *basicFlatFile) read(v *View) ([]Set, error) {
// 	var (
// 		//i int
// 		d kvUnmarsh
// 		err error
// 	)
// 	c := make([]Set, 0, len(b.data))
// 	for _, s := range b.data {
// 		d = &recDec{data:s}
// 		err = b.eval(v, d, &c)
// 		if err != nil { 
// 			return nil, err	
// 		}
// 	}
// 	return c, nil
// }


// func (b *basicFlatFile) eval(v *View, d kvUnmarsh, c *[]Set) error {
// 	var (
// 		nr Set
// 		ex bool
// 		err error
// 	)
// 	nr, ex = v.filter(d)
// 	if ex {
// 		ex, err = v.evaulatePredicates(d)
// 		if err!= nil { 
// 			return err 
// 		}
// 		if ex {
// 			if nr != nil {
// 				//*c = append(*c, nr)
// 				v.sort(c, nr)
// 			} else {
// 				//unmarshal all fields
// 				nr, err = d.unmarshalAll()
// 				if err!= nil { 
// 					return err 
// 				}
// 				//*c = append(*c, nr)
// 				v.sort(c, nr)
// 			}
// 		}
// 	}
// 	return nil
// }
 
// func (b *basicFlatFile) Select(viewName string, args ...Value) ([]Set, error) {
// 	v, err := b.dbConn.view(viewName)
// 	if err != nil {
// 		return nil, feedErr(err, 1)
// 	}

// 	v, err = v.parse(args...)
// 	if err != nil {
// 		return nil, feedErr(err, 2)
// 	}
// 	/*if v.from != nil {
// 		return b.join(v), nil
// 	}*/
// 	return b.read(v)
// }


//  