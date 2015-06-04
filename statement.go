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


func (b *basicFlatFile) Insert() *InsertStmt {
	return &InsertStmt{Statement: &Statement{b: b, from: nil, where: nil}, bif: nil, aft: nil}
}
func (b *basicFlatFile) Select(s *Statement) *SelectStmt {
	if s == nil {
		s = NewStatement()
	}
	s.b = b
	return &SelectStmt{Statement: s}
}
func (b *basicFlatFile) Update(s *Statement) *UpdateStmt {
	if s == nil {
		s = NewStatement()
	}
	s.b = b
	return &UpdateStmt{Statement: s, bif: nil, aft: nil}
}
func (b *basicFlatFile) Delete(s *Statement) *DeleteStmt {
	if s == nil {
		s = NewStatement()
	}
	s.b = b
	return &DeleteStmt{Statement: s, bif: nil}
}

// Statement is basis for a DML statement
type Statement struct {
	b     *basicFlatFile
	from  interface{}
	where *Predicate
}

func NewStatement() *Statement {
	return &Statement{b: nil, from: nil, where: nil}
}

// From clause assigns data to be read
func (s *Statement) From(f interface{}) *Statement {
	s.from = f
	return s
}

// Where clause specifies which Sets to retrieve
func (s *Statement) Where(p *Predicate) *Statement {
	s.where = p
	return s
}

// InsertStmt defines a insert statement
type InsertStmt struct {
	*Statement
	bif func(Trx, Set) error
	aft func(Trx, Set) error
}

// BeforeTrigger assigns a trigger which will be executed before the insert statemnet
// in before triger you can modify the inserted Set or prevent the specific Set to be inserted or cancel whole insert statement
func (i *InsertStmt) BeforeTrigger(f func(Trx, Set) error) *InsertStmt {
	i.bif = f
	return i
}

// AfterTrigger assigns a trigger which will be executed after the insert statemnet
// in after triger you can not modify the inserted Set
func (i *InsertStmt) AfterTrigger(f func(Trx, Set) error) *InsertStmt {
	i.aft = f
	return i
}

// Values inserts provided Sets into database
func (i *InsertStmt) Values(r ...Set) error {
	var err error

	if i.bif != nil {
		for _, or := range r {
			err = i.bif(i.b, or)
			if err != nil {
				return feedErrDetail(err, 1, "insert statement error")
			} 
			if err = i.b.encodeData(or); err != nil {
				return feedErrDetail(err, 2, "insert statement error")
			}
		}
	} else {
		//bulk insert
		if err = i.b.encodeData(r...); err != nil {
			return feedErrDetail(err, 3, "insert statement error")
		}
	}

	if i.aft != nil {
		for _, or := range r {
			err = i.aft(i.b, or)
			if err != nil {
				return feedErrDetail(err, 4, "insert statement error")
			}
		}
	}

	i.b.stats.Inserted = i.b.stats.Inserted + len(r)
	i.b.needStore = true
	return nil
}

// InsertStmt defines a select statement
type SelectStmt struct {
	*Statement
}

// All reads all Sets from database which fulfills statements parameters
func (s *SelectStmt) All() ([]Set, error) {
	var (
		err error
		d   kvUnmarsh
		nr  Set
		ve  bool
		//ex bool
	)
	c := make([]Set, 0, len(s.b.data))
	if s.from != nil {
		var data []Set

		switch vsfrom := s.from.(type) {
		case *Statement:
			data, err = s.b.Select(vsfrom).All()
			if err != nil {
				return nil, feedErrDetail(err, 1, "select statement error")
			}
		case []Set:
			data = vsfrom
		default:
			return nil, feedErrDetail(newError(fmt.Errorf("invalid from parameter %s", vsfrom)), 2, "select statement error")
		}

		for _, os := range data {
			ve, err = evalPredic(s.where, os)
			if err != nil {
				return nil, feedErrDetail(err, 3, "select statement error")
			}
			if ve {
				nr, err = os.unmarshalAll()
				if err != nil {
					return nil, feedErrDetail(err, 4, "select statement error")
				}
				c = append(c, nr)
			}
		}

	} else {

		for _, os := range s.b.data {
			d = &recDec{data: os}
			ve, err = evalPredic(s.where, d)
			if err != nil {
				return nil, feedErrDetail(err, 5, "select statement error") 
			}
			if ve {
				nr, err = d.unmarshalAll()
				if err != nil {
					return nil, feedErrDetail(err, 6, "select statement error")
				}
				c = append(c, nr)
			}

		}
	}
	return c, nil
}

// First returns first Sets from database which fulfills statements parameters
func (s *SelectStmt) First() (Set, error) {
	var (
		err error
		d   kvUnmarsh
		nr  Set
		ve  bool
	)
	if s.from != nil {
		var data []Set

		switch vsfrom := s.from.(type) {
		case *Statement:
			data, err = s.b.Select(vsfrom).All()
			if err != nil {
				return nil, feedErrDetail(err, 1, "select statement error")
			}
		case []Set:
			data = vsfrom
		default:
			return nil, feedErrDetail(newError(fmt.Errorf("invalid from parameter %s", vsfrom)), 2, "select statement error")
		}

		for _, os := range data {
			ve, err = evalPredic(s.where, os)
			if err != nil {
				return nil, feedErrDetail(err, 3, "select statement error")
			}
			if ve {
				nr, err = os.unmarshalAll()
				if err != nil {
					return nil, feedErrDetail(err, 4, "select statement error")
				}
				return nr, nil
			}
		}

	} else {

		for _, os := range s.b.data {
			d = &recDec{data: os}
			ve, err = evalPredic(s.where, d)
			if err != nil {
				return nil, feedErrDetail(err, 5, "select statement error")
			}
			if ve {
				nr, err = d.unmarshalAll()
				if err != nil {
					return nil, feedErrDetail(err, 6, "select statement error")
				}
				return nr, nil
			}

		}
	}
	return nil, nil
}

func evalPredic(p *Predicate, d kvUnmarsh) (bool, error) {
	if p == nil {
		return true, nil
	}

	e := p.eval(d)
	if e == nil {
		return false, nil
	}
	ve, ex := e.(bool)
	if !ex {
		return false, newError(fmt.Errorf("invalid predicate result: %v", e))
	}
	return ve, nil
}

type UpdateStmt struct {
	*Statement
	bif func(Trx, Set, Set) error
	aft func(Trx, Set) error
}

func (u *UpdateStmt) BeforeTrigger(f func(Trx, Set, Set) error) *UpdateStmt {
	u.bif = f
	return u
}
func (u *UpdateStmt) AfterTrigger(f func(Trx, Set) error) *UpdateStmt {
	u.aft = f
	return u
}

func (u *UpdateStmt) Set(s Set) (int, error) {
	var (
		err error
		d   kvUnmarsh
		//nr Set
		ve  bool
		num int
	)
	num = 0
	for _, os := range u.b.data {
		d = &recDec{data: os}
		ve, err = evalPredic(u.where, d)
		if err != nil {
			return num, feedErrDetail(err, 1, "update statement error")
		}

		if ve {
			ve = false
			for key, val := range s {
				for i := 0; i < len(os); i = i + 2 {
					if Key(os[i]) == key {
						if u.bif != nil {
							oldv, err := decodeValue(os[i+1])
							if err != nil {
								return num, feedErrDetail(err, 2, "update statement error")
							}
							err = u.bif(u.b, map[Key]Value{key: oldv}, map[Key]Value{key: val})
							if err != nil {
								return num, feedErrDetail(err, 3, "update statement error")
							}
						}

						nval, err := encodeValue(val)
						if err != nil {
							return num, feedErrDetail(err, 4, "update statement error")
						}
						os[i+1] = nval
						ve = true
						u.b.needStore = true

						if u.aft != nil {
							err = u.aft(u.b, map[Key]Value{key: val})
							if err != nil {
								return num, feedErrDetail(err, 5, "update statement error")
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

func (u *UpdateStmt) Add(s Set) (int, error) {
	var (
		err error
		d   kvUnmarsh
		//nr Set
		ve  bool
		num int
		ex  bool
	)
	num = 0
	for osi, os := range u.b.data {
		d = &recDec{data: os}
		ve, err = evalPredic(u.where, d)
		if err != nil {
			return num, feedErrDetail(err, 1, "update statement error")
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
						err = u.bif(u.b, nil, map[Key]Value{key: val})
						if err != nil {
							return num, feedErrDetail(err, 2, "update statement error")
						}
					}

					nval, err := encodeValue(val)
					if err != nil {
						return num, feedErrDetail(err, 3, "update statement error")
					}
					s := make([][]byte, 2)
					s[0] = []byte(key)
					s[1] = nval
					u.b.data[osi] = append(u.b.data[osi], s[0], s[1])
					ve = true
					u.b.needStore = true

					if u.aft != nil {
						err = u.aft(u.b, map[Key]Value{key: val})
						if err != nil {
							return num, feedErrDetail(err, 4, "update statement error")
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

type DeleteStmt struct {
	*Statement
	bif func(Trx, Set) error
}

func (u *DeleteStmt) BeforeTrigger(f func(Trx, Set) error) *DeleteStmt {
	u.bif = f
	return u
}
func (u *DeleteStmt) All() (int, error) {
	var (
		err error
		d   kvUnmarsh
		ve  bool
		num int
	//ex bool
	)
	num = 0
	for i, os := range u.b.data {
		d = &recDec{data: os}
		ve, err = evalPredic(u.where, d)
		if err != nil {
			return num, feedErrDetail(err, 1, "delete statement error")
		}
		if ve {

			if u.bif != nil {
				s, err := d.unmarshalAll()
				if err != nil {
					return num, feedErrDetail(err, 2, "delete statement error")
				}
				err = u.bif(u.b, s)
				if err != nil {
					return num, feedErrDetail(err, 3, "delete statement error")
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

func (u *DeleteStmt) Key(k ...Key) (int, error) {
	var (
		err error
		d   kvUnmarsh
		ve  bool
		num int
		ex  bool
		di  int
	)
	num = 0
	START:

	for i, os := range u.b.data {
		d = &recDec{data: os}
		ve, err = evalPredic(u.where, d)
		if err != nil {
			return num, feedErrDetail(err, 1, "delete statement error")
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
							return num, feedErrDetail(err, 2, "delete statement error")
						}
						err = u.bif(u.b, map[Key]Value{key: v})
						if err != nil {
							return num, feedErrDetail(err, 3, "delete statement error")
						}
					}

					copy(u.b.data[i][di:], u.b.data[i][di+2:])
					u.b.data[i][len(u.b.data[i])-2] = nil
					u.b.data[i][len(u.b.data[i])-1] = nil
					u.b.data[i] = u.b.data[i][:len(u.b.data[i])-2]

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
