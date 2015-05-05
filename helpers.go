package goflat

import (
	//"bytes"
	//"encoding/binary"
	"fmt"
	//"time"
	"sync"
)

func NewRecordSet() Set {
	return make(map[Key]Value)
}

type flatFileVer struct {
	version int
	sync.RWMutex
}
func (f *flatFileVer) get() int {
	f.RLock()
	defer f.RUnlock()
	return f.version
}
func (f *flatFileVer) raise() int {
	f.Lock()
	defer f.Unlock()
	f.version++
	return f.version
}

type kvUnmarsh interface {
	unmarshal(Key) (interface{}, error)
	unmarshalAll() (Set, error)
}
/*func (r Record) unmarshal(k Key) (interface{}, error) {
	return r[k], nil
}
func (r Record) unmarshalAll() (Record, error) {
	return r, nil
}*/


type recDec struct {
	//b     *basicFlatFile
	data  [][]byte
}

func (r *recDec) unmarshal(key Key) (interface{}, error) {
	for i := 0; i < len(r.data); i = i + 2 {
		if Key(r.data[i]) == key {
			return decodeValue(r.data[i+1])
		}
	}
	return nil, nil
}
func (r *recDec) unmarshalAll() (Set, error) {
	return decodeSet(r.data)
}

/*type drec struct {
	b     *basicFlatFile
	data  map[uint][]byte
	sloth Record
	//i int
}

func (b *basicFlatFile) prepDrec(i int) kvUnmarsh {
	//fmt.Printf("%v %v\n", len(b.sloth), i)
	if b.sloth[i] == nil {
		b.sloth[i] = NewRecord()
	}
	return  &drec{b: b, data: b.data[i], sloth: b.sloth[i]}
}*/


/*func newDrec(ib *basicFlatFile, ii int) kvUnmarsh {
	//return &drec{b:ib, data:ib.data[ii], sloth:ib.sloth[ii], i:ii}
	return &drec{b: ib, data: ib.data[ii], sloth: ib.sloth[ii]}
}*/

/*func (d *drec) unmarshal(k Key) (interface{}, error) {
	var (
		val interface{}
		ex bool
	)
	if val, ex = d.sloth[k]; ex {
		return val, nil
	}

	c, ex := d.data[d.b.keysid[k]]
	if !ex {
		return nil, fmt.Errorf("not existing key \"%v\"", k)
	}

	switch c[0] {
	case BOOL_PFX:
		if c[1] == 'T' {
			val = true
		} else {
			val = false
		}
	case INT_PFX:
		var in int64
		buf := bytes.NewReader(c[1:])
		err := binary.Read(buf, binary.LittleEndian, &in)
		if err != nil {
			return nil, fmt.Errorf("cannot convert value to float64: %v", err)
		}
		val = in
	case FLOAT_PFX:
		var fl float64
		buf := bytes.NewReader(c[1:])
		err := binary.Read(buf, binary.LittleEndian, &fl)
		if err != nil {
			return nil, fmt.Errorf("cannot convert value to float64: %v", err)
		}
		val = fl
	case BYTE_PFX:

	default:
		val = string(d.data[d.b.keysid[k]][1:])
	}

	d.sloth[k] = val
	return val, nil
}

func (d *drec) unmarshalAll() (Record, error) {
	var err error
	for _, k := range d.b.idkeys {
		_, err = d.unmarshal(k)
		if err != nil {
			return nil, err
		}
	}
	return d.sloth, nil
}*/






func (c ConControl) String() string {
	switch c {
	case OPTIMISTIC:
		return "optimistic"
	case PESSIMISTIC:
		return "pessimsitic"
	case NOWAIT:
		return "nowait"
	default:
		return "unknown"
	}
}
/*
func (b basicFlatFile) String() string {
	if b.stats.error != nil {
		return fmt.Sprintf("[%s] transaction %s with %s in %v (waited:%v, restarted:%v), rows inserted:%v updated:%v deleted:%v, i/o:%v/%v ", b.user, b.stats.statement, b.stats.error, b.stats.duration, b.stats.waited, b.stats.restarted, statNumber(b.stats.inserted), statNumber(b.stats.updated), statNumber(b.stats.deleted), b.stats.readed, b.stats.writed)
	}
	return fmt.Sprintf("[%s] transaction %s in %v (waited:%v, restarted:%v), rows inserted:%v updated:%v deleted:%v, i/o:%v/%v ", b.user, b.stats.statement, b.stats.duration, b.stats.waited, b.stats.restarted, statNumber(b.stats.inserted), statNumber(b.stats.updated), statNumber(b.stats.deleted), b.stats.readed, b.stats.writed)
}*/

func (b *basicFlatFile) String() string {
	return fmt.Sprintf("[%v:%s] transaction %s in %v (waited:%v, restarted:%v), rows inserted:%v updated:%v deleted:%v", b.id, b.user, b.stats.LastStatement, b.stats.Duration, b.stats.Waited, statNumber(b.stats.Restarts), statNumber(b.stats.Inserted), statNumber(b.stats.Updated), statNumber(b.stats.Deleted))
}



func (o StatementType) String() string {
	switch o {
	case SELECT:
		return "select"
	case INSERT:
		return "insert"
	case UPDATE:
		return "update"
	case DELETE:
		return "delete"
	case COMMIT:
		return "commit"
	case ROLLBACK:
		return "rollback"
	default:
		return "unknown"
	}
}


//Binary prefix
type byteSize float64
const (
	_           = iota // ignore first value by assigning to blank identifier
	KB byteSize = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
	ZB
	YB
)

func (b byteSize) String() string {
	switch {
	case b >= YB:
		return fmt.Sprintf("%.2fYiB", b/YB)
	case b >= ZB:
		return fmt.Sprintf("%.2fZiB", b/ZB)
	case b >= EB:
		return fmt.Sprintf("%.2fEiB", b/EB)
	case b >= PB:
		return fmt.Sprintf("%.2fPiB", b/PB)
	case b >= TB:
		return fmt.Sprintf("%.2fTiB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2fGiB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2fMiB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.2fKiB", b/KB)
	}
	return fmt.Sprintf("%dB", int(b))
}

//SI prefixes
type statNumber float64
const (
	KILO statNumber = 1e3
	MEGA statNumber = 1e6
	GIGA statNumber = 1e9
	TERA statNumber = 1e12
	PETA statNumber = 1e15
	EXA  statNumber = 1e18
)

func (b statNumber) String() string {
	switch {
	case b >= EXA:
		return fmt.Sprintf("%.2fE", b/EXA)
	case b >= PETA:
		return fmt.Sprintf("%.2fP", b/PETA)
	case b >= TERA:
		return fmt.Sprintf("%.2fT", b/TERA)
	case b >= GIGA:
		return fmt.Sprintf("%.2fG", b/GIGA)
	case b >= MEGA:
		return fmt.Sprintf("%.2fM", b/MEGA)
	case b >= KILO:
		return fmt.Sprintf("%.2fk", b/KILO)
	}
	return fmt.Sprintf("%d", int(b))
}



