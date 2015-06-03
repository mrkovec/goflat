package goflat

import (
	//"bytes"
	//"encoding/binary"
	"fmt"
	//"time"
	"sync"
	"os"
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
type recDec struct {
	data [][]byte
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

func (s Set) unmarshal(key Key) (interface{}, error) {
	v, _ := s[key]
	return v, nil
}
func (s Set) unmarshalAll() (Set, error) {
	return s, nil
}

func NewEmptyDatabase(name string) error {
	f, err := os.OpenFile(name ,os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_TRUNC, os.FileMode(0600))
	if err != nil {
		return err
	}
	f.Close()	
	return nil
}

func (b *basicFlatFile) String() string {
	return fmt.Sprintf("[%v:%s] transaction %s in %v (waited:%v, restarted:%v), rows inserted:%v updated:%v deleted:%v", b.id, b.user, b.stats.LastStatement, b.stats.Duration, b.stats.Waited, statNumber(b.stats.Restarts), statNumber(b.stats.Inserted), statNumber(b.stats.Updated), statNumber(b.stats.Deleted))
}

func (o statementType) String() string {
	switch o {
	case sELECT:
		return "select"
	case iNSERT:
		return "insert"
	case uPDATE:
		return "update"
	case dELETE:
		return "delete"
	case cOMMIT:
		return "commit"
	case rOLLBACK:
		return "rollback"
	default:
		return "unknown"
	}
}

//Binary prefix
type byteSize float64

const (
	xx           = iota
	bKB byteSize = 1 << (10 * iota)
	bMB
	bGB
	bTB
	bPB
	bEB
	bZB
	bYB
)

func (b byteSize) String() string {
	switch {
	case b >= bYB:
		return fmt.Sprintf("%.2fYiB", b/bYB)
	case b >= bZB:
		return fmt.Sprintf("%.2fZiB", b/bZB)
	case b >= bEB:
		return fmt.Sprintf("%.2fEiB", b/bEB)
	case b >= bPB:
		return fmt.Sprintf("%.2fPiB", b/bPB)
	case b >= bTB:
		return fmt.Sprintf("%.2fTiB", b/bTB)
	case b >= bGB:
		return fmt.Sprintf("%.2fGiB", b/bGB)
	case b >= bMB:
		return fmt.Sprintf("%.2fMiB", b/bMB)
	case b >= bKB:
		return fmt.Sprintf("%.2fKiB", b/bKB)
	}
	return fmt.Sprintf("%dB", int(b))
}

//SI prefixes
type statNumber float64

const (
	sKILO statNumber = 1e3
	sMEGA statNumber = 1e6
	sGIGA statNumber = 1e9
	sTERA statNumber = 1e12
	sPETA statNumber = 1e15
	sEXA  statNumber = 1e18
)

func (b statNumber) String() string {
	switch {
	case b >= sEXA:
		return fmt.Sprintf("%.2fE", b/sEXA)
	case b >= sPETA:
		return fmt.Sprintf("%.2fP", b/sPETA)
	case b >= sTERA:
		return fmt.Sprintf("%.2fT", b/sTERA)
	case b >= sGIGA:
		return fmt.Sprintf("%.2fG", b/sGIGA)
	case b >= sMEGA:
		return fmt.Sprintf("%.2fM", b/sMEGA)
	case b >= sKILO:
		return fmt.Sprintf("%.2fk", b/sKILO)
	}
	return fmt.Sprintf("%d", int(b))
}
