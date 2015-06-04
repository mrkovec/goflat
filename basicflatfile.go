package goflat

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	//"bytes"
	"io"
	"time"
	//"runtime"
	// "github.com/mrkovec/goflat"
	"bytes"
	"hash/fnv"
	"sync"
)

var (
	uberLock    map[string]chan struct{}
	uberVersion map[string]*flatFileVer
	sessionPool sync.Pool
)

var (
	// default timeout value for new transactions
	DefaultTrxTimeout time.Duration = 10 * time.Second
	// default locking method for new transactions
	DefaultTrxLocking ConControl = OPTIMISTIC
)

//http://en.wikipedia.org/wiki/Concurrency_control#Database_transaction_and_the_ACID_rules
// concurrency control method
type ConControl int

const (
	OPTIMISTIC ConControl = iota
	PESSIMISTIC
	NOWAIT
)

// transaction parameters
type Config struct {
	Timeout time.Duration
	Locking ConControl
}

type statementType int

const (
	sELECT statementType = iota
	iNSERT
	uPDATE
	dELETE
	cOMMIT
	rOLLBACK
)

// cumulative session statistics
type Stats struct {
	// last executed statement
	LastStatement  statementType
	StatementError error
	// number of records processed in session transactions
	Inserted, Updated, Deleted int
	// sum of transactions execution times
	Duration time.Duration
	// sum of transactions waiting times
	Waited time.Duration
	// number of transaction restarts
	Restarts int
	// io statistics
	Readed byteSize
	Writed byteSize
}

type basicFlatFile struct {
	//db params
	dbConn     *connector
	dbFilename string
	//hdrFilename string
	dbLock  chan struct{}
	dbVer   *flatFileVer
	lastVer int
	sync.Mutex

	//session params
	id    uint32
	user  string
	stats Stats

	//trx params
	config    *Config
	haveLock  bool
	needStore bool

	//data params
	/*lastid uint
	idkeys map[uint]Key
	keysid map[Key]uint
	data  []map[uint][]byte
	sloth []Record*/
	data [][][]byte
}

// satisfy Connector interface
type connector struct {
	sync.RWMutex
	ses *basicFlatFile
}

func NewConnector() Connector {
	return &connector{}
}
func (c *connector) Connect(db, user string) (Session, error) {
	c.Lock()
	defer c.Unlock()
	if c.ses != nil {
		return nil, errAlreadyConnected
	}

	if uberLock == nil {
		uberLock = make(map[string]chan struct{})
	}
	fl, e := uberLock[db]
	if !e {
		fl = make(chan struct{}, 1)
		uberLock[db] = fl
	}

	if uberVersion == nil {
		uberVersion = make(map[string]*flatFileVer)
	}
	fv, e := uberVersion[db]
	if !e {
		fv = &flatFileVer{version: 0}
		uberVersion[db] = fv
	}

	t, _ := time.Now().MarshalBinary()
	var ids [][]byte = [][]byte{[]byte(db), []byte(user), t}

	sp := sessionPool.Get()

	if sp == nil {
		c.ses = &basicFlatFile{config: &Config{}, data: make([][][]byte, 0, 1024)}
	} else {
		c.ses = sp.(*basicFlatFile)
	}
	//c.ses.dbFilename = db + ".dtb"
	c.ses.dbFilename = db
	//c.ses.hdrFilename = db + ".hdr"
	c.ses.dbLock = fl
	c.ses.dbVer = fv
	h := fnv.New32a()
	h.Write(bytes.Join(ids, []byte("")))
	c.ses.id = h.Sum32()
	c.ses.user = user
	c.ses.config.Timeout = DefaultTrxTimeout
	c.ses.config.Locking = DefaultTrxLocking
	c.ses.dbConn = c
	return c.ses, nil
}

func (c *connector) Disconnect() error {
	c.Lock()
	defer c.Unlock()
	if c.ses == nil {
		return errAlreadyDisconnected
	}
	c.ses.data = c.ses.data[0:0]
	c.ses.dbConn = nil
	sessionPool.Put(c.ses)
	c.ses = nil
	return nil
}

// satisfy Session interface

func (b *basicFlatFile) load() error {
	var err error

	if b.config.Locking == PESSIMISTIC {
		if err := b.lock(); err != nil {
			return feedErr(err, 1)
		}
	}

	f, err := os.OpenFile(b.dbFilename, os.O_RDONLY|os.O_SYNC, os.FileMode(0600))
	if err != nil {
		return feedErr(newError(err), 2)
	}
	defer f.Close()

	needReload := b.lastVer != b.dbVer.get()

	if !needReload {
		//return nil
	}

	b.data = b.data[0:0]

	dec := gob.NewDecoder(f)
	err = dec.Decode(&b.data)
	if err != nil && err != io.EOF {
		return feedErr(newError(err), 3)
	}
	return nil
}
func (b *basicFlatFile) store() error {

	if !b.needStore {
		return nil
	}

	f, err := os.OpenFile(b.dbFilename, os.O_WRONLY|os.O_SYNC, os.FileMode(0600))
	if err != nil {
		return feedErr(newError(err), 1)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	err = enc.Encode(b.data)
	if err != nil {
		return feedErr(newError(err), 2)
	}
	b.lastVer = b.dbVer.raise()
	return nil
}

func (b *basicFlatFile) commit() error {
	var err error
	b.stats.LastStatement = cOMMIT

	if b.needStore {
		switch b.config.Locking {
		case PESSIMISTIC:
		case OPTIMISTIC:
			if b.lastVer != b.dbVer.get() {
				b.stats.Restarts++
				b.config.Locking = PESSIMISTIC
				return feedErr(errTransBlocked, 3)
			}
			if err = b.nowaitlock(); err != nil {
				b.stats.Restarts++
				b.config.Locking = PESSIMISTIC
				return feedErr(errTransBlocked, 4)
			}
		case NOWAIT:
			if b.lastVer != b.dbVer.get() {
				return b.rollback(nil)
			}
			if err = b.nowaitlock(); err != nil {
				return b.rollback(nil)
			}
		default:
			return newError(fmt.Errorf("unknown locking mode"))
		}
	}

	if err = b.store(); err != nil {
		return feedErr(err, 6)
	}
	return nil
}

func (b *basicFlatFile) rollback(e error) error {
	b.stats.LastStatement = rOLLBACK
	b.stats.StatementError = e
	b.needStore = false
	return e
}

// runs a ACID transaction
func (b *basicFlatFile) Transaction(f func(Trx) error) error {
	var err error

	err = b.runTransaction(f)
	if err != nil {
		if err == errTransBlocked {
			if err = b.Transaction(f); err != nil {
				return err
			}
			return nil
		}
		return err
	}
	return nil
}

func (b *basicFlatFile) runTransaction(f func(Trx) error) error {
	b.Lock()
	var err error
	t := time.Now()

	defer func() {
		/*if r := recover(); r != nil {
			fmt.Printf("%s panicked with %s", b, r)
		     err = fmt.Errorf("%s panicked with %t", b, r)
		}*/
		if e := b.unlock(); e != nil && err == nil {
			err = feedErr(e, 1)
		}
		b.stats.Duration = b.stats.Duration + time.Since(t)
		b.Unlock()
	}()

	b.haveLock = false
	b.needStore = false

	if err = b.load(); err != nil {
		//uugly but...
		if err = b.load(); err != nil {
			return feedErr(err, 2)
		}
	}

	if err = f(b); err != nil {
		e, is := err.(*intError)
		if is {
			//internal statement error
			return feedErr(b.rollback(e), 3)
		}
		//user error == rollback
		return b.rollback(err)
	}
	//commit
	return feedErr(b.commit(), 4)
}

func (b *basicFlatFile) nowaitlock() error {
	if b.haveLock {
		return nil
	}

	select {
	case b.dbLock <- struct{}{}:
		b.haveLock = true
		return nil
	default:
		return feedErr(ErrTransTimeout, 1)
	}
	return nil
}

func (b *basicFlatFile) lock() error {
	if b.haveLock {
		return nil
	}

	start := time.Now()
	select {
	case b.dbLock <- struct{}{}:
		b.haveLock = true
		b.stats.Waited = b.stats.Waited + time.Since(start)
		return nil
	case <-time.After(b.config.Timeout):
		b.stats.Waited = b.stats.Waited + time.Since(start)
		return feedErr(ErrTransTimeout, 2)
	}
	return nil
}

func (b *basicFlatFile) unlock() (err error) {
	if !b.haveLock {
		return nil
	}

	select {
	case <-b.dbLock:
		return nil
	default:
		return newError(fmt.Errorf("unlocking and not having lock"))
	}

	return nil

}

const (
	bool_PFX   byte = byte('o')
	int_PFX    byte = byte('i')
	float_PFX  byte = byte('f')
	string_PFX byte = byte('s')
	time_PFX   byte = byte('t')
	byte_PFX   byte = byte('b')
)

func encodeValue(val interface{}) ([]byte, error) {
	switch r := val.(type) {
	case bool:
		if r {
			return []byte{bool_PFX, 'T'}, nil
		}
		return []byte{bool_PFX, 'F'}, nil
	case int64:
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, r)
		if err != nil {
			return nil, newError(err)
		}
		n := make([]byte, len(buf.Bytes())+1)
		n[0] = int_PFX
		copy(n[1:], buf.Bytes())
		return n, nil
	case float64:
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, r)
		if err != nil {
			return nil, newError(err)
		}
		n := make([]byte, len(buf.Bytes())+1)
		n[0] = float_PFX
		copy(n[1:], buf.Bytes())
		return n, nil
	case string:
		/*n := make([]byte, len(r)+1)
		n[0] = STRING_PFX
		copy(n[1:], []byte(r))*/
		//return []byte{STRING_PFX, []byte(r)...}, nil
		return append([]byte{string_PFX}, []byte(r)...), nil
	case time.Time:
		b, err := r.MarshalBinary()
		if err != nil {
			return nil, newError(err)
		}
		n := make([]byte, len(b)+1)
		n[0] = time_PFX
		copy(n[1:], b)
		return n, nil
	case []byte:
		n := make([]byte, len(r)+1)
		n[0] = byte_PFX
		copy(n[1:], r)
		return n, nil
	default:
		//errWrongRecordValue.err = fmt.Errorf("%v has a unsuported value type (%T)", r, r)
		return nil, fmt.Errorf("%v has a unsuported value type (%T)", r, r)
	}
}

func encodeSet(r Set) ([][]byte, error) {
	s := make([][]byte, len(r)*2)
	i := 0
	for key, val := range r {
		s[i] = []byte(key)
		i++
		n, err := encodeValue(val)
		if err != nil {
			return nil, feedErr(err,1)
		}
		s[i] = n
		i++
	}
	return s, nil
}

func (b *basicFlatFile) encodeData(r ...Set) error {
	for _, d := range r {
		n, err := encodeSet(d)
		if err != nil {
			return feedErr(err, 1)
		}
		b.data = append(b.data, n)
	}
	return nil
}

func decodeValue(c []byte) (interface{}, error) {
	switch c[0] {
	case bool_PFX:
		if c[1] == 'T' {
			return true, nil
		}
		return false, nil
	case int_PFX:
		var in int64
		buf := bytes.NewReader(c[1:])
		err := binary.Read(buf, binary.LittleEndian, &in)
		if err != nil {
			return nil, newError(fmt.Errorf("cannot convert value to int64: %v", err))
		}
		return in, nil
	case float_PFX:
		var fl float64
		buf := bytes.NewReader(c[1:])
		err := binary.Read(buf, binary.LittleEndian, &fl)
		if err != nil {
			return nil, newError(fmt.Errorf("cannot convert value to float64: %v", err))
		}
		return fl, nil
	case string_PFX:
		return string(c[1:]), nil
	case time_PFX:
		var t time.Time
		if err := t.UnmarshalBinary(c[1:]); err != nil {
			return nil, newError(fmt.Errorf("cannot convert value to time: %v", err))
		}
		return t, nil
	case byte_PFX:
		return c[1:], nil
	default:
		return nil, newError(fmt.Errorf("unknown value type"))
	}
}
func decodeSet(s [][]byte) (Set, error) {
	n := NewRecordSet()
	//i := 0
	for i := 0; i < len(s); i = i + 2 {
		key := string(s[i])
		val, err := decodeValue(s[i+1])
		if err != nil {
			return nil, feedErr(err, 1)
		}
		n[Key(key)] = val
	}
	return n, nil
}

/*func (b *basicFlatFile) decodeData(r ...RecordSet) error {

	for _, d := range r {
		n := make([][]byte, len(d)*2)
		i := 0
		for key, val := range d {
			n[i] = []byte(key)
			i++
			n, err := encodeValue(val)
			if err != nil {
				return feedErr(err, 1)
			}
			n[i] = n
			i++
		}
		b.data = append(b.data, n)
	}
	return nil
}*/
