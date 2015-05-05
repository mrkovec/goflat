package goflat

import (
	"fmt"
	//"time"
	"errors"
	//"runtime"
	"strconv"
	"strings"
)


var (
	errAlreadyConnected = &Error{err: errors.New("connection already established")}
	errAlreadyDisconnected = &Error{err: errors.New("connection already closed")}

	errWrongRecordValue = &Error{}
)

var (
	ErrTransTimeout  = &Error{err: errors.New("timeout")}
	errTransBlocked  = &Error{err: errors.New("transaction was blocked")}
)

type Error struct {
	err   error
	attr  string
}

func (f *Error) Error() string {
	return fmt.Sprintf("FLE-%s: %s", strings.Repeat("0", 5-len(f.attr))+f.attr, f.err)
}

func feedErr(e error, i int) error {
	if e == nil {
		return nil
	}
	p := strconv.Itoa(i)

	f, is := e.(*Error)
	if !is {
		return &Error{err: e, attr: p}
	}
	f.attr = p + f.attr
	return f
}
