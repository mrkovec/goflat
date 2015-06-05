package goflat

import (
	"fmt"
	//"time"
	"errors"
	//"runtime"
	"strconv"
	//"strings"
)

var (
	errAlreadyConnected    = &intError{parent: errors.New("connection already established")}
	errAlreadyDisconnected = &intError{parent: errors.New("connection already closed")}

	errWrongRecordValue = &intError{}
)

var (
	ErrTransTimeout = &intError{parent: errors.New("timeout")}
	errTransBlocked = &intError{parent: errors.New("transaction was blocked")}
)

type intError struct {
	parent  error
	text string
	attr string
}
func newError(e error) error {
	if e == nil {
		return nil
	}
 	return &intError{parent:e}
}

func (f *intError) Error() string {
	if f.text != "" {
		return fmt.Sprintf("%s - %s", f.text, f.parent.Error())
	}
	return f.parent.Error()
}

func feedErrDetail(e error, i int, format string, a ...interface{}) error {
	if e == nil {
		return nil
	}
	p := strconv.Itoa(i)

	f, is := e.(*intError)
	if !is {
		return e
	}
	if f == errTransBlocked {
		return f
	}	
	return &intError{parent:e, attr:p + f.attr, text:fmt.Sprintf(format, a...)}
}

func feedErr(e error, i int) error {
	if e == nil {
		return nil
	}
	p := strconv.Itoa(i)

	f, is := e.(*intError)
	if !is {
		return e
	}
	if f == errTransBlocked {
		return f
	}
 	return &intError{parent:e, attr:p + f.attr}
}
