package goflat

import (
 	"testing"
 	"time"
 	"errors"
) 

func TestCommit(t *testing.T){
	err := NewEmptyDatabase("test.dtb") 
    if err != nil {
    	t.Errorf(err.Error())
    	return
    } 
	db := NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
    	t.Errorf(err.Error())
    	return
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr Trx) error {
		err = tr.Insert().Values(Set{"string": "xxx", "number": int64(1), "float": float64(3.14), "boolean": true, "time": time.Now(), "byte": []byte{0, 0, 0, 0, 0}})
		if err != nil {
    		return err
		}
		return nil
	}); err != nil {
    	t.Errorf(err.Error())
		return
	}

	if err = session.Transaction(func(tr Trx) error {
		data, err := tr.Select(nil).All()
		if err != nil {
    		return err
		}

		//
		e := 1
		g := len(data)
		if e != g {
			t.Errorf("expected %v and got %v", e, g )
		}
		return nil
	}); err != nil {
    	t.Errorf(err.Error())
		return
	}
}

func TestRollback(t *testing.T){
	err := NewEmptyDatabase("test.dtb") 
    if err != nil {
    	t.Errorf(err.Error())
    	return
    } 
	db := NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
    	t.Errorf(err.Error())
    	return
	}
	defer db.Disconnect()

	myRollback := errors.New("Rollback")
	if err = session.Transaction(func(tr Trx) error {
		err = tr.Insert().Values(Set{"string": "xxx", "number": int64(1), "float": float64(3.14), "boolean": true, "time": time.Now(), "byte": []byte{0, 0, 0, 0, 0}})
		if err != nil {
    		return err
		}
		return myRollback
	}); err != nil && err!= myRollback {
    	t.Errorf(err.Error())
		return
	}

	if err = session.Transaction(func(tr Trx) error {
		data, err := tr.Select(nil).All()
		if err != nil {
    		return err
		}

		//
		e := 0
		g := len(data)
		if e != g {
			t.Errorf("expected %v and got %v", e, g )
		}
		return nil
	}); err != nil {
    	t.Errorf(err.Error())
		return
	}
}