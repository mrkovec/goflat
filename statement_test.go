package goflat 

import (
 	"testing"
 	"time"
 	"errors"
)

func TestTriggerrollback(t *testing.T){
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

	insData := []Set{Set{"id": int64(1)}, Set{"id": int64(2)}}
	myRollback := errors.New("rollback this")

	if err = session.Transaction(func(tr Trx) error {
		err = tr.Insert().BeforeTrigger(func(t Trx, s Set) error {
			if s["id"] == int64(1) {
				return myRollback
			}
			return nil
		}).Values(insData...)
		if err != nil && err != myRollback {
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

func BenchmarkSingleInsert(b *testing.B) {
	err := NewEmptyDatabase("test.dtb") 
    if err != nil {
    	b.Errorf(err.Error())
    	return
    } 
    db := NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
    	b.Errorf(err.Error())
    	return
	}
	defer db.Disconnect()

	b.ResetTimer()
    for i := 0;  i < b.N; i++ {
		if err = session.Transaction(func(tr Trx) error {
			err = tr.Insert().Values(Set{"string": "xxx", "number": int64(1), "float": float64(3.14), "boolean": true, "time": time.Now(), "byte": []byte{0, 0, 0, 0, 0}})
			return err
		}); err != nil {
	    	b.Errorf(err.Error())
			return
		}
    }
}

func BenchmarkBulkInsert(b *testing.B) {
	err := NewEmptyDatabase("test.dtb") 
    if err != nil {
    	b.Errorf(err.Error())
    	return
    } 
    db := NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
    	b.Errorf(err.Error())
    	return
	}
	defer db.Disconnect()	
	b.ResetTimer()

	data := make([]Set,b.N)
    for i := 0;  i < b.N; i++ {
    	data[i] = Set{"string": "xxx", "number": int64(1), "float": float64(3.14), "boolean": true, "time": time.Now(), "byte": []byte{0, 0, 0, 0, 0}}
    }

	if err = session.Transaction(func(tr Trx) error {
		err = tr.Insert().Values(data...)
		return err
	}); err != nil {
    	b.Errorf(err.Error())
		return
	}
}