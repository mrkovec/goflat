package goflat 

import (
 	"testing"
 	"time"
)


func BenchmarkInsert(b *testing.B) {
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