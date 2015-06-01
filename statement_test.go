package goflat_test

import (
	"log"
	"time"

	"github.com/mrkovec/goflat"
)

func ExampleTrx_Select() {
	db := goflat.NewConnector()
	session, err := db.Connect("test", "user/pasword")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		data, err := tr.Select().Where(goflat.KeyTerm("id").Equal(goflat.IntTerm(int64(5)))).AllRows()
		_ = data			
		return err
	}); err != nil {
		log.Print(err)
	}

} 

func ExampleTrx_Insert() {
	db := goflat.NewConnector()
	session, err := db.Connect("test", "user/pasword")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		err = tr.Insert().Values(goflat.Set{"string": "xxx", "number": int64(1), "float": float64(3.14), "boolean": true, "time": time.Now(), "byte": []byte{0, 0, 0, 0, 0}})
		return err
	}); err != nil {
		log.Print(err)
	}

} 

func ExampleUpdStatement_Update() {
	db := goflat.NewConnector()
	session, err := db.Connect("test", "user/pasword")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		rowsUpdated, err := tr.Update().Where(goflat.KeyTerm("timestamp").Less(goflat.TimeTerm(time.Now()))).Set(goflat.Set{"timestamp": time.Now()})
		_ = rowsUpdated			
		return err
	}); err != nil {
		log.Print(err)
	}

} 

func ExampleTrx_Delete() {
	db := goflat.NewConnector()
	session, err := db.Connect("test", "user/pasword")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		data, err := tr.Delete().Where(goflat.KeyTerm("id").Equal(goflat.IntTerm(int64(5)))).Row()
		_ = data			
		return err
	}); err != nil {
		log.Print(err)
	}

} 