package goflat_test

import (
	"log"
	"time"
	"strings"
	"regexp"

	"github.com/mrkovec/goflat"
)

func ExampleSelectStmt() {
	db := goflat.NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		data, err := tr.Select(goflat.NewStatement().Where(goflat.KeyTerm("id").Equals(goflat.ValueTerm(int64(5))))).All()
		_ = data
		return err
	}); err != nil {
		log.Print(err)
		return
	}
	log.Print(session)
}

func ExampleInsertStmt() {
	db := goflat.NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		err = tr.Insert().Values(goflat.Set{"boolean": true, "number": int64(1), "float": float64(3.14), "time": time.Now(), "byte": []byte{0, 0, 0, 0, 0}, "string": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."})
		return err
	}); err != nil {
		log.Print(err)
		return
	}
	log.Print(session)
}

func ExampleUpdateStmt() {
	db := goflat.NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		rowsUpdated, err := tr.Update(goflat.NewStatement().Where(goflat.KeyTerm("timestamp").Less(goflat.ValueTerm(time.Now())))).Set(goflat.Set{"timestamp": time.Now()})
		_ = rowsUpdated
		return err
	}); err != nil {
		log.Print(err)
		return
	}
	log.Print(session)
}

func ExampleDeleteStmt() {
	db := goflat.NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		rowsDeleted, err := tr.Delete(goflat.NewStatement().Where(goflat.KeyTerm("id").Equals(goflat.ValueTerm(int64(5))))).All()
		_ = rowsDeleted
		return err
	}); err != nil {
		log.Print(err)
		return
	}
	log.Print(session)
}


func ExampleTerm_StringEval(){
	db := goflat.NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		
		a := goflat.KeyTerm("firstname")
		b := goflat.ValueTerm("john")
		data, err := tr.Select(goflat.NewStatement().Where(a.StringEval(b, strings.EqualFold))).All()
		_ = data
		return err
	}); err != nil {
		log.Print(err)
		return
	}
	log.Print(session)
}
func ExampleTerm_StringEval_userDefinedFunc(){
	db := goflat.NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	if err = session.Transaction(func(tr goflat.Trx) error {
		
		a := goflat.KeyTerm("surname")
		b := goflat.ValueTerm("foo.*")
		fn := func(a string, b string) bool {
			matched, err := regexp.MatchString(b, a)
			if err != nil {
				return false
			}
			return matched
		}

		data, err := tr.Select(goflat.NewStatement().Where(a.StringEval(b, fn))).All()
		_ = data
		return err
	}); err != nil {
		log.Print(err)
		return
	}
	log.Print(session)
}