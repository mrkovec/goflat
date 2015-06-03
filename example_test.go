package goflat_test

import (
	"errors"
	"log"
	"time"

	"github.com/mrkovec/goflat"
)

var (
	errPK = errors.New("primary key error")
	errFK = errors.New("foreign key error")
)

func Example() {
	db := goflat.NewConnector()
	session, err := db.Connect("test.dtb", "user")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	deptData := []goflat.Set{{"table": "dept", "dept_no": int64(1), "dept_name": "dept1", "active": true}, {"table": "dept", "dept_no": int64(2), "dept_name": "dept2", "active": false}}
	empData := []goflat.Set{{"table": "emp", "emp_name": "john", "fk_dept_no": int64(1)}, {"table": "emp", "emp_name": "bill", "fk_dept_no": int64(1)}}

	deptTableProxi := goflat.NewStatement().Where(goflat.KeyTerm("table").Equals(goflat.StringTerm("dept")))
	empTableProxi := goflat.NewStatement().Where(goflat.KeyTerm("table").Equals(goflat.StringTerm("emp")))

	deptBI := func(t goflat.Trx, s goflat.Set) error {
		s["timestamp"] = time.Now()

		deptNo := s["dept_no"].(int64)
		predic := goflat.KeyTerm("dept_no").Equals(goflat.IntTerm(deptNo))
		d, err := t.Select(goflat.NewStatement().From(deptTableProxi).Where(predic)).First()
		if d != nil {
			return errPK
		}
		return err
	}

	empBI := func(t goflat.Trx, s goflat.Set) error {
		s["timestamp"] = time.Now()

		deptNo := s["fk_dept_no"].(int64)
		predic := goflat.KeyTerm("dept_no").Equals(goflat.IntTerm(deptNo))
		d, err := t.Select(goflat.NewStatement().From(deptTableProxi).Where(predic)).First()
		if d == nil {
			return errFK
		}
		return err
	}

	if err = session.Transaction(func(tr goflat.Trx) error {
		err = tr.Insert().BeforeTrigger(deptBI).Values(deptData...)
		return err
	}); err != nil {
		log.Print(err)
		return
	}
	log.Print(session)
	if err = session.Transaction(func(tr goflat.Trx) error {
		err = tr.Insert().BeforeTrigger(empBI).Values(empData...)
		return err
	}); err != nil {
		log.Print(err)
		return
	}
	log.Print(session)

	if err = session.Transaction(func(tr goflat.Trx) error {
		dData, err := tr.Select(goflat.NewStatement().From(deptTableProxi).Where(goflat.KeyTerm("active").Equals(goflat.BoolTerm(true)))).All()
		if err != nil {
			return err
		}
		for _, dept := range dData {
			deptNo := dept["dept_no"].(int64)
			eData, err := tr.Select(goflat.NewStatement().From(empTableProxi).Where(goflat.KeyTerm("fk_dept_no").Equals(goflat.IntTerm(deptNo)))).All()
			if err != nil {
				return err
			}
			log.Printf("%v: %v employees; created %v \n", dept["dept_name"], len(eData), dept["timestamp"])
			for _, emp := range eData {
				log.Printf("\t%v\n", emp["emp_name"])
			}
		}
		return nil
	}); err != nil {
		log.Print(err)
	}
	log.Print(session)
}
