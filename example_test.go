package goflat_test

import (
	"errors"
	"log"
	"time"

	"github.com/mrkovec/goflat"
)

var (
	pkErr = errors.New("primary key error")
	fkErr = errors.New("foreign key error")
)

func Example() {
	db := goflat.NewConnector()
	session, err := db.Connect("test", "user/pasword")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Disconnect()

	deptData := []goflat.Set{{"table": "dept", "dept_no": int64(1), "dept_name": "dept1", "active": true}, {"table": "dept", "dept_no": int64(2), "dept_name": "dept2", "active": false}}
	empData := []goflat.Set{{"table": "emp", "emp_name": "john", "xdept_no": int64(1)}, {"table": "emp", "emp_name": "bill", "xdept_no": int64(1)}}

	deptAI := func(t goflat.Trx, s goflat.Set) error {
		s["timestamp"] = time.Now()

		deptNo, e := s["dept_no"].(int64)
		if !e {
			return pkErr
		}
		pred := goflat.KeyTerm("table").Equal(goflat.StringTerm("dept")).And(goflat.KeyTerm("dept_no").Equal(goflat.IntTerm(deptNo)))
		d, err := t.Select().Where(pred).AllRows()
		if len(d) > 1 {
			return pkErr
		}
		return err
	}

	empBI := func(t goflat.Trx, s goflat.Set) error {
		s["timestamp"] = time.Now()

		deptNo, e := s["xdept_no"].(int64)
		if !e {
			return fkErr
		}
		pred := goflat.KeyTerm("table").Equal(goflat.StringTerm("dept")).And(goflat.KeyTerm("dept_no").Equal(goflat.IntTerm(deptNo)))
		d, err := t.Select().Where(pred).AllRows()
		if len(d) == 0 {
			return fkErr
		}
		return err
	}

	if err = session.Transaction(func(tr goflat.Trx) error {
		err = tr.Insert().AfterTrigger(deptAI).Values(deptData...)
		if err != nil {
			return err
		}
		err = tr.Insert().BeforeTrigger(empBI).Values(empData...)
		return err
	}); err != nil {
		log.Print(err)
	}

	if err = session.Transaction(func(tr goflat.Trx) error {
		dData, err := tr.Select().Where(goflat.KeyTerm("table").Equal(goflat.StringTerm("dept")).And(goflat.KeyTerm("active").Equal(goflat.BoolTerm(true)))).AllRows()
		if err != nil {
			return err
		}
		for _, dept := range dData {
			deptNo := dept["dept_no"].(int64)
			eData, err := tr.Select().Where(goflat.KeyTerm("table").Equal(goflat.StringTerm("emp")).And(goflat.KeyTerm("xdept_no").Equal(goflat.IntTerm(deptNo)))).AllRows()
			if err != nil {
				return err
			}
			log.Printf("%v\n", dept["dept_name"])
			for _, emp := range eData {
				log.Printf("\t%v\n", emp["emp_name"])
			}
		}
		return err
	}); err != nil {
		log.Print(err)
	}

}
