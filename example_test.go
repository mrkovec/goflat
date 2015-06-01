package goflat_test

import (
	"errors"
	"log"
    "time"

	"github.com/mrkovec/goflat"
)

var (
	pk_err error = errors.New("primary key error")
    fk_err error = errors.New("foreign key error")
)

func Example() {
    db := goflat.NewConnector()
    defer db.Disconnect()

    session, err := db.Connect("test", "user/pasword") 
    
    dept_data := []goflat.Set{goflat.Set{"table":"dept", "dept_no": int64(1), "dept_name": "dept1", "active": true}, goflat.Set{"table":"dept", "dept_no": int64(2), "dept_name": "dept2", "active": false}}
    emp_data := []goflat.Set{goflat.Set{"table":"emp", "emp_name": "john", "xdept_no": int64(1)}, goflat.Set{"table":"emp", "emp_name": "bill", "xdept_no": int64(1)}}

    
    dept_ai := func(t goflat.Trx,s goflat.Set) error {
        s["timestamp"] = time.Now()
        
        dept_no, e := s["dept_no"].(int64)
        if !e {
            return pk_err
        }
        pred := goflat.KeyTerm("table").Equal(goflat.StringTerm("dept")).And(goflat.KeyTerm("dept_no").Equal(goflat.IntTerm(dept_no)))
        d, err :=t.Select().Where(pred).AllRows()
        if len(d) > 1 {
            return pk_err
        }        
        return err
    }
    
    emp_bi := func(t goflat.Trx,s goflat.Set) error {
        s["timestamp"] = time.Now()

        xdept_no, e := s["xdept_no"].(int64)
        if !e {
            return fk_err
        }
        pred := goflat.KeyTerm("table").Equal(goflat.StringTerm("dept")).And(goflat.KeyTerm("dept_no").Equal(goflat.IntTerm(xdept_no)))
        d, err :=t.Select().Where(pred).AllRows()
        if len(d) == 0 {
            return fk_err
        }
        return err
    }

    if err = session.Transaction(func(tr goflat.Trx) error {
        err = tr.Insert().AfterTrigger(dept_ai).Values(dept_data...)
        if err != nil {
            return err
        }
        err = tr.Insert().BeforeTrigger(emp_bi).Values(emp_data...) 
        return err 
     }); err != nil {
         log.Print(err)  
     }

    if err = session.Transaction(func(tr goflat.Trx) error {
        ddata, err := tr.Select().Where(goflat.KeyTerm("table").Equal(goflat.StringTerm("dept")).And(goflat.KeyTerm("active").Equal(goflat.BoolTerm(true)))).AllRows()
        if err != nil {
            return err
        }
        for _, dept := range ddata {
            dept_no := dept["dept_no"].(int64)
            edata, err := tr.Select().Where(goflat.KeyTerm("table").Equal(goflat.StringTerm("emp")).And(goflat.KeyTerm("xdept_no").Equal(goflat.IntTerm(dept_no)))).AllRows()
            if err != nil {
                return err
            }
            log.Printf("%v\n", dept["dept_name"])
            for _, emp := range edata {
                log.Printf("\t%v\n", emp["emp_name"])    
            }

        }
        return err 
     }); err != nil {
         log.Print(err)  
     }



}