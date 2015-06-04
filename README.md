# goflat
[![GoDoc](https://godoc.org/github.com/mrkovec/goflat?status.svg)](https://godoc.org/github.com/mrkovec/goflat)
[![Coverage Status](https://coveralls.io/repos/mrkovec/goflat/badge.svg?branch=master)](https://coveralls.io/r/mrkovec/goflat?branch=master)
[![Build Status](https://drone.io/github.com/mrkovec/goflat/status.png)](https://drone.io/github.com/mrkovec/goflat/latest)

**goflat** is a flat file NoSQL-like KV database with SQL-like DML syntax.

Very alpha, but feel free to test it. 

Functionality outline:
```go
    //setup
    goflat.NewEmptyDatabase("test.dtb") 
    db := goflat.NewConnector()
    session, err := db.Connect("test.dtb", "user")
    defer db.Disconnect()
    
    myData := []goflat.Set{goflat.Set{"table": "emp", "name": "John", "id":nil}, goflat.Set{"table": "emp", "name": "Jane", "id":nil}}
    
    myStatement1 := goflat.NewStatement().Where(goflat.KeyTerm("table").Equals(goflat.ValueTerm("emp")))
    myStatement2 := goflat.NewStatement().From(myStatement1).Where(goflat.KeyTerm("name").Equals(goflat.ValueTerm("John")))
    
    var seq int64
    myTrigger := func(t goflat.Trx, s goflat.Set) error {
        if s["id"] == nil {
            s["id"] = seq
            seq ++
        }
        return nil
    }

    //insert
    if err = session.Transaction(func(tr goflat.Trx) error {
        err = tr.Insert().BeforeTrigger(myTrigger).Values(myData...)
        return err
    }); err != nil {
        log.Print(err)
        return
    }

    //update + review
    if err = session.Transaction(func(tr goflat.Trx) error {
        _, err := tr.Update(myStatement2).Set(goflat.Set{"name": "Bill"})
        data, err := tr.Select(myStatement1).All()
        log.Print(data)
        return err
    }); err != nil {
        log.Print(err)
        return
    }
    log.Print(session)    
```
The basic data unit is a `Set` in the form of `map[string]interface{}`. Every DML statement is executed in an ACID transaction in form of a `func(goflat.Trx) error` function:
```go
session.Transaction(func(tr goflat.Trx) error {
		...
	});
```
DML statements consist of a "main" statement parameters descriptor `Statement` with `Where` (and `From`) conditions. 
```go
myStatement1 := goflat.NewStatement().Where(goflat.KeyTerm("table").Equals(goflat.ValueTerm("emp")))
myStatement2 := goflat.NewStatement().From(myStatement1).Where(goflat.KeyTerm("name").Equals(goflat.ValueTerm("John")))
```
Every DML statement uses only those conditions that are needed.
```go
//select uses both Where and From conditions
data, err := tr.Select(myStatement2).All()
//update uses only Where condition
nUpdated, err := tr.Update(myStatement2).Set(goflat.Set{"name": "Bill"})
```
More examples are on GoDoc or in test/benchmark files.
