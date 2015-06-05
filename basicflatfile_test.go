package goflat

import (
 	"testing"
 	"time"
 	"errors"
 	"fmt"
 	"sync"
 	"math/rand"
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
		err = tr.Insert().Values(Set{"boolean": true, "number": int64(1), "float": float64(3.14), "time": time.Now(), "byte": []byte{0, 0, 0, 0, 0}, "string": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."})
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
		err = tr.Insert().Values(Set{"boolean": true, "number": int64(1), "float": float64(3.14), "time": time.Now(), "byte": []byte{0, 0, 0, 0, 0}, "string": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."})
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

var numTrx = 10 //300

func TestConcurrencyOptimisticShort_RdO(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 0, 0)
}	
func TestConcurrencyOptimisticShort_RdM(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 20, 0)
}	
func TestConcurrencyOptimisticShort_RdWr(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 50, 0)
}	
func TestConcurrencyOptimisticShort_WrM(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 80, 0)
}	
func TestConcurrencyOptimisticShort_WrO(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 100, 0)
}	

func TestConcurrencyPESSIMISTICShort_RdO(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 0, 0)
}	
func TestConcurrencyPESSIMISTICShort_RdM(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 20, 0)
}	
func TestConcurrencyPESSIMISTICShort_RdWr(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 50, 0)
}	
func TestConcurrencyPESSIMISTICShort_WrM(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 80, 0)
}	
func TestConcurrencyPESSIMISTICShort_WrO(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 100, 0)
}	

func TestConcurrencyNOWAITShort_RdO(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 0, 0)
}	
func TestConcurrencyNOWAITShort_RdM(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 20, 0)
}	
func TestConcurrencyNOWAITShort_RdWr(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 50, 0)
}	
func TestConcurrencyNOWAITShort_WrM(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 80, 0)
}	
func TestConcurrencyNOWAITShort_WrO(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 100, 0)
}	
/***/
func TestConcurrencyOptimisticMedium_RdO(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 0, 10)
}	
func TestConcurrencyOptimisticMedium_RdM(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 20, 10)
}	
func TestConcurrencyOptimisticMedium_RdWr(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 50, 10)
}	
func TestConcurrencyOptimisticMedium_WrM(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 80, 10)
}	
func TestConcurrencyOptimisticMedium_WrO(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 100, 10)
}	

func TestConcurrencyPESSIMISTICMedium_RdO(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 0, 10)
}	
func TestConcurrencyPESSIMISTICMedium_RdM(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 20, 10)
}	
func TestConcurrencyPESSIMISTICMedium_RdWr(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 50, 10)
}	
func TestConcurrencyPESSIMISTICMedium_WrM(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 80, 10)
}	
func TestConcurrencyPESSIMISTICMedium_WrO(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 100, 10)
}	

func TestConcurrencyNOWAITMedium_RdO(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 0, 10)
}	
func TestConcurrencyNOWAITMedium_RdM(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 20, 10)
}	
func TestConcurrencyNOWAITMedium_RdWr(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 50, 10)
}	
func TestConcurrencyNOWAITMedium_WrM(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 80, 10)
}	
func TestConcurrencyNOWAITMedium_WrO(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 100, 10)
}	
/***/
func TestConcurrencyOptimisticLong_RdO(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 0, 100)
}	
func TestConcurrencyOptimisticLong_RdM(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 20, 100)
}	
func TestConcurrencyOptimisticLong_RdWr(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 50, 100)
}	
func TestConcurrencyOptimisticLong_WrM(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 80, 100)
}	
func TestConcurrencyOptimisticLong_WrO(t *testing.T) {
	testConcurrency(t, OPTIMISTIC, numTrx, 100, 100)
}	

func TestConcurrencyPESSIMISTICLong_RdO(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 0, 100)
}	
func TestConcurrencyPESSIMISTICLong_RdM(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 20, 100)
}	
func TestConcurrencyPESSIMISTICLong_RdWr(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 50, 100)
}	
func TestConcurrencyPESSIMISTICLong_WrM(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 80, 100)
}	
func TestConcurrencyPESSIMISTICLong_WrO(t *testing.T) {
	testConcurrency(t, PESSIMISTIC, numTrx, 100, 100)
}	

func TestConcurrencyNOWAITLong_RdO(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 0, 100)
}	
func TestConcurrencyNOWAITLong_RdM(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 20, 100)
}	
func TestConcurrencyNOWAITLong_RdWr(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 50, 100)
}	
func TestConcurrencyNOWAITLong_WrM(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 80, 100)
}	
func TestConcurrencyNOWAITLong_WrO(t *testing.T) {
	testConcurrency(t, NOWAIT, numTrx, 100, 100)
}	
/***/
 
var (
	wg sync.WaitGroup
	waits chan time.Duration
	durations chan time.Duration
	retries chan int
)

func testConcurrency(t *testing.T, cc ConControl, numTests int, writperc int, duration int) {
	t.Parallel()
	if testing.Short() {
		numTests = 1
	}

	waits = make(chan time.Duration, numTests)
	durations = make(chan time.Duration, numTests)
	retries = make(chan int, numTests)

	err := NewEmptyDatabase("test.dtb") 
	st := time.Now()
    for i := 0;  i < numTests; i++ {
    	wg.Add(1)
    	go func(){
    		defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		    db := NewConnector()
			session, _ := db.Connect("test.dtb", "user")
			defer db.Disconnect()    		
			
			session.SetConfig(Config{Locking:cc, Timeout: 600 * time.Second})

			if err = session.Transaction(func(tr Trx) error {
				
				if rand.Intn(100) < writperc {
					err = tr.Insert().Values(Set{"number": int64(1)})
				}
				
				time.Sleep(time.Duration(duration) * time.Millisecond)				
				return err
			}); err != nil {
		    	t.Errorf(err.Error())
				return
			}
			waits <- session.Stats().Waited
			durations <- session.Stats().Duration  
			retries <- session.Stats().Restarts			
		}()
    }
    wg.Wait()
    ed := time.Since(st)

	var w time.Duration
	var maw time.Duration
	miw, _ := time.ParseDuration("1h")
	var d time.Duration
	var mad time.Duration
	mid, _ := time.ParseDuration("1h")
	var ret int
	for i := 0; i < numTests; i++ {
		tw := <-waits
		w = w + tw
		if tw > maw { maw = tw}
		if tw < miw { miw = tw}
		td := <-durations
		d = d + td
		if td > mad { mad = td}
		if td < mid { mid = td}
		ret = ret + <-retries
	}
	close(waits)
	close(durations)


    var td string
    switch duration {
    case 10:
    	td = "medium"
    case 100:
    	td = "long"
    default:
    	td = "short"
    }

   	fmt.Printf("fired %v %s %s trx/s in %v (%5.2f trx/s) r:%v%% w:%v%%, avg duration %v (%v/%v), wait %v (%v/%v), retries %v", numTests, td, cc, ed, float64(numTests)/ed.Seconds(), 100-writperc, writperc,  time.Duration(float64(d)/float64(numTests)), mid, mad, time.Duration(float64(w)/float64(numTests)), miw, maw, ret)

  	db := NewConnector()
	session, _ := db.Connect("test.dtb", "user")
	defer db.Disconnect()   
	if err = session.Transaction(func(tr Trx) error {
		data, err := tr.Select(nil).All()
		if err != nil {
    		return err
		}

		//
		//e := numTests
		g := len(data)
		fmt.Printf(" - %v trx commited", g)
		//if e != g {
			//if cc == NOWAIT {
				//fmt.Printf(" - from %v trx %v commited", e, g)
			//} else {
			//	t.Errorf("expected %v and got %v", e, g )
			//}
		//}
		return nil
	}); err != nil {
    	t.Errorf(err.Error())
		return
	}
	fmt.Printf("\n")
}

 