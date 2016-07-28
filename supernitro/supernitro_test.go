package supernitro

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

/*

func TestInsert(t *testing.T) {
	db := New()
	defer db.Close()

	w := db.NewWriter()
	for i := 0; i < 2000000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	for i := 1750; i < 2000000; i++ {
		w.Delete([]byte(fmt.Sprintf("%010d", i)))
	}
	snap, _ := w.NewSnapshot()
	defer snap.Close()

	for i := 2000; i < 2000000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	snap2, _ := w.NewSnapshot()
	defer snap2.Close()

	var snp *Snapshot
	for i := 0; i < 200; i++ {
		x, _ := w.NewSnapshot()
		for m := 0; m < 1000000; m++ {
			w.Put([]byte(fmt.Sprintf("%010d", i*1000000+m)))
		}
		//x.Close()
		snp = x
	}

	itr := db.NewIterator(snp)
	count := 0
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		count++

	}
	fmt.Println("count", count)

}

*/

func TestInsert(t *testing.T) {
	db := New()
	defer db.Close()

	w := db.NewWriter()
	for i := 0; i < 2000000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	for i := 1750000; i < 2000000; i++ {
		w.Delete([]byte(fmt.Sprintf("%010d", i)))
	}
	snap, _ := w.NewSnapshot()
	defer snap.Close()

	for i := 2000000; i < 5000000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	snap2, _ := w.NewSnapshot()
	defer snap2.Close()

	count := 0
	itr := db.NewIterator(snap)
	defer itr.Close()

	itr.SeekFirst()
	itr.Seek([]byte(fmt.Sprintf("%010d", 1500000)))
	for ; itr.Valid(); itr.Next() {
		expected := fmt.Sprintf("%010d", count+1500000)
		got := string(itr.Get())
		count++
		if got != expected {
			t.Errorf("Expected %s, got %v", expected, got)
		}
	}

	if count != 250000 {
		t.Errorf("Expected count = 250, got %v", count)
	}
}

func doInsert(id int, db *SuperNitro, ch chan bool, wg *sync.WaitGroup, n int, isRand bool, shouldSnap bool) {
	defer wg.Done()
	w := db.NewWriter()
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	for i := 0; i < n; i++ {
		var val int
		if isRand {
			val = rnd.Int()%1000000000 + id*10000000000
		} else {
			val = i + id*n
		}
		if shouldSnap && i%100000 == 0 {
			ch <- true
			<-ch
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(val))
		w.Put(buf)
	}
}

func TestInsertPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := New()
	defer db.Close()

	workers := 8
	n := 20000000 / workers
	t0 := time.Now()
	total := n * workers
	ch := make([]chan bool, workers)
	for i := 0; i < workers; i++ {
		ch[i] = make(chan bool)
	}
	go func() {

		for {
			for i := 0; i < workers; i++ {
				<-ch[i]
			}
			snap, _ := db.NewSnapshot()
			snap.Close()

			for i := 0; i < workers; i++ {
				ch[i] <- true
			}
		}
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go doInsert(i, db, ch[i], &wg, n, false, true)
	}
	wg.Wait()

	snap, _ := db.NewSnapshot()
	db.Sync()
	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s\n",
		total, dur, float64(total)/float64(dur.Seconds()))

	itr := db.NewIterator(snap)
	fmt.Println("snap", snap)
	c := 0
	x := uint64(0)
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		v := binary.BigEndian.Uint64(itr.Get())
		if v < x {
			//panic(fmt.Sprint(x, v))
			fmt.Println("bad", x, v)
		}
		//		fmt.Println(v)
		x = v
		c++
	}
	fmt.Println("count", c)

	/*
		itr1 := snap.snaps[0].NewIterator()
		for itr1.SeekFirst(); itr1.Valid(); itr1.Next() {
			fmt.Println("first", binary.BigEndian.Uint64(itr1.Get()))
		}
		fmt.Println("")

		itr1 = snap.snaps[1].NewIterator()
		for itr1.SeekFirst(); itr1.Valid(); itr1.Next() {
			fmt.Println("second", binary.BigEndian.Uint64(itr1.Get()))
		}
	*/
	itr.Close()
	snap.Close()
}

func doGet(t *testing.T, db *SuperNitro, snap *Snapshot, wg *sync.WaitGroup, n int) {
	defer wg.Done()
	rnd := rand.New(rand.NewSource(int64(rand.Int())))

	buf := make([]byte, 8)
	itr := db.NewIterator(snap)
	defer itr.Close()
	for i := 0; i < n; i++ {
		val := rnd.Int() % n
		//binary.BigEndian.PutUint64(buf, uint64(val))
		//exp := fmt.Sprintf("%010d", val)
		//itr.Seek(buf)
		itr.SeekFirst()
		if !itr.Valid() {
			t.Errorf("Expected to find %v", val)
		}
		if !bytes.Equal(buf, itr.Get()) {
			panic(string(itr.Get()))
		}
	}
}

func TestGetPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := New()
	defer db.Close()
	n := 1000
	wg.Add(1)
	go doInsert(0, db, make(chan bool), &wg, n, false, false)
	wg.Wait()
	snap, _ := db.NewSnapshot()
	defer snap.Close()

	fmt.Println("built index")

	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	//for i := 0; i < runtime.GOMAXPROCS(0); i++ {
	wg.Add(1)
	//	go doGet(t, db, snap, &wg, n)
	//}
	//wg.Wait()
	doGet(t, db, snap, &wg, n)
	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
}

func TestSimpleGet(t *testing.T) {
	db := New()
	w := db.NewWriter()

	n := 1000000
	buf := make([]byte, 8)
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		w.Put(buf)
		if i%100000 == 0 {
			snap, _ := w.NewSnapshot()
			snap.Close()
			time.Sleep(time.Second)
		}
	}

	snap, _ := w.NewSnapshot()
	itr := db.NewIterator(snap)

	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(buf, uint64(i))
		itr.Seek(buf)
		if !itr.Valid() {
			t.Errorf("invalid %v buf:%v", i, buf)
			continue
		}

		x := binary.BigEndian.Uint64(itr.Get())
		if uint64(i) != x {
			t.Errorf("failed to lookup %v, got %v", i, x)
		}
	}
}
