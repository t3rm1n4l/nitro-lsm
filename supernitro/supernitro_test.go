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

func doInsert(db *SuperNitro, wg *sync.WaitGroup, n int, isRand bool, shouldSnap bool) {
	defer wg.Done()
	w := db.NewWriter()
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	for i := 0; i < n; i++ {
		var val int
		if isRand {
			val = rnd.Int()
		} else {
			val = i
		}
		if shouldSnap && i%100000 == 0 {
			s, _ := w.NewSnapshot()
			s.Close()
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		w.Put(buf)
	}
}

func TestInsertPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := New()
	defer db.Close()
	n := 20000000 / runtime.GOMAXPROCS(0)
	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(db, &wg, n, true, true)
	}
	wg.Wait()

	snap, _ := db.NewSnapshot()
	defer snap.Close()
	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s\n",
		total, dur, float64(total)/float64(dur.Seconds()))
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
	itr.Close()
}

func TestGetPerf(t *testing.T) {
	var wg sync.WaitGroup
	db := New()
	defer db.Close()
	n := 1000000
	wg.Add(1)
	go doInsert(db, &wg, n, false, true)
	wg.Wait()
	snap, _ := db.NewSnapshot()
	defer snap.Close()

	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doGet(t, db, snap, &wg, n)
	}
	wg.Wait()
	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))
}
