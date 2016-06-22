package supernitro

import (
	"fmt"
	"testing"
)

func TestInsert(t *testing.T) {
	db := New()
	defer db.Close()

	w := db.NewWriter()
	for i := 0; i < 2000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	for i := 1750; i < 2000; i++ {
		w.Delete([]byte(fmt.Sprintf("%010d", i)))
	}
	snap, _ := w.NewSnapshot()
	defer snap.Close()

	for i := 2000; i < 5000; i++ {
		w.Put([]byte(fmt.Sprintf("%010d", i)))
	}

	snap2, _ := w.NewSnapshot()
	defer snap2.Close()

	/*
		count := 0
		itr := db.NewIterator(snap)
		defer itr.Close()

		itr.SeekFirst()
		itr.Seek([]byte(fmt.Sprintf("%010d", 1500)))
		for ; itr.Valid(); itr.Next() {
			expected := fmt.Sprintf("%010d", count+1500)
			got := string(itr.Get())
			count++
			if got != expected {
				t.Errorf("Expected %s, got %v", expected, got)
			}
		}

		if count != 250 {
			t.Errorf("Expected count = 250, got %v", count)
		}
	*/
}
