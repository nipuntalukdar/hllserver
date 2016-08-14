package hllstore

import (
	"fmt"
	"reflect"
	"testing"
)

func TestBoltStore(t *testing.T) {
	bs := NewBoltStore("/tmp/abc", "mybolt.db")
	i := 0
	value := []byte("some value for test")
	for i < 20000 {
		bs.Update(fmt.Sprintf("mykey%d", i), uint64(i), value)
		i++
	}
	bs.FlushAndStop()
	bs = NewBoltStore("/tmp/abc", "mybolt.db")
	i = 0
	bs.ProcessAll(func(key []byte, expiry uint64, value []byte) bool {
		if i&255 == 255 {
			t.Logf("Key %s, value: %s, expiry: %d", key, value, expiry)
		}
		i++
		return true
	})
	if i != 20000 {
		t.Fatal("Db store test failed expected 20000, got %d", i)
	}

	data, exp, err := bs.Get("mykey999")
	if err != nil {
		t.Fatalf("Failed %s", err)
	}
	if exp != 999 {
		t.Fatal("Incorrect expiry value")
	}
	if !reflect.DeepEqual(data, value) {
		t.Fatal("Incorrect data read")
	}
	t.Logf("Key: mykey999, value: %s, expiry: %d", data, exp)
	exp, err = bs.GetExpiry("mykey1111")
	if err != nil {
		t.Fatalf("Failed %s", err)
	}
	if exp != 1111 {
		t.Fatal("Incorrect expiry value")
	}
	t.Logf("Key: mykey1111, expiry: %d", exp)
}
