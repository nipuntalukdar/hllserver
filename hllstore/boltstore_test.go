package hllstore

import (
	"fmt"
	"github.com/nipuntalukdar/hllserver/hllogs"
	"os"
	"reflect"
	"testing"
	"time"
)

type KeyValProc struct {
	t         *testing.T
	printeach int
	tmp       int
}

func (proc *KeyValProc) Process(key string, expiry uint64, value []byte) error {
	if proc.tmp == 0 {
		proc.t.Logf("Key processing key:%s, expiry: %d, value %s", key, expiry, value)
	}
	proc.tmp++
	if proc.tmp == proc.printeach {
		proc.tmp = 0
	}

	return nil
}

func TestMain(m *testing.M) {
	hllogs.InitLogger(10, 1024, "a.log", "INFO")
	m.Run()
	time.Sleep(2 * time.Second)
}

func TestBoltStore(t *testing.T) {
	bs := NewBoltStore("/tmp", "mybolt.db")
	i := 0
	value := []byte("some value for test")
	for i < 20000 {
		bs.Update(fmt.Sprintf("mykey%d", i), uint64(i), value)
		i++
	}
	bs.FlushAndStop()
	bs = NewBoltStore("/tmp", "mybolt.db")
	i = 0
	kp := &KeyValProc{t, 20, 0}
	bs.ProcessAll(kp)

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
	bs.FlushAndStop()
	os.Remove("/tmp/mybolt.db")

}
