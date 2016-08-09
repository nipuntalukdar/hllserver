package hll

import (
	"math/rand"
	"testing"
)

func TestHyperLog(t *testing.T) {
	hpl := newHyperLog()
	hpl2 := newHyperLog()
	var i uint32 = 0
	mp := make(map[uint32]uint32)
	rand.Seed(42)
	for i < 100000 {
		i++
		v := rand.Uint32()
		hpl.addhash(v)
		mp[v] = 1
	}
	t.Logf("Real cardinality %d", len(mp))
	t.Logf("Computed Cardinality %d\n", hpl.count_cardinality())
	rand.Seed(10)
	i = 0
	for i < 10 {
		i++
		v := rand.Uint32()
		hpl2.addhash(v)
	}
	t.Logf("Computed Cardinality for small inputs %d\n", hpl2.count_cardinality())
}

func TestHyperLogSerialize(t *testing.T) {
	hpl := newHyperLog()
	i := 0
	for i < 10 {
		i++
		v := rand.Uint32()
		hpl.addhash(v)
	}
	bytes := hpl.serialize()
	if len(bytes) != 21 {
		t.Fatal("Serialization failed %d", len(bytes))
	}
	t.Logf("The returned array size %d", len(bytes))
	hpl2 := newHyperLog()
	i = 0
	rand.Seed(42)
	for i < 100000 {
		i++
		v := rand.Uint32()
		hpl2.addhash(v)
	}
	bytes = hpl2.serialize()
	if len(bytes) != 161 {
		t.Fatalf("Serialization failed %d", len(bytes))
	}
	t.Logf("The returned array size %d", len(bytes))
}
