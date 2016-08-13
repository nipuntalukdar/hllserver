package hutil

import "testing"
import "sort"

func TestU64Slice(t *testing.T) {
	var s Uint64Slice
	s = append(s, uint64(10))
	s = append(s, uint64(1))
	s = append(s, uint64(12))
	s = append(s, uint64(6))
	sort.Sort(s)
	t.Logf("The sorted array %v", s)
	if s[0] != 1 || s[1] != 6 || s[2] != 10 || s[3] != 12 {
		t.Fatal("Sorting failed")
	}
}
