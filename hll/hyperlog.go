package hll

import (
	"github.com/nipuntalukdar/bitset"
	"math"
	"sync"
)

type hyperlog struct {
	slot           []byte
	numslot        uint32
	numnonzeroslot uint32
	lock           *sync.RWMutex
}

const (
	sLOT     uint32  = 256
	bITSS    uint32  = (256 * 5) / 8
	sLOTF    float64 = float64(sLOT)
	tWOPO32  uint64  = 0x00000100000000
	tWOPO32F float64 = float64(tWOPO32)
	fVAL     float64 = (0.7213 / (1 + 1.079/sLOTF)) * sLOTF * sLOTF
	cMP1     float64 = 2.5 * sLOTF
	cMP2     float64 = tWOPO32F / 30.0
)

func newHyperLog() *hyperlog {
	slot := make([]byte, sLOT)
	return &hyperlog{slot: slot, numslot: sLOT, numnonzeroslot: 0, lock: &sync.RWMutex{}}
}

func (hpl *hyperlog) addhash(val uint32) {
	idx := byte(val >> 24)
	var andwth uint32 = 0x00800000
	var leadzs uint32 = 0
	i := 23
	for {
		if (val & andwth) != 0 {
			break
		}
		leadzs += 1
		if i == 0 {
			break
		}
		i--
		andwth = andwth >> 1
	}
	// value at index set to leading zeros count + 1
	leadzs += 1
	hpl.lock.Lock()
	defer hpl.lock.Unlock()
	if hpl.slot[idx] < byte(leadzs) {
		if hpl.slot[idx] == 0 {
			hpl.numnonzeroslot += 1
		}
		hpl.slot[idx] = byte(leadzs)
	}
}

func (hpl *hyperlog) count_cardinality() uint64 {
	var i uint32 = 0
	sum := 0.0
	hpl.lock.RLock()
	defer hpl.lock.RUnlock()
	for i < sLOT {
		x := 1 << hpl.slot[i]
		sum += 1.0 / float64(x)
		i++
	}
	ret := fVAL / sum
	if ret <= cMP1 {
		i = 0
		var v float64 = float64(sLOT - hpl.numnonzeroslot)
		if v != 0 {
			return uint64(sLOTF * math.Log(sLOTF/v))
		} else {
			return uint64(ret)
		}
	} else if ret <= cMP2 {
		return uint64(ret)
	}
	ret = -(tWOPO32F * math.Log(1.0-ret/tWOPO32F))
	return uint64(ret)
}

func (hpl *hyperlog) serialize() []byte {
	hpl.lock.RLock()
	defer hpl.lock.RUnlock()

	// if number of set index <= 80, then return the array as
	// array of set indices, followed by the array of values set at those
	// indices,
	// Otherwise return an arry from bitset
	if hpl.numnonzeroslot <= 80 {
		ret := make([]byte, hpl.numnonzeroslot<<1+1)
		i := uint32(0)
		curset := uint32(1)
		ret[0] = byte(hpl.numnonzeroslot)
		for i < sLOT {
			if hpl.slot[i] != 0 {
				ret[curset] = byte(i)
				ret[curset+hpl.numnonzeroslot] = hpl.slot[i]
				curset++
			}
			i++
		}
		return ret
	}
	bs := bitset.NewBitset(bITSS + 1)
	bs.SetVal(0, 7, 0xff)
	pos := uint32(8)
	for _, v := range hpl.slot {
		bs.SetVal(pos, pos+4, uint32(v))
		pos += 5
	}
	return bs.GetBytesUnsafe()
}

func deserialize(data []byte) (bool, *hyperlog) {
	datalen := uint32(len(data))
	if datalen <= 1 || datalen&1 == 0 {
		return false, nil
	}
	if data[0] == 0xff {
		if datalen != bITSS+1 {
			return false, nil
		}
	} else {
		if datalen != uint32(data[0])<<1+1 {
			return false, nil
		}
	}
	hpl := newHyperLog()
	if data[0] != 0xff {
		actual_size := uint32(data[0])
		start := uint32(1)
		for start <= actual_size {
			hpl.slot[data[start]] = data[start+actual_size]
			start++
		}
	} else {
		// Data was in a bitset
		bs := bitset.NewBitsetFromArray(data[1:])
		start := uint32(0)
		cur_indx := uint32(0)
		for cur_indx < 256 {
			val, err := bs.GetVal(start, start+4)
			if err != nil {
				return false, nil
			}
			hpl.slot[cur_indx] = byte(val)
			cur_indx++
			start += 5
		}
	}
	return true, hpl
}
