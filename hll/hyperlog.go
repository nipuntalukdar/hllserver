package hll

import (
	"github.com/nipuntalukdar/bitset"
	"math"
	"sync"
	"sync/atomic"
)

type hyperlog struct {
	key            string
	slot           []uint32
	numslot        uint32
	numnonzeroslot uint32
	lock           *sync.RWMutex
	updated        int32
	deleted        uint32
	expiry         uint64
	delwait        sync.WaitGroup
	delwaiter      uint32
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

func newHyperLog(logkey string, expiry uint64) *hyperlog {
	slot := make([]uint32, sLOT)
	return &hyperlog{key: logkey, slot: slot, numslot: sLOT, numnonzeroslot: 0,
		lock: &sync.RWMutex{}, updated: 0, expiry: expiry}
}

func (hpl *hyperlog) addhash(val uint32) (int32, bool) {
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
	curval := atomic.LoadUint32(&hpl.slot[idx])
	if curval >= leadzs {
		return 0, false
	}

	updated := false
	hpl.lock.RLock()
	defer hpl.lock.RUnlock()
	for curval < leadzs {
		if atomic.CompareAndSwapUint32(&hpl.slot[idx], curval, leadzs) {
			if curval == 0 {
				atomic.AddUint32(&hpl.numnonzeroslot, 1)
			}
			updated = true
			break
		} else {
			curval = atomic.LoadUint32(&hpl.slot[idx])
		}
	}
	if updated {
		return atomic.AddInt32(&hpl.updated, 1), true
	} else {
		return 0, false
	}
}

func (hpl *hyperlog) processed(delta int32) int32 {
	return atomic.AddInt32(&hpl.updated, delta)
}

func (hpl *hyperlog) getUpdCount() int32 {
	return atomic.LoadInt32(&hpl.updated)
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
	hpl.lock.Lock()
	defer hpl.lock.Unlock()

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
				ret[curset+hpl.numnonzeroslot] = byte(hpl.slot[i])
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

func deserialize(key string, expiry uint64, data []byte) (bool, *hyperlog) {
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
	hpl := newHyperLog(key, expiry)
	if data[0] != 0xff {
		actual_size := uint32(data[0])
		start := uint32(1)
		for start <= actual_size {
			hpl.slot[data[start]] = uint32(data[start+actual_size])
			start++
		}
		hpl.numnonzeroslot = actual_size
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
			hpl.slot[cur_indx] = val
			if val > 0 {
				hpl.numnonzeroslot += 1
			}
			cur_indx++
			start += 5
		}
	}
	return true, hpl
}
