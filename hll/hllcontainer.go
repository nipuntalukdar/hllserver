package hll

import (
	"fmt"
	"github.com/nipuntalukdar/hllserver/hutil"
	"sort"
	"sync"
	"time"
)

const (
	mAXSLOTS  = 2048
	mIINSLOTS = 4
	sEED      = 32
)

type hllMap struct {
	mutex *sync.RWMutex
	logm  map[string]*hyperlog
	hlc   *HllContainer
	slot  uint32
}

type expm struct {
	part uint32
	log  *hyperlog
}

type HllContainer struct {
	hllmaps  []*hllMap
	expirym  map[uint64]map[string]*expm
	exmutex  *sync.RWMutex
	hslot    uint32
	ticker   *time.Ticker
	shutdown chan bool
}

func newExpm(part uint32, log *hyperlog) *expm {
	return &expm{part, log}
}

func newHllMap(hlc *HllContainer, slot uint32) *hllMap {
	logm := make(map[string]*hyperlog)
	return &hllMap{logm: logm, mutex: &sync.RWMutex{}, hlc: hlc, slot: slot}
}

func NewHllContainer(slots uint32) *HllContainer {
	if slots < mIINSLOTS {
		slots = mIINSLOTS
	}
	if slots > mAXSLOTS {
		slots = mAXSLOTS
	}
	if slots&(slots-1) != 0 {
		powered := uint32(1)
		for slots > 0 {
			slots >>= 1
			powered <<= 1
		}
		slots = powered
	}
	hllmaps := make([]*hllMap, slots)
	exmutex := &sync.RWMutex{}
	ticker := time.NewTicker(60 * time.Second)
	hlc := &HllContainer{hllmaps: hllmaps, expirym: make(map[uint64]map[string]*expm),
		exmutex: exmutex, hslot: slots - 1, ticker: ticker,
		shutdown: make(chan bool)}
	i := uint32(0)
	for i < slots {
		hllmaps[i] = newHllMap(hlc, i)
		i++
	}
	go hlc.cleanup()
	return hlc
}

func (hc *HllContainer) AddLog(key string, entry []byte, expiry uint64) {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	hlog := hm.getOrAddLog(key, expiry)
	if entry != nil {
		entryh := murmur3_32(entry, sEED)
		hlog.addhash(entryh)
	}
}

func (hc *HllContainer) AddMLog(key string, entry [][]byte, expiry uint64) {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	hlog := hm.getOrAddLog(key, expiry)
	for _, e := range entry {
		entryh := murmur3_32(e, sEED)
		hlog.addhash(entryh)
	}
}

func (hm *hllMap) getOrAddLog(key string, expiry uint64) *hyperlog {
	hm.mutex.RLock()
	hlog, ok := hm.logm[key]
	hm.mutex.RUnlock()
	if !ok {
		hm.mutex.Lock()
		hlog, ok = hm.logm[key]
		if !ok {
			// we are adding a new log key
			hlog = newHyperLog()
			if expiry > 0 {
				exp := newExpm(hm.slot, hlog)
				expiry += uint64(time.Now().Unix())
				expbkt := expiry - expiry&127
				hm.hlc.exmutex.Lock()
				em, ok := hm.hlc.expirym[expbkt]
				if !ok {
					em = make(map[string]*expm)
					hm.hlc.expirym[expbkt] = em
				}
				em[key] = exp
				hm.hlc.exmutex.Unlock()
			}
			hm.logm[key] = hlog
		}
		hm.mutex.Unlock()
	}
	return hlog
}

func (hm *hllMap) getLog(key string) *hyperlog {
	hm.mutex.RLock()
	hlog, ok := hm.logm[key]
	hm.mutex.RUnlock()
	if !ok {
		return nil
	}
	return hlog
}

func (hc *HllContainer) DelLog(key string) {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	if hm.getLog(key) != nil {
		hm.mutex.Lock()
		defer hm.mutex.Unlock()
		delete(hm.logm, key)
	}
}

func (hc *HllContainer) GetCardinality(key string) uint64 {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	hlog := hm.getLog(key)
	if hlog != nil {
		return hlog.count_cardinality()
	} else {
		return 0
	}
}

func (hc *HllContainer) Shutdown() {
	hc.shutdown <- true
}

func (hc *HllContainer) doCleanup() {
	hc.exmutex.RLock()
	lenb := len(hc.expirym)
	hc.exmutex.RUnlock()
	if lenb == 0 {
		return
	}
	var bkts hutil.Uint64Slice
	tim := time.Now().Unix()
	hc.exmutex.RLock()
	for bkt, _ := range hc.expirym {
		if bkt < uint64(tim) {
			bkts = append(bkts, bkt)
		}
		// enough for cleaning up 40 buckets at a time
		if bkts.Len() >= 40 {
			break
		}
	}
	hc.exmutex.RUnlock()
	if bkts.Len() == 0 {
		return
	}
	sort.Sort(bkts)
	for _, bkt := range bkts {
		hc.exmutex.Lock()
		exps, ok := hc.expirym[bkt]
		if !ok {
			hc.exmutex.Unlock()
			continue
		}
		delete(hc.expirym, bkt)
		hc.exmutex.Unlock()
		for key, expel := range exps {
			part := expel.part
			hc.hllmaps[part].mutex.Lock()
			fmt.Printf("Trying to expire %s\n", key)
			delete(hc.hllmaps[part].logm, key)
			hc.hllmaps[part].mutex.Unlock()
		}
	}
}

func (hc *HllContainer) cleanup() {
	for {
		select {
		case _ = <-hc.ticker.C:
			hc.doCleanup()
		case _ = <-hc.shutdown:
			break
		}
	}
}
