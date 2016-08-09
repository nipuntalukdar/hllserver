package hll

import (
	"sync"
)

const (
	mAXSLOTS  = 2048
	mIINSLOTS = 4
	sEED      = 32
)

type hllMap struct {
	mutex *sync.RWMutex
	logm  map[string]*hyperlog
}

type HllContainer struct {
	hllmaps []*hllMap
	hslot   uint32
}

func newHllMap() *hllMap {
	logm := make(map[string]*hyperlog)
	return &hllMap{logm: logm, mutex: &sync.RWMutex{}}
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
	i := uint32(0)
	for i < slots {
		hllmaps[i] = newHllMap()
		i++
	}
	return &HllContainer{hllmaps: hllmaps, hslot: slots - 1}
}

func (hc *HllContainer) AddLog(key string, entry []byte) {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	hlog := hm.getOrAddLog(key)
	if entry != nil {
		entryh := murmur3_32(entry, sEED)
		hlog.addhash(entryh)
	}
}

func (hc *HllContainer) AddMLog(key string, entry [][]byte) {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	hlog := hm.getOrAddLog(key)
	for _, e := range entry {
		entryh := murmur3_32(e, sEED)
		hlog.addhash(entryh)
	}
}

func (hm *hllMap) getOrAddLog(key string) *hyperlog {
	hm.mutex.RLock()
	hlog, ok := hm.logm[key]
	hm.mutex.RUnlock()
	if !ok {
		hm.mutex.Lock()
		hlog, ok = hm.logm[key]
		if !ok {
			hlog = newHyperLog()
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
