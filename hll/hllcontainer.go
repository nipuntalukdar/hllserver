package hll

import (
	"container/list"
	"errors"
	"github.com/nipuntalukdar/hllserver/hllogs"
	"github.com/nipuntalukdar/hllserver/hllstore"
	"github.com/nipuntalukdar/hllserver/hutil"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	mAXSLOTS  = 2048
	mIINSLOTS = 4
	sEED      = 32
	nUPDL     = 8
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

type updLogs struct {
	lock *sync.RWMutex
	lst  *list.List
}

type HllContainer struct {
	hllmaps      []*hllMap
	expirym      map[uint64]map[string]*expm
	exmutex      *sync.RWMutex
	hslot        uint32
	ticker       *time.Ticker
	shutdown     chan bool
	store        hllstore.HllStore
	updates      []*updLogs
	updchan      chan *hyperlog
	delete_first []string
}

func newExpm(part uint32, log *hyperlog) *expm {
	return &expm{part, log}
}

func newUpdLogs() *updLogs {
	return &updLogs{&sync.RWMutex{}, list.New()}
}

func newHllMap(hlc *HllContainer, slot uint32) *hllMap {
	logm := make(map[string]*hyperlog)
	return &hllMap{logm: logm, mutex: &sync.RWMutex{}, hlc: hlc, slot: slot}
}

func NewHllContainer(slots uint32, store hllstore.HllStore) *HllContainer {
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
	var updls []*updLogs
	if store != nil {
		updls = make([]*updLogs, nUPDL)
		i := 0
		for i < 8 {
			updls[i] = newUpdLogs()
			i++
		}
	}
	hlc := &HllContainer{hllmaps: hllmaps, expirym: make(map[uint64]map[string]*expm),
		exmutex: exmutex, hslot: slots - 1, ticker: ticker,
		shutdown: make(chan bool), store: store, updates: updls,
		updchan: make(chan *hyperlog, 20480), delete_first: []string{}}

	i := uint32(0)
	for i < slots {
		hllmaps[i] = newHllMap(hlc, i)
		i++
	}
	if store != nil {
		// First restore from store
		hlc.restore()
		i = 0
		for i < 8 {
			go hlc.savechanges(hlc.updates[i])
			i++
		}
	}
	go hlc.storeUpdates()
	go hlc.cleanup()
	if store != nil && len(hlc.delete_first) > 0 {
		for _, key := range hlc.delete_first {
			hlc.store.Delete(key)
		}
		hlc.delete_first = nil
		hlc.store.Flush()
	}
	hllogs.Log.Info("HLLContainer initialized")
	return hlc
}

func (hc *HllContainer) AddLog(key string, entry []byte, expiry uint64) {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	hlog := hm.getOrAddLog(key, expiry)
	if entry != nil {
		entryh := murmur3_32(entry, sEED)
		newval, updated := hlog.addhash(entryh)
		if hc.store != nil {
			if updated && newval == 1 {
				hc.enqueueStoreUpd(slot, hlog)
			}
		}
	}
}

func (hc *HllContainer) AddMLog(key string, entry [][]byte, expiry uint64) {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	hlog := hm.getOrAddLog(key, expiry)
	enqueue := false
	for _, e := range entry {
		entryh := murmur3_32(e, sEED)
		newval, updated := hlog.addhash(entryh)
		if newval == 1 && updated {
			enqueue = true
		}
	}
	if enqueue && hc.store != nil {
		hc.enqueueStoreUpd(slot, hlog)
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
			hlog = newHyperLog(key, expiry)
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

func (hc *HllContainer) DelLog(key string) bool {
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	if hm.getLog(key) != nil {
		hm.mutex.Lock()
		hlog, ok := hm.logm[key]
		if ok {
			delete(hm.logm, key)
		}
		hm.mutex.Unlock()
		if ok {
			hlog.delwait.Add(1)
			atomic.StoreUint32(&hlog.deleted, 1)
			newval := atomic.AddInt32(&hlog.updated, 1)
			if newval == 1 {
				hc.enqueueStoreUpd(slot, hlog)
			}
			// Wait for delete actually applies to store
			hlog.delwait.Wait()
			return true
		}
	}
	return true
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

func (hc *HllContainer) addToDb(upds *updLogs, maxupd uint32) {
	i := uint32(0)
	for i < maxupd {
		upds.lock.RLock()
		l := upds.lst.Len()
		upds.lock.RUnlock()
		if l == 0 {
			break
		}
		upds.lock.Lock()
		front := upds.lst.Front()
		hlog := upds.lst.Remove(front).(*hyperlog)
		upds.lock.Unlock()
		hc.updchan <- hlog
		i++
	}
}

func (hc *HllContainer) savechanges(upds *updLogs) {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case _ = <-ticker.C:
			hc.addToDb(upds, 256)
		case _ = <-hc.shutdown:
			break
		}
	}
}

func (hc *HllContainer) enqueueStoreUpd(slot uint32, hlog *hyperlog) {
	lstupd := hc.updates[slot&7]
	lstupd.lock.Lock()
	defer lstupd.lock.Unlock()
	lstupd.lst.PushBack(hlog)
}

func (hc *HllContainer) updateStore(hlog *hyperlog) {
	key := hlog.key
	deleted := atomic.LoadUint32(&hlog.deleted)
	var updcount int32
	if deleted > 0 {
		hc.store.Delete(key)
		hc.store.Flush()
		hlog.delwait.Done()
	} else {
		updcount = hlog.getUpdCount()
		data := hlog.serialize()
		expiry := hlog.expiry
		hc.store.Update(key, expiry, data)
	}
	if deleted != 1 && hlog.processed(-updcount) > 0 {
		slot := murmur3_32([]byte(hlog.key), sEED) & hc.hslot
		hc.enqueueStoreUpd(slot, hlog)
	}
}

func (hc *HllContainer) storeUpdates() {
	for {
		select {
		case hlog := <-hc.updchan:
			hc.updateStore(hlog)
		case _ = <-hc.shutdown:
			break
		}
	}
}

func (hc *HllContainer) Process(key string, expiry uint64, data []byte) error {
	hllogs.Log.Infof("Trying to restore %s", key)
	now := uint64(time.Now().Unix())
	if expiry > 0 && expiry <= now {
		hllogs.Log.Infof("Key %s, has expiry %d, less than current time %d, deleting...",
			key, expiry, now)
		hc.delete_first = append(hc.delete_first, key)
		return nil
	}
	ok, hlog := deserialize(key, expiry, data)
	if !ok {
		return errors.New("Error in decoding hyperlog")
	}
	slot := murmur3_32([]byte(key), sEED) & hc.hslot
	hm := hc.hllmaps[slot]
	hllogs.Log.Infof("Restored log for key:%s", key)
	hm.logm[key] = hlog
	return nil
}

func (hc *HllContainer) restore() {
	if hc.store == nil {
		return
	}
	hllogs.Log.Info("Trying to restore")
	hc.store.ProcessAll(hc)
}
