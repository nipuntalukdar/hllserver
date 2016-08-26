package hllstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"hash/crc32"
	"sync"
	"time"
)

const (
	bKTPREFIX = "bkt"
)

const (
	UPD uint16 = iota
	DEL
)

type mutation struct {
	op    uint16
	bktn  uint16
	key   []byte
	value []byte
}

type BoltStore struct {
	dbdir   string
	dbname  string
	db      *bolt.DB
	bucketn []string
	buckets []*bolt.Bucket
	works   chan *mutation
	flush   chan *sync.WaitGroup
}

func NewBoltStore(dbdir string, dbname string) *BoltStore {
	dbpath := fmt.Sprintf("%s/%s", dbdir, dbname)
	db, err := bolt.Open(dbpath, 0644, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	i := 0
	bucketn := make([]string, 8)
	for i < 8 {
		bucketn[i] = fmt.Sprintf("%s_%d", bKTPREFIX, i)
		i++
	}
	buckets := make([]*bolt.Bucket, 8)
	t, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	i = 0
	for i < 8 {
		_, err := t.CreateBucketIfNotExists([]byte(bucketn[i]))
		if err != nil {
			panic(err)
		}
		i++
	}
	err = t.Commit()
	if err != nil {
		panic(err)
	}
	works := make(chan *mutation, 10240)
	flushchan := make(chan *sync.WaitGroup, 10)
	bs := &BoltStore{dbdir, dbname, db, bucketn, buckets, works, flushchan}
	go bs.writeToDb()
	return bs
}

func (bs *BoltStore) Update(key string, expiry uint64, value []byte) bool {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, expiry)
	n, err := buf.Write(value)
	if n != len(value) || err != nil {
		return false
	}
	bs.works <- &mutation{UPD, uint16(crc32.ChecksumIEEE([]byte(key)) & 7), []byte(key),
		buf.Bytes()}
	return true
}

func (bs *BoltStore) Delete(key string) {
	bs.works <- &mutation{DEL, uint16(crc32.ChecksumIEEE([]byte(key)) & 7), []byte(key), nil}
}

func (bs *BoltStore) processBucket(bkt *bolt.Bucket, processor KeyValProcessor) (error, bool) {
	cursor := bkt.Cursor()
	var expiry uint64
	ret := true
	var err error = nil
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if len(v) <= 8 {
			return errors.New("Invalid data"), true
		}
		rd := bytes.NewReader(v)
		err = binary.Read(rd, binary.LittleEndian, &expiry)
		if err != nil {
			return err, true
		}
		err = processor.Process(string(k), expiry, v[8:])
		if err != nil {
			return err, false
		}
	}
	return err, ret
}

func (bs *BoltStore) ProcessAll(processor KeyValProcessor) error {
	var err error
	var ret bool
	for _, bktn := range bs.bucketn {
		tx, err := bs.db.Begin(false)
		bkt := tx.Bucket([]byte(bktn))
		if bkt != nil {
			err, ret = bs.processBucket(bkt, processor)
		}
		tx.Rollback()
		if err != nil || !ret {
			break
		}
	}
	return err
}

func (bs *BoltStore) Get(key string) ([]byte, uint64, error) {
	bktnum := crc32.ChecksumIEEE([]byte(key)) & 7
	tx, err := bs.db.Begin(false)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	bkt := tx.Bucket([]byte(bs.bucketn[bktnum]))
	if bkt == nil {
		return nil, 0, errors.New("Bucket not found")
	}
	val := bkt.Get([]byte(key))
	if val == nil || len(val) <= 8 {
		return nil, 0, errors.New("Key not exists or invalid value for key")
	}
	var expiry uint64
	rd := bytes.NewReader(val)
	binary.Read(rd, binary.LittleEndian, &expiry)
	retval := make([]byte, len(val)-8)
	copy(retval, val[8:])
	return retval, expiry, nil
}

func (bs *BoltStore) GetExpiry(key string) (uint64, error) {
	bktnum := crc32.ChecksumIEEE([]byte(key)) & 7
	tx, err := bs.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	bkt := tx.Bucket([]byte(bs.bucketn[bktnum]))
	if bkt == nil {
		return 0, errors.New("Bucket not found")
	}
	val := bkt.Get([]byte(key))
	if val == nil || len(val) <= 8 {
		return 0, errors.New("Key not exists or invalid value for key")
	}
	var expiry uint64
	rd := bytes.NewReader(val)
	binary.Read(rd, binary.LittleEndian, &expiry)
	return expiry, nil
}

func (bs *BoltStore) initTransactions() (*bolt.Tx, error) {
	tx, err := bs.db.Begin(true)
	if err != nil {
		return nil, err
	}
	i := 0
	for i < 8 {
		bs.buckets[i] = tx.Bucket([]byte(bs.bucketn[i]))
		i++
	}
	return tx, nil
}

func (bs *BoltStore) dbUpdate(tx *bolt.Tx, key []byte, value []byte, bktn uint32) error {
	return bs.buckets[bktn].Put(key, value)
}

func (bs *BoltStore) dbDelete(tx *bolt.Tx, key []byte, bktn uint32) error {
	return bs.buckets[bktn].Delete(key)
}

func (bs *BoltStore) writeToDb() {
	var added uint32 = 0
	commited := true
	timer := time.NewTicker(1 * time.Second)
	var tx *bolt.Tx
	var err error
	for {
		select {
		case _ = <-timer.C:
			if added > 0 {
				if commited {
					tx, err = bs.initTransactions()
					if err != nil {
						panic(err)
					}
				}
				tx.Commit()
				commited = true
				added = 0
			}
		case mut := <-bs.works:
			added++
			if commited {
				tx, err = bs.initTransactions()
				if err != nil {
					panic(err)
				}
				commited = false
			}
			if mut.op == UPD {
				err = bs.dbUpdate(tx, mut.key, mut.value, uint32(mut.bktn))
			} else {
				err = bs.dbDelete(tx, mut.key, uint32(mut.bktn))
			}
			if err != nil {
				panic(err)
			}
			if added > 10000 {
				tx.Commit()
				commited = true
				added = 0
			}
		case wg := <-bs.flush:
			if added > 0 {
				if commited {
					tx, err = bs.initTransactions()
					if err != nil {
						panic(err)
					}
				}
				tx.Commit()
				commited = true
				added = 0
			}
			wg.Done()
		}
	}
}

func (bs *BoltStore) Flush() {
	var wg sync.WaitGroup
	wg.Add(1)
	bs.flush <- &wg
	wg.Wait()
}

func (bs *BoltStore) FlushAndStop() {
	time.Sleep(2 * time.Second)
	bs.db.Close()
}
