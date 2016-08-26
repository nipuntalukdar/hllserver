package hllstore

type KeyExpiry struct {
	Key    string
	Expiry uint64
}

type KeyValProcessor interface {
	Process(key string, expiry uint64, value []byte) error
}

type HllStore interface {
	Update(key string, expiry uint64, value []byte) bool
	Delete(key string)
	ProcessAll(processor KeyValProcessor) error
	Get(key string) ([]byte, uint64, error)
	GetExpiry(key string) (uint64, error)
	FlushAndStop()
	Flush()
}
