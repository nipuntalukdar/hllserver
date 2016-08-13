package hllstore

type KeyExpiry struct {
	Key    string
	Expiry uint64
}

type HllStore interface {
	Update(key string, expiry uint64, value []byte) bool
	Delete(key string)
	GetAll() []*KeyExpiry
	Get(key string) ([]byte, uint64, error)
	GetExpiry(key string) (uint64, error)
}