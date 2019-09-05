package Cache

import "goRedis/stat"


type Scanner interface {
	Scan() bool
	Key() string
	Value() []byte
	Close()
}

type Cache interface {
	Set(k string,v []byte) error
	Get(k string) ([]byte,error)
	Del(k string) error
	GetStat() stat.Stat
	NewScanner() Scanner
}
func New(t string) Cache {
	if t == "inmemory"{
		return newInMemoryCache()
	}
	return nil
}