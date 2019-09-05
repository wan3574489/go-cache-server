package Cache

import (
	"goRedis/stat"
	"sync"
)

type inMemoryScanner struct {
	pair
	pairCh chan *pair
	closeCh chan struct{}
}

func (s *inMemoryScanner) Close()  {
	close(s.closeCh)
}

func (s *inMemoryScanner) Scan() bool  {
	p ,ok := <- s.pairCh
	if ok {
		s.k,s.v =p.k,p.v
	}
	return ok
}

func (s *inMemoryScanner) Key() string {
	return s.k
}
func (s *inMemoryScanner) Value() []byte {
	return s.v
}

type inMemoryCache struct {
	c     map[string][]byte
	mutex sync.RWMutex
	Stat  stat.Stat
}

func (cache *inMemoryCache) NewScanner() Scanner {
	pairCh := make(chan *pair)
	closeCh := make(chan  struct{})

	go func() {
		//最后关闭pairch
		defer close(pairCh)
		//加读锁
		cache.mutex.RLock()

		for k,v := range cache.c {
			cache.mutex.RUnlock()
			select {
			case <-closeCh:
				return
			case pairCh <- &pair{k,v}:
			}
			cache.mutex.RLock()
		}

		cache.mutex.RUnlock()

	}()
	return &inMemoryScanner{pair{},pairCh,closeCh}
}

func (cache *inMemoryCache) Set(k string,v []byte) error {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	if cache.c[k] != nil{
		cache.Stat.Del(k,cache.c[k])
		delete(cache.c, k)

	}
	cache.c[k] = v
	cache.Stat.Add(k,v)
	return nil
}

func (cache *inMemoryCache) Get(k string) ([]byte, error) {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()
	return cache.c[k],nil
}

func (cache *inMemoryCache) Del(k string) error {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if cache.c[k] != nil{
		cache.Stat.Del(k,cache.c[k])
		delete(cache.c,k)
	}
	return nil
}

func (cache *inMemoryCache) GetStat() stat.Stat {
	return cache.Stat
}

func newInMemoryCache() *inMemoryCache {
	return &inMemoryCache{
		make(map[string][]byte),
		sync.RWMutex{},
		stat.Stat{},
	}
}