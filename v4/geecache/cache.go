package geecache

import (
	"sync"
	"v4/geecache/lru"
)

type cache struct {
	mux        sync.Mutex
	lru        *lru.Cache
	cacheBytes int64
}

// 延迟绑定，需要的时候才创建，可以减少内存，比较灵活
func (c *cache) add(key string, value ByteView) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}

	c.lru.Add(key, value)
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.lru == nil {
		return
	}
	if value, ok := c.lru.Get(key); ok {
		return value.(ByteView), true
	}
	return
}
