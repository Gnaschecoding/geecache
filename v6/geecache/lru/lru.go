package lru

import "container/list"

// Cache is a LRU cache. It is not safe for concurrent access.
type Cache struct {
	maxBytes  int64                         // 允许使用的最大内存（字节）
	nbytes    int64                         // 当前已使用的内存大小（字节）
	ll        *list.List                    // 双向链表，存储缓存数据的访问顺序
	cache     map[string]*list.Element      // 哈希表，键是字符串，值是链表节点指针
	OnEvicted func(key string, value Value) // 当数据被移除时的回调函数
}

type entry struct {
	key   string
	value Value
}

// Value use Len to count how many bytes it takes
type Value interface {
	Len() int
}

// New is the Constructor of Cache
func New(maxBytes int64, OnEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: OnEvicted,
	}
}

// Get look ups a key's value
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return
}

// RemoveOldest removes the oldest item

func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key string, value Value) {
	if c.maxBytes <= 0 {
		// 如果 maxBytes 为 0，则不添加数据
		return
	}

	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len()) //新的减去旧的，如果变成了就是加上一个正值，反之就是负值
		kv.value = value
	} else {
		c.cache[key] = c.ll.PushFront(&entry{key, value})
		c.nbytes += int64(len(key)) + int64(value.Len())
	}

	//可能内存溢出了，需要去掉不用的  注意内容可能是非正的情况 不能执行移除
	for c.maxBytes > 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// Len the number of cache entries
func (c *Cache) Len() int {
	return c.ll.Len()
}
