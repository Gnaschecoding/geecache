package singleflight

import "sync"

// call 代表正在进行中，或已经结束的请求。使用 sync.WaitGroup 锁避免重入。
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group 是 singleflight 的主数据结构，管理不同 key 的请求(call)。
type Group struct {
	mu sync.Mutex // protects m
	m  map[string]*call
}

// 这段代码是一个 请求合并（singleflight）机制 的实现。
// 它的目的是：多个 goroutine 同时请求相同的 key 时，只让第一个发起真正的函数调用，其他人等着结果共享。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()
	c.val, c.err = fn()
	c.wg.Done()
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, c.err
}
