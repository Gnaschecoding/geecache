package singleflight

import (
	"sync"
)

// call 代表正在进行中，或已经结束的请求。使用 sync.WaitGroup 锁避免重入。
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group 是 singleflight 的主数据结构，管理不同 key 的请求(call)。
type Flight struct {
	mu     sync.Mutex // protects m
	flight map[string]*call
}

// 这段代码是一个 请求合并（singleflight）机制 的实现。
// 它的目的是：多个 goroutine 同时请求相同的 key 时，只让第一个发起真正的函数调用，其他人等着结果共享。
func (f *Flight) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	f.mu.Lock() //1.如果同时进来，只有一个能拿到锁，其他的都阻塞
	if f.flight == nil {
		f.flight = make(map[string]*call)
	}
	//3. 其他的也进来了，发现有人在调用，那就等着共享结果，剩下所有的都会在c.wg.Wait()阻塞，等待唤醒
	if c, ok := f.flight[key]; ok {
		f.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	//2. 第一个拿到锁的先存进map，然后释放锁，
	c := new(call)
	c.wg.Add(1)
	f.flight[key] = c
	f.mu.Unlock()
	c.val, c.err = fn()
	c.wg.Done()
	//4. 第一个进来的执行完了，就把结果 共享出去了，然后上锁把这个删除掉，必须要删除不能存起来，否则内存越来越大，
	//2是不删除相当于引进了二级缓存，无法保持数据同步

	//time.Sleep(10 * time.Second)    //用于 搞那个统一恢
	f.mu.Lock()
	delete(f.flight, key)
	f.mu.Unlock()
	return c.val, c.err
}
