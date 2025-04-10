package geecache

import (
	"fmt"
	"log"
	"sync"
	"v8/geecache/geecachepb"
	"v8/geecache/singleflight"
)

// Retriever 要求对象实现从数据源获取数据的能力
type Retriever interface {
	retrieve(key string) ([]byte, error)
}

type RetrieverFunc func(key string) ([]byte, error)

// RetrieverFunc 通过实现retrieve方法，使得任意匿名函数func
// 通过被RetrieverFunc(func)类型强制转换后，实现了 Retriever 接口的能力
func (f RetrieverFunc) retrieve(key string) ([]byte, error) {
	return f(key)
}

// A Group is a cache namespace and associated data loaded spread over
// Group 提供命名管理缓存/填充缓存的能力
type Group struct {
	name      string    //每个 Group 拥有一个唯一的名称 name
	retriever Retriever //第二个属性是 retriever Retriever，即缓存都不存在时获取源数据的回调(callback)。
	mainCache *cache    //第三个属性是 mainCache cache，即一开始实现的并发缓存。

	peers PeerPicker //节点  就是 HTTPPool类型
	// use singleflight.Group to make sure that
	// each key is only fetched once
	loader *singleflight.Flight
}

var (
	mux    sync.RWMutex
	groups = make(map[string]*Group)
)

// NewGroup create a new instance of Group
func NewGroup(name string, cacheBytes int64, retriever Retriever) *Group {
	if retriever == nil {
		panic("getter is nil")
	}
	mux.Lock()
	defer mux.Unlock()
	g := &Group{
		name:      name,
		retriever: retriever,
		mainCache: &cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Flight{},
	}
	groups[name] = g
	return g
}

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mux.RLock()
	g := groups[name]
	mux.RUnlock()
	return g
}

// 假设某一个组下线了
func DestroyGroup(name string) {
	g := GetGroup(name)
	if g != nil {
		svr := g.peers.(*Server)
		svr.Stop()
		delete(groups, name)
		log.Printf("Destroy cache [%s %s]", name, svr.addr)
	}
}

// Get value for a key from cache
// 流程 ⑴ ：从 mainCache 中查找缓存，如果存在则返回缓存值。
// 流程 ⑶ ：缓存不存在，则调用 load 方法，
// load 调用 getLocally（分布式场景下会调用 getFromPeer 从其他节点获取），
// getLocally 调用用户回调函数 g.getter.Get() 获取源数据，
// 并且将源数据添加到缓存 mainCache 中（通过 populateCache 方法）
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}
	return g.load(key)
}

func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.

	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		return g.getLocally(key)
	})
	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

func (g *Group) getFromPeer(peer Fetcher, key string) (ByteView, error) {
	req := &geecachepb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &geecachepb.Response{}
	err := peer.Fetch(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: res.Value}, nil
}

func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.retriever.retrieve(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers

}
