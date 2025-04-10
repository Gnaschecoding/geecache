package geecache

import "v8/geecache/geecachepb"

// PeerPicker is the interface that must be implemented to locate
// the peer that owns a specific key.
// Picker 定义了获取分布式节点的能力
type PeerPicker interface {
	PickPeer(key string) (peer Fetcher, ok bool)
}

// 接口 PeerGetter 的 Get() 方法用于从对应 group 查找缓存值。PeerGetter 就对应于上述流程中的 HTTP 客户端。

// Fetcher 定义了从远端获取缓存的能力
// 所以每个Peer应实现这个接口
type Fetcher interface {
	//Get(group string, key string) ([]byte, error)
	Fetch(in *geecachepb.Request, out *geecachepb.Response) error
}
