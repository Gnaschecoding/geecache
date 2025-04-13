package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash maps bytes to uint32
type Hash func(data []byte) uint32

// Map 是一致性哈希算法的主数据结构
// Map constains all hashed keys
type Consistency struct {
	hash     Hash           //Hash 函数 hash
	replicas int            //虚拟节点倍数 replicas
	ring     []int          // uint32哈希环
	hashMap  map[int]string //虚拟节点与真实节点的映射表 hashMap，键是虚拟节点的哈希值，值是真实节点的名称。
}

// New creates a Map instance
func New(replicas int, fn Hash) *Consistency {
	m := &Consistency{
		hash:     fn,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
	if fn == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add adds some keys to the hash.
// 添加真实节点/机器的 Add() 方法,注册更好些
// Register 将各个peer注册到哈希环上
func (m *Consistency) Register(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ { //分别求key对应的每个虚拟节点的，然后插入到哈希环上
			hash := int(m.hash([]byte(strconv.Itoa(i) + key))) //求哈希值
			m.ring = append(m.ring, hash)                      //把虚拟机节点 存入到 哈希环的 m.keys上
			m.hashMap[hash] = key                              //虚拟节点对应的节点key是多少
		}
	}
	sort.Ints(m.ring)
}

// Destroy 将各个peer注销到哈希环上
func (m *Consistency) Destroy(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ { //分别求key对应的每个虚拟节点的，然后插入到哈希环上
			hash := int(m.hash([]byte(strconv.Itoa(i) + key))) //求哈希值
			index := sort.SearchInts(m.ring, hash)             //把虚拟机节点 存入到 哈希环的 m.keys上
			if index < len(m.ring) && m.ring[index] == hash {
				// 删除 hash
				m.ring = append(m.ring[:index], m.ring[index+1:]...)
				delete(m.hashMap, hash) //虚拟节点对应的节点key是多少
			}
		}
	}
}

// Get gets the closest item in the hash to the provided key.
// 选择节点的 Get() 方法，一个key打过来，来找到下一个节点，就是找到key应该存到哪个节点上
func (m *Consistency) Get(key string) string {
	if len(m.ring) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	// Binary search for appropriate replica.
	//顺时针找到第一个匹配的虚拟节点的下标 idx
	idx := sort.Search(len(m.ring), func(i int) bool { return m.ring[i] >= hash })
	//如果 idx == len(m.keys)，说明应选择 m.keys[0]，因为 m.keys 是一个环状结构，所以用取余数的方式来处理这种情况。
	return m.hashMap[m.ring[idx%len(m.ring)]]
}
