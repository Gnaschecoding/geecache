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
type Map struct {
	hash     Hash           //Hash 函数 hash
	replicas int            //虚拟节点倍数 replicas
	keys     []int          // Sorted //哈希环 keys
	hashMap  map[int]string //虚拟节点与真实节点的映射表 hashMap，键是虚拟节点的哈希值，值是真实节点的名称。
}

// New creates a Map instance
func New(replicas int, fn Hash) *Map {
	m := &Map{
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
// 添加真实节点/机器的 Add() 方法
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ { //分别求key对应的每个虚拟节点的，然后插入到哈希环上
			hash := int(m.hash([]byte(strconv.Itoa(i) + key))) //求哈希值
			m.keys = append(m.keys, hash)                      //把虚拟机节点 存入到 哈希环的 m.keys上
			m.hashMap[hash] = key                              //虚拟节点对应的节点key是多少
		}
	}
	sort.Ints(m.keys)
}

// Get gets the closest item in the hash to the provided key.
// 选择节点的 Get() 方法，一个key打过来，来找到下一个节点，就是找到key应该存到哪个节点上
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	// Binary search for appropriate replica.
	//顺时针找到第一个匹配的虚拟节点的下标 idx
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })
	//如果 idx == len(m.keys)，说明应选择 m.keys[0]，因为 m.keys 是一个环状结构，所以用取余数的方式来处理这种情况。
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
