package consistenthash

import (
	"strconv"
	"testing"
)

func TestConsistCache(t *testing.T) {
	hash := New(3, func(data []byte) uint32 {
		t, _ := strconv.Atoi(string(data))
		return uint32(t)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	//哈希环分别对应的映射节点是 2 ：2 12 22 、  4：4 14 24    、 6：6 16 26
	hash.Add("6", "4", "2")

	//模拟进来的值是否落入到正确的对应的虚拟节点 映射节点上
	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("%s failed", k)
		}
	}

	//	更改一下，假如加入一个 8
	// Adds 8, 18, 28
	hash.Add("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("%s failed", k)
		}
	}

}
