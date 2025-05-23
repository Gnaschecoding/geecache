package geecache

import (
	"fmt"
	"log"
	"reflect"
	"testing"
)

func TestGetter(t *testing.T) {
	var f Retriever = RetrieverFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})
	expect := []byte("key")
	if v, _ := f.retrieve("key"); !reflect.DeepEqual(expect, v) {
		t.Errorf("expect:%v,actual:%v", expect, v)
	}
}

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))
	gee := NewGroup("score", 2<<10, RetrieverFunc(func(key string) ([]byte, error) {
		log.Println("[SlowDB] search key", key)
		if v, ok := db[key]; ok {
			if _, ok := loadCounts[key]; !ok {
				loadCounts[key] = 0
			}
			loadCounts[key]++
			return []byte(v), nil
		}
		return nil, fmt.Errorf("%s not found", key)
	}))
	for k, v := range db {
		if view, err := gee.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value of Tom")
		}
		if _, err := gee.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}

	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknow should be empty, but %s got", view)
	}
}
