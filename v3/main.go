package main

import (
	"fmt"
	"log"
	"net/http"
	"v3/geecache"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func main() {
	geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
	addr := "localhost:9999"
	peers := geecache.NewHTTPPool(addr)
	log.Println("geecache is running at", addr)
	log.Println(http.ListenAndServe(addr, peers))
}
