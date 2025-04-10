package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"v7/geecache"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func createGroup() *geecache.Group {

	return geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

func startCacheServer(addr string, addrs []string, gee *geecache.Group) {
	peers := geecache.NewHTTPPool(addr)
	peers.Set(addrs...)      //把 httppool中的信息构造好，包括哈希环和记录对应节点的哈希表
	gee.RegisterPeers(peers) //把httppool赋值给 group中的peers中
	log.Println("geecache is running at", addr)
	log.Fatal(http.ListenAndServe(addr[7:], peers))
}
func startAPIServer(apiAddr string, gee *geecache.Group) {
	http.Handle("/api", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		view, err := gee.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(view.ByteSlice())
	}))

	log.Println("fontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))
}

func main() {

	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	//apiAddr := "http://localhost:9999"
	//addrMap := map[int]string{
	//	8001: "http://localhost:8001",
	//	8002: "http://localhost:8002",
	//	8003: "http://localhost:8003",
	//}

	apiAddr := "http://49.123.84.136:9999"
	addrMap := map[int]string{
		8001: "http://49.123.84.136:8001",
		8002: "http://49.123.84.136:8002",
		8003: "http://49.123.84.136:8003",
	}

	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}

	gee := createGroup()
	if api {
		go startAPIServer(apiAddr, gee)
	}

	startCacheServer(addrMap[port], []string(addrs), gee)

}
