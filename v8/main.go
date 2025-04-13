// example.go file
// 运行前，你需要在本地启动Etcd实例，作为服务中心。

package main

import (
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"v8/geecache"
	"v8/geecache/geecachepb"

	"log"
)

var db = map[string]string{
	//"Tom":  "630",
	//"Jack": "589",
	//"Sam":  "567",
	"Tom":  "💩,630：拉了满分的粑粑", // 630：拉了满分的粑粑
	"Jack": "🤡,589：小丑竟是我自己", // 589：小丑竟是我自己
	"Sam":  "😭,567：哭晕在厕所",   // 567：哭晕在厕所
}

func CreateGroup() *geecache.Group {

	return geecache.NewGroup("scores", 2<<10, geecache.RetrieverFunc(
		func(key string) ([]byte, error) {
			log.Println("[db] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

func startCacheServer(svr *geecache.Server, addr string, addrs []string, group *geecache.Group) {

	svr.SetPeers(addrs...)

	// 将服务与cache绑定 因为cache和server是解耦合的
	group.RegisterPeers(svr)
	log.Println("geecache is running at", addr)
	// 启动服务(注册服务至etcd/计算一致性哈希...)
	go func() {
		// Start将不会return 除非服务stop或者抛出error
		err := svr.Start()
		if err != nil {
			log.Fatal(err)
		}
	}()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}

	defer lis.Close()

	//创建服务
	grpcServer := grpc.NewServer()
	geecachepb.RegisterGroupCacheServer(grpcServer, svr)
	//	RegisterSayHelloServer(grpcServer, &server{})

	//启动服务
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
		return
	}

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

		//w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write(view.ByteSlice())
	}))

	log.Println("fontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))
}

func main() {
	// 模拟MySQL数据库 用于peanutcache从数据源获取值
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "Geecache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	apiAddr := "http://49.123.84.136:9999"
	addrMap := map[int]string{
		8001: "49.123.84.136:8001",
		8002: "49.123.84.136:8002",
		8003: "49.123.84.136:8003",
	}

	// 新建cache实例
	group := CreateGroup()
	// New一个服务实例
	//var addr string = "localhost:9999"
	var addr string = addrMap[port]
	svr, err := geecache.NewServer(addr)
	if err != nil {
		log.Fatal(err)
	}
	// 设置同伴节点IP(包括自己)
	// todo: 这里的peer地址从etcd获取(服务发现)
	addrs, err := svr.GetPeersFromEtcd()
	if err != nil {
		log.Fatal(err)
	}
	addrs = append(addrs, addr) //把自己注册到了哈希环
	log.Println(addrs)
	if api {
		go startAPIServer(apiAddr, group)
	}

	startCacheServer(svr, addrMap[port], []string(addrs), group)
}
