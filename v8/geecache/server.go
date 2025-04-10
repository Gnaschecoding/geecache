package geecache

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"net"
	"strings"
	"v8/geecache/registry"

	"log"
	"sync"
	"time"
	"v8/geecache/consistenthash"
	"v8/geecache/geecachepb"
)

var (
	defaultAddr     = "127.0.0.1:6324"
	defaultReplicas = 50
)

var (
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"192.168.172.129:2379"},
		DialTimeout: 5 * time.Second,
	}
)

// server 和 Group 是解耦合的 所以server要自己实现并发控制
type Server struct {
	geecachepb.UnimplementedGroupCacheServer
	addr       string     // format: ip:port
	status     bool       // true: running false: stop
	stopSignal chan error // 通知registry revoke服务
	mux        sync.Mutex
	consHash   *consistenthash.Consistency
	clients    map[string]*Client
}

// NewServer 创建cache的svr 若addr为空 则使用defaultAddr
func NewServer(addr string) (*Server, error) {
	if addr == "" {
		addr = defaultAddr
	}

	if !validPeerAddr(addr) {
		return nil, fmt.Errorf("invalid addr %s, it should be x.x.x.x:port", addr)
	}
	return &Server{addr: addr}, nil
}

// Get 实现PeanutCache service的Get接口
// 把 http中 serverHttp中内容进行了改进
func (s *Server) Get(ctx context.Context, in *geecachepb.Request) (*geecachepb.Response, error) {
	groupName, key := in.GetGroup(), in.GetKey()
	resp := &geecachepb.Response{}

	log.Printf("[peanutcache_svr %s] Recv RPC Request - (%s)/(%s)", s.addr, groupName, key)

	if key == "" {
		return resp, fmt.Errorf("key required")
	}
	group := GetGroup(groupName)
	if group == nil {
		return resp, fmt.Errorf("group not found")
	}

	view, err := group.Get(key)
	if err != nil {
		return resp, err
	}

	resp.Value = view.ByteSlice()

	return resp, nil
}

// SetPeers 将各个远端主机IP配置到Server里
// 这样Server就可以Pick他们了
// 注意: 此操作是*覆写*操作！
// 注意: peersIP必须满足 x.x.x.x:port的格式
func (s *Server) SetPeers(peersAddrs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.consHash = consistenthash.New(defaultReplicas, nil)
	s.consHash.Register(peersAddrs...) //这里面的peers切片  就是 peer 的值为 http://192.168.1.100，这代表一个远程节点的地址。
	s.clients = make(map[string]*Client, len(peersAddrs))

	for _, peerAddr := range peersAddrs {
		if !validPeerAddr(peerAddr) {
			panic(fmt.Sprintf("[peer %s] invalid address format, it should be x.x.x.x:port", peerAddr))
		}
		service := fmt.Sprintf("geecache/%s", peerAddr)
		s.clients[peerAddr] = NewClient(service)
	}
}

// PickPeer 根据一致性哈希选举出key应存放在的cache
// return false 代表从本地获取cache,或者是没找到peerAddr == ""
func (s *Server) PickPeer(key string) (peer Fetcher, ok bool) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if peerAddr := s.consHash.Get(key); peerAddr != "" && peerAddr != s.addr {
		log.Printf("[cache %s] pick remote peer: %s\n", s.addr, peerAddr)
		return s.clients[peerAddr], true
	}
	return nil, false
}

// 测试Server是否实现了Picker接口
var _ PeerPicker = (*Server)(nil)

// Start 启动cache服务
func (s *Server) Start() error {
	s.mux.Lock()
	if s.status == true {
		s.mux.Unlock()
		return fmt.Errorf("server already started")
	}
	// -----------------启动服务----------------------
	// 1. 设置status为true 表示服务器已在运行
	// 2. 初始化stop channal,这用于通知registry stop keep alive
	// 3. 初始化tcp socket并开始监听
	// 4. 注册rpc服务至grpc 这样grpc收到request可以分发给server处理
	// 5. 将自己的服务名/Host地址注册至etcd 这样client可以通过etcd
	//    获取服务Host地址 从而进行通信。这样的好处是client只需知道服务名
	//    以及etcd的Host即可获取对应服务IP 无需写死至client代码中
	// ----------------------------------------------
	s.status = true
	s.stopSignal = make(chan error)
	port := strings.Split(s.addr, ":")[1]
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	geecachepb.RegisterGroupCacheServer(grpcServer, s)

	// 注册服务至etcd
	go func() {
		// Register never return unless stop singnal received
		err := registry.Register("geecache", s.addr, s.stopSignal)
		if err != nil {
			log.Fatalf(err.Error())
		}
		// Close channel
		close(s.stopSignal)
		// Close tcp listen
		err = lis.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
		log.Printf("[%s] Revoke service and close tcp socket ok.", s.addr)
	}()
	//log.Printf("[%s] register service ok\n", s.addr)
	s.mux.Unlock()

	if err := grpcServer.Serve(lis); s.status && err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}

// Stop 停止server运行 如果server没有运行 这将是一个no-op
func (s *Server) Stop() {
	s.mux.Lock() //第一个进去的拿到锁，将s.status设成false，后面的直接等待然后return
	if s.status == false {
		s.mux.Unlock()
		return
	}
	s.stopSignal <- nil // 发送停止keepalive信号
	s.status = false    // 设置server运行状态为stop
	s.clients = nil     // 清空一致性哈希信息 有助于垃圾回收
	s.consHash = nil
	s.mux.Unlock()
}
