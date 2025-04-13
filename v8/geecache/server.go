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
	defaultAddr     = "127.0.0.1:8081"
	defaultReplicas = 50
)

var (
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"192.168.172.129:2379"},
		DialTimeout: 5 * time.Second,
	}

	serviceLocker            sync.Mutex
	serviceEndpointKeyPrefix = "geecache"
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

	ctx    context.Context //添加上下文信息可以用于 ，程序退出 监听的停止
	cancel context.CancelFunc
}

// NewServer 创建cache的svr 若addr为空 则使用defaultAddr
func NewServer(addr string) (*Server, error) {
	if addr == "" {
		addr = defaultAddr
	}

	if !validPeerAddr(addr) {
		return nil, fmt.Errorf("invalid addr %s, it should be x.x.x.x:port", addr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:   addr,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Get 实现PeanutCache service的Get接口
// 把 http中 serverHttp中内容进行了改进
func (s *Server) Get(ctx context.Context, in *geecachepb.Request) (*geecachepb.Response, error) {
	groupName, key := in.GetGroup(), in.GetKey()
	resp := &geecachepb.Response{}

	log.Printf("[geecache_svr %s] Recv RPC Request - (%s)/(%s)", s.addr, groupName, key)

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
		s.clients[peerAddr] = NewClient(service, peerAddr)
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
	lis, err := net.Listen("tcp", ":"+port)
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

	// ✅ 然后再进行服务发现
	// 等待片刻，确保注册成功（最保险）
	time.Sleep(500 * time.Millisecond)
	go ServiceDiscovery(s)

	if err := grpcServer.Serve(lis); s.status && err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}

// ServiceDiscovery 服务发现
func ServiceDiscovery(s *Server) {
	cli, err := clientv3.New(defaultEtcdConfig)
	if err != nil {
		log.Printf("create etcd client failed: %v", err)
		return
	}

	go func() {
		//程序崩溃可以恢复过来
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ServiceDiscovery panic: %v", r)
			}
		}()

		//ctx := context.Background()
		serviceKey := fmt.Sprintf("%s/", serviceEndpointKeyPrefix)
		// 获取当前所有服务入口
		getRes, err := cli.Get(s.ctx, serviceKey, clientv3.WithPrefix())
		if err != nil {
			log.Printf("initial etcd get failed: %v", err)
			return
		}

		//在main函数中注册了 哈希环，到开启发现etcd服务 之间可能有新的节点上线 进行处理
		for _, v := range getRes.Kvs {
			// 💡注册 client & 哈希环（避免重复）
			serviceLocker.Lock()
			if _, ok := s.clients[string(v.Value)]; !ok {
				s.clients[string(v.Value)] = NewClient(string(v.Key), string(v.Value))
				s.consHash.Register(string(v.Value))
			}
			serviceLocker.Unlock()
		}

		fmt.Printf("[service_endpoint_change] [%s] service %s get endpoints success\n", serviceEndpointKeyPrefix, serviceEndpointKeyPrefix)
		ch := cli.Watch(s.ctx, serviceKey, clientv3.WithPrefix(), clientv3.WithPrevKV())
		for {
			select {
			case <-s.ctx.Done():
				log.Println("Service discovery exited due to context cancellation.")
				return
			case v, ok := <-ch:
				if !ok {
					log.Println("Watch channel closed, attempting to re-watch...")
					time.Sleep(time.Second)
					ch = cli.Watch(s.ctx, serviceKey, clientv3.WithPrefix(), clientv3.WithPrevKV())
					continue
				}
				for _, v := range v.Events {
					key := string(v.Kv.Key) //这里的key应该是前缀加addr:port
					endpoint := string(v.Kv.Value)
					switch v.Type {
					// PUT，新增或替换   , todo 先考虑 新增节点的情况
					case clientv3.EventTypePut:
						//增加对应的客户端 增加对应的哈希环节点
						serviceLocker.Lock()
						if old := s.clients[endpoint]; old != nil {
							// todo 可能只是元数据更新，不重复注册 client
						} else {
							s.clients[endpoint] = NewClient(key, endpoint)
							s.consHash.Register(endpoint) //哈希环上要注册
						}
						serviceLocker.Unlock()

						//todo  应该还要发生哈希环数据的迁移，确保一致性
						// todo: 需要对临界资源哈希环进行更新，同时还要迁移哈希环上的数据，要确保数据的同步，如果迁移过程中又有新的访问，怎么进行同步呢访问
					// DELETE
					case clientv3.EventTypeDelete:
						//删除节点
						serviceLocker.Lock()
						if _, ok := s.clients[endpoint]; ok {
							delete(s.clients, endpoint)
							s.consHash.Destroy(endpoint)
						}
						serviceLocker.Unlock()
						////todo: 删除也需要更新hash 环，同时还要控制同步问题，删除过程中 一个请求过来了咋办？
					}
				}

			}
		}

	}()
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

// 从etcd获取peer地址
func (s *Server) GetPeersFromEtcd() ([]string, error) {
	cli, err := clientv3.New(defaultEtcdConfig)
	if err != nil {
		return nil, fmt.Errorf("create etcd client failed: %v", err)
	}
	defer cli.Close()

	serviceKey := fmt.Sprintf("%s/", serviceEndpointKeyPrefix)
	getRes, err := cli.Get(s.ctx, serviceKey, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("initial etcd get failed: %v", err)
	}

	var endpoints []string
	for _, v := range getRes.Kvs {
		endpoints = append(endpoints, string(v.Value))
	}
	return endpoints, nil
}
