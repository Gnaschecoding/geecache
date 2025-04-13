package geecache

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"time"
	"v8/geecache/geecachepb"
)

type Client struct {
	name string // 服务名称 geecache/ip:addr
	addr string //就是记录 ip 加上端口的 形式  ip:port
}

func NewClient(name, addr string) *Client {
	return &Client{name: name, addr: addr}
}

// 判断是否实现了 PeerGetter
var _ Fetcher = (*Client)(nil)

func (c *Client) Fetch(in *geecachepb.Request, out *geecachepb.Response) error {
	// 创建一个etcd client
	//log.Println("1111111111111111")
	//cli, err := clientv3.New(defaultEtcdConfig)
	//if err != nil {
	//	return err
	//}
	//defer cli.Close()

	// 发现服务 取得与服务的连接
	conn, err := grpc.NewClient(
		c.addr,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	grpcClient := geecachepb.NewGroupCacheClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := grpcClient.Get(ctx, in)

	if err != nil {
		return fmt.Errorf("could not get %s/%s from peer %s,err is %s", in.GetGroup(), in.GetKey(), c.name, err.Error())
	}
	*out = *resp // ✅ 把 resp 的值 copy 到外部传入的 out 指针
	resp = nil   //置空不再使用的变量,可以让 GC 更早地知道 resp 不再被用到了
	return nil
}
