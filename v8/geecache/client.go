package geecache

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
	"v8/geecache/geecachepb"
	"v8/geecache/registry"
)

type Client struct {
	name string // 服务名称 geecache/ip:addr
}

func NewClient(name string) *Client {
	return &Client{name}
}

// 判断是否实现了 PeerGetter
var _ Fetcher = (*Client)(nil)

func (c *Client) Fetch(in *geecachepb.Request, out *geecachepb.Response) error {
	// 创建一个etcd client
	cli, err := clientv3.New(defaultEtcdConfig)
	if err != nil {
		return err
	}
	defer cli.Close()

	// 发现服务 取得与服务的连接
	conn, err := registry.EtcdDial(cli, c.name)
	if err != nil {
		return err
	}
	defer conn.Close()

	grpcClient := geecachepb.NewGroupCacheClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err = grpcClient.Get(ctx, in)
	if err != nil {
		return fmt.Errorf("could not get %s/%s from peer %s,err is %s", in.GetGroup(), in.GetKey(), c.name, err.Error())
	}

	return nil
}
