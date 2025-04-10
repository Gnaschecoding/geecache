package registry

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"log"
	"time"
)

var (
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
)

// etcdAdd 在租赁模式添加一对kv至etcd
func etcdAdd(c *clientv3.Client, lid clientv3.LeaseID, service string, addr string) error {
	//创建一个用于管理指定服务端点信息的管理器实例
	em, err := endpoints.NewManager(c, service)
	if err != nil {
		return err
	}
	return em.AddEndpoint(
		c.Ctx(),
		service+"/"+addr,
		endpoints.Endpoint{Addr: addr},
		clientv3.WithLease(lid),
	)
}

// Register 注册一个服务至etcd
// 注意 Register将不会return 如果没有error的话
func Register(service string, addr string, stop chan error) error {
	// 创建一个etcd client
	cli, err := clientv3.New(defaultEtcdConfig)
	if err != nil {
		return fmt.Errorf("create etcd client failed: %v", err)
	}
	defer cli.Close()
	// 创建一个租约 配置5秒过期
	leaseResp, err := cli.Grant(context.Background(), 5)
	if err != nil {
		return fmt.Errorf("create lease failed: %v", err)
	}
	leaseId := leaseResp.ID

	// 注册服务
	err = etcdAdd(cli, leaseId, service, addr)
	if err != nil {
		return fmt.Errorf("add etcd record failed: %v", err)
	}
	// 设置服务心跳检测,续约
	ch, err := cli.KeepAlive(context.Background(), leaseId)
	if err != nil {
		return fmt.Errorf("keep alive etcd failed: %v", err)
	}
	log.Printf("[%s] register service ok\n", addr)
	for {
		select {
		case err := <-stop:
			if err != nil {
				log.Println(err)
			}
			return err
			//etcd这个服务关闭了
		case <-cli.Ctx().Done():
			log.Println("service closed")
			return nil

			//通过ok来判断channel是否关闭，如果关闭不就说明下线了，续约失败了
		case _, ok := <-ch:
			// 监听租约
			if !ok {
				log.Println("keep alive channel closed")
				_, err := cli.Revoke(context.Background(), leaseId) //撤销指定的租约
				return err
			}
		}
	}
}
