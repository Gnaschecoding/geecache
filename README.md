------

# GeeCache v8 🚀

一个使用 Go 实现的 **分布式缓存系统**，支持集群部署、服务发现、gRPC 通信、一致性哈希分布、LRU 缓存淘汰和防缓存击穿机制，适用于构建高性能、高可用的缓存服务。

------

## ✨ 项目亮点

- ✅ 支持集群部署，节点可横向扩展
- 🔍 基于 etcd 实现服务注册与发现
- 🧩 基于一致性哈希保证数据分布均衡与容错性
- 🧠 支持 LRU 缓存淘汰策略
- 🛡 使用 singleflight 机制防止缓存击穿
- 📦 gRPC 实现节点间通信，性能高效
- ⏱ 使用 etcd 租约机制管理节点生命周期，失联节点自动清除

------

## 📦 项目结构

```
geecache/
│
├── geecache/            # 核心缓存逻辑（Group、ByteView、LRU 等）
├── consistenthash/      # 一致性哈希实现
├── geecachepb/          # gRPC 协议文件（protobuf 生成）
├── main/                # 启动入口
├── etcd/                # etcd 服务注册、发现与租约管理
├── v8/                  # 当前版本实现
└── README.md            # 项目说明文档
```

------

## 🧠 技术实现

### 缓存核心逻辑

- 缓存分组 `Group`：支持命名空间、支持从主存加载数据
- 缓存值结构 `ByteView`：只读封装，支持共享
- 使用 `sync.Mutex` 实现线程安全
- 使用 `lru.Cache` 实现最近最少使用淘汰策略

### 分布式通信与服务发现

- 使用 **etcd** 注册当前节点，分配租约防止僵尸节点
- 使用 **gRPC** 进行节点间请求转发（peer fetcher）
- 自动发现服务节点并更新 peers 列表

### 一致性哈希分布

- 实现虚拟节点技术提升分布均衡性
- 支持动态添加/删除节点
- 保证最小数据迁移

### 缓存击穿保护

- 使用 Go 的 `singleflight.Group` 机制
- 保证相同 key 的请求只发起一次真实访问，其他阻塞等待结果

------

## 🚀 快速开始

### 启动 etcd

在你的 Linux 虚拟机中启动 etcd（以单节点为例）：

```bash
etcd
```

确保监听地址为 `192.168.172.129:2379`。

### 启动节点

进入 `v8/` 目录，启动多个缓存节点：

```bash
go run main.go -port=8001
go run main.go -port=8002
go run main.go -port=8003
```

默认注册到 etcd，并开启服务发现。

### 启动 API 网关

```bash
go run main.go -port=9999 -api=true
```

访问方式：

```
http://localhost:9999/api?key=Tom
```

------

## 🛠 示例输出

```json
{
  "key": "Tom",
  "value": "😈💣🔥"
}
```

------

## 📚 依赖组件

- Go 1.18+
- [etcd](https://github.com/etcd-io/etcd)
- gRPC
- Protocol Buffers (`protoc`)
- Google `singleflight` 库

------

## 💡 可拓展方向

- 热 Key 统计和预加载
- 支持 TTL 过期机制
- 多级缓存支持（本地 + Redis）
- 数据持久化与容灾恢复
- 权重一致性哈希 / 节点负载感知

------

## 💡 需要改进的地方

- 节点上线的时候并没有实现数据的迁移，节点上线之后需要到哈希环他后面的节点里面的数据迁移到自己
- 节点下线的时候没有进行数据迁移，如果热点数据下线了很危险，
- 改进LRU cache，使其具备TTL的能力，以及改进锁的粒度，提高并发度。
- 增加FIFO、LFU 、ARC等底层淘汰算法
- 这里面哈希环 每个节点的分布是均等的，没有考虑到每个节点的性能不同，可以增加每个节点不同的权重
- 实现热点互备来避免 hot key 频繁请求⽹络影响性能（groupcache中提出的优化）
- 还有就是保留了 节点的配置信息更改如何处理哈希环，本项目只处理上线的情况

## 📄 License

MIT License © Gnaschecoding

