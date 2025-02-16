# taskl (demo, no test)
A lightweight asynchronous task processing framework developed in Go language.

轻量级的go语言异步处理框架

## Framework
```
+-------------------+       +-------------------+       +-------------------+
|                   |       |                   |       |                   |
|   Task Producer   | ----> |   HybridBroker    | ----> |   Task Consumer   |
|                   |       |                   |       |                   |
+-------------------+       +-------------------+       +-------------------+
                                |       ^                        |
                                |       |                        |
                                v       |                        v
                        +-------------------+       +-------------------+
                        |                   |       |                   |
                        |   Storage Layer   |       |   Transport Layer |
                        |                   |       |                   |
                        +-------------------+       +-------------------+
```

## 核心

- 任务管理模块 (types.Task)
  - 定义了任务的结构，包括任务ID、状态、创建时间等字段。
  - 任务状态包括Pending、Processing等。
- 存储模块 (storage.Storage)
  - 提供任务的持久化存储功能，包括任务的保存、状态更新和查询。
  - 支持从存储中加载待处理任务。
- 传输模块 (transport.Transport)
  - 用于在集群模式下分发和订阅任务。
  - 提供了PublishTask和SubscribeTasks接口。
- Broker模块 (HybridBroker)
  - 核心任务队列管理模块，负责任务的入队、消费、状态更新和集群任务分发。
  - 使用内存队列和持久化队列结合的方式处理任务。
  - 支持集群模式下的任务分发和消费。
