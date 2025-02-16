package transport

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/chhz0/taskl/types"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// Transport 接口定义
type Transport interface {
	PublishTask(ctx context.Context, task *types.Task) error
	SubscribeTasks(ctx context.Context) (<-chan *types.Task, error)
	RegisterNode(nodeID string) error
	DiscoverNodes() ([]string, error)
	Close() error
}

// RedisPubSub 实现
type RedisPubSub struct {
	client        *redis.Client
	ctx           context.Context
	cancel        context.CancelFunc
	nodeID        string
	subscription  *redis.PubSub
	nodes         map[string]time.Time // 节点ID:最后心跳时间
	nodesMutex    sync.RWMutex
	channelPrefix string
}

var (
	TaskChannel       = "async_tasks"
	NodeChannel       = "node_heartbeats"
	DiscoveryKey      = "async_task_nodes"
	HeartbeatInterval = 5 * time.Second
	NodeTimeout       = 15 * time.Second
)

func NewRedisTransport(addr, password string, db int) (*RedisPubSub, error) {
	ctx, cancel := context.WithCancel(context.Background())

	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     100,
		MinIdleConns: 10,
		IdleTimeout:  5 * time.Minute,
	})

	// 验证连接
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, err
	}

	rs := &RedisPubSub{
		client:        client,
		ctx:           ctx,
		cancel:        cancel,
		nodeID:        uuid.New().String(),
		channelPrefix: "async_task_",
		nodes:         make(map[string]time.Time),
	}

	// 启动后台协程
	go rs.heartbeatLoop()
	go rs.nodeDiscoveryLoop()

	return rs, nil
}

// 发布任务到集群
func (rs *RedisPubSub) PublishTask(ctx context.Context, task *types.Task) error {
	taskData, err := json.Marshal(task)
	if err != nil {
		return err
	}

	return rs.client.Publish(ctx, rs.channelPrefix+TaskChannel, taskData).Err()
}

// 在RedisPubSub中添加批量接口
func (rs *RedisPubSub) PublishBatchTasks(ctx context.Context, tasks []*types.Task) error {
	pipe := rs.client.Pipeline()
	for _, task := range tasks {
		data, _ := json.Marshal(task)
		pipe.Publish(ctx, rs.channelPrefix+TaskChannel, data)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// 订阅任务流
func (rs *RedisPubSub) SubscribeTasks(ctx context.Context) (<-chan *types.Task, error) {
	ch := make(chan *types.Task, 100)
	pubsub := rs.client.Subscribe(ctx, rs.channelPrefix+TaskChannel)

	go func() {
		defer close(ch)
		for msg := range pubsub.Channel() {
			var task types.Task
			if err := json.Unmarshal([]byte(msg.Payload), &task); err == nil {
				select {
				case ch <- &task:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}

// 节点注册与发现
func (rs *RedisPubSub) RegisterNode(nodeID string) error {
	// 使用有序集合维护节点列表
	return rs.client.ZAdd(rs.ctx, DiscoveryKey, &redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: nodeID,
	}).Err()
}

func (rs *RedisPubSub) DiscoverNodes() ([]string, error) {
	// 获取最近活跃的节点
	nodes, err := rs.client.ZRangeByScore(rs.ctx, DiscoveryKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(time.Now().Add(-NodeTimeout).Unix(), 10),
		Max: "+inf",
	}).Result()

	if err != nil {
		return nil, err
	}

	// 更新本地节点缓存
	rs.nodesMutex.Lock()
	defer rs.nodesMutex.Unlock()
	for _, node := range nodes {
		if _, exists := rs.nodes[node]; !exists {
			rs.nodes[node] = time.Now()
		}
	}
	return nodes, nil
}

// 关闭连接
func (rs *RedisPubSub) Close() error {
	rs.cancel()
	rs.client.Close()
	return nil
}

// 后台任务
// 心跳循环
func (rs *RedisPubSub) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 更新有序集合中的节点时间戳
			rs.client.ZAdd(rs.ctx, DiscoveryKey, &redis.Z{
				Score:  float64(time.Now().Unix()),
				Member: rs.nodeID,
			})

			// 广播心跳
			rs.client.Publish(rs.ctx, rs.channelPrefix+NodeChannel, rs.nodeID)

		case <-rs.ctx.Done():
			return
		}
	}
}

// 节点发现循环
func (rs *RedisPubSub) nodeDiscoveryLoop() {
	pubsub := rs.client.Subscribe(rs.ctx, rs.channelPrefix+NodeChannel)
	defer pubsub.Close()

	for {
		select {
		case msg := <-pubsub.Channel():
			nodeID := msg.Payload
			rs.nodesMutex.Lock()
			rs.nodes[nodeID] = time.Now()
			rs.nodesMutex.Unlock()

		case <-rs.ctx.Done():
			return
		}
	}
}

// 负载均衡策略
func (rs *RedisPubSub) SelectNode() (string, error) {
	rs.nodesMutex.RLock()
	defer rs.nodesMutex.RUnlock()

	// 简单返回第一个可用节点
	for nodeID := range rs.nodes {
		return nodeID, nil
	}
	return "", errors.New("no available nodes")
}
