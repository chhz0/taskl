// core/broker.go
package core

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/chhz0/taskl/storage"
	"github.com/chhz0/taskl/transport"
	"github.com/chhz0/taskl/types"
)

type Broker interface {
	Enqueue(ctx context.Context, task *types.Task) error
	Consume(ctx context.Context) <-chan *types.Task
	UpdateTaskStatus(ctx context.Context, taskID string, status types.TaskStatus) error
	Close() error
	Storage() storage.Storage

	DistributeTask(ctx context.Context, task *types.Task) error
}

type HybridBroker struct {
	memQueue    chan *types.Task
	persistChan chan *types.Task
	storage     storage.Storage
	memSize     int
	mu          sync.RWMutex
	closed      bool

	transport   transport.Transport
	clusterMode bool
}

// NewHybridBroker 构造函数（补充存储参数校验）
func NewHybridBroker(storage storage.Storage, memSize int) (*HybridBroker, error) {
	if storage == nil {
		return nil, errors.New("storage cannot be nil")
	}

	return &HybridBroker{
		memQueue:    make(chan *types.Task, memSize),
		persistChan: make(chan *types.Task, memSize/2),
		storage:     storage,
		memSize:     memSize,
	}, nil
}

func NewHybridBrokerWithCluster(storage storage.Storage, transport transport.Transport, memSize int) (*HybridBroker, error) {
	hb, err := NewHybridBroker(storage, memSize)
	if err != nil {
		return nil, err
	}

	hb.transport = transport
	hb.clusterMode = true

	// 启动集群任务消费
	go hb.consumeClusterTasks()
	return hb, nil
}

// 消费集群任务
func (hb *HybridBroker) consumeClusterTasks() {
	ch, err := hb.transport.SubscribeTasks(context.Background())
	if err != nil {
		return
	}

	for task := range ch {
		select {
		case hb.memQueue <- task:
		default:
			// 本地队列满时直接持久化
			_ = hb.storage.SaveTask(context.Background(), task)
		}
	}
}
func (hb *HybridBroker) Enqueue(ctx context.Context, task *types.Task) error {
	if task == nil {
		return errors.New("cannot enqueue nil task")
	}

	select {
	case hb.memQueue <- task:
		return nil
	default:
		// 内存队列满时持久化
		hb.mu.Lock()
		defer hb.mu.Unlock()

		// 设置任务初始状态
		task.Status = types.StatusPending
		task.CreatedAt = time.Now().UTC()

		if err := hb.storage.SaveTask(ctx, task); err != nil {
			return err
		}

		select {
		case hb.persistChan <- task:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return errors.New("persistent queue is full")
		}
	}
}

// Consume方法
func (hb *HybridBroker) Consume(ctx context.Context) <-chan *types.Task {
	out := make(chan *types.Task, hb.memSize)

	go func() {
		defer close(out)

		// 优先消费内存队列
		for task := range hb.memQueue {
			select {
			case out <- task:
			case <-ctx.Done():
				return
			}
		}

		// 消费持久化队列
		for task := range hb.persistChan {
			select {
			case out <- task:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

// syncLoop
func (hb *HybridBroker) syncLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if hb.closed {
			return
		}

		// 从存储加载待处理任务
		tasks, err := hb.storage.GetPendingTasks(context.Background(), hb.memSize)
		if err != nil {
			continue
		}

		hb.mu.Lock()
		for _, task := range tasks {
			select {
			case hb.memQueue <- task:
				// 成功写入内存队列后更新状态
				_ = hb.storage.UpdateTaskStatus(context.Background(), task.ID, types.StatusProcessing)
			default:
				// 内存队列满时跳过
			}
		}
		hb.mu.Unlock()
	}
}

func (hb *HybridBroker) UpdateTaskStatus(ctx context.Context, taskID string, status types.TaskStatus) error {
	return hb.storage.UpdateTaskStatus(ctx, taskID, status)
}

// 关闭方法补充存储关闭
func (hb *HybridBroker) Close() error {
	hb.closed = true
	close(hb.memQueue)
	close(hb.persistChan)
	return hb.storage.Close()
}

func (hb *HybridBroker) Storage() storage.Storage {
	return hb.storage
}

// 分布式任务分发
func (hb *HybridBroker) DistributeTask(ctx context.Context, task *types.Task) error {
	if hb.transport == nil {
		return errors.New("cluster mode not enabled")
	}
	return hb.transport.PublishTask(ctx, task)
}
