// core/worker_pool.go
package core

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/chhz0/taskl/types"
)

type TaskHandler func(ctx context.Context, task *types.Task) error

type WorkerPool struct {
	broker     Broker
	handler    TaskHandler
	maxWorkers int
	wg         sync.WaitGroup
	cancel     context.CancelFunc
	mu         sync.Mutex
	running    bool
}

func NewWorkerPool(broker Broker, handler TaskHandler, maxWorkers int) *WorkerPool {
	return &WorkerPool{
		broker:     broker,
		handler:    handler,
		maxWorkers: maxWorkers,
	}
}

func (wp *WorkerPool) Start() {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if wp.running {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	wp.cancel = cancel
	wp.running = true

	for i := 0; i < wp.maxWorkers; i++ {
		wp.wg.Add(1)
		go wp.runWorker(ctx)
	}
}

func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if !wp.running {
		return
	}

	wp.cancel()
	wp.wg.Wait()
	wp.broker.Close()
	wp.running = false
}

func (wp *WorkerPool) runWorker(ctx context.Context) {
	defer wp.wg.Done()

	for task := range wp.broker.Consume(ctx) {
		select {
		case <-ctx.Done():
			return
		default:
			wp.processTask(ctx, task)
		}
	}
}

func (wp *WorkerPool) processTask(ctx context.Context, task *types.Task) {
	// 更新任务状态为处理中
	_ = wp.broker.Storage().UpdateTaskStatus(ctx, task.ID, types.StatusProcessing)

	// 执行任务
	err := wp.executeWithRetry(ctx, task)

	// 更新最终状态
	finalStatus := types.StatusSuccess
	if err != nil {
		finalStatus = types.StatusFailed
	}
	_ = wp.broker.Storage().UpdateTaskStatus(ctx, task.ID, finalStatus)
}

func (wp *WorkerPool) executeWithRetry(ctx context.Context, task *types.Task) error {
	for attempt := 0; attempt <= task.MaxRetry; attempt++ {
		errCh := make(chan error, 1)
		panicCh := make(chan interface{}, 1)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
			}()
			errCh <- wp.handler(ctx, task)
		}()

		select {
		case err := <-errCh:
			if err == nil {
				return nil
			}
			if !shouldRetry(err) {
				return err
			}
		case <-panicCh:
			// 记录panic信息
			return errors.New("task panicked")
		case <-time.After(task.Timeout):
			_ = errors.New("task timed out")
		}

		// 准备重试
		task.Retries++
		task.NextRetry = time.Now().Add(backoff(attempt))
		_ = wp.broker.Storage().UpdateTaskStatus(ctx, task.ID, types.StatusRetry)
	}

	return errors.New("max retries exceeded")
}

// 指数退避策略
func backoff(attempt int) time.Duration {
	return time.Duration(attempt^2) * time.Second
}

func shouldRetry(err error) bool {
	// 可自定义重试条件
	return true
}
