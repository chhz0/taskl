// storage/memory_store.go
package storage

import (
	"context"
	"sync"
	"time"

	"github.com/chhz0/taskl/types"
	"github.com/google/uuid"
)

type MemoryStorage struct {
	tasks map[string]*types.Task
	mu    sync.RWMutex
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		tasks: make(map[string]*types.Task),
	}
}

func (s *MemoryStorage) SaveTask(ctx context.Context, task *types.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if task.ID == "" {
		task.ID = generateID()
	}
	task.CreatedAt = time.Now()
	s.tasks[task.ID] = task
	return nil
}

func (s *MemoryStorage) GetPendingTasks(ctx context.Context, limit int) ([]*types.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*types.Task
	for _, t := range s.tasks {
		if t.Status == types.StatusPending || t.Status == types.StatusRetry {
			result = append(result, t)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (s *MemoryStorage) UpdateTaskStatus(ctx context.Context, taskID string, status types.TaskStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return ErrTaskNotFound
	}
	task.Status = types.TaskStatus(status)
	return nil
}

func (s *MemoryStorage) Close() error {
	return nil // 无需关闭操作
}

func generateID() string {
	return uuid.New().String() // 需要引入uuid库
}
