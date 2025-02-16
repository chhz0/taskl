// storage/redis_store.go
package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/chhz0/taskl/types"
	"github.com/go-redis/redis/v8"
)

type RedisStorage struct {
	client *redis.Client
	prefix string
}

func NewRedisStorage(addr, password string, db int) *RedisStorage {
	return &RedisStorage{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
		prefix: "async_task:",
	}
}

func (s *RedisStorage) key(id string) string {
	return s.prefix + id
}

func (s *RedisStorage) SaveTask(ctx context.Context, task *types.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.key(task.ID), data, 24*time.Hour).Err()
}

func (s *RedisStorage) GetPendingTasks(ctx context.Context, limit int) ([]*types.Task, error) {
	// 需要配合Redis Stream/ZSET实现（此处简化）
	keys, err := s.client.Keys(ctx, s.prefix+"*").Result()
	if err != nil {
		return nil, err
	}

	var tasks []*types.Task
	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}

		var task types.Task
		if err := json.Unmarshal(data, &task); err != nil {
			continue
		}

		if task.Status == types.StatusPending || task.Status == types.StatusRetry {
			tasks = append(tasks, &task)
			if len(tasks) >= limit {
				break
			}
		}
	}
	return tasks, nil
}

func (s *RedisStorage) UpdateTaskStatus(ctx context.Context, taskID string, status types.TaskStatus) error {
	data, err := s.client.Get(ctx, s.key(taskID)).Bytes()
	if err != nil {
		return err
	}

	var task types.Task
	if err := json.Unmarshal(data, &task); err != nil {
		return err
	}

	task.Status = types.TaskStatus(status)
	newData, err := json.Marshal(task)
	if err != nil {
		return err
	}

	return s.client.Set(ctx, s.key(taskID), newData, 0).Err()
}

func (s *RedisStorage) Close() error {
	return s.client.Close()
}
