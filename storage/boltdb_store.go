// storage/boltdb_store.go
package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/chhz0/taskl/types"
	bolt "go.etcd.io/bbolt"
)

var (
	taskBucket = []byte("tasks")
)

type BoltStorage struct {
	db *bolt.DB
}

func NewBoltStorage(path string) (*BoltStorage, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	// 初始化Bucket
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(taskBucket)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &BoltStorage{db: db}, nil
}

func (s *BoltStorage) SaveTask(ctx context.Context, task *types.Task) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(taskBucket)
		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		return b.Put([]byte(task.ID), data)
	})
}

func (s *BoltStorage) GetPendingTasks(ctx context.Context, limit int) ([]*types.Task, error) {
	var tasks []*types.Task
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(taskBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var task types.Task
			if err := json.Unmarshal(v, &task); err != nil {
				continue // 跳过无效数据
			}

			if task.Status == types.StatusPending || task.Status == types.StatusRetry {
				tasks = append(tasks, &task)
				if len(tasks) >= limit {
					break
				}
			}
		}
		return nil
	})
	return tasks, err
}

func (s *BoltStorage) UpdateTaskStatus(ctx context.Context, taskID string, status types.TaskStatus) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(taskBucket)
		data := b.Get([]byte(taskID))
		if data == nil {
			return ErrTaskNotFound
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

		return b.Put([]byte(taskID), newData)
	})
}

func (s *BoltStorage) Close() error {
	return s.db.Close()
}
