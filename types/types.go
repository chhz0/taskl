// types/types.go
package types

import (
	"encoding/json"
	"time"
)

// 任务状态枚举
type TaskStatus int

const (
	StatusPending TaskStatus = iota
	StatusProcessing
	StatusSuccess
	StatusFailed
	StatusRetry
)

// 任务元数据（原core.Task迁移至此）
type Task struct {
	ID        string
	Type      string
	Payload   []byte
	Status    TaskStatus
	Retries   int
	MaxRetry  int
	NextRetry time.Time
	CreatedAt time.Time
	Timeout   time.Duration
}

// 序列化任务
func (t *Task) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

// 反序列化任务
func DeserializeTask(data []byte) (*Task, error) {
	var task Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, err
	}
	return &task, nil
}
