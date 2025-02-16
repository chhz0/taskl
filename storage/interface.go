package storage

import (
	"context"
	"errors"

	"github.com/chhz0/taskl/types"
)

var (
	ErrTaskNotFound = errors.New("task not found")
)

type Storage interface {
	SaveTask(ctx context.Context, task *types.Task) error
	GetPendingTasks(ctx context.Context, limit int) ([]*types.Task, error)
	UpdateTaskStatus(ctx context.Context, taskID string, status types.TaskStatus) error
	Close() error
}
