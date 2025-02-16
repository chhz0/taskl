// retry/retry.go
package retry

import (
	"time"

	"github.com/chhz0/taskl/types"
)

type RetryManager struct {
	Policy RetryPolicy
}

func NewRetryManager(policy RetryPolicy) *RetryManager {
	return &RetryManager{Policy: policy}
}

func (rm *RetryManager) ShouldRetry(task *types.Task) (time.Duration, bool) {
	if task.Retries >= task.MaxRetry {
		return 0, false
	}
	return rm.Policy.NextRetry(task.Retries)
}

func (rm *RetryManager) ApplyRetry(task *types.Task) {
	delay, shouldRetry := rm.ShouldRetry(task)
	if shouldRetry {
		task.Retries++
		task.NextRetry = time.Now().Add(delay)
		task.Status = types.StatusRetry
	} else {
		task.Status = types.StatusFailed
	}
}
