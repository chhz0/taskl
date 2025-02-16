// retry/policy.go
package retry

import (
	"math"
	"time"
)

// 重试策略接口
type RetryPolicy interface {
	NextRetry(attempt int) (time.Duration, bool)
}

// 指数退避策略
type ExponentialBackoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	MaxAttempts  int
}

func (p *ExponentialBackoff) NextRetry(attempt int) (time.Duration, bool) {
	if attempt >= p.MaxAttempts {
		return 0, false
	}

	delay := p.InitialDelay * time.Duration(math.Pow(2, float64(attempt)))
	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return delay, true
}

// 固定间隔策略
type FixedInterval struct {
	Interval    time.Duration
	MaxAttempts int
}

func (p *FixedInterval) NextRetry(attempt int) (time.Duration, bool) {
	if attempt >= p.MaxAttempts {
		return 0, false
	}
	return p.Interval, true
}

// 组合策略
type CompositePolicy struct {
	Policies []RetryPolicy
}

func (p *CompositePolicy) NextRetry(attempt int) (time.Duration, bool) {
	for _, policy := range p.Policies {
		if delay, ok := policy.NextRetry(attempt); ok {
			return delay, true
		}
	}
	return 0, false
}
