// middleware/middleware.go
package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/chhz0/taskl/types"
)

type Handler func(ctx context.Context, task *types.Task) error
type Middleware func(next Handler) Handler

// 中间件链
func Chain(middlewares ...Middleware) Middleware {
	return func(final Handler) Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

// 超时中间件
func Timeout(d time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, task *types.Task) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, task)
		}
	}
}

// 日志中间件
func Logger() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, task *types.Task) error {
			start := time.Now()
			fmt.Printf("Task [%s] started\n", task.ID)

			err := next(ctx, task)

			duration := time.Since(start)
			if err != nil {
				fmt.Printf("Task [%s] failed after %s: %v\n", task.ID, duration, err)
			} else {
				fmt.Printf("Task [%s] completed in %s\n", task.ID, duration)
			}
			return err
		}
	}
}

// 指标收集中间件
func Metrics() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, task *types.Task) error {
			start := time.Now()
			err := next(ctx, task)
			recordMetrics(task.Type, time.Since(start), err)
			return err
		}
	}
}

func recordMetrics(taskType string, duration time.Duration, err error) {
	// todo: 实际集成到Prometheus等监控系统
}
