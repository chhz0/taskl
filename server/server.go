// server/server.go
package server

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chhz0/taskl/core"
	"github.com/chhz0/taskl/storage"
	"github.com/chhz0/taskl/types"
)

type Server struct {
	broker     core.Broker
	workerPool *core.WorkerPool
	httpServer *http.Server
}

type Config struct {
	HTTPAddr       string
	WorkerCount    int
	QueueSize      int
	StorageBackend storage.Storage
}

func NewServer(cfg Config) (*Server, error) {
	broker, err := core.NewHybridBroker(cfg.StorageBackend, cfg.QueueSize)
	if err != nil {
		return nil, err
	}

	workerPool := core.NewWorkerPool(
		broker,
		defaultHandler, // 需要注册实际处理器
		cfg.WorkerCount,
	)

	return &Server{
		broker:     broker,
		workerPool: workerPool,
		httpServer: &http.Server{
			Addr:    cfg.HTTPAddr,
			Handler: newRouter(broker),
		},
	}, nil
}

func (s *Server) Start() error {
	// 优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动Worker池
	s.workerPool.Start()

	// 启动HTTP服务器
	serverErr := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	// 处理信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	select {
	case err := <-serverErr:
		return err
	case <-quit:
		// 优雅关闭
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 30*time.Second)
		defer shutdownCancel()

		s.workerPool.Stop()
		return s.httpServer.Shutdown(shutdownCtx)
	}
}

func newRouter(b core.Broker) *http.ServeMux {
	mux := http.NewServeMux()

	// 管理API
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		// 暴露监控指标
	})

	mux.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
		// 任务提交API
	})

	return mux
}

// 默认处理器（需替换为实际注册的处理器）
func defaultHandler(ctx context.Context, task *types.Task) error {
	return nil
}
