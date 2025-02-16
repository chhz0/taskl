// core/registry.go
package core

import "sync"

type TaskRegistry struct {
	handlers map[string]TaskHandler
	mu       sync.RWMutex
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		handlers: make(map[string]TaskHandler),
	}
}

func (r *TaskRegistry) Register(taskType string, handler TaskHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[taskType] = handler
}

func (r *TaskRegistry) GetHandler(taskType string) (TaskHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[taskType]
	return h, ok
}
