package storage

import (
	"context"
	"database/sql"
	"time"

	"github.com/chhz0/taskl/types"
	_ "modernc.org/sqlite" // 纯Go SQLite驱动
)

type SQLiteStorage struct {
	db *sql.DB
}

func NewSQLiteStorage(path string) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}

	// 创建表结构
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS tasks (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			payload BLOB,
			status INTEGER NOT NULL,
			retries INTEGER DEFAULT 0,
			max_retry INTEGER DEFAULT 3,
			next_retry DATETIME,
			created_at DATETIME NOT NULL,
			timeout INTEGER
		);
		CREATE INDEX idx_status ON tasks(status);
	`)
	if err != nil {
		return nil, err
	}

	return &SQLiteStorage{db: db}, nil
}

func (s *SQLiteStorage) SaveTask(ctx context.Context, task *types.Task) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tasks
		(id, type, payload, status, retries, max_retry, next_retry, created_at, timeout)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.ID, task.Type, task.Payload, task.Status, task.Retries,
		task.MaxRetry, task.NextRetry, task.CreatedAt, int64(task.Timeout),
	)
	return err
}

func (s *SQLiteStorage) GetPendingTasks(ctx context.Context, limit int) ([]*types.Task, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, type, payload, status, retries, max_retry, next_retry, created_at, timeout
		FROM tasks
		WHERE status IN (?, ?)
		ORDER BY created_at ASC
		LIMIT ?`,
		types.StatusPending, types.StatusRetry, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*types.Task
	for rows.Next() {
		var t types.Task
		var timeout int64
		err := rows.Scan(
			&t.ID, &t.Type, &t.Payload, &t.Status, &t.Retries,
			&t.MaxRetry, &t.NextRetry, &t.CreatedAt, &timeout,
		)
		if err != nil {
			return nil, err
		}
		t.Timeout = time.Duration(timeout)
		tasks = append(tasks, &t)
	}
	return tasks, nil
}

func (s *SQLiteStorage) UpdateTaskStatus(ctx context.Context, taskID string, status types.TaskStatus) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET status = ? WHERE id = ?`,
		status, taskID,
	)
	return err
}

func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}
