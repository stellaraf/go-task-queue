package taskqueue

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// BasicTaskQueue implements a FIFO task queue of string values.
type BasicTaskQueue struct {
	Name    string
	Redis   redis.UniversalClient
	ctx     context.Context
	noRetry bool
}

// NewBasic creates a new BasicTaskQueue instance.
func NewBasic(name string, option ...Option) (*BasicTaskQueue, error) {
	options, err := getOptions(option)
	if err != nil {
		return nil, err
	}
	redisOptions := &redis.Options{
		Addr:         options.Host,
		Username:     options.Username,
		Password:     options.Password,
		TLSConfig:    options.TLSConfig,
		ReadTimeout:  options.Timeout,
		WriteTimeout: options.Timeout,
		Network:      "tcp",
	}
	redisClient := redis.NewClient(redisOptions)
	_, err = redisClient.Ping(options.Context).Result()
	if err != nil {
		return nil, err
	}
	taskQueue := &BasicTaskQueue{
		Name:    name,
		Redis:   redisClient,
		ctx:     options.Context,
		noRetry: options.NoRetry,
	}
	return taskQueue, nil
}

// Size returns the number of items in the queue.
func (q *BasicTaskQueue) Size() uint64 {
	zcard := q.Redis.LLen(q.ctx, q.Name)
	size, err := zcard.Uint64()
	if err != nil {
		return 0
	}
	return size
}

// Pop removes and returns the first task from the queue. If the queue is empty, the return value
// will be nil.
func (q *BasicTaskQueue) Pop() *string {
	pop := q.Redis.LPop(q.ctx, q.Name)
	value, err := pop.Result()
	if err != nil {
		return nil
	}
	return &value
}

// Add adds any number of tasks to the queue in order. Items provided will be marshaled to JSON.
func (q *BasicTaskQueue) Add(tasks ...any) error {
	if len(tasks) == 0 {
		return nil
	}
	added, err := q.Redis.RPush(q.ctx, q.Name, tasks...).Result()
	if err != nil {
		return err
	}
	if added == 0 {
		return fmt.Errorf("failed to add tasks to queue")
	}
	return nil
}

// Remove removes a task from the queue.
func (q *BasicTaskQueue) Remove(task any) error {
	_, err := q.Redis.LRem(q.ctx, q.Name, 0, task).Result()
	if err != nil {
		return err
	}
	return nil
}

// Clear removes all tasks from the queue.
func (q *BasicTaskQueue) Clear() error {
	_, err := q.Redis.Del(q.ctx, q.Name).Result()
	return err
}

// Get retrieves an item from the queue based on its index.
func (q *BasicTaskQueue) Get(index int64) (string, error) {
	value, err := q.Redis.LIndex(q.ctx, q.Name, index).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	return value, nil
}

func (q *BasicTaskQueue) RemoveIndex(index int64) error {
	value, err := q.Redis.LIndex(q.ctx, q.Name, index).Result()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("index %d does not exist in queue", index)
		}
		return err
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(value))
	_, err = q.Redis.LSet(q.ctx, q.Name, index, encoded).Result()
	if err != nil {
		return err
	}
	removed, err := q.Redis.LRem(q.ctx, q.Name, 1, encoded).Result()
	if err != nil {
		return err
	}
	if removed == 0 {
		return fmt.Errorf("failed to remove value item %d from queue", index)
	}
	return nil
}
