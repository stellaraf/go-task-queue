package taskqueue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// JSONTaskQueue implements a FIFO task queue with any JSON-able value as the value.
type JSONTaskQueue struct {
	// Name represents the Redis key.
	Name string
	// Redis is the underlying Redis instance.
	Redis   redis.UniversalClient
	ctx     context.Context
	noRetry bool
}

// NewJSON creates a new JSONTaskQueue instance.
func NewJSON(name string, option ...Option) (*JSONTaskQueue, error) {
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
	taskQueue := &JSONTaskQueue{
		Name:    name,
		Redis:   redisClient,
		ctx:     options.Context,
		noRetry: options.NoRetry,
	}
	return taskQueue, nil
}

// Size returns the number of items in the queue.
func (q *JSONTaskQueue) Size() uint64 {
	zcard := q.Redis.LLen(q.ctx, q.Name)
	size, err := zcard.Uint64()
	if err != nil {
		return 0
	}
	return size
}

// Has determines if a queue has an given task.
func (q *JSONTaskQueue) Has(value any) bool {
	bValue, err := json.Marshal(value)
	if err != nil {
		return false
	}
	sValue := string(bValue)
	count, err := q.Redis.LPosCount(q.ctx, q.Name, sValue, 0, redis.LPosArgs{}).Result()
	if err != nil {
		return false
	}
	return len(count) > 0
}

// Add adds any number of tasks to the queue in order. Items provided will be marshaled to JSON.
func (q *JSONTaskQueue) Add(tasks ...any) error {
	if len(tasks) == 0 {
		return nil
	}
	bTasks := [][]byte{}
	for _, task := range tasks {
		bTask, err := json.Marshal(task)
		if err != nil {
			return err
		}
		bTasks = append(bTasks, bTask)
	}
	added := int64(0)
	for _, task := range bTasks {
		a, err := q.Redis.RPush(q.ctx, q.Name, task).Result()
		if err != nil {
			return err
		}
		added += a
	}
	if added == 0 {
		return fmt.Errorf("failed to add tasks to queue")
	}
	return nil
}

// Pop removes the first task from the queue and unmarshals the value.
func (q *JSONTaskQueue) Pop(value any) error {
	pop := q.Redis.LPop(q.ctx, q.Name)
	popped, err := pop.Bytes()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("no values remain in queue")
		}
		return err
	}
	err = json.Unmarshal(popped, value)
	if err != nil {
		if !q.noRetry {
			added, err := q.Redis.RPush(q.ctx, q.Name, popped).Result()
			if err != nil {
				return errors.Wrap(err, "failed to re-add task to queue after unmarshal failure")
			}
			if added == 0 {
				return fmt.Errorf("failed to re-add task to queue after unmarshal failure")
			}
			return nil
		}
		return err
	}
	return nil
}

func (q *JSONTaskQueue) PopBytes() ([]byte, error) {
	return q.Redis.LPop(q.ctx, q.Name).Bytes()
}

// Remove removes a task from the queue.
func (q *JSONTaskQueue) Remove(task any) error {
	bTask, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = q.Redis.LRem(q.ctx, q.Name, 0, bTask).Result()
	if err != nil {
		return err
	}
	return nil
}

// Clear removes all tasks from the queue.
func (q *JSONTaskQueue) Clear() error {
	_, err := q.Redis.Del(q.ctx, q.Name).Result()
	return err
}

func (q *JSONTaskQueue) Get(index int64, target any) error {
	value, err := q.Redis.LIndex(q.ctx, q.Name, index).Bytes()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("index %d does not exist in queue", index)
		}
		return err
	}
	err = json.Unmarshal(value, target)
	if err != nil {
		if !q.noRetry {
			_, err := q.Redis.LSet(q.ctx, q.Name, index, value).Result()
			if err != nil {
				return errors.Wrap(err, "failed to re-add task to queue after unmarshal failure")
			}
			return nil
		}
		return err
	}
	return nil
}

func (q *JSONTaskQueue) RemoveIndex(index int64) error {
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
