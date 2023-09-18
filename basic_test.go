package taskqueue_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	taskqueue "github.com/stellaraf/go-task-queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const zero uint64 = uint64(0)

var Addr string
var Ctx context.Context
var UseMini bool
var RunT func(t miniredis.Tester) *miniredis.Miniredis = func(t miniredis.Tester) *miniredis.Miniredis {
	Addr = "localhost:6379"
	Ctx = context.Background()
	UseMini = false
	return nil
}

func init() {
	ci := os.Getenv("CI") == "true"
	if !ci {
		RunT = miniredis.RunT
		UseMini = true
	}
}

func Test_NewBasicTaskQueue(t *testing.T) {
	name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
	mr := RunT(t)
	var ctx, addr taskqueue.Option
	if UseMini {
		ctx = taskqueue.WithContext(mr.Ctx)
		addr = taskqueue.WithHost(mr.Addr())
	} else {
		ctx = taskqueue.WithContext(Ctx)
		addr = taskqueue.WithHost(Addr)
	}

	t.Run("base", func(t *testing.T) {
		t.Parallel()
		_, err := taskqueue.NewBasic(name, ctx, addr)
		require.NoError(t, err)
	})

	t.Run("with uri", func(t *testing.T) {
		t.Parallel()
		_, err := taskqueue.NewBasic(name, ctx, taskqueue.WithURI(fmt.Sprintf("redis://%s", Addr)))
		require.NoError(t, err)
	})
}

func TestBasicTaskQueue_Clear(t *testing.T) {
	mr := RunT(t)
	var ctx, addr taskqueue.Option
	if UseMini {
		ctx = taskqueue.WithContext(mr.Ctx)
		addr = taskqueue.WithHost(mr.Addr())
	} else {
		ctx = taskqueue.WithContext(Ctx)
		addr = taskqueue.WithHost(Addr)
	}

	name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
	queue, err := taskqueue.NewBasic(name, ctx, addr)
	require.NoError(t, err)

	t.Run("add", func(t *testing.T) {
		err := queue.Add("value")
		require.NoError(t, err)
	})

	t.Run("clear", func(t *testing.T) {
		err := queue.Clear()
		require.NoError(t, err)
	})
	t.Run("ensure cleared", func(t *testing.T) {
		size := queue.Size()
		assert.Equal(t, zero, size)
	})
}

func TestBasicTaskQueue_Size(t *testing.T) {
	mr := RunT(t)
	var ctx, addr taskqueue.Option
	if UseMini {
		ctx = taskqueue.WithContext(mr.Ctx)
		addr = taskqueue.WithHost(mr.Addr())
	} else {
		ctx = taskqueue.WithContext(Ctx)
		addr = taskqueue.WithHost(Addr)
	}

	name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
	queue, err := taskqueue.NewBasic(name, ctx, addr)
	defer queue.Clear()
	require.NoError(t, err)

	t.Run("add", func(t *testing.T) {
		err := queue.Add("value")
		require.NoError(t, err)
	})
	t.Run("ensure size", func(t *testing.T) {
		size := queue.Size()
		assert.Equal(t, uint64(1), size)
	})
}

func TestBasicTaskQueue_Add(t *testing.T) {
	mr := RunT(t)
	var ctx, addr taskqueue.Option
	if UseMini {
		ctx = taskqueue.WithContext(mr.Ctx)
		addr = taskqueue.WithHost(mr.Addr())
	} else {
		ctx = taskqueue.WithContext(Ctx)
		addr = taskqueue.WithHost(Addr)
	}

	name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
	queue, err := taskqueue.NewBasic(name, ctx, addr)
	defer queue.Clear()
	require.NoError(t, err)
	value := fmt.Sprintf("%s--%s--1", t.Name(), time.Now().Format(time.RFC3339Nano))

	t.Run("add", func(t *testing.T) {
		err := queue.Add(value)
		require.NoError(t, err)
	})
	t.Run("check size", func(t *testing.T) {
		size := queue.Size()
		assert.Equal(t, uint64(1), size)
	})
	t.Run("pop", func(t *testing.T) {
		popped := queue.Pop()
		assert.Equal(t, value, *popped)
	})
}

func TestBasicTaskQueue_Pop(t *testing.T) {
	mr := RunT(t)
	var ctx, addr taskqueue.Option
	if UseMini {
		ctx = taskqueue.WithContext(mr.Ctx)
		addr = taskqueue.WithHost(mr.Addr())
	} else {
		ctx = taskqueue.WithContext(Ctx)
		addr = taskqueue.WithHost(Addr)
	}

	name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
	queue, err := taskqueue.NewBasic(name, ctx, addr)
	defer queue.Clear()
	require.NoError(t, err)
	t.Run("add values", func(t *testing.T) {
		err := queue.Add("one")
		require.NoError(t, err)
		err = queue.Add("two")
		require.NoError(t, err)
		err = queue.Add("three")
		require.NoError(t, err)
	})
	t.Run("pop values", func(t *testing.T) {
		one := queue.Pop()
		assert.Equal(t, "one", *one)
		two := queue.Pop()
		assert.Equal(t, "two", *two)
		three := queue.Pop()
		assert.Equal(t, "three", *three)
	})
	t.Run("nil when empty", func(t *testing.T) {
		value := queue.Pop()
		assert.Nil(t, value)
	})
}

func TestBasicTaskQueue_Remove(t *testing.T) {
	mr := RunT(t)
	var ctx, addr taskqueue.Option
	if UseMini {
		ctx = taskqueue.WithContext(mr.Ctx)
		addr = taskqueue.WithHost(mr.Addr())
	} else {
		ctx = taskqueue.WithContext(Ctx)
		addr = taskqueue.WithHost(Addr)
	}

	name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
	queue, err := taskqueue.NewBasic(name, ctx, addr)
	defer queue.Clear()
	require.NoError(t, err)
	value := "value"
	t.Run("add value", func(t *testing.T) {
		err := queue.Add(value)
		require.NoError(t, err)
	})
	t.Run("remove value", func(t *testing.T) {
		err := queue.Remove(value)
		require.NoError(t, err)
	})
	t.Run("ensure removed", func(t *testing.T) {
		size := queue.Size()
		assert.Equal(t, zero, size)
	})
}
