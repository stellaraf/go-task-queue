package taskqueue_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	taskqueue "github.com/stellaraf/go-task-queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type RetryValue struct {
	Value string `json:"value"`
}

func (v *RetryValue) UnmarshalJSON([]byte) error {
	return fmt.Errorf("wrong")
}

func Test_NewJSONTaskQueue(t *testing.T) {
	name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
	mr := RunT(t)
	var ctx, addr, uri taskqueue.Option
	if UseMini {
		ctx = taskqueue.WithContext(mr.Ctx)
		addr = taskqueue.WithHost(mr.Addr())
		uri = taskqueue.WithURI(fmt.Sprintf("redis://%s", mr.Addr()))
	} else {
		ctx = taskqueue.WithContext(Ctx)
		addr = taskqueue.WithHost(Addr)
		uri = taskqueue.WithURI(fmt.Sprintf("redis://%s", Addr))
	}
	t.Run("base", func(t *testing.T) {
		t.Parallel()
		_, err := taskqueue.NewJSON(name, ctx, addr)
		require.NoError(t, err)
	})

	t.Run("with uri", func(t *testing.T) {
		t.Parallel()
		_, err := taskqueue.NewJSON(name, ctx, uri)
		require.NoError(t, err)
	})
}

func TestJSONTaskQueue_Clear(t *testing.T) {
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
	queue, err := taskqueue.NewJSON(name, ctx, addr)
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

func TestJSONTaskQueue_Size(t *testing.T) {
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
	queue, err := taskqueue.NewJSON(name, ctx, addr)
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

func TestJSONTaskQueue_Add(t *testing.T) {
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
	queue, err := taskqueue.NewJSON(name, ctx, addr)
	defer queue.Clear()
	require.NoError(t, err)

	type Value struct {
		String string `json:"string"`
		Number int    `json:"number"`
		Bool   bool   `json:"bool"`
	}

	value := Value{String: "string", Number: 1, Bool: true}

	t.Run("add", func(t *testing.T) {
		err := queue.Add(value)
		require.NoError(t, err)
	})
	t.Run("ensure size", func(t *testing.T) {
		size := queue.Size()
		assert.Equal(t, uint64(1), size)
	})
	t.Run("pop", func(t *testing.T) {
		var popped *Value
		err := queue.Pop(&popped)
		require.NoError(t, err)
		assert.Equal(t, value.String, popped.String)
		assert.Equal(t, value.Number, popped.Number)
		assert.Equal(t, value.Bool, popped.Bool)
	})
}

func TestJSONTaskQueue_Pop(t *testing.T) {
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
	queue, err := taskqueue.NewJSON(name, ctx, addr)
	defer queue.Clear()
	require.NoError(t, err)

	type Value struct {
		String string `json:"string"`
		Number int    `json:"number"`
		Bool   bool   `json:"bool"`
	}

	value1 := Value{String: "string1", Number: 1, Bool: true}
	value2 := Value{String: "string2", Number: 2, Bool: false}
	value3 := Value{String: "string3", Number: 3, Bool: true}

	t.Run("add", func(t *testing.T) {
		err := queue.Add(value1)
		require.NoError(t, err)
		err = queue.Add(value2)
		require.NoError(t, err)
		err = queue.Add(value3)
		require.NoError(t, err)
	})
	t.Run("pop", func(t *testing.T) {
		var popped *Value
		err := queue.Pop(&popped)
		require.NoError(t, err)
	})
	t.Run("ensure size", func(t *testing.T) {
		size := queue.Size()
		assert.Equal(t, uint64(2), size)
	})
	t.Run("ensure correct item was popped", func(t *testing.T) {
		var popped1 *Value
		var popped2 *Value
		err := queue.Pop(&popped1)
		require.NoError(t, err)
		err = queue.Pop(&popped2)
		require.NoError(t, err)
		assert.Equal(t, value2.Number, popped1.Number)
		assert.Equal(t, value3.Number, popped2.Number)
		size := queue.Size()
		assert.Equal(t, zero, size)
	})

	t.Run("with retry", func(t *testing.T) {
		name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
		queue, err := taskqueue.NewJSON(name, ctx, addr)
		require.NoError(t, err)
		v := RetryValue{"value"}
		err = queue.Add(v)
		require.NoError(t, err)
		var popped *RetryValue
		err = queue.Pop(&popped)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), queue.Size())
		queue.Clear()
	})

	t.Run("with no retry", func(t *testing.T) {
		name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
		queue, err := taskqueue.NewJSON(name, ctx, addr, taskqueue.WithNoRetry())
		require.NoError(t, err)
		v := RetryValue{"value"}
		err = queue.Add(v)
		require.NoError(t, err)
		var popped *RetryValue
		err = queue.Pop(&popped)
		assert.Error(t, err)
		assert.Equal(t, zero, queue.Size())
	})

	t.Run("retry value is equal to original", func(t *testing.T) {
		name := fmt.Sprintf("%s--%s", t.Name(), time.Now().Format(time.RFC3339Nano))
		queue, err := taskqueue.NewJSON(name, ctx, addr)
		require.NoError(t, err)
		in := RetryValue{"value"}
		inB, err := json.Marshal(in)
		require.NoError(t, err)
		err = queue.Add(in)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), queue.Size())

		var out *RetryValue
		err = queue.Pop(&out)
		assert.NoError(t, err)
		outB, err := queue.PopBytes()
		require.NoError(t, err)
		assert.Equal(t, inB, outB)
		t.Cleanup(func() {
			queue.Clear()
		})
	})
}

func TestJSONTaskQueue_Remove(t *testing.T) {
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
	queue, err := taskqueue.NewJSON(name, ctx, addr)
	defer queue.Clear()
	require.NoError(t, err)

	type Value struct {
		String string `json:"string"`
		Number int    `json:"number"`
		Bool   bool   `json:"bool"`
	}

	value1 := Value{String: "string1", Number: 1, Bool: true}
	value2 := Value{String: "string2", Number: 2, Bool: false}

	t.Run("add", func(t *testing.T) {
		err := queue.Add(value1)
		require.NoError(t, err)
		err = queue.Add(value2)
		require.NoError(t, err)
	})
	t.Run("remove", func(t *testing.T) {
		err := queue.Remove(value1)
		require.NoError(t, err)
	})
	t.Run("ensure size", func(t *testing.T) {
		size := queue.Size()
		assert.Equal(t, uint64(1), size)
	})
	t.Run("ensure correct item was removed", func(t *testing.T) {
		var popped *Value
		err := queue.Pop(&popped)
		require.NoError(t, err)
		assert.Equal(t, value2.String, popped.String)
		assert.Equal(t, value2.Number, popped.Number)
		assert.Equal(t, value2.Bool, popped.Bool)
	})
}

func TestJSONTaskQueue_Complex(t *testing.T) {
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
	queue, err := taskqueue.NewJSON(name, ctx, addr)
	defer queue.Clear()
	require.NoError(t, err)

	type Nested struct {
		String string `json:"string"`
	}

	array := []string{"one", "two"}

	type Value struct {
		String        string    `json:"string"`
		Number        int       `json:"number"`
		Time          time.Time `json:"time"`
		Nested        Nested    `json:"nested"`
		Array         []string  `json:"array"`
		NestedPointer *Nested   `json:"nested_pointer"`
	}
	now := time.Now()
	value := Value{
		String:        "string",
		Number:        1,
		Time:          now,
		Nested:        Nested{String: "nested"},
		Array:         array,
		NestedPointer: &Nested{String: "pointer"},
	}
	t.Run("add", func(t *testing.T) {
		err := queue.Add(value)
		require.NoError(t, err)
	})
	t.Run("pop", func(t *testing.T) {
		var popped *Value
		err := queue.Pop(&popped)
		require.NoError(t, err)
		assert.Equal(t, value.String, popped.String)
		assert.Equal(t, value.Number, popped.Number)
		assert.Equal(t, value.Time.Format(time.RFC3339Nano), popped.Time.Format(time.RFC3339Nano))
		assert.EqualValues(t, value.Array, popped.Array)
		assert.Equal(t, value.Nested.String, popped.Nested.String)
		assert.NotNil(t, popped.NestedPointer)
		assert.Equal(t, value.NestedPointer.String, popped.NestedPointer.String)
	})

	t.Run("add pointer", func(t *testing.T) {
		err := queue.Clear()
		require.NoError(t, err)
		value := &Value{
			String:        "string",
			Number:        1,
			Time:          now,
			Nested:        Nested{String: "nested"},
			Array:         array,
			NestedPointer: &Nested{String: "pointer"},
		}
		err = queue.Add(value)
		require.NoError(t, err)
	})
	t.Run("pop pointer", func(t *testing.T) {
		var popped *Value
		err := queue.Pop(&popped)
		require.NoError(t, err)
		assert.Equal(t, value.String, popped.String)
		assert.Equal(t, value.Number, popped.Number)
		assert.Equal(t, value.Time.Format(time.RFC3339Nano), popped.Time.Format(time.RFC3339Nano))
		assert.EqualValues(t, value.Array, popped.Array)
		assert.Equal(t, value.Nested.String, popped.Nested.String)
		assert.NotNil(t, popped.NestedPointer)
		assert.Equal(t, value.NestedPointer.String, popped.NestedPointer.String)
	})
}
