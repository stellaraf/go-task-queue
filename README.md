<div align="center">
  <br/>
  <img src="https://res.cloudinary.com/stellaraf/image/upload/v1604277355/stellar-logo-gradient.svg" width="300" />
  <br/>
  <h3><code>taskqueue</code></h3>
  <br/>
  <a href="https://pkg.go.dev/github.com/stellaraf/go-task-queue">
    <img src="https://pkg.go.dev/badge/github.com/stellaraf/go-task-queue.svg" alt="Go Reference">
  </a>
  <a href="https://github.com/stellaraf/go-task-queue/tags">
    <img alt="GitHub tag (latest SemVer)" src="https://img.shields.io/github/v/tag/stellaraf/go-task-queue?color=%2306D6A0&label=version">
  </a>
  <a href="https://github.com/stellaraf/go-utils/actions/workflows/tests.yml">
    <img alt="GitHub Workflow Status" src="https://img.shields.io/github/actions/workflow/status/stellaraf/go-utils/tests.yml">
  </a>
  <br/>
  <br/>

</div>

# Usage

  ## Basic Task Queue
  `BasicTaskQueue` is a simple task queue that stores tasks as strings.

  ```go
  queue, err := taskqueue.NewBasic("queue-name")
  queue.Add("example1", "example2")
  value := queue.Pop()
  // example1
  value = queue.Pop()
  // example2
  ```

  ## JSON Task Queue
  `JSONTaskQueue` provides a very similar API to `BasicTaskQueue` and has the ability to store virtually any objet type as a task.

  ```go
  type Data struct {
    System string
    Count int
  }

  type Task struct {
    ID string `json:"id"`
    Details []string `json:"details"`
    Timestamp time.Time `json:"timestamp"`
    Data *Data `json:"data"`
  }

  task := Task{
    ID: "1234",
    Details: []string{"some", "details"},
    Timestamp: time.Now(),
    Data: &Data{System: "aws", Count: 5},
  }

  queue, err := taskqueue.NewJSON("queue-name")
  queue.Add(task)
  var fromQueue *Task
  queue.Pop(&fromQueue)
  ```

  ## Options

  Both `BasicTaskQueue` and `JSONTaskQueue` support the same options:

  ```go
  taskqueue.NewBasic(
    "queue-name",
    // Use a Redis connection string instead of WithHost/WithUsername/WithPassword.
    taskqueue.WithURI("redis://redis-username:redispassword@your-redis-host.example.com:55032"),
    // Set the Redis hostname. By default, this is localhost.
    taskqueue.WithHost("your-redis-host.example.com"),
    // Set the Redis username.
    taskqueue.WithUsername("redis-username"),
    // Set the Redis password.
    taskqueue.WithPassword("redis-password"),
    // Use your own context object. Useful if operating from within a web request.
    taskqueue.WithContext(context.Background()),
    // Set a Redis read/write timeout.
    taskqueue.WithTimeout(time.Seconds*10),
    // Add your own TLS configuration.
    taskqueue.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}),    
  )
  ```