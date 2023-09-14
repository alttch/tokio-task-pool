# tokio-task-pool

Task pool for Tokio Runtime

<https://crates.io/crates/tokio-task-pool>

## The problem

A typical pattern

```rust,ignore
loop {
    let (socket, _) = listener.accept().await.unwrap();
    tokio::spawn(async move {
        process(socket).await;
    });
}
```

is actually an anti-pattern which may break your production.

Why? Because this pattern behaves equally to an unbounded channel. If the
producer has higher rate than consumers, it floods runtime with tasks and
sooner or later causes memory overflow.

## Solution

* Use a pool of workers instead

* Use task spawning but manually limit the number of active tasks with a
semaphore

* Use this crate which does the job out-of-the-box

## Features provided

* Pool objects with safe spawn methods, which automatically limit number of
  tasks

* Tasks can be automatically aborted if run timeout is set, global or per task

## Code example

Simple spawning is pretty similar to tokio::spawn, but async because the
producer must be blocked until there is an empty task slot in the pool:

```rust
use std::time::Duration;
use tokio_task_pool::Pool;

#[tokio::main]
async fn main() {
    let pool = Pool::bounded(5)
        .with_spawn_timeout(Duration::from_secs(5))
        .with_run_timeout(Duration::from_secs(10));
    pool.spawn(async move {
        // do some job
    }).await.unwrap();
}
```

## More tricks

Refer to the crate documentation.

## Features

* A "log" feature enables automatic error logging for timed out tasks (via the
*log* crate)
