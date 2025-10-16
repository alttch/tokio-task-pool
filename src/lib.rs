#![ doc = include_str!( concat!( env!( "CARGO_MANIFEST_DIR" ), "/", "README.md" ) ) ]
#[cfg(feature = "log")]
use log::error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
#[cfg(feature = "tracing")]
use tracing::{event, Level};

pub type SpawnResult<T> = Result<JoinHandle<Result<<T as Future>::Output, Error>>, Error>;

/// Task ID, can be created from &'static str or String
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TaskId {
    Static(&'static str),
    Owned(String),
}

impl From<&'static str> for TaskId {
    #[inline]
    fn from(s: &'static str) -> Self {
        Self::Static(s)
    }
}

impl From<String> for TaskId {
    #[inline]
    fn from(s: String) -> Self {
        Self::Owned(s)
    }
}

impl TaskId {
    #[inline]
    fn as_str(&self) -> &str {
        match self {
            TaskId::Static(v) => v,
            TaskId::Owned(s) => s.as_str(),
        }
    }
}

/// Task
///
/// Contains Future, can contain custom ID and timeout
pub struct Task<T>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    id: Option<TaskId>,
    timeout: Option<Duration>,
    future: T,
}

impl<T> Task<T>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    #[inline]
    pub fn new(future: T) -> Self {
        Self {
            id: None,
            timeout: None,
            future,
        }
    }
    #[inline]
    pub fn with_id<I: Into<TaskId>>(mut self, id: I) -> Self {
        self.id = Some(id.into());
        self
    }
    #[inline]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Error {
    SpawnTimeout,
    RunTimeout(Option<TaskId>),
    SpawnSemaphoneAcquireError,
    NotAvailable,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::SpawnTimeout => write!(f, "task spawn timeout"),
            Error::RunTimeout(id) => {
                if let Some(i) = id {
                    write!(f, "task {} run timeout", i.as_str())
                } else {
                    write!(f, "task run timeout")
                }
            }
            Error::SpawnSemaphoneAcquireError => write!(f, "task spawn semaphore error"),
            Error::NotAvailable => write!(f, "no available task slots"),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio::sync::AcquireError> for Error {
    fn from(_: tokio::sync::AcquireError) -> Self {
        Self::SpawnSemaphoneAcquireError
    }
}

/// Task pool
#[derive(Debug)]
pub struct Pool {
    id: Option<Arc<String>>,
    spawn_timeout: Option<Duration>,
    run_timeout: Option<Duration>,
    limiter: Option<Arc<Semaphore>>,
    capacity: Option<usize>,
    #[cfg(any(feature = "log", feature = "tracing"))]
    logging_enabled: bool,
}

impl Default for Pool {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl Pool {
    /// Creates a bounded pool (recommended)
    pub fn bounded(capacity: usize) -> Self {
        Self {
            id: None,
            spawn_timeout: None,
            run_timeout: None,
            limiter: Some(Arc::new(Semaphore::new(capacity))),
            capacity: Some(capacity),
            #[cfg(any(feature = "log", feature = "tracing"))]
            logging_enabled: true,
        }
    }
    /// Creates an unbounded pool
    pub fn unbounded() -> Self {
        Self {
            id: None,
            spawn_timeout: None,
            run_timeout: None,
            limiter: None,
            capacity: None,
            #[cfg(any(feature = "log", feature = "tracing"))]
            logging_enabled: true,
        }
    }
    pub fn with_id<I: Into<String>>(mut self, id: I) -> Self {
        self.id.replace(Arc::new(id.into()));
        self
    }
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref().map(String::as_str)
    }
    /// Sets spawn timeout
    ///
    /// (ignored for unbounded)
    #[inline]
    pub fn with_spawn_timeout(mut self, timeout: Duration) -> Self {
        self.spawn_timeout = Some(timeout);
        self
    }
    /// Sets the default task run timeout
    #[inline]
    pub fn with_run_timeout(mut self, timeout: Duration) -> Self {
        self.run_timeout = Some(timeout);
        self
    }
    /// Sets both spawn and run timeouts
    #[inline]
    pub fn with_timeout(self, timeout: Duration) -> Self {
        self.with_spawn_timeout(timeout).with_run_timeout(timeout)
    }
    #[cfg(any(feature = "log", feature = "tracing"))]
    /// Disables internal error logging
    #[inline]
    pub fn with_no_logging_enabled(mut self) -> Self {
        self.logging_enabled = false;
        self
    }
    /// Returns pool capacity
    #[inline]
    pub fn capacity(&self) -> Option<usize> {
        self.capacity
    }
    /// Returns pool available task permits
    #[inline]
    pub fn available_permits(&self) -> Option<usize> {
        self.limiter.as_ref().map(|v| v.available_permits())
    }
    /// Returns pool busy task permits
    #[inline]
    pub fn busy_permits(&self) -> Option<usize> {
        self.limiter
            .as_ref()
            .map(|v| self.capacity.unwrap_or_default() - v.available_permits())
    }
    /// Spawns a future
    #[inline]
    pub fn spawn<T>(&self, future: T) -> impl Future<Output = SpawnResult<T>> + '_
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.spawn_task(Task::new(future))
    }
    /// Spawns a future with a custom timeout
    #[inline]
    pub fn spawn_with_timeout<T>(
        &self,
        future: T,
        timeout: Duration,
    ) -> impl Future<Output = SpawnResult<T>> + '_
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.spawn_task(Task::new(future).with_timeout(timeout))
    }
    /// Spawns a task (a future which can have a custom ID and timeout)
    pub async fn spawn_task<T>(&self, task: Task<T>) -> SpawnResult<T>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        #[cfg(any(feature = "log", feature = "tracing"))]
        let id = self.id.as_ref().cloned();
        let perm = if let Some(ref limiter) = self.limiter {
            if let Some(spawn_timeout) = self.spawn_timeout {
                Some(
                    tokio::time::timeout(spawn_timeout, limiter.clone().acquire_owned())
                        .await
                        .map_err(|_| Error::SpawnTimeout)??,
                )
            } else {
                Some(limiter.clone().acquire_owned().await?)
            }
        } else {
            None
        };
        if let Some(rtimeout) = task.timeout.or(self.run_timeout) {
            #[cfg(any(feature = "log", feature = "tracing"))]
            let logging_enabled = self.logging_enabled;
            Ok(tokio::spawn(async move {
                let _p = perm;
                if let Ok(v) = tokio::time::timeout(rtimeout, task.future).await {
                    Ok(v)
                } else {
                    let e = Error::RunTimeout(task.id);
                    #[cfg(any(feature = "log", feature = "tracing"))]
                    if logging_enabled {
                        #[cfg(feature = "log")]
                        error!("{}: {}", id.as_deref().map_or("", |v| v.as_str()), e);

                        #[cfg(feature = "tracing")]
                        event!(
                            Level::ERROR,
                            error = ?e,
                            id = id.as_deref().map_or("", |v| v.as_str())
                        );
                    }
                    Err(e)
                }
            }))
        } else {
            Ok(tokio::spawn(async move {
                let _p = perm;
                Ok(task.future.await)
            }))
        }
    }
    /// Tries to spawn a future if there is an available permit. Returns `Error::NotAvailable` if no
    /// permit available
    pub fn try_spawn<T>(&self, future: T) -> SpawnResult<T>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.try_spawn_task(Task::new(future))
    }
    /// Tries to spawn a future with a custom timeout if there is an available permit. Returns
    /// `Error::NotAvailable` if no permit available
    pub fn try_spawn_with_timeout<T>(&self, future: T, timeout: Duration) -> SpawnResult<T>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.try_spawn_task(Task::new(future).with_timeout(timeout))
    }
    /// Spawns a task (a future which can have a custom ID and timeout) if there is an available
    /// permit. Returns `Error::NotAvailable` if no permit available
    pub fn try_spawn_task<T>(&self, task: Task<T>) -> SpawnResult<T>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        #[cfg(any(feature = "log", feature = "tracing"))]
        let id = self.id.as_ref().cloned();
        let perm = if let Some(ref limiter) = self.limiter {
            Some(
                limiter
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::NotAvailable)?,
            )
        } else {
            None
        };
        if let Some(rtimeout) = task.timeout.or(self.run_timeout) {
            #[cfg(any(feature = "log", feature = "tracing"))]
            let logging_enabled = self.logging_enabled;
            Ok(tokio::spawn(async move {
                let _p = perm;
                if let Ok(v) = tokio::time::timeout(rtimeout, task.future).await {
                    Ok(v)
                } else {
                    let e = Error::RunTimeout(task.id);
                    #[cfg(any(feature = "log", feature = "tracing"))]
                    if logging_enabled {
                        #[cfg(feature = "log")]
                        error!("{}: {}", id.as_deref().map_or("", |v| v.as_str()), e);

                        #[cfg(feature = "tracing")]
                        event!(
                            Level::ERROR,
                            error = ?e,
                            id = id.as_deref().map_or("", |v| v.as_str())
                        );
                    }
                    Err(e)
                }
            }))
        } else {
            Ok(tokio::spawn(async move {
                let _p = perm;
                Ok(task.future.await)
            }))
        }
    }
}

#[cfg(test)]
mod test {
    use super::Pool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::channel;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_spawn() {
        let pool = Pool::bounded(5);
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 1..=5 {
            let counter_c = counter.clone();
            pool.spawn(async move {
                sleep(Duration::from_secs(2)).await;
                counter_c.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap();
        }
        sleep(Duration::from_secs(3)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_spawn_timeout() {
        let pool = Pool::bounded(5).with_spawn_timeout(Duration::from_secs(1));
        for _ in 1..=5 {
            let (tx, mut rx) = channel(1);
            pool.spawn(async move {
                tx.send(()).await.unwrap();
                sleep(Duration::from_secs(2)).await;
            })
            .await
            .unwrap();
            rx.recv().await;
        }
        dbg!(pool.available_permits(), pool.busy_permits());
        assert!(pool
            .spawn(async move {
                sleep(Duration::from_secs(2)).await;
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_run_timeout() {
        let pool = Pool::bounded(5).with_run_timeout(Duration::from_secs(2));
        let counter = Arc::new(AtomicUsize::new(0));
        for i in 1..=5 {
            let counter_c = counter.clone();
            pool.spawn(async move {
                sleep(Duration::from_secs(if i == 5 { 3 } else { 1 })).await;
                counter_c.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap();
        }
        sleep(Duration::from_secs(5)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }
}
