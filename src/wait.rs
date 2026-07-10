//! Generic poll-until-condition combinator.
//!
//! [`wait_for`] is the primitive every `wait_for_*` helper in this crate
//! (`RedisCli::wait_for_ready`, `RedisClusterHandle::wait_for_healthy`,
//! `RedisSentinelHandle::wait_for_healthy`, and the observability waits added
//! alongside it) is built on. It's also exported directly for test authors
//! who need to wait on a custom predicate without writing their own sleep
//! loop.

use std::future::Future;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// Poll `check` until it returns `true`, or fail with [`Error::Timeout`] once
/// `timeout` has elapsed.
///
/// Each iteration: run `check`; if it returns `true`, return `Ok(())`
/// immediately -- a first-try success never sleeps. Otherwise, if the
/// elapsed time since the call began exceeds `timeout`, return
/// `Err(Error::Timeout { message })`. Otherwise sleep for `interval` and try
/// again.
///
/// `message` is preserved verbatim in the returned timeout error, so callers
/// keep their existing, specific error text (e.g. "cluster did not become
/// healthy in time") instead of a generic one.
///
/// # Example
///
/// ```no_run
/// use redis_server_wrapper::wait::wait_for;
/// use std::time::Duration;
///
/// # async fn example() {
/// let mut attempts = 0;
/// wait_for(
///     || {
///         attempts += 1;
///         async move { attempts >= 3 }
///     },
///     Duration::from_secs(5),
///     Duration::from_millis(100),
///     "condition never became true",
/// )
/// .await
/// .unwrap();
/// # }
/// ```
pub async fn wait_for<F, Fut>(
    mut check: F,
    timeout: Duration,
    interval: Duration,
    message: impl Into<String>,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let message = message.into();
    let start = Instant::now();
    loop {
        if check().await {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(Error::Timeout { message });
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn succeeds_immediately_without_sleeping() {
        let calls = AtomicU32::new(0);
        let start = Instant::now();
        wait_for(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { true }
            },
            Duration::from_secs(5),
            Duration::from_secs(5),
            "should not time out",
        )
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn retries_until_true() {
        let calls = AtomicU32::new(0);
        wait_for(
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                async move { n >= 3 }
            },
            Duration::from_secs(5),
            Duration::from_millis(10),
            "never became true",
        )
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn times_out_with_preserved_message() {
        let err = wait_for(
            || async { false },
            Duration::from_millis(50),
            Duration::from_millis(10),
            "custom timeout message",
        )
        .await
        .unwrap_err();
        match err {
            Error::Timeout { message } => assert_eq!(message, "custom timeout message"),
            other => panic!("expected Error::Timeout, got {other:?}"),
        }
    }
}
