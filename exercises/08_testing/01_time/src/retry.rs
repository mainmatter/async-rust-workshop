//! Doing it again, but not straight away.

use std::time::Duration;

use tokio::time::sleep;

/// How many times the client tries to reach a server before it gives up.
pub const ATTEMPTS: u32 = 5;

/// How long to wait after the first failure. Every failure after that doubles it.
pub const BASE_DELAY: Duration = Duration::from_millis(100);

/// Runs `operation` until it succeeds or has failed `attempts` times, waiting longer each time.
///
/// The wait after the first failure is `base`, and it doubles after every failure after that. The
/// error from the last attempt is the one that comes back, and a failure nobody will retry is not
/// waited for.
pub async fn with_backoff<O, F, T, E>(
    attempts: u32,
    base: Duration,
    mut operation: O,
) -> Result<T, E>
where
    O: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
{
    let mut attempt = 1;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt >= attempts => return Err(error),
            Err(_) => {
                sleep(base * 2u32.pow(attempt - 1)).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, time::Duration};

    use tokio::time::Instant;

    use crate::retry::{ATTEMPTS, BASE_DELAY, with_backoff};

    #[tokio::test(start_paused = true)]
    async fn an_operation_that_works_is_not_waited_for() {
        let attempts = Cell::new(0);
        let started = Instant::now();

        let outcome = with_backoff(ATTEMPTS, BASE_DELAY, || {
            attempts.set(attempts.get() + 1);
            async { Ok::<_, &str>("hello") }
        })
        .await;

        assert_eq!(outcome, Ok("hello"));
        assert_eq!(attempts.get(), 1);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn the_wait_doubles_after_every_failure() {
        let attempts = Cell::new(0);
        let started = Instant::now();

        let outcome = with_backoff(ATTEMPTS, BASE_DELAY, || {
            attempts.set(attempts.get() + 1);
            let attempt = attempts.get();

            async move {
                match attempt {
                    1 | 2 => Err("refused"),
                    _ => Ok("hello"),
                }
            }
        })
        .await;

        assert_eq!(outcome, Ok("hello"));
        assert_eq!(attempts.get(), 3);
        assert_eq!(
            started.elapsed(),
            BASE_DELAY * 3,
            "100ms after the first failure, 200ms after the second"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn giving_up_returns_the_last_error() {
        let attempts = Cell::new(0);
        let started = Instant::now();

        let outcome = with_backoff(ATTEMPTS, BASE_DELAY, || {
            attempts.set(attempts.get() + 1);
            let attempt = attempts.get();

            async move { Err::<&str, _>(attempt) }
        })
        .await;

        assert_eq!(outcome, Err(ATTEMPTS));
        assert_eq!(attempts.get(), ATTEMPTS);
        assert_eq!(
            started.elapsed(),
            BASE_DELAY * 15,
            "100 + 200 + 400 + 800, and nothing after the last attempt"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_attempt_is_a_plain_call() {
        let attempts = Cell::new(0);
        let started = Instant::now();

        let outcome = with_backoff(1, BASE_DELAY, || {
            attempts.set(attempts.get() + 1);
            async { Err::<&str, _>("refused") }
        })
        .await;

        assert_eq!(outcome, Err("refused"));
        assert_eq!(attempts.get(), 1);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }
}
