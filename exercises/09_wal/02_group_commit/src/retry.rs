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
