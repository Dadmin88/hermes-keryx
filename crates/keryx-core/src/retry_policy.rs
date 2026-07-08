//! Retry and dead-letter policy for worker-reported task failures.

use serde::{Deserialize, Serialize};

/// Controls how many times a failed leased task may return to the queue before dead-lettering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of failure-driven requeues (`retry_count` after increment must be `<=` this).
    pub max_retries: u32,
    /// Suggested delay before the next claim attempt (milliseconds). Scheduling is out of scope for
    /// the store; callers may use this for worker backoff hints.
    pub backoff_ms: u64,
    /// Failure attempt count at or above which the task is dead-lettered instead of requeued.
    /// Defaults to `max_retries + 1` when constructed via [`RetryPolicy::default`].
    pub dead_letter_after: u32,
}

impl RetryPolicy {
    /// Policy that preserves pre-retry behavior: first failure is terminal without dead-letter metadata.
    #[must_use]
    pub const fn no_retries() -> Self {
        Self {
            max_retries: 0,
            backoff_ms: 0,
            dead_letter_after: 1,
        }
    }

    /// Whether another requeue is allowed after recording one more failure.
    #[must_use]
    pub const fn should_retry_after_failure(self, current_retry_count: u32) -> bool {
        self.max_retries > 0 && current_retry_count.saturating_add(1) <= self.max_retries
    }

    /// Whether the next failure should dead-letter rather than requeue.
    #[must_use]
    pub const fn should_dead_letter_after_failure(self, current_retry_count: u32) -> bool {
        let next = current_retry_count.saturating_add(1);
        self.max_retries == 0 || next > self.max_retries || next >= self.dead_letter_after
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff_ms: 1_000,
            dead_letter_after: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RetryPolicy;

    #[test]
    fn no_retries_never_requeues_and_dead_letters_immediately() {
        let policy = RetryPolicy::no_retries();
        assert!(!policy.should_retry_after_failure(0));
        assert!(policy.should_dead_letter_after_failure(0));
    }

    #[test]
    fn default_policy_requeues_until_max_then_dead_letters() {
        let policy = RetryPolicy::default();
        assert!(policy.should_retry_after_failure(0));
        assert!(policy.should_retry_after_failure(2));
        assert!(!policy.should_retry_after_failure(3));
        assert!(policy.should_dead_letter_after_failure(3));
    }
}
