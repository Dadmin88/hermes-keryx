//! Configurable resource limits for Keryx daemons.
//!
//! Pure domain types — no I/O, no store, no network.

use serde::{Deserialize, Serialize};

/// Default maximum pending (queued) tasks in the local store.
pub const DEFAULT_MAX_PENDING_TASKS: u64 = 10_000;

/// Default maximum bytes for a SubmitTask envelope (task payload).
pub const DEFAULT_MAX_ENVELOPE_BYTES: u64 = 4 * 1024 * 1024; // 4 MiB

/// Identifies which limit was exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    /// Number of pending tasks exceeds `max_pending_tasks`.
    PendingTasks,
    /// Submit envelope byte size exceeds `max_envelope_bytes`.
    EnvelopeBytes,
}

impl LimitKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PendingTasks => "pending_tasks",
            Self::EnvelopeBytes => "envelope_bytes",
        }
    }
}

impl std::fmt::Display for LimitKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Configurable daemon resource limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitsConfig {
    /// Maximum number of tasks in `Pending` status. 0 = unlimited.
    pub max_pending_tasks: u64,
    /// Maximum byte size for a SubmitTask envelope. 0 = unlimited.
    pub max_envelope_bytes: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_pending_tasks: DEFAULT_MAX_PENDING_TASKS,
            max_envelope_bytes: DEFAULT_MAX_ENVELOPE_BYTES,
        }
    }
}

impl LimitsConfig {
    /// No limits — everything passes.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            max_pending_tasks: 0,
            max_envelope_bytes: 0,
        }
    }

    /// Check whether the pending task count is already at or over the configured limit.
    pub const fn check_pending_tasks(&self, current: u64) -> Result<(), LimitExceeded> {
        if self.max_pending_tasks > 0 && current >= self.max_pending_tasks {
            Err(LimitExceeded {
                kind: LimitKind::PendingTasks,
                current,
                max: self.max_pending_tasks,
            })
        } else {
            Ok(())
        }
    }

    /// Check whether an envelope byte size exceeds the configured limit.
    pub const fn check_envelope_bytes(&self, byte_len: u64) -> Result<(), LimitExceeded> {
        if self.max_envelope_bytes > 0 && byte_len > self.max_envelope_bytes {
            Err(LimitExceeded {
                kind: LimitKind::EnvelopeBytes,
                current: byte_len,
                max: self.max_envelope_bytes,
            })
        } else {
            Ok(())
        }
    }
}

/// Error returned when a configurable limit is exceeded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitExceeded {
    pub kind: LimitKind,
    pub current: u64,
    pub max: u64,
}

impl std::fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "limit exceeded: {} current={} max={}",
            self.kind, self.current, self.max
        )
    }
}

impl std::error::Error for LimitExceeded {}

impl From<LimitExceeded> for crate::ValidationError {
    fn from(value: LimitExceeded) -> Self {
        Self::LimitExceeded {
            kind: value.kind.as_str().to_owned(),
            current: value.current,
            max: value.max,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limits_match_phase_10_contract() {
        let limits = LimitsConfig::default();

        assert_eq!(limits.max_pending_tasks, 10_000);
        assert_eq!(limits.max_envelope_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn zero_limits_are_unlimited() {
        let limits = LimitsConfig::unlimited();

        assert!(limits.check_pending_tasks(u64::MAX).is_ok());
        assert!(limits.check_envelope_bytes(u64::MAX).is_ok());
    }

    #[test]
    fn pending_limit_rejects_when_queue_is_at_capacity() {
        let limits = LimitsConfig {
            max_pending_tasks: 2,
            max_envelope_bytes: 0,
        };

        assert_eq!(limits.check_pending_tasks(0), Ok(()));
        assert_eq!(limits.check_pending_tasks(1), Ok(()));
        assert_eq!(
            limits.check_pending_tasks(2),
            Err(LimitExceeded {
                kind: LimitKind::PendingTasks,
                current: 2,
                max: 2,
            })
        );
    }

    #[test]
    fn envelope_limit_rejects_only_when_size_exceeds_limit() {
        let limits = LimitsConfig {
            max_pending_tasks: 0,
            max_envelope_bytes: 4,
        };

        assert_eq!(limits.check_envelope_bytes(4), Ok(()));
        assert_eq!(
            limits.check_envelope_bytes(5),
            Err(LimitExceeded {
                kind: LimitKind::EnvelopeBytes,
                current: 5,
                max: 4,
            })
        );
    }
}
