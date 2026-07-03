//! Legacy event compatibility for richer proto / relay event types.
//!
//! The strict four-state lifecycle persists only `pending`, `running`, `completed`, and
//! `failed`. Older clients may still emit operational or historical event names. This module
//! maps those inputs onto canonical lifecycle transitions or validated no-op operational
//! events.
//!
//! ## Lifecycle collapse (returns [`CanonicalTransition`])
//!
//! | Legacy event | Required `from` | Canonical `to` | Canonical `event_type` |
//! | --- | --- | --- | --- |
//! | `TaskLeased` | `Pending` | `Running` | `TaskStarted` |
//! | `TaskStarted` | `Pending` | `Running` | `TaskStarted` |
//! | `TaskCompleted` | `Running` | `Completed` | `TaskCompleted` |
//! | `TaskFailed` | `Running` | `Failed` | `TaskFailed` |
//! | `TaskCanceled` | `Running` | `Failed` | `TaskFailed` |
//! | `TaskTimedOut` | `Running` | `Failed` | `TaskFailed` |
//! | `TaskDeadLettered` | `Running` | `Failed` | `TaskFailed` |
//! | `TaskApprovalDenied` | `Pending` | `Failed` | `TaskFailed` |
//!
//! ## Operational (no lifecycle transition; [`normalize_legacy_transition`] returns `None`)
//!
//! | Legacy event | Required `from` | Status after append |
//! | --- | --- | --- |
//! | `TaskQueued` | `Pending` | `Pending` |
//! | `TaskApprovalRequested` | `Pending` | `Pending` |
//! | `TaskApprovalGranted` | `Pending` | `Pending` |
//! | `TaskAwaitingInput` | `Running` | `Running` |
//!
//! Any other `(from_status, legacy_event)` pair is rejected by the store.

use crate::task::{KeryxEventType, TaskStatus, TaskTransition};

/// Richer historical / proto event types accepted by the compatibility layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LegacyEventType {
    TaskQueued,
    TaskApprovalRequested,
    TaskApprovalGranted,
    TaskApprovalDenied,
    TaskLeased,
    TaskStarted,
    TaskAwaitingInput,
    TaskCompleted,
    TaskFailed,
    TaskCanceled,
    TaskTimedOut,
    TaskDeadLettered,
}

/// Canonical lifecycle transition produced after normalizing a legacy lifecycle event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalTransition {
    pub from: TaskStatus,
    pub to: TaskStatus,
    pub event_type: KeryxEventType,
}

impl CanonicalTransition {
    #[must_use]
    pub const fn into_task_transition(self) -> TaskTransition {
        TaskTransition {
            from: self.from,
            to: self.to,
            event_type: self.event_type,
        }
    }
}

impl LegacyEventType {
    #[must_use]
    pub const fn as_keryx_event_type(self) -> KeryxEventType {
        match self {
            Self::TaskQueued => KeryxEventType::TaskQueued,
            Self::TaskApprovalRequested => KeryxEventType::TaskApprovalRequested,
            Self::TaskApprovalGranted => KeryxEventType::TaskApprovalGranted,
            Self::TaskApprovalDenied => KeryxEventType::TaskApprovalDenied,
            Self::TaskLeased => KeryxEventType::TaskLeased,
            Self::TaskStarted => KeryxEventType::TaskStarted,
            Self::TaskAwaitingInput => KeryxEventType::TaskAwaitingInput,
            Self::TaskCompleted => KeryxEventType::TaskCompleted,
            Self::TaskFailed => KeryxEventType::TaskFailed,
            Self::TaskCanceled => KeryxEventType::TaskCanceled,
            Self::TaskTimedOut => KeryxEventType::TaskTimedOut,
            Self::TaskDeadLettered => KeryxEventType::TaskDeadLettered,
        }
    }

    pub fn from_keryx_event_type(event_type: KeryxEventType) -> Option<Self> {
        match event_type {
            KeryxEventType::TaskQueued => Some(Self::TaskQueued),
            KeryxEventType::TaskApprovalRequested => Some(Self::TaskApprovalRequested),
            KeryxEventType::TaskApprovalGranted => Some(Self::TaskApprovalGranted),
            KeryxEventType::TaskApprovalDenied => Some(Self::TaskApprovalDenied),
            KeryxEventType::TaskLeased => Some(Self::TaskLeased),
            KeryxEventType::TaskStarted => Some(Self::TaskStarted),
            KeryxEventType::TaskAwaitingInput => Some(Self::TaskAwaitingInput),
            KeryxEventType::TaskCompleted => Some(Self::TaskCompleted),
            KeryxEventType::TaskFailed => Some(Self::TaskFailed),
            KeryxEventType::TaskCanceled => Some(Self::TaskCanceled),
            KeryxEventType::TaskTimedOut => Some(Self::TaskTimedOut),
            KeryxEventType::TaskDeadLettered => Some(Self::TaskDeadLettered),
            KeryxEventType::TaskAccepted | KeryxEventType::RecoveryAction => None,
        }
    }
}

/// Maps a legacy lifecycle event to the canonical transition, if one applies.
///
/// Returns `None` for operational legacy events (status unchanged) and for pairs that are
/// not listed in this module's compatibility tables.
#[must_use]
pub fn normalize_legacy_transition(
    from_status: TaskStatus,
    legacy_event: LegacyEventType,
) -> Option<CanonicalTransition> {
    match (from_status, legacy_event) {
        (TaskStatus::Pending, LegacyEventType::TaskLeased | LegacyEventType::TaskStarted) => {
            Some(CanonicalTransition {
                from: TaskStatus::Pending,
                to: TaskStatus::Running,
                event_type: KeryxEventType::TaskStarted,
            })
        }
        (TaskStatus::Running, LegacyEventType::TaskCompleted) => Some(CanonicalTransition {
            from: TaskStatus::Running,
            to: TaskStatus::Completed,
            event_type: KeryxEventType::TaskCompleted,
        }),
        (
            TaskStatus::Running,
            LegacyEventType::TaskFailed
            | LegacyEventType::TaskCanceled
            | LegacyEventType::TaskTimedOut
            | LegacyEventType::TaskDeadLettered,
        ) => Some(CanonicalTransition {
            from: TaskStatus::Running,
            to: TaskStatus::Failed,
            event_type: KeryxEventType::TaskFailed,
        }),
        (TaskStatus::Pending, LegacyEventType::TaskApprovalDenied) => Some(CanonicalTransition {
            from: TaskStatus::Pending,
            to: TaskStatus::Failed,
            event_type: KeryxEventType::TaskFailed,
        }),
        _ => None,
    }
}

/// Whether `(from_status, legacy_event)` is a valid operational legacy append (no status change).
#[must_use]
pub fn is_valid_operational_legacy(from_status: TaskStatus, legacy_event: LegacyEventType) -> bool {
    matches!(
        (from_status, legacy_event),
        (TaskStatus::Pending, LegacyEventType::TaskQueued)
            | (TaskStatus::Pending, LegacyEventType::TaskApprovalRequested)
            | (TaskStatus::Pending, LegacyEventType::TaskApprovalGranted)
            | (TaskStatus::Running, LegacyEventType::TaskAwaitingInput)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_leased_collapses_to_task_started_transition() {
        let normalized =
            normalize_legacy_transition(TaskStatus::Pending, LegacyEventType::TaskLeased)
                .expect("pending lease maps to running");
        assert_eq!(normalized.from, TaskStatus::Pending);
        assert_eq!(normalized.to, TaskStatus::Running);
        assert_eq!(normalized.event_type, KeryxEventType::TaskStarted);
    }

    #[test]
    fn terminal_legacy_failures_map_to_task_failed() {
        for legacy in [
            LegacyEventType::TaskCanceled,
            LegacyEventType::TaskTimedOut,
            LegacyEventType::TaskDeadLettered,
        ] {
            let normalized =
                normalize_legacy_transition(TaskStatus::Running, legacy).expect("running failure");
            assert_eq!(normalized.to, TaskStatus::Failed);
            assert_eq!(normalized.event_type, KeryxEventType::TaskFailed);
        }
    }

    #[test]
    fn operational_legacy_returns_none_for_normalize() {
        assert!(
            normalize_legacy_transition(TaskStatus::Pending, LegacyEventType::TaskQueued).is_none()
        );
        assert!(is_valid_operational_legacy(
            TaskStatus::Pending,
            LegacyEventType::TaskQueued
        ));
    }

    #[test]
    fn unknown_combinations_are_not_operational_and_do_not_normalize() {
        assert!(
            normalize_legacy_transition(TaskStatus::Running, LegacyEventType::TaskQueued).is_none()
        );
        assert!(!is_valid_operational_legacy(
            TaskStatus::Running,
            LegacyEventType::TaskQueued
        ));
        assert!(
            normalize_legacy_transition(TaskStatus::Pending, LegacyEventType::TaskCompleted)
                .is_none()
        );
    }
}
