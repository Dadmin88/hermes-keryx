//! Pure domain model for Hermes Keryx.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_ID_LEN: usize = 128;

fn validate_id(kind: &'static str, value: &str) -> Result<String, ValidationError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ValidationError::MissingIdValue { kind });
    }
    if trimmed.len() > MAX_ID_LEN || !trimmed.chars().all(is_allowed_id_char) {
        return Err(ValidationError::InvalidIdValue {
            kind,
            value: value.to_string(),
        });
    }
    Ok(trimmed.to_string())
}

const fn is_allowed_id_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':')
}

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> Result<Self, ValidationError> {
                validate_id(stringify!($name), value.as_ref()).map(Self)
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = ValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

id_type!(AgentId);
id_type!(NodeId);
id_type!(CapabilityId);
id_type!(TaskId);
id_type!(CorrelationId);
id_type!(IdempotencyKey);
id_type!(RouteId);
id_type!(LeaseId);
id_type!(AttemptId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Created,
    Accepted,
    Queued,
    AwaitingApproval,
    Leased,
    Running,
    AwaitingInput,
    Completed,
    Failed,
    Canceled,
    TimedOut,
    Rejected,
    DeadLettered,
}

impl TaskStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Failed
                | Self::Canceled
                | Self::TimedOut
                | Self::Rejected
                | Self::DeadLettered
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeryxEventType {
    TaskAccepted,
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
    RecoveryAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTransition {
    pub from: TaskStatus,
    pub to: TaskStatus,
    pub event_type: KeryxEventType,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("missing {kind} value")]
    MissingIdValue { kind: &'static str },
    #[error("invalid {kind} value: {value:?}")]
    InvalidIdValue { kind: &'static str, value: String },
    #[error("invalid task state transition: {from:?} -> {to:?}")]
    InvalidTaskTransition { from: TaskStatus, to: TaskStatus },
    #[error("terminal task state cannot transition: {from:?} -> {to:?}")]
    TerminalTaskTransition { from: TaskStatus, to: TaskStatus },
    #[error("task id is required")]
    MissingTaskId,
    #[error("idempotency key is required")]
    MissingIdempotencyKey,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum KeryxCoreError {
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("state transition error: {0}")]
    StateTransition(String),
    #[error("routing error: {0}")]
    Routing(String),
    #[error("agent not found: {0}")]
    AgentNotFound(String),
    #[error("capability not found: {0}")]
    CapabilityNotFound(String),
    #[error("task not found: {0}")]
    TaskNotFound(String),
    #[error("policy denied: {0}")]
    PolicyDenied(String),
    #[error("approval required: {0}")]
    ApprovalRequired(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

pub type CoreResult<T> = Result<T, ValidationError>;

#[must_use]
pub fn is_legal_transition(from: TaskStatus, to: TaskStatus) -> bool {
    matches!(
        (from, to),
        (TaskStatus::Created, TaskStatus::Accepted)
            | (TaskStatus::Accepted, TaskStatus::Queued)
            | (TaskStatus::Queued, TaskStatus::AwaitingApproval)
            | (TaskStatus::AwaitingApproval, TaskStatus::Queued)
            | (TaskStatus::AwaitingApproval, TaskStatus::Rejected)
            | (TaskStatus::Queued, TaskStatus::Leased)
            | (TaskStatus::Leased, TaskStatus::Running)
            | (TaskStatus::Running, TaskStatus::AwaitingInput)
            | (TaskStatus::AwaitingInput, TaskStatus::Running)
            | (TaskStatus::Running, TaskStatus::Completed)
            | (TaskStatus::Running, TaskStatus::Failed)
            | (TaskStatus::Running, TaskStatus::Canceled)
            | (TaskStatus::Running, TaskStatus::TimedOut)
            | (TaskStatus::Queued, TaskStatus::Canceled)
            | (TaskStatus::Queued, TaskStatus::DeadLettered)
            | (TaskStatus::Leased, TaskStatus::TimedOut)
    )
}

pub fn event_for_transition(from: TaskStatus, to: TaskStatus) -> CoreResult<KeryxEventType> {
    if !is_legal_transition(from, to) {
        return Err(if from.is_terminal() {
            ValidationError::TerminalTaskTransition { from, to }
        } else {
            ValidationError::InvalidTaskTransition { from, to }
        });
    }

    let event_type = match (from, to) {
        (TaskStatus::Created, TaskStatus::Accepted) => KeryxEventType::TaskAccepted,
        (TaskStatus::Accepted, TaskStatus::Queued) => KeryxEventType::TaskQueued,
        (TaskStatus::Queued, TaskStatus::AwaitingApproval) => KeryxEventType::TaskApprovalRequested,
        (TaskStatus::AwaitingApproval, TaskStatus::Queued) => KeryxEventType::TaskApprovalGranted,
        (TaskStatus::AwaitingApproval, TaskStatus::Rejected) => KeryxEventType::TaskApprovalDenied,
        (TaskStatus::Queued, TaskStatus::Leased) => KeryxEventType::TaskLeased,
        (TaskStatus::Leased, TaskStatus::Running) => KeryxEventType::TaskStarted,
        (TaskStatus::Running, TaskStatus::AwaitingInput) => KeryxEventType::TaskAwaitingInput,
        (TaskStatus::AwaitingInput, TaskStatus::Running) => KeryxEventType::TaskStarted,
        (TaskStatus::Running, TaskStatus::Completed) => KeryxEventType::TaskCompleted,
        (TaskStatus::Running, TaskStatus::Failed) => KeryxEventType::TaskFailed,
        (TaskStatus::Running, TaskStatus::Canceled)
        | (TaskStatus::Queued, TaskStatus::Canceled) => KeryxEventType::TaskCanceled,
        (TaskStatus::Running, TaskStatus::TimedOut)
        | (TaskStatus::Leased, TaskStatus::TimedOut) => KeryxEventType::TaskTimedOut,
        (TaskStatus::Queued, TaskStatus::DeadLettered) => KeryxEventType::TaskDeadLettered,
        _ => unreachable!("guarded by is_legal_transition"),
    };

    Ok(event_type)
}

pub fn validate_transition(from: TaskStatus, to: TaskStatus) -> CoreResult<TaskTransition> {
    let event_type = event_for_transition(from, to)?;
    Ok(TaskTransition {
        from,
        to,
        event_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEGAL_TRANSITIONS: &[(TaskStatus, TaskStatus, KeryxEventType)] = &[
        (
            TaskStatus::Created,
            TaskStatus::Accepted,
            KeryxEventType::TaskAccepted,
        ),
        (
            TaskStatus::Accepted,
            TaskStatus::Queued,
            KeryxEventType::TaskQueued,
        ),
        (
            TaskStatus::Queued,
            TaskStatus::AwaitingApproval,
            KeryxEventType::TaskApprovalRequested,
        ),
        (
            TaskStatus::AwaitingApproval,
            TaskStatus::Queued,
            KeryxEventType::TaskApprovalGranted,
        ),
        (
            TaskStatus::AwaitingApproval,
            TaskStatus::Rejected,
            KeryxEventType::TaskApprovalDenied,
        ),
        (
            TaskStatus::Queued,
            TaskStatus::Leased,
            KeryxEventType::TaskLeased,
        ),
        (
            TaskStatus::Leased,
            TaskStatus::Running,
            KeryxEventType::TaskStarted,
        ),
        (
            TaskStatus::Running,
            TaskStatus::AwaitingInput,
            KeryxEventType::TaskAwaitingInput,
        ),
        (
            TaskStatus::AwaitingInput,
            TaskStatus::Running,
            KeryxEventType::TaskStarted,
        ),
        (
            TaskStatus::Running,
            TaskStatus::Completed,
            KeryxEventType::TaskCompleted,
        ),
        (
            TaskStatus::Running,
            TaskStatus::Failed,
            KeryxEventType::TaskFailed,
        ),
        (
            TaskStatus::Running,
            TaskStatus::Canceled,
            KeryxEventType::TaskCanceled,
        ),
        (
            TaskStatus::Running,
            TaskStatus::TimedOut,
            KeryxEventType::TaskTimedOut,
        ),
        (
            TaskStatus::Queued,
            TaskStatus::Canceled,
            KeryxEventType::TaskCanceled,
        ),
        (
            TaskStatus::Queued,
            TaskStatus::DeadLettered,
            KeryxEventType::TaskDeadLettered,
        ),
        (
            TaskStatus::Leased,
            TaskStatus::TimedOut,
            KeryxEventType::TaskTimedOut,
        ),
    ];

    #[test]
    fn legal_transitions_produce_typed_events() {
        for (from, to, event_type) in LEGAL_TRANSITIONS {
            let transition = validate_transition(*from, *to).expect("legal transition should pass");
            assert_eq!(transition.event_type, *event_type);
        }
    }

    #[test]
    fn illegal_transition_is_rejected() {
        let err = validate_transition(TaskStatus::Created, TaskStatus::Completed).unwrap_err();
        assert_eq!(
            err,
            ValidationError::InvalidTaskTransition {
                from: TaskStatus::Created,
                to: TaskStatus::Completed,
            }
        );
    }

    #[test]
    fn terminal_status_cannot_mutate() {
        let err = validate_transition(TaskStatus::Completed, TaskStatus::Queued).unwrap_err();
        assert_eq!(
            err,
            ValidationError::TerminalTaskTransition {
                from: TaskStatus::Completed,
                to: TaskStatus::Queued,
            }
        );
    }

    #[test]
    fn terminal_detection_covers_all_terminal_states() {
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Canceled.is_terminal());
        assert!(TaskStatus::TimedOut.is_terminal());
        assert!(TaskStatus::Rejected.is_terminal());
        assert!(TaskStatus::DeadLettered.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
    }
}
