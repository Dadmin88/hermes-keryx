use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{
    error::{validate_identifier, KeryxCoreError, ValidationError},
    PeerId,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(String);

impl TaskId {
    pub fn new(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        validate_identifier("TaskId", value).map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
}

impl TaskStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    id: TaskId,
    status: TaskStatus,
    assignee: Option<PeerId>,
}

impl Task {
    pub fn new(id: TaskId) -> Self {
        Self {
            id,
            status: TaskStatus::Pending,
            assignee: None,
        }
    }

    pub fn with_assignee(id: TaskId, assignee: PeerId) -> Self {
        Self {
            id,
            status: TaskStatus::Pending,
            assignee: Some(assignee),
        }
    }

    #[must_use]
    pub fn id(&self) -> &TaskId {
        &self.id
    }

    #[must_use]
    pub fn status(&self) -> TaskStatus {
        self.status
    }

    #[must_use]
    pub fn assignee(&self) -> Option<&PeerId> {
        self.assignee.as_ref()
    }

    pub fn transition_to(&mut self, next: TaskStatus) -> Result<TaskTransition, KeryxCoreError> {
        let transition = validate_transition(self.status, next)?;
        self.status = next;
        Ok(transition)
    }

    pub fn mark_running(&mut self) -> Result<TaskTransition, KeryxCoreError> {
        self.transition_to(TaskStatus::Running)
    }

    pub fn mark_completed(&mut self) -> Result<TaskTransition, KeryxCoreError> {
        self.transition_to(TaskStatus::Completed)
    }

    pub fn mark_failed(&mut self) -> Result<TaskTransition, KeryxCoreError> {
        self.transition_to(TaskStatus::Failed)
    }
}

#[must_use]
pub fn is_legal_transition(from: TaskStatus, to: TaskStatus) -> bool {
    matches!(
        (from, to),
        (TaskStatus::Pending, TaskStatus::Running)
            | (TaskStatus::Running, TaskStatus::Completed)
            | (TaskStatus::Running, TaskStatus::Failed)
    )
}

pub fn event_for_transition(
    from: TaskStatus,
    to: TaskStatus,
) -> Result<KeryxEventType, KeryxCoreError> {
    if !is_legal_transition(from, to) {
        return Err(KeryxCoreError::Validation(if from.is_terminal() {
            ValidationError::TerminalTaskTransition { from, to }
        } else {
            ValidationError::InvalidTaskTransition { from, to }
        }));
    }

    let event_type = match (from, to) {
        (TaskStatus::Pending, TaskStatus::Running) => KeryxEventType::TaskStarted,
        (TaskStatus::Running, TaskStatus::Completed) => KeryxEventType::TaskCompleted,
        (TaskStatus::Running, TaskStatus::Failed) => KeryxEventType::TaskFailed,
        _ => unreachable!("guarded by is_legal_transition"),
    };

    Ok(event_type)
}

pub fn validate_transition(
    from: TaskStatus,
    to: TaskStatus,
) -> Result<TaskTransition, KeryxCoreError> {
    let event_type = event_for_transition(from, to)?;

    Ok(TaskTransition {
        from,
        to,
        event_type,
    })
}
