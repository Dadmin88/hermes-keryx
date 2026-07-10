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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskCancellationEventType {
    CancelRequested,
    Canceled,
}

impl TaskCancellationEventType {
    #[must_use]
    pub const fn lifecycle_event_type(self) -> Option<KeryxEventType> {
        match self {
            Self::CancelRequested => None,
            Self::Canceled => Some(KeryxEventType::TaskCanceled),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequested {
    pub task_id: TaskId,
    pub status: TaskStatus,
}

impl CancelRequested {
    pub fn new(task_id: TaskId, status: TaskStatus) -> Result<Self, KeryxCoreError> {
        if is_cancel_applicable(status) {
            Ok(Self { task_id, status })
        } else {
            Err(KeryxCoreError::Validation(
                ValidationError::CancelNotApplicable { status },
            ))
        }
    }

    #[must_use]
    pub const fn event_type(&self) -> TaskCancellationEventType {
        TaskCancellationEventType::CancelRequested
    }

    #[must_use]
    pub const fn lifecycle_event_type(&self) -> Option<KeryxEventType> {
        self.event_type().lifecycle_event_type()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Canceled {
    pub task_id: TaskId,
    pub transition: TaskTransition,
}

impl Canceled {
    pub fn new(task_id: TaskId, from: TaskStatus) -> Result<Self, KeryxCoreError> {
        let transition = validate_cancel_transition(from)?;
        Ok(Self {
            task_id,
            transition,
        })
    }

    #[must_use]
    pub const fn event_type(&self) -> TaskCancellationEventType {
        TaskCancellationEventType::Canceled
    }

    #[must_use]
    pub const fn lifecycle_event_type(&self) -> Option<KeryxEventType> {
        self.event_type().lifecycle_event_type()
    }
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
    #[serde(default)]
    retry_count: u32,
    #[serde(default)]
    dead_lettered: bool,
}

impl Task {
    pub fn new(id: TaskId) -> Self {
        Self {
            id,
            status: TaskStatus::Pending,
            assignee: None,
            retry_count: 0,
            dead_lettered: false,
        }
    }

    pub fn with_assignee(id: TaskId, assignee: PeerId) -> Self {
        Self {
            id,
            status: TaskStatus::Pending,
            assignee: Some(assignee),
            retry_count: 0,
            dead_lettered: false,
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

    #[must_use]
    pub const fn retry_count(&self) -> u32 {
        self.retry_count
    }

    #[must_use]
    pub const fn dead_lettered(&self) -> bool {
        self.dead_lettered
    }

    pub fn set_retry_count(&mut self, retry_count: u32) {
        self.retry_count = retry_count;
    }

    pub fn set_dead_lettered(&mut self, dead_lettered: bool) {
        self.dead_lettered = dead_lettered;
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

    pub fn request_cancel(&self) -> Result<CancelRequested, KeryxCoreError> {
        CancelRequested::new(self.id.clone(), self.status)
    }

    pub fn mark_canceled(&mut self) -> Result<TaskTransition, KeryxCoreError> {
        let transition = validate_cancel_transition(self.status)?;
        self.status = transition.to;
        Ok(transition)
    }

    pub fn cancel(&mut self) -> Result<Canceled, KeryxCoreError> {
        let canceled = Canceled::new(self.id.clone(), self.status)?;
        self.status = canceled.transition.to;
        Ok(canceled)
    }
}

#[must_use]
pub const fn is_cancel_applicable(status: TaskStatus) -> bool {
    matches!(status, TaskStatus::Pending | TaskStatus::Running)
}

pub fn event_for_cancel_transition(from: TaskStatus) -> Result<KeryxEventType, KeryxCoreError> {
    if is_cancel_applicable(from) {
        Ok(KeryxEventType::TaskCanceled)
    } else {
        Err(KeryxCoreError::Validation(
            ValidationError::CancelNotApplicable { status: from },
        ))
    }
}

pub fn validate_cancel_transition(from: TaskStatus) -> Result<TaskTransition, KeryxCoreError> {
    let event_type = event_for_cancel_transition(from)?;

    Ok(TaskTransition {
        from,
        to: TaskStatus::Failed,
        event_type,
    })
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
