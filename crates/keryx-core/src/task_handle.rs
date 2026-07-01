use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{
    error::{validate_identifier, ValidationError},
    AgentId, TaskId,
};

macro_rules! string_id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> Result<Self, ValidationError> {
                validate_identifier(stringify!($name), value).map(Self)
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

string_id_type!(IdempotencyKey);
string_id_type!(LeaseId);
string_id_type!(CorrelationId);
string_id_type!(RouteId);
string_id_type!(AttemptId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskHandle {
    task_id: TaskId,
    assignee: AgentId,
}

impl TaskHandle {
    pub fn new(task_id: TaskId, assignee: AgentId) -> Self {
        Self { task_id, assignee }
    }

    #[must_use]
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    #[must_use]
    pub fn assignee(&self) -> &AgentId {
        &self.assignee
    }
}
