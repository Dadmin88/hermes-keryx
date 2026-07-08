use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::task::TaskStatus;

const MAX_ID_LEN: usize = 128;

pub(crate) fn validate_identifier(
    kind: &'static str,
    value: impl AsRef<str>,
) -> Result<String, ValidationError> {
    let raw = value.as_ref();
    let trimmed = raw.trim();

    if trimmed.is_empty() {
        return Err(ValidationError::MissingIdValue { kind });
    }

    if trimmed.len() > MAX_ID_LEN || !trimmed.chars().all(is_allowed_id_char) {
        return Err(ValidationError::InvalidIdValue {
            kind,
            value: raw.to_string(),
        });
    }

    Ok(trimmed.to_owned())
}

const fn is_allowed_id_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':')
}

pub type CoreResult<T> = Result<T, ValidationError>;

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationError {
    #[error("missing {kind} value")]
    MissingIdValue { kind: &'static str },
    #[error("invalid {kind} value: {value:?}")]
    InvalidIdValue { kind: &'static str, value: String },
    #[error("artifact too large: {byte_len} bytes exceeds limit of {limit_bytes}")]
    ArtifactTooLarge { byte_len: u64, limit_bytes: u64 },
    #[error("invalid digest: {0}")]
    InvalidDigest(String),
    #[error("artifact not found: {0}")]
    ArtifactNotFound(String),
    #[error("limit exceeded: {kind} current={current} max={max}")]
    LimitExceeded {
        kind: String,
        current: u64,
        max: u64,
    },
    #[error("invalid task state transition: {from:?} -> {to:?}")]
    InvalidTaskTransition { from: TaskStatus, to: TaskStatus },
    #[error("task cancellation is not applicable for status: {status:?}")]
    CancelNotApplicable { status: TaskStatus },
    #[error("terminal task state cannot transition: {from:?} -> {to:?}")]
    TerminalTaskTransition { from: TaskStatus, to: TaskStatus },
}

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize)]
pub enum KeryxCoreError {
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("task not found: {0}")]
    TaskNotFound(String),
    #[error("policy denied: {0}")]
    PolicyDenied(String),
}
