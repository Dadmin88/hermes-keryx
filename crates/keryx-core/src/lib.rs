//! Pure domain model for Hermes Keryx.
//!
//! `keryx-core` defines task lifecycle, identity, and agent-card types without
//! networking, persistence, daemon, or filesystem concerns.

pub mod agent_card;
pub mod artifact;
pub mod error;
pub mod legacy;
pub mod limits;
pub mod peer_id;
pub mod retry_policy;
pub mod task;
pub mod task_handle;

pub use agent_card::{AgentCard, AgentId, CapabilityId, Skill};
pub use artifact::{
    should_inline, validate_artifact_size, ArtifactId, ArtifactMeta, Digest, MediaType,
    MAX_BLOB_BYTES, MAX_INLINE_ARTIFACT_BYTES,
};
pub use error::{
    CoreResult, KeryxCoreError, ValidationError, ValidationError::CancelNotApplicable,
};
pub use legacy::{
    is_valid_operational_legacy, normalize_legacy_transition, CanonicalTransition, LegacyEventType,
};
pub use limits::{
    LimitExceeded, LimitKind, LimitsConfig, DEFAULT_MAX_ENVELOPE_BYTES, DEFAULT_MAX_PENDING_TASKS,
};
pub use peer_id::{NodeId, PeerId};
pub use retry_policy::RetryPolicy;
pub use task::TaskCancellationEventType::{
    CancelRequested as CancelRequestedEventType, Canceled as CanceledEventType,
};
pub use task::{
    event_for_cancel_transition, event_for_transition, is_cancel_applicable, is_legal_transition,
    validate_cancel_transition, validate_transition, CancelRequested, Canceled, KeryxEventType,
    Task, TaskCancellationEventType, TaskId, TaskStatus, TaskTransition,
};
pub use task_handle::{AttemptId, CorrelationId, IdempotencyKey, LeaseId, RouteId, TaskHandle};
