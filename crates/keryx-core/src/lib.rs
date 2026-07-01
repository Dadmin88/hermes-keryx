//! Pure domain model for Hermes Keryx.
//!
//! `keryx-core` defines task lifecycle, identity, and agent-card types without
//! networking, persistence, daemon, or filesystem concerns.

pub mod agent_card;
pub mod error;
pub mod peer_id;
pub mod task;
pub mod task_handle;

pub use agent_card::{AgentCard, AgentId, CapabilityId, Skill};
pub use error::{CoreResult, KeryxCoreError, ValidationError};
pub use peer_id::{NodeId, PeerId};
pub use task::{
    event_for_transition, is_legal_transition, validate_transition, KeryxEventType, Task, TaskId,
    TaskStatus, TaskTransition,
};
pub use task_handle::{AttemptId, CorrelationId, IdempotencyKey, LeaseId, RouteId, TaskHandle};
