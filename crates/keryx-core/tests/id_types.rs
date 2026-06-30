use std::str::FromStr;

use keryx_core::{AgentId, CapabilityId, IdempotencyKey, NodeId, TaskId, ValidationError};

#[test]
fn id_types_parse_display_and_serialize_as_strings() {
    let node = NodeId::from_str("node.local-1").expect("valid node id");
    let json = serde_json::to_string(&node).expect("serialize");

    assert_eq!(node.to_string(), "node.local-1");
    assert_eq!(json, "\"node.local-1\"");
    assert_eq!(serde_json::from_str::<NodeId>(&json).unwrap(), node);
}

#[test]
fn all_core_id_types_reject_empty_values() {
    assert_eq!(
        AgentId::from_str("").unwrap_err(),
        ValidationError::MissingIdValue { kind: "AgentId" }
    );
    assert_eq!(
        CapabilityId::from_str("   ").unwrap_err(),
        ValidationError::MissingIdValue {
            kind: "CapabilityId"
        }
    );
    assert_eq!(
        TaskId::from_str("").unwrap_err(),
        ValidationError::MissingIdValue { kind: "TaskId" }
    );
    assert_eq!(
        IdempotencyKey::from_str("").unwrap_err(),
        ValidationError::MissingIdValue {
            kind: "IdempotencyKey"
        }
    );
}

#[test]
fn id_types_reject_control_characters_and_slashes() {
    assert!(matches!(
        NodeId::from_str("node/one"),
        Err(ValidationError::InvalidIdValue { .. })
    ));
    assert!(matches!(
        TaskId::from_str("task\n1"),
        Err(ValidationError::InvalidIdValue { .. })
    ));
}
