//! Protobuf-generated Hermes Keryx protocol types.

pub mod v1 {
    tonic::include_proto!("hermes.keryx.v1");
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::v1::{
        KeryxEventType, NodescaleIdentityChallengeV1, NodescaleIdentityChallengeV2, TaskStatus,
    };

    #[test]
    fn generated_task_status_values_are_available() {
        assert_eq!(TaskStatus::Created as i32, 1);
        assert_eq!(TaskStatus::DeadLettered as i32, 13);
    }

    #[test]
    fn generated_event_type_values_are_available() {
        assert_eq!(KeryxEventType::TaskCreated as i32, 10);
        assert_eq!(KeryxEventType::RecoveryAction as i32, 40);
    }

    #[test]
    fn nodescale_challenge_v1_wire_is_frozen_and_v2_owns_provider_binding_tag_four() {
        let v1 = NodescaleIdentityChallengeV1 {
            operation_id: "op".into(),
            network_id: "net".into(),
            device_id: "dev".into(),
            join_session_id: "session".into(),
            agent_version: "v1".into(),
        };
        assert_eq!(
            v1.encode_to_vec(),
            vec![
                0x0a, 0x02, b'o', b'p', 0x12, 0x03, b'n', b'e', b't', 0x1a, 0x03, b'd', b'e', b'v',
                0x22, 0x07, b's', b'e', b's', b's', b'i', b'o', b'n', 0x2a, 0x02, b'v', b'1',
            ]
        );

        let v2 = NodescaleIdentityChallengeV2 {
            operation_id: "op".into(),
            network_id: "net".into(),
            device_id: "dev".into(),
            provider_binding_id: "provider".into(),
            agent_version: "v2".into(),
        };
        let v2_bytes = v2.encode_to_vec();
        assert!(v2_bytes
            .windows(10)
            .any(|window| window == [0x22, 0x08, b'p', b'r', b'o', b'v', b'i', b'd', b'e', b'r']));
        assert_ne!(v2_bytes, v1.encode_to_vec());
    }
}
