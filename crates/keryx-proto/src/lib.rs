//! Protobuf-generated Hermes Keryx protocol types.

pub mod v1 {
    tonic::include_proto!("hermes.keryx.v1");
}

#[cfg(test)]
mod tests {
    use super::v1::{KeryxEventType, TaskStatus};

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
}
