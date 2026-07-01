use keryx_core::{KeryxCoreError, TaskStatus, ValidationError};

#[test]
fn core_error_model_has_task_and_policy_variants() {
    let missing = KeryxCoreError::TaskNotFound("task-1".to_string());
    let policy = KeryxCoreError::PolicyDenied("capability requires approval".to_string());

    assert!(missing.to_string().contains("task-1"));
    assert!(policy.to_string().contains("capability requires approval"));
}

#[test]
fn state_transition_errors_promote_to_validation_wrapped_core_error() {
    let err =
        keryx_core::validate_transition(TaskStatus::Completed, TaskStatus::Pending).unwrap_err();

    assert_eq!(
        err,
        KeryxCoreError::Validation(ValidationError::TerminalTaskTransition {
            from: TaskStatus::Completed,
            to: TaskStatus::Pending,
        })
    );
}
