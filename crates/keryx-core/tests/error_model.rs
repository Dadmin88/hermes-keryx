use keryx_core::{KeryxCoreError, TaskStatus};

#[test]
fn core_error_model_has_task_and_policy_variants() {
    let missing = KeryxCoreError::TaskNotFound("task-1".to_string());
    let policy = KeryxCoreError::PolicyDenied("capability requires approval".to_string());

    assert!(missing.to_string().contains("task-1"));
    assert!(policy.to_string().contains("capability requires approval"));
}

#[test]
fn state_transition_errors_promote_to_core_error() {
    let err =
        keryx_core::validate_transition(TaskStatus::Completed, TaskStatus::Queued).unwrap_err();
    let core: KeryxCoreError = err.into();

    assert!(matches!(core, KeryxCoreError::Validation(_)));
}
