use std::str::FromStr;

use keryx_core::{
    event_for_cancel_transition, validate_cancel_transition, validate_transition, AgentCard,
    AgentId, CancelRequested, Canceled, CapabilityId, KeryxCoreError, KeryxEventType, PeerId,
    Skill, Task, TaskCancellationEventType, TaskId, TaskStatus,
};

fn assert_terminal_transition_error(
    result: Result<keryx_core::TaskTransition, KeryxCoreError>,
    from: TaskStatus,
    to: TaskStatus,
) {
    assert_eq!(
        result.unwrap_err(),
        KeryxCoreError::Validation(keryx_core::ValidationError::TerminalTaskTransition {
            from,
            to,
        })
    );
}

#[test]
fn task_defaults_to_pending_on_construction() {
    let task_id = TaskId::from_str("task-1").expect("valid task id");
    let task = Task::new(task_id.clone());

    assert_eq!(task.id(), &task_id);
    assert_eq!(task.status(), TaskStatus::Pending);
    assert_eq!(task.assignee(), None);
}

#[test]
fn pending_running_completed_lifecycle_is_allowed() {
    let mut task = Task::with_assignee(
        TaskId::from_str("task-2").expect("valid task id"),
        PeerId::from_str("peer-1").expect("valid peer id"),
    );

    assert_eq!(
        task.mark_running().expect("pending -> running").to,
        TaskStatus::Running
    );
    assert_eq!(
        task.mark_completed().expect("running -> completed").to,
        TaskStatus::Completed
    );
}

#[test]
fn pending_running_failed_lifecycle_is_allowed() {
    let mut task = Task::new(TaskId::from_str("task-3").expect("valid task id"));

    task.mark_running().expect("pending -> running");
    assert_eq!(
        task.mark_failed().expect("running -> failed").to,
        TaskStatus::Failed
    );
}

#[test]
fn invalid_and_terminal_lifecycle_violations_use_validation_wrapped_core_error() {
    let err = Task::new(TaskId::from_str("task-4").expect("valid task id"))
        .transition_to(TaskStatus::Completed)
        .unwrap_err();
    assert_eq!(
        err,
        KeryxCoreError::Validation(keryx_core::ValidationError::InvalidTaskTransition {
            from: TaskStatus::Pending,
            to: TaskStatus::Completed,
        })
    );

    let err = validate_transition(TaskStatus::Failed, TaskStatus::Pending).unwrap_err();
    assert_eq!(
        err,
        KeryxCoreError::Validation(keryx_core::ValidationError::TerminalTaskTransition {
            from: TaskStatus::Failed,
            to: TaskStatus::Pending,
        })
    );
}

#[test]
fn completed_task_rejects_all_subsequent_transitions() {
    let mut task = Task::new(TaskId::from_str("task-4-completed").expect("valid task id"));
    task.mark_running().expect("pending -> running");
    task.mark_completed().expect("running -> completed");

    assert_terminal_transition_error(
        task.mark_running(),
        TaskStatus::Completed,
        TaskStatus::Running,
    );
    assert_terminal_transition_error(
        task.mark_completed(),
        TaskStatus::Completed,
        TaskStatus::Completed,
    );
    assert_terminal_transition_error(
        task.mark_failed(),
        TaskStatus::Completed,
        TaskStatus::Failed,
    );
    assert_terminal_transition_error(
        task.transition_to(TaskStatus::Pending),
        TaskStatus::Completed,
        TaskStatus::Pending,
    );
}

#[test]
fn failed_task_rejects_all_subsequent_transitions() {
    let mut task = Task::new(TaskId::from_str("task-4-failed").expect("valid task id"));
    task.mark_running().expect("pending -> running");
    task.mark_failed().expect("running -> failed");

    assert_terminal_transition_error(task.mark_running(), TaskStatus::Failed, TaskStatus::Running);
    assert_terminal_transition_error(
        task.mark_completed(),
        TaskStatus::Failed,
        TaskStatus::Completed,
    );
    assert_terminal_transition_error(task.mark_failed(), TaskStatus::Failed, TaskStatus::Failed);
    assert_terminal_transition_error(
        task.transition_to(TaskStatus::Pending),
        TaskStatus::Failed,
        TaskStatus::Pending,
    );
}

#[test]
fn cancellation_request_is_an_operational_event_for_pending_and_running_tasks() {
    let task_id = TaskId::from_str("task-cancel-request").expect("valid task id");
    let mut task = Task::new(task_id.clone());

    let pending_request = task.request_cancel().expect("pending cancel request");
    assert_eq!(
        pending_request,
        CancelRequested::new(task_id.clone(), TaskStatus::Pending).unwrap()
    );
    assert_eq!(pending_request.task_id, task_id);
    assert_eq!(pending_request.status, TaskStatus::Pending);
    assert_eq!(
        pending_request.event_type(),
        TaskCancellationEventType::CancelRequested
    );
    assert_eq!(pending_request.lifecycle_event_type(), None);

    task.mark_running().expect("pending -> running");
    let running_request = task.request_cancel().expect("running cancel request");
    assert_eq!(running_request.status, TaskStatus::Running);
    assert_eq!(running_request.lifecycle_event_type(), None);
}

#[test]
fn cancellation_terminal_event_moves_pending_or_running_task_to_failed() {
    let pending_id = TaskId::from_str("task-cancel-pending").expect("valid task id");
    let mut pending = Task::new(pending_id.clone());
    let pending_canceled = pending.cancel().expect("pending cancel");
    assert_eq!(pending.status(), TaskStatus::Failed);
    assert_eq!(
        pending_canceled,
        Canceled::new(pending_id.clone(), TaskStatus::Pending).unwrap()
    );
    assert_eq!(pending_canceled.task_id, pending_id);
    assert_eq!(pending_canceled.transition.from, TaskStatus::Pending);
    assert_eq!(pending_canceled.transition.to, TaskStatus::Failed);
    assert_eq!(
        pending_canceled.transition.event_type,
        KeryxEventType::TaskCanceled
    );
    assert_eq!(
        pending_canceled.event_type(),
        TaskCancellationEventType::Canceled
    );
    assert_eq!(
        pending_canceled.lifecycle_event_type(),
        Some(KeryxEventType::TaskCanceled)
    );

    let running_id = TaskId::from_str("task-cancel-running").expect("valid task id");
    let mut running = Task::new(running_id);
    running.mark_running().expect("pending -> running");
    let running_transition = running.mark_canceled().expect("running cancel");
    assert_eq!(running.status(), TaskStatus::Failed);
    assert_eq!(running_transition.from, TaskStatus::Running);
    assert_eq!(running_transition.to, TaskStatus::Failed);
    assert_eq!(running_transition.event_type, KeryxEventType::TaskCanceled);
}

#[test]
fn cancellation_rejects_terminal_tasks() {
    let mut completed =
        Task::new(TaskId::from_str("task-cancel-completed").expect("valid task id"));
    completed.mark_running().expect("pending -> running");
    completed.mark_completed().expect("running -> completed");

    let err = completed.request_cancel().unwrap_err();
    assert_eq!(
        err,
        KeryxCoreError::Validation(keryx_core::ValidationError::CancelNotApplicable {
            status: TaskStatus::Completed,
        })
    );

    let err = completed.mark_canceled().unwrap_err();
    assert_eq!(
        err,
        KeryxCoreError::Validation(keryx_core::ValidationError::CancelNotApplicable {
            status: TaskStatus::Completed,
        })
    );
}

#[test]
fn cancellation_transition_helpers_emit_task_canceled_event() {
    assert_eq!(
        event_for_cancel_transition(TaskStatus::Pending).expect("pending cancel event"),
        KeryxEventType::TaskCanceled
    );
    let transition = validate_cancel_transition(TaskStatus::Running).expect("running cancel");
    assert_eq!(transition.from, TaskStatus::Running);
    assert_eq!(transition.to, TaskStatus::Failed);
    assert_eq!(transition.event_type, KeryxEventType::TaskCanceled);

    let err = event_for_cancel_transition(TaskStatus::Failed).unwrap_err();
    assert_eq!(
        err,
        KeryxCoreError::Validation(keryx_core::ValidationError::CancelNotApplicable {
            status: TaskStatus::Failed,
        })
    );
}

#[test]
fn agent_card_skill_identity_is_deduplicated_and_lookup_is_stable() {
    let mut card = AgentCard::new(
        AgentId::from_str("agent-1").expect("valid agent id"),
        "worker-alpha",
        "test worker",
    )
    .expect("valid card");
    let skill_id = CapabilityId::from_str("task.run").expect("valid capability id");

    assert!(card.add_skill(Skill::new(skill_id.clone())));
    assert!(!card.add_skill(Skill::new(skill_id.clone())));
    assert!(card.has_skill(&skill_id));
    assert_eq!(card.skill(&skill_id).expect("skill lookup").id(), &skill_id);
    assert_eq!(card.skills().len(), 1);
}

#[test]
fn core_error_variants_cover_lifecycle_and_existing_policy_paths() {
    let lifecycle = Task::new(TaskId::from_str("task-5").expect("valid task id"))
        .transition_to(TaskStatus::Completed)
        .unwrap_err();
    assert_eq!(
        lifecycle,
        KeryxCoreError::Validation(keryx_core::ValidationError::InvalidTaskTransition {
            from: TaskStatus::Pending,
            to: TaskStatus::Completed,
        })
    );

    let missing = KeryxCoreError::TaskNotFound("task-x".to_owned());
    let denied = KeryxCoreError::PolicyDenied("requires approval".to_owned());
    assert!(missing.to_string().contains("task-x"));
    assert!(denied.to_string().contains("requires approval"));
}
