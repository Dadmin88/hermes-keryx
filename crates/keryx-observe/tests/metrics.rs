use keryx_observe::{KeryxMetrics, MetricsSnapshot};

#[test]
fn counters_increment_independently() {
    let metrics = KeryxMetrics::new();
    metrics.increment_tasks_submitted();
    metrics.increment_tasks_submitted();
    metrics.increment_tasks_claimed();
    metrics.increment_heartbeats();
    metrics.increment_recovery_ticks();
    metrics.increment_dead_letters();

    let snap = metrics.snapshot();
    assert_eq!(
        snap,
        MetricsSnapshot {
            tasks_submitted: 2,
            tasks_claimed: 1,
            tasks_completed: 0,
            tasks_failed: 0,
            heartbeats: 1,
            leases_recovered: 0,
            recovery_ticks: 1,
            dead_letters: 1,
            active_leases: 1,
        }
    );
}

#[test]
fn active_leases_gauge_tracks_claim_complete_fail_and_recovery() {
    let metrics = KeryxMetrics::new();

    metrics.increment_tasks_claimed();
    metrics.increment_tasks_claimed();
    assert_eq!(metrics.snapshot().active_leases, 2);

    metrics.increment_tasks_completed();
    assert_eq!(metrics.snapshot().active_leases, 1);

    metrics.increment_tasks_failed();
    assert_eq!(metrics.snapshot().active_leases, 0);

    metrics.increment_tasks_claimed();
    metrics.increment_leases_recovered();
    assert_eq!(metrics.snapshot().active_leases, 0);
    assert_eq!(metrics.snapshot().leases_recovered, 1);
}

#[test]
fn decrement_active_leases_can_go_negative_when_misused() {
    let metrics = KeryxMetrics::new();
    metrics.decrement_active_leases();
    assert_eq!(metrics.snapshot().active_leases, -1);
}
