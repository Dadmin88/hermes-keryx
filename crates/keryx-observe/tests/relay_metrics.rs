use keryx_observe::{RelayMetrics, RelayMetricsSnapshot};

#[test]
fn relay_counters_track_peers_registry_and_routing() {
    let metrics = RelayMetrics::new();
    metrics.increment_connected_peers();
    metrics.increment_connected_peers();
    metrics.decrement_connected_peers();
    metrics.set_registry_size(4);
    metrics.increment_tasks_routed();
    metrics.increment_tasks_routed();

    assert_eq!(
        metrics.snapshot(),
        RelayMetricsSnapshot {
            connected_peers: 1,
            registry_size: 4,
            tasks_routed: 2,
        }
    );
}
