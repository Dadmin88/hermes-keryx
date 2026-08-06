use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use keryx_core::{NodeId, PeerId, TaskId as CoreTaskId, TaskStatus as CoreTaskStatus};
use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_proto::v1::{
    AgentId, ClaimTaskRequest, CompleteTaskRequest, PublishResultRequest, PublishTaskRequest,
    TaskEnvelope, TaskId, TaskResultEnvelope, TaskStatus as ProtoTaskStatus, TerminalOutcome,
};
use keryx_store::{
    ResultDeliveryState, TaskEnvelopeRecord, TaskRecord, TaskTransportContextRecord,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::TcpListenerStream;

use super::*;
use crate::health_server::serve_grpc_health_with_auth;
use crate::registry::SkillRegistry;
use crate::runtime::RelayRuntime;
use crate::security::NodeTokenAuth;

#[tokio::test]
async fn relay_stream_clean_eof_reconnects() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task_attempts = Arc::clone(&attempts);
    let supervisor = tokio::spawn(supervise_relay_stream(
        move || {
            let attempt = task_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let observed_tx = observed_tx.clone();
            async move {
                let _ = observed_tx.send(attempt);
                Ok(())
            }
        },
        shutdown_rx,
        RelayReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(4)),
    ));

    assert_eq!(observed_rx.recv().await, Some(1));
    assert_eq!(observed_rx.recv().await, Some(2));
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_millis(200), supervisor)
        .await
        .unwrap()
        .unwrap();
    assert!(attempts.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn relay_stream_transport_failure_reconnects() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task_attempts = Arc::clone(&attempts);
    let supervisor = tokio::spawn(supervise_relay_stream(
        move || {
            let attempt = task_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let observed_tx = observed_tx.clone();
            async move {
                let _ = observed_tx.send(attempt);
                anyhow::bail!("synthetic transport failure")
            }
        },
        shutdown_rx,
        RelayReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(4)),
    ));

    assert_eq!(observed_rx.recv().await, Some(1));
    assert_eq!(observed_rx.recv().await, Some(2));
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_millis(200), supervisor)
        .await
        .unwrap()
        .unwrap();
    assert!(attempts.load(Ordering::SeqCst) >= 2);
}

#[test]
fn relay_reconnect_backoff_is_bounded() {
    let maximum = Duration::from_secs(5);
    assert_eq!(
        next_reconnect_delay(Duration::from_secs(4), maximum),
        maximum
    );
    assert_eq!(next_reconnect_delay(maximum, maximum), maximum);
}

#[test]
fn permanent_publish_rejections_are_dead_lettered_without_acknowledgement() {
    for code in [
        Code::InvalidArgument,
        Code::Unauthenticated,
        Code::PermissionDenied,
        Code::NotFound,
        Code::AlreadyExists,
        Code::FailedPrecondition,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::DataLoss,
    ] {
        assert!(publish_result_failure_is_permanent(code), "{code:?}");
    }
    for code in [
        Code::Cancelled,
        Code::Unknown,
        Code::DeadlineExceeded,
        Code::ResourceExhausted,
        Code::Aborted,
        Code::Internal,
        Code::Unavailable,
    ] {
        assert!(!publish_result_failure_is_permanent(code), "{code:?}");
    }
}

#[tokio::test]
async fn relay_stream_shutdown_interrupts_reconnect_backoff() {
    let (attempt_tx, mut attempt_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let supervisor = tokio::spawn(supervise_relay_stream(
        move || {
            let attempt_tx = attempt_tx.clone();
            async move {
                let _ = attempt_tx.send(());
                anyhow::bail!("synthetic transport failure")
            }
        },
        shutdown_rx,
        RelayReconnectPolicy::new(Duration::from_secs(60), Duration::from_secs(60)),
    ));

    attempt_rx.recv().await.unwrap();
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_millis(200), supervisor)
        .await
        .expect("shutdown must interrupt reconnect sleep")
        .unwrap();
}

fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn reserve_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn spawn_relay_server(
    addr: SocketAddr,
    runtime: Arc<RelayRuntime>,
    registry: Arc<SkillRegistry>,
    auth: Arc<NodeTokenAuth>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        serve_grpc_health_with_auth(runtime, registry, auth, addr)
            .await
            .unwrap();
    })
}

fn spawn_tcp_proxy(
    listen_addr: SocketAddr,
    target_addr: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(listen_addr).await.unwrap();
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut downstream, _) = accepted.unwrap();
                    connections.spawn(async move {
                        let mut upstream = TcpStream::connect(target_addr).await?;
                        tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await?;
                        Ok::<(), std::io::Error>(())
                    });
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    let _ = completed;
                }
            }
        }
    })
}

async fn publish_remote_task(
    addr: SocketAddr,
    source_node: &str,
    source_token: &str,
    destination_node: &str,
    task_id: &str,
) {
    let endpoint = format!("http://{addr}");
    let channel = loop {
        match Endpoint::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
        {
            Ok(channel) => break channel,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("target_node_id".to_string(), destination_node.to_string());
    let mut request = Request::new(PublishTaskRequest {
        task: Some(TaskEnvelope {
            task_id: Some(TaskId {
                value: task_id.to_string(),
            }),
            correlation_id: None,
            idempotency_key: None,
            status: ProtoTaskStatus::Created as i32,
            messages: Vec::new(),
            metadata,
            deadline_ms: unix_ms_now() + 60_000,
        }),
        target_node_id: destination_node.to_string(),
        source_node_id: source_node.to_string(),
    });
    add_node_auth_metadata(&mut request, source_node, Some(source_token)).unwrap();
    KeryxRelayClient::new(channel)
        .publish_task(request)
        .await
        .unwrap();
}

async fn seed_origin_task(
    runtime: &KeryxDaemonRuntime,
    task_id: &str,
    source_node: &str,
    destination_node: &str,
    deadline_ms: Option<i64>,
) {
    let task_id = CoreTaskId::new(task_id).unwrap();
    let mut task = TaskRecord::new(task_id.clone(), CoreTaskStatus::Pending, None);
    task.deadline_ms = deadline_ms;
    runtime
        .accept_pending_remote_task_with_backpressure(
            task,
            TaskEnvelopeRecord::new(task_id.clone(), b"test-envelope".to_vec(), 1),
            TaskTransportContextRecord {
                task_id,
                authenticated_sender_peer_id: Some(PeerId::new(destination_node).unwrap()),
                expected_executor_peer_id: Some(PeerId::new(source_node).unwrap()),
                destination_peer_id: PeerId::new(destination_node).unwrap(),
                relay_frame_id: Some("origin-submit-frame".to_string()),
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();
}

async fn seed_executor_task(
    runtime: &KeryxDaemonRuntime,
    task_id: &str,
    origin_node: &str,
    executor_node: &str,
) {
    let task_id = CoreTaskId::new(task_id).unwrap();
    runtime
        .accept_pending_remote_task_with_backpressure(
            TaskRecord::new(task_id.clone(), CoreTaskStatus::Pending, None),
            TaskEnvelopeRecord::new(task_id.clone(), b"test-envelope".to_vec(), 1),
            TaskTransportContextRecord {
                task_id,
                authenticated_sender_peer_id: Some(PeerId::new(origin_node).unwrap()),
                expected_executor_peer_id: Some(PeerId::new(executor_node).unwrap()),
                destination_peer_id: PeerId::new(executor_node).unwrap(),
                relay_frame_id: Some("executor-submit-frame".to_string()),
                received_at_ms: 1,
            },
        )
        .await
        .unwrap();
}

async fn publish_remote_result(
    addr: SocketAddr,
    source_node: &str,
    source_token: &str,
    destination_node: &str,
    task_id: &str,
    frame_id: &str,
) {
    let channel = loop {
        match Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
        {
            Ok(channel) => break channel,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    let mut request = Request::new(PublishResultRequest {
        result: Some(TaskResultEnvelope {
            protocol_version: 1,
            task_id: Some(TaskId {
                value: task_id.to_string(),
            }),
            correlation_id: None,
            outcome: TerminalOutcome::Completed as i32,
            executor_peer_id: source_node.to_string(),
            duration_ms: 10,
            completed_at_ms: unix_ms_now(),
            error_reason: String::new(),
            result_metadata: Default::default(),
            output_artifacts: Vec::new(),
        }),
        target_node_id: destination_node.to_string(),
        source_node_id: source_node.to_string(),
        frame_id: frame_id.to_string(),
    });
    add_node_auth_metadata(&mut request, source_node, Some(source_token)).unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        KeryxRelayClient::new(channel).publish_result(request),
    )
    .await
    .expect("authenticated destination ACK must settle result publication")
    .unwrap();
}

async fn wait_for_task(runtime: &KeryxDaemonRuntime, task_id: &str) {
    let task_id = CoreTaskId::new(task_id).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if runtime.store().get_task(&task_id).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("relay frame must reach daemon");
}

async fn wait_for_mailbox_depth(runtime: &RelayRuntime, node_id: &str, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime.mailbox_depth(node_id) == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("relay mailbox must reach expected depth");
}

#[tokio::test]
async fn relay_restart_reconnects_reapplies_auth_and_processes_later_task() {
    const SOURCE: &str = "reconnect-source";
    const DESTINATION: &str = "reconnect-destination";
    const SOURCE_TOKEN: &str = "reconnect-source-token";
    const DESTINATION_TOKEN: &str = "reconnect-destination-token";

    let data_dir = tempfile::tempdir().unwrap();
    let daemon_runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(data_dir.path().join("daemon"), 0)
                .with_local_peer_id(PeerId::new(DESTINATION).unwrap()),
        )
        .await
        .unwrap(),
    );
    let daemon_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let daemon_addr = daemon_listener.local_addr().unwrap();
    let daemon_server = tokio::spawn(serve_daemon_rpc(
        daemon_runtime.as_ref().clone(),
        TcpListenerStream::new(daemon_listener),
    ));

    let registry = Arc::new(SkillRegistry::new());
    for node_id in [SOURCE, DESTINATION] {
        registry
            .register_with_features(
                PeerId::new(node_id).unwrap(),
                Vec::new(),
                node_id.to_string(),
                String::new(),
                vec![
                    "absolute_deadlines_v1".to_string(),
                    "result_artifact_bytes_v1".to_string(),
                ],
                None,
            )
            .await;
    }
    let auth = Arc::new(NodeTokenAuth::new(
        HashMap::from([
            (NodeId::new(SOURCE).unwrap(), SOURCE_TOKEN.to_string()),
            (
                NodeId::new(DESTINATION).unwrap(),
                DESTINATION_TOKEN.to_string(),
            ),
        ]),
        Default::default(),
    ));
    let first_backend_addr = reserve_loopback_addr().await;
    let first_runtime = RelayRuntime::new("relay-before-restart");
    first_runtime.mark_transport_listening();
    let first_relay = spawn_relay_server(
        first_backend_addr,
        Arc::clone(&first_runtime),
        Arc::clone(&registry),
        Arc::clone(&auth),
    );
    let relay_addr = reserve_loopback_addr().await;
    let first_proxy = spawn_tcp_proxy(relay_addr, first_backend_addr);
    let attempts = Arc::new(AtomicUsize::new(0));
    let task_attempts = Arc::clone(&attempts);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = tokio::spawn(supervise_relay_stream(
        move || {
            task_attempts.fetch_add(1, Ordering::SeqCst);
            run_relay_stream(
                format!("http://{relay_addr}"),
                DESTINATION.to_string(),
                Some(DESTINATION_TOKEN.to_string()),
                format!("http://{daemon_addr}"),
            )
        },
        shutdown_rx,
        RelayReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40)),
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if first_runtime
                .peer_identity(DESTINATION)
                .is_some_and(|peer| peer.connected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("authenticated edge stream must connect before clean EOF");
    first_runtime.disconnect_node(DESTINATION);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if attempts.load(Ordering::SeqCst) >= 2
                && first_runtime
                    .peer_identity(DESTINATION)
                    .is_some_and(|peer| peer.connected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("clean relay EOF must reconnect without restarting the edge");

    publish_remote_task(
        relay_addr,
        SOURCE,
        SOURCE_TOKEN,
        DESTINATION,
        "task-before-relay-restart",
    )
    .await;
    wait_for_task(&daemon_runtime, "task-before-relay-restart").await;

    first_proxy.abort();
    let _ = first_proxy.await;
    first_relay.abort();
    let _ = first_relay.await;
    let second_backend_addr = reserve_loopback_addr().await;
    let second_runtime = RelayRuntime::new("relay-after-restart");
    second_runtime.mark_transport_listening();
    let second_relay = spawn_relay_server(
        second_backend_addr,
        second_runtime,
        Arc::clone(&registry),
        Arc::clone(&auth),
    );
    let second_proxy = spawn_tcp_proxy(relay_addr, second_backend_addr);
    publish_remote_task(
        relay_addr,
        SOURCE,
        SOURCE_TOKEN,
        DESTINATION,
        "task-after-relay-restart",
    )
    .await;
    wait_for_task(&daemon_runtime, "task-after-relay-restart").await;
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "edge must reconnect to the replacement relay process"
    );

    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), supervisor)
        .await
        .unwrap()
        .unwrap();
    second_proxy.abort();
    second_relay.abort();
    daemon_server.abort();
}

#[tokio::test]
async fn late_result_is_settled_and_next_result_continues_on_same_stream() {
    const SOURCE: &str = "late-source";
    const DESTINATION: &str = "late-destination";
    const SOURCE_TOKEN: &str = "late-source-token";
    const DESTINATION_TOKEN: &str = "late-destination-token";
    const LATE_TASK: &str = "result-after-origin-deadline";
    const NEXT_TASK: &str = "result-after-settled-late-frame";
    const VALID_TASK: &str = "task-after-settled-late-frame";

    let data_dir = tempfile::tempdir().unwrap();
    let daemon_runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(data_dir.path().join("daemon"), 0)
                .with_local_peer_id(PeerId::new(DESTINATION).unwrap()),
        )
        .await
        .unwrap(),
    );
    seed_origin_task(
        &daemon_runtime,
        LATE_TASK,
        SOURCE,
        DESTINATION,
        Some(unix_ms_now() - 1),
    )
    .await;
    seed_origin_task(&daemon_runtime, NEXT_TASK, SOURCE, DESTINATION, None).await;
    assert_eq!(
        daemon_runtime
            .store()
            .fail_expired_deadlines(unix_ms_now(), None)
            .await
            .unwrap()
            .len(),
        1
    );
    let late_id = CoreTaskId::new(LATE_TASK).unwrap();
    let late_events = daemon_runtime
        .store()
        .events_for_task(&late_id)
        .await
        .unwrap();

    let daemon_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let daemon_addr = daemon_listener.local_addr().unwrap();
    let daemon_server = tokio::spawn(serve_daemon_rpc(
        daemon_runtime.as_ref().clone(),
        TcpListenerStream::new(daemon_listener),
    ));
    let relay_addr = reserve_loopback_addr().await;
    let relay_runtime = RelayRuntime::new("late-result-settlement-test");
    relay_runtime.mark_transport_listening();
    let registry = Arc::new(SkillRegistry::new());
    for node_id in [SOURCE, DESTINATION] {
        registry
            .register_with_features(
                PeerId::new(node_id).unwrap(),
                Vec::new(),
                node_id.to_string(),
                String::new(),
                vec![
                    "absolute_deadlines_v1".to_string(),
                    "result_artifact_bytes_v1".to_string(),
                ],
                None,
            )
            .await;
    }
    let auth = Arc::new(NodeTokenAuth::new(
        HashMap::from([
            (NodeId::new(SOURCE).unwrap(), SOURCE_TOKEN.to_string()),
            (
                NodeId::new(DESTINATION).unwrap(),
                DESTINATION_TOKEN.to_string(),
            ),
        ]),
        Default::default(),
    ));
    let relay_server = spawn_relay_server(relay_addr, Arc::clone(&relay_runtime), registry, auth);
    let attempts = Arc::new(AtomicUsize::new(0));
    let task_attempts = Arc::clone(&attempts);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = tokio::spawn(supervise_relay_stream(
        move || {
            task_attempts.fetch_add(1, Ordering::SeqCst);
            run_relay_stream(
                format!("http://{relay_addr}"),
                DESTINATION.to_string(),
                Some(DESTINATION_TOKEN.to_string()),
                format!("http://{daemon_addr}"),
            )
        },
        shutdown_rx,
        RelayReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40)),
    ));

    publish_remote_result(
        relay_addr,
        SOURCE,
        SOURCE_TOKEN,
        DESTINATION,
        LATE_TASK,
        "late-result-delivery",
    )
    .await;
    assert_eq!(
        daemon_runtime
            .store()
            .get_task(&late_id)
            .await
            .unwrap()
            .status,
        CoreTaskStatus::Failed
    );
    assert_eq!(
        daemon_runtime
            .store()
            .events_for_task(&late_id)
            .await
            .unwrap(),
        late_events
    );
    assert!(daemon_runtime
        .store()
        .get_terminal_result(&late_id)
        .await
        .is_err());
    wait_for_mailbox_depth(&relay_runtime, DESTINATION, 0).await;
    assert_eq!(relay_runtime.mailbox_depth(DESTINATION), 0);

    publish_remote_result(
        relay_addr,
        SOURCE,
        SOURCE_TOKEN,
        DESTINATION,
        NEXT_TASK,
        "next-result-delivery",
    )
    .await;
    let next_id = CoreTaskId::new(NEXT_TASK).unwrap();
    assert_eq!(
        daemon_runtime
            .store()
            .get_task(&next_id)
            .await
            .unwrap()
            .status,
        CoreTaskStatus::Completed
    );
    assert!(daemon_runtime
        .store()
        .get_terminal_result(&next_id)
        .await
        .is_ok());
    wait_for_mailbox_depth(&relay_runtime, DESTINATION, 0).await;
    assert_eq!(relay_runtime.mailbox_depth(DESTINATION), 0);

    publish_remote_task(relay_addr, SOURCE, SOURCE_TOKEN, DESTINATION, VALID_TASK).await;
    wait_for_task(&daemon_runtime, VALID_TASK).await;
    wait_for_mailbox_depth(&relay_runtime, DESTINATION, 0).await;
    assert_eq!(relay_runtime.mailbox_depth(DESTINATION), 0);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), supervisor)
        .await
        .unwrap()
        .unwrap();
    relay_server.abort();
    daemon_server.abort();
}

#[tokio::test]
async fn result_outbox_survives_relay_drop_then_reconnects_delivers_and_processes_next_task() {
    const ORIGIN: &str = "drop-origin";
    const EXECUTOR: &str = "drop-executor";
    const ORIGIN_TOKEN: &str = "drop-origin-token";
    const EXECUTOR_TOKEN: &str = "drop-executor-token";
    const RESULT_TASK: &str = "in-flight-result-during-relay-drop";
    const NEXT_TASK: &str = "task-after-result-reconnect";

    let origin_dir = tempfile::tempdir().unwrap();
    let executor_dir = tempfile::tempdir().unwrap();
    let origin_runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(origin_dir.path().join("daemon"), 0)
                .with_local_peer_id(PeerId::new(ORIGIN).unwrap()),
        )
        .await
        .unwrap(),
    );
    let executor_runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(executor_dir.path().join("daemon"), 0)
                .with_local_peer_id(PeerId::new(EXECUTOR).unwrap()),
        )
        .await
        .unwrap(),
    );
    seed_origin_task(&origin_runtime, RESULT_TASK, EXECUTOR, ORIGIN, None).await;
    seed_executor_task(&executor_runtime, RESULT_TASK, ORIGIN, EXECUTOR).await;

    let origin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_addr = origin_listener.local_addr().unwrap();
    let origin_server = tokio::spawn(serve_daemon_rpc(
        origin_runtime.as_ref().clone(),
        TcpListenerStream::new(origin_listener),
    ));
    let executor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let executor_addr = executor_listener.local_addr().unwrap();
    let executor_server = tokio::spawn(serve_daemon_rpc(
        executor_runtime.as_ref().clone(),
        TcpListenerStream::new(executor_listener),
    ));

    let first_backend_addr = reserve_loopback_addr().await;
    let first_runtime = RelayRuntime::new("result-outbox-before-relay-restart");
    first_runtime.mark_transport_listening();
    let registry = Arc::new(SkillRegistry::new());
    for node_id in [ORIGIN, EXECUTOR] {
        registry
            .register_with_features(
                PeerId::new(node_id).unwrap(),
                Vec::new(),
                node_id.to_string(),
                String::new(),
                vec![
                    "absolute_deadlines_v1".to_string(),
                    "result_artifact_bytes_v1".to_string(),
                ],
                None,
            )
            .await;
    }
    let auth = Arc::new(NodeTokenAuth::new(
        HashMap::from([
            (NodeId::new(ORIGIN).unwrap(), ORIGIN_TOKEN.to_string()),
            (NodeId::new(EXECUTOR).unwrap(), EXECUTOR_TOKEN.to_string()),
        ]),
        Default::default(),
    ));
    let first_relay = spawn_relay_server(
        first_backend_addr,
        Arc::clone(&first_runtime),
        Arc::clone(&registry),
        Arc::clone(&auth),
    );
    let relay_addr = reserve_loopback_addr().await;
    let first_proxy = spawn_tcp_proxy(relay_addr, first_backend_addr);

    let origin_attempts = Arc::new(AtomicUsize::new(0));
    let origin_task_attempts = Arc::clone(&origin_attempts);
    let (origin_shutdown_tx, origin_shutdown_rx) = watch::channel(false);
    let origin_edge = tokio::spawn(supervise_relay_stream(
        move || {
            origin_task_attempts.fetch_add(1, Ordering::SeqCst);
            run_relay_stream(
                format!("http://{relay_addr}"),
                ORIGIN.to_string(),
                Some(ORIGIN_TOKEN.to_string()),
                format!("http://{origin_addr}"),
            )
        },
        origin_shutdown_rx,
        RelayReconnectPolicy::new(Duration::from_millis(100), Duration::from_millis(100)),
    ));
    let executor_attempts = Arc::new(AtomicUsize::new(0));
    let executor_task_attempts = Arc::clone(&executor_attempts);
    let (executor_shutdown_tx, executor_shutdown_rx) = watch::channel(false);
    let executor_edge = tokio::spawn(supervise_relay_stream(
        move || {
            executor_task_attempts.fetch_add(1, Ordering::SeqCst);
            run_relay_stream(
                format!("http://{relay_addr}"),
                EXECUTOR.to_string(),
                Some(EXECUTOR_TOKEN.to_string()),
                format!("http://{executor_addr}"),
            )
        },
        executor_shutdown_rx,
        RelayReconnectPolicy::new(Duration::from_millis(100), Duration::from_millis(100)),
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if first_runtime
                .peer_identity(ORIGIN)
                .is_some_and(|peer| peer.connected)
                && first_runtime
                    .peer_identity(EXECUTOR)
                    .is_some_and(|peer| peer.connected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("both authenticated edge streams must connect");

    first_proxy.abort();
    let _ = first_proxy.await;
    first_relay.abort();
    let _ = first_relay.await;

    let mut executor_client = KeryxDaemonClient::connect(format!("http://{executor_addr}"))
        .await
        .unwrap();
    let claim = executor_client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: RESULT_TASK.to_string(),
            }),
            worker_id: Some(AgentId {
                value: "drop-worker".to_string(),
            }),
            lease_duration_ms: 30_000,
        })
        .await
        .unwrap()
        .into_inner();
    executor_client
        .complete_task(CompleteTaskRequest {
            task_id: Some(TaskId {
                value: RESULT_TASK.to_string(),
            }),
            lease_id: claim.lease_id,
            worker_id: Some(AgentId {
                value: "drop-worker".to_string(),
            }),
            duration_ms: 5,
            result_metadata: Default::default(),
            output_artifacts: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        executor_runtime
            .store()
            .pending_result_deliveries(10)
            .await
            .unwrap()
            .len(),
        1,
        "result outbox must remain durable while relay is down"
    );

    let second_backend_addr = reserve_loopback_addr().await;
    let second_runtime = RelayRuntime::new("result-outbox-after-relay-restart");
    second_runtime.mark_transport_listening();
    let second_relay = spawn_relay_server(
        second_backend_addr,
        Arc::clone(&second_runtime),
        Arc::clone(&registry),
        Arc::clone(&auth),
    );
    let second_proxy = spawn_tcp_proxy(relay_addr, second_backend_addr);

    let result_id = CoreTaskId::new(RESULT_TASK).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let completed = origin_runtime
                .store()
                .get_task(&result_id)
                .await
                .map(|task| task.status == CoreTaskStatus::Completed)
                .unwrap_or(false);
            let outbox_delivered = executor_runtime
                .store()
                .result_delivery_for_task(&result_id)
                .await
                .map(|row| {
                    row.is_some_and(|delivery| delivery.state == ResultDeliveryState::Delivered)
                })
                .unwrap_or(false);
            if completed && outbox_delivered {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("result must deliver and receive authenticated destination ACK after reconnect");
    assert!(origin_attempts.load(Ordering::SeqCst) >= 2);
    assert!(executor_attempts.load(Ordering::SeqCst) >= 2);

    publish_remote_task(relay_addr, ORIGIN, ORIGIN_TOKEN, EXECUTOR, NEXT_TASK).await;
    wait_for_task(&executor_runtime, NEXT_TASK).await;
    wait_for_mailbox_depth(&second_runtime, ORIGIN, 0).await;
    wait_for_mailbox_depth(&second_runtime, EXECUTOR, 0).await;
    assert_eq!(second_runtime.mailbox_depth(ORIGIN), 0);
    assert_eq!(second_runtime.mailbox_depth(EXECUTOR), 0);

    origin_shutdown_tx.send(true).unwrap();
    executor_shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), origin_edge)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor_edge)
        .await
        .unwrap()
        .unwrap();
    second_proxy.abort();
    second_relay.abort();
    origin_server.abort();
    executor_server.abort();
}

#[tokio::test]
async fn temporary_daemon_failure_does_not_ack_and_retries_after_recovery() {
    const SOURCE: &str = "temporary-source";
    const DESTINATION: &str = "temporary-destination";
    const SOURCE_TOKEN: &str = "temporary-source-token";
    const DESTINATION_TOKEN: &str = "temporary-destination-token";
    const TASK_ID: &str = "temporary-daemon-result";

    let data_dir = tempfile::tempdir().unwrap();
    let daemon_runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(data_dir.path().join("daemon"), 0)
                .with_local_peer_id(PeerId::new(DESTINATION).unwrap()),
        )
        .await
        .unwrap(),
    );
    seed_origin_task(&daemon_runtime, TASK_ID, SOURCE, DESTINATION, None).await;
    let daemon_addr = reserve_loopback_addr().await;
    let relay_addr = reserve_loopback_addr().await;
    let relay_runtime = RelayRuntime::new("temporary-daemon-retry-test");
    relay_runtime.mark_transport_listening();
    let registry = Arc::new(SkillRegistry::new());
    for node_id in [SOURCE, DESTINATION] {
        registry
            .register_with_features(
                PeerId::new(node_id).unwrap(),
                Vec::new(),
                node_id.to_string(),
                String::new(),
                vec![
                    "absolute_deadlines_v1".to_string(),
                    "result_artifact_bytes_v1".to_string(),
                ],
                None,
            )
            .await;
    }
    let auth = Arc::new(NodeTokenAuth::new(
        HashMap::from([
            (NodeId::new(SOURCE).unwrap(), SOURCE_TOKEN.to_string()),
            (
                NodeId::new(DESTINATION).unwrap(),
                DESTINATION_TOKEN.to_string(),
            ),
        ]),
        Default::default(),
    ));
    let relay_server = spawn_relay_server(relay_addr, Arc::clone(&relay_runtime), registry, auth);
    let attempts = Arc::new(AtomicUsize::new(0));
    let task_attempts = Arc::clone(&attempts);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = tokio::spawn(supervise_relay_stream(
        move || {
            task_attempts.fetch_add(1, Ordering::SeqCst);
            run_relay_stream(
                format!("http://{relay_addr}"),
                DESTINATION.to_string(),
                Some(DESTINATION_TOKEN.to_string()),
                format!("http://{daemon_addr}"),
            )
        },
        shutdown_rx,
        RelayReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40)),
    ));
    let publisher = tokio::spawn(publish_remote_result(
        relay_addr,
        SOURCE,
        SOURCE_TOKEN,
        DESTINATION,
        TASK_ID,
        "temporary-daemon-frame",
    ));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if relay_runtime.mailbox_depth(DESTINATION) == 1 && attempts.load(Ordering::SeqCst) >= 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(relay_runtime.mailbox_depth(DESTINATION), 1);
    assert!(!publisher.is_finished());
    assert_eq!(
        daemon_runtime
            .store()
            .get_task(&CoreTaskId::new(TASK_ID).unwrap())
            .await
            .unwrap()
            .status,
        CoreTaskStatus::Pending
    );

    let daemon_listener = TcpListener::bind(daemon_addr).await.unwrap();
    let daemon_server = tokio::spawn(serve_daemon_rpc(
        daemon_runtime.as_ref().clone(),
        TcpListenerStream::new(daemon_listener),
    ));
    tokio::time::timeout(Duration::from_secs(2), publisher)
        .await
        .expect("temporary daemon failure must retry after recovery")
        .unwrap();
    wait_for_mailbox_depth(&relay_runtime, DESTINATION, 0).await;
    assert_eq!(relay_runtime.mailbox_depth(DESTINATION), 0);
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        daemon_runtime
            .store()
            .get_task(&CoreTaskId::new(TASK_ID).unwrap())
            .await
            .unwrap()
            .status,
        CoreTaskStatus::Completed
    );

    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), supervisor)
        .await
        .unwrap()
        .unwrap();
    relay_server.abort();
    daemon_server.abort();
}

#[tokio::test]
async fn remote_plaintext_registry_endpoint_fails_closed() {
    let error = connect_registry_client("http://192.0.2.1:50053")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("require TLS"));
}

#[test]
fn remote_plaintext_relay_control_endpoint_fails_closed() {
    let error = secure_endpoint_builder("http://192.0.2.1:50052").unwrap_err();
    assert!(error.to_string().contains("require TLS"));
}

#[test]
fn https_relay_control_endpoint_uses_tls() {
    let endpoint = secure_endpoint_builder("https://relay.example:50052").unwrap();
    assert_eq!(endpoint.uri().scheme_str(), Some("https"));
}
