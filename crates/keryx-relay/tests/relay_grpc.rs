use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use keryx_proto::v1::keryx_relay_client::KeryxRelayClient;
use keryx_proto::v1::{
    AckTaskRequest, NodeFrame, NodeId, PublishTaskRequest, RegisterNodeRequest, TaskEnvelope,
    TaskId, TaskStatus,
};
use keryx_relay::health_server::{serve_grpc_health, NODE_ID_METADATA_KEY};
use keryx_relay::runtime::RelayRuntime;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

#[tokio::test]
async fn connect_node_relays_node_frames_to_connected_target() {
    let runtime = RelayRuntime::new("relay-grpc-frame-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let mut admin = KeryxRelayClient::new(channel.clone());
    register(&mut admin, "node-a").await;
    register(&mut admin, "node-b").await;

    let (_b_tx, b_rx) = mpsc::channel(4);
    let mut node_b = KeryxRelayClient::new(channel.clone());
    let mut b_stream = node_b
        .connect_node(connect_request("node-b", b_rx))
        .await
        .expect("connect node-b")
        .into_inner();

    let (a_tx, a_rx) = mpsc::channel(4);
    let mut node_a = KeryxRelayClient::new(channel);
    let mut _a_stream = node_a
        .connect_node(connect_request("node-a", a_rx))
        .await
        .expect("connect node-a")
        .into_inner();

    a_tx.send(NodeFrame {
        frame_id: "frame-a-to-b".to_string(),
        task: Some(task("task-a-to-b", "node-b")),
    })
    .await
    .expect("send node frame");

    let delivered = tokio::time::timeout(Duration::from_secs(3), b_stream.next())
        .await
        .expect("relay frame timeout")
        .expect("relay stream ended")
        .expect("relay frame status");

    assert_eq!(delivered.frame_id, "frame-a-to-b");
    assert_eq!(task_id(&delivered.task.unwrap()), "task-a-to-b");
    assert_eq!(runtime.metrics().snapshot().tasks_routed, 1);
}

#[tokio::test]
async fn publish_task_stores_offline_mailbox_and_delivers_on_reconnect() {
    let runtime = RelayRuntime::new("relay-grpc-offline-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let mut client = KeryxRelayClient::new(channel.clone());
    register(&mut client, "node-offline").await;

    let response = client
        .publish_task(PublishTaskRequest {
            task: Some(task("task-offline", "node-offline")),
        })
        .await
        .expect("publish offline task")
        .into_inner();
    assert_eq!(response.task_id.unwrap().value, "task-offline");
    assert_eq!(runtime.mailbox_depth("node-offline"), 1);

    let (_node_tx, node_rx) = mpsc::channel(4);
    let mut node_client = KeryxRelayClient::new(channel.clone());
    let mut stream = node_client
        .connect_node(connect_request("node-offline", node_rx))
        .await
        .expect("connect offline node")
        .into_inner();

    let delivered = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("offline delivery timeout")
        .expect("relay stream ended")
        .expect("relay frame status");
    assert_eq!(task_id(&delivered.task.unwrap()), "task-offline");
    assert_eq!(runtime.mailbox_depth("node-offline"), 0);

    let acked = client
        .ack_task(AckTaskRequest {
            task_id: Some(TaskId {
                value: "task-offline".to_string(),
            }),
        })
        .await
        .expect("ack task")
        .into_inner();
    assert!(acked.accepted);
}

#[tokio::test]
async fn ack_task_removes_pending_offline_mailbox_entry() {
    let runtime = RelayRuntime::new("relay-grpc-ack-test");
    runtime.mark_transport_listening();
    let addr = spawn_relay(Arc::clone(&runtime)).await;
    let channel = connect_grpc(addr).await;

    let mut client = KeryxRelayClient::new(channel);
    register(&mut client, "node-pending").await;
    client
        .publish_task(PublishTaskRequest {
            task: Some(task("task-pending", "node-pending")),
        })
        .await
        .expect("publish pending task");
    assert_eq!(runtime.mailbox_depth("node-pending"), 1);

    let acked = client
        .ack_task(AckTaskRequest {
            task_id: Some(TaskId {
                value: "task-pending".to_string(),
            }),
        })
        .await
        .expect("ack pending task")
        .into_inner();
    assert!(acked.accepted);
    assert_eq!(runtime.mailbox_depth("node-pending"), 0);
}

async fn spawn_relay(runtime: Arc<RelayRuntime>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(async move {
        let _ = serve_grpc_health(runtime, None, addr).await;
    });
    addr
}

async fn connect_grpc(addr: SocketAddr) -> Channel {
    let uri = format!("http://{addr}");
    for _ in 0..40 {
        if let Ok(channel) = Endpoint::from_shared(uri.clone()).unwrap().connect().await {
            return channel;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("failed to connect gRPC relay at {uri}");
}

async fn register(client: &mut KeryxRelayClient<Channel>, node_id: &str) {
    let response = client
        .register_node(RegisterNodeRequest {
            node_id: Some(NodeId {
                value: node_id.to_string(),
            }),
            token: String::new(),
        })
        .await
        .expect("register node")
        .into_inner();
    assert!(response.accepted);
}

fn connect_request(
    node_id: &str,
    rx: mpsc::Receiver<NodeFrame>,
) -> Request<ReceiverStream<NodeFrame>> {
    let mut request = Request::new(ReceiverStream::new(rx));
    request
        .metadata_mut()
        .insert(NODE_ID_METADATA_KEY, node_id.parse().unwrap());
    request
}

fn task(task_id: &str, target_node_id: &str) -> TaskEnvelope {
    let mut metadata = HashMap::new();
    metadata.insert("target_node_id".to_string(), target_node_id.to_string());
    TaskEnvelope {
        task_id: Some(TaskId {
            value: task_id.to_string(),
        }),
        correlation_id: None,
        idempotency_key: None,
        status: TaskStatus::Created as i32,
        messages: vec![],
        metadata,
    }
}

fn task_id(task: &TaskEnvelope) -> &str {
    task.task_id.as_ref().unwrap().value.as_str()
}
