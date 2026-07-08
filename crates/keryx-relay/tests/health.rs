use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use keryx_core::PeerId;
use keryx_proto::v1::keryx_relay_client::KeryxRelayClient;
use keryx_proto::v1::HealthRequest;
use keryx_relay::{
    health_server::serve_grpc_health,
    registry::{SkillRegistry, StoredSkill},
    runtime::RelayRuntime,
    transport::{
        build_ping_node_swarm, build_relay_server_swarm, listen_on_ephemeral_tcp, test_keypair,
    },
    RelayServerOptions,
};
use libp2p::swarm::SwarmEvent;
use libp2p::Multiaddr;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Endpoint};

#[tokio::test]
async fn grpc_health_reports_peer_and_registry_counts() {
    let runtime = RelayRuntime::new("test-relay-peer");
    runtime.mark_transport_listening();

    let registry = Arc::new(SkillRegistry::new());
    registry
        .register(
            PeerId::new("agent-a").unwrap(),
            vec![StoredSkill {
                skill_id: "rust".into(),
                description: String::new(),
                tags: vec![],
            }],
            "a".into(),
            String::new(),
            None,
        )
        .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let rt = Arc::clone(&runtime);
    let reg = Arc::clone(&registry);
    tokio::spawn(async move {
        let _ = serve_grpc_health(rt, Some(reg), addr).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let channel = connect_grpc(addr).await;
    let mut client = KeryxRelayClient::new(channel);
    let response = client
        .health(HealthRequest {})
        .await
        .expect("health rpc")
        .into_inner();

    assert!(response.healthy);
    assert_eq!(response.connected_peers, 0);
    assert_eq!(response.registry_size, 1);
    assert_eq!(response.transport_status, "listening");
}

#[tokio::test]
async fn swarm_connection_events_update_peer_metric() {
    let runtime = RelayRuntime::new("relay");
    runtime.mark_transport_listening();

    let mut relay = build_relay_server_swarm(
        test_keypair(10),
        &RelayServerOptions {
            config: keryx_relay::RelayConfig {
                listen_addresses: vec!["0".into()],
                enable_mdns: false,
                ..Default::default()
            },
            allowlist: None,
        },
    )
    .unwrap();
    let relay_addr = listen_on_ephemeral_tcp(&mut relay).await.unwrap();
    let relay_peer = *relay.local_peer_id();

    let mut client = build_ping_node_swarm(test_keypair(11)).unwrap();
    client
        .listen_on("/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap())
        .unwrap();
    client
        .dial(relay_addr.with(libp2p::multiaddr::Protocol::P2p(relay_peer)))
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for connection");
        }
        tokio::select! {
            event = relay.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { .. } = event {
                    runtime.note_connection_established();
                    break;
                }
            }
            event = client.select_next_some() => {
                if matches!(event, SwarmEvent::ConnectionEstablished { .. }) {
                    continue;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
    }

    assert_eq!(runtime.metrics().snapshot().connected_peers, 1);
}

async fn connect_grpc(addr: std::net::SocketAddr) -> Channel {
    let uri = format!("http://{addr}");
    for _ in 0..40 {
        if let Ok(channel) = Endpoint::from_shared(uri.clone()).unwrap().connect().await {
            return channel;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("failed to connect gRPC health at {uri}");
}
