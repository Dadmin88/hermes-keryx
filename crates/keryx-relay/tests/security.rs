use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use keryx_relay::{
    config::RelayConfig,
    security::{Allowlist, EmptyAllowlistPolicy},
    transport::{
        build_relay_client_swarm, build_relay_server_swarm, listen_on_ephemeral_tcp, test_keypair,
        NodeSwarmOptions, RelayServerOptions,
    },
};
use libp2p::core::multiaddr::Protocol;
use libp2p::swarm::SwarmEvent;

fn allowlist_with(peer: libp2p::PeerId) -> Allowlist {
    let mut peers = std::collections::HashSet::new();
    peers.insert(peer);
    Allowlist::new(peers, EmptyAllowlistPolicy::Deny)
}

#[tokio::test]
async fn allowed_peer_connects_to_relay_server() {
    let client_key = test_keypair(41);
    let client_peer = client_key.public().to_peer_id();

    let mut relay = build_relay_server_swarm(
        test_keypair(40),
        &RelayServerOptions {
            config: RelayConfig::default(),
            allowlist: Some(allowlist_with(client_peer)),
        },
    )
    .expect("relay swarm");
    let relay_addr = listen_on_ephemeral_tcp(&mut relay)
        .await
        .expect("relay listen");
    let relay_routable = relay_addr.with(Protocol::P2p(*relay.local_peer_id()));
    let relay_task = tokio::spawn(async move {
        loop {
            if let SwarmEvent::ConnectionEstablished { .. } = relay.select_next_some().await {
                break;
            }
        }
    });

    let mut client =
        build_relay_client_swarm(client_key, &NodeSwarmOptions::default()).expect("client");
    client.dial(relay_routable).expect("dial relay");

    let connected = wait_for_connection(&mut client, Duration::from_secs(15)).await;
    relay_task.abort();
    assert!(connected, "allowlisted peer should connect");
}

#[tokio::test]
async fn rejected_peer_does_not_connect_to_relay_server() {
    let allowed = test_keypair(50).public().to_peer_id();
    let rejected_key = test_keypair(51);

    let mut relay = build_relay_server_swarm(
        test_keypair(52),
        &RelayServerOptions {
            config: RelayConfig::default(),
            allowlist: Some(allowlist_with(allowed)),
        },
    )
    .expect("relay swarm");
    let relay_addr = listen_on_ephemeral_tcp(&mut relay)
        .await
        .expect("relay listen");
    let relay_routable = relay_addr.with(Protocol::P2p(*relay.local_peer_id()));
    let server_connected = Arc::new(AtomicBool::new(false));
    let server_flag = Arc::clone(&server_connected);
    let relay_task = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            match relay.select_next_some().await {
                SwarmEvent::ConnectionEstablished { .. } => {
                    server_flag.store(true, Ordering::SeqCst);
                    break;
                }
                SwarmEvent::IncomingConnectionError { .. } => break,
                _ => {}
            }
        }
    });

    let mut client =
        build_relay_client_swarm(rejected_key, &NodeSwarmOptions::default()).expect("client");
    client.dial(relay_routable).expect("dial relay");
    let _ = wait_for_connection(&mut client, Duration::from_secs(5)).await;
    relay_task.abort();
    assert!(
        !server_connected.load(Ordering::SeqCst),
        "rejected peer must not complete inbound connection to relay"
    );
}

#[tokio::test]
async fn config_reload_picks_up_new_allowlist_keys() {
    let peer_a = test_keypair(61).public().to_peer_id();
    let peer_b = test_keypair(62).public().to_peer_id();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("allow.toml");
    std::fs::write(&path, format!("[[allowed]]\npeer_id = \"{peer_a}\"\n")).unwrap();

    let mut list = Allowlist::load(&path, EmptyAllowlistPolicy::Deny).unwrap();
    assert!(list.is_allowed(&peer_a));
    assert!(!list.is_allowed(&peer_b));

    std::fs::write(
        &path,
        format!("[[allowed]]\npeer_id = \"{peer_a}\"\n\n[[allowed]]\npeer_id = \"{peer_b}\"\n"),
    )
    .unwrap();
    list.reload(&path).unwrap();
    assert!(list.is_allowed(&peer_b));
}

#[tokio::test]
async fn empty_allowlist_denies_by_default() {
    let list = Allowlist::new(std::collections::HashSet::new(), EmptyAllowlistPolicy::Deny);
    let peer = test_keypair(70).public().to_peer_id();
    assert!(!list.is_allowed(&peer));

    let mut relay = build_relay_server_swarm(
        test_keypair(71),
        &RelayServerOptions {
            config: RelayConfig::default(),
            allowlist: Some(list),
        },
    )
    .expect("relay");
    let relay_addr = listen_on_ephemeral_tcp(&mut relay).await.expect("listen");
    let relay_routable = relay_addr.with(Protocol::P2p(*relay.local_peer_id()));
    let server_connected = Arc::new(AtomicBool::new(false));
    let server_flag = Arc::clone(&server_connected);
    let relay_task = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while tokio::time::Instant::now() < deadline {
            if let SwarmEvent::ConnectionEstablished { .. } = relay.select_next_some().await {
                server_flag.store(true, Ordering::SeqCst);
                break;
            }
        }
    });

    let mut client =
        build_relay_client_swarm(test_keypair(72), &NodeSwarmOptions::default()).expect("client");
    client.dial(relay_routable).unwrap();
    let _ = wait_for_connection(&mut client, Duration::from_secs(4)).await;
    relay_task.abort();
    assert!(
        !server_connected.load(Ordering::SeqCst),
        "empty deny-all allowlist should reject inbound peers on the relay"
    );
}

async fn wait_for_connection(
    swarm: &mut libp2p::Swarm<keryx_relay::RelayClientBehaviour>,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        tokio::select! {
            event = swarm.select_next_some() => {
                if matches!(event, SwarmEvent::ConnectionEstablished { .. }) {
                    return true;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    false
}
