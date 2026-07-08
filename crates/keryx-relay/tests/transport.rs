use std::time::Duration;

use futures::StreamExt;
use keryx_relay::transport::{build_ping_node_swarm, listen_on_ephemeral_tcp, test_keypair};
use libp2p::swarm::SwarmEvent;
use libp2p::Multiaddr;

#[tokio::test]
async fn two_nodes_connect_over_tcp() {
    let mut listener = build_ping_node_swarm(test_keypair(1)).expect("listener swarm");
    let listen_addr = listen_on_ephemeral_tcp(&mut listener)
        .await
        .expect("listener address");
    let listener_peer = *listener.local_peer_id();

    let mut dialer = build_ping_node_swarm(test_keypair(2)).expect("dialer swarm");
    dialer
        .listen_on("/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap())
        .unwrap();
    dialer
        .dial(listen_addr.with(libp2p::multiaddr::Protocol::P2p(listener_peer)))
        .unwrap();

    let connected = wait_for_connection(&mut listener, &mut dialer, Duration::from_secs(10)).await;
    assert!(connected, "nodes should establish a TCP connection");
}

async fn wait_for_connection(
    listener: &mut libp2p::Swarm<keryx_relay::PingNodeBehaviour>,
    dialer: &mut libp2p::Swarm<keryx_relay::PingNodeBehaviour>,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::select! {
            event = listener.select_next_some() => {
                if matches!(event, SwarmEvent::ConnectionEstablished { .. }) {
                    return true;
                }
            }
            event = dialer.select_next_some() => {
                if matches!(event, SwarmEvent::ConnectionEstablished { .. }) {
                    return true;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}
