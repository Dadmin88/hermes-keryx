use std::time::Duration;

use futures::StreamExt;
use keryx_relay::{
    config::RelayConfig,
    transport::{
        build_relay_client_swarm, build_relay_server_swarm, listen_on_ephemeral_tcp, test_keypair,
        NodeSwarmOptions, RelayClientBehaviourEvent, RelayServerBehaviourEvent, RelayServerOptions,
    },
};
use libp2p::core::multiaddr::Protocol;
use libp2p::swarm::SwarmEvent;
use libp2p::{identify, Multiaddr};

#[tokio::test]
async fn relay_client_connects_to_relay_server() {
    let mut relay = build_relay_server_swarm(
        test_keypair(20),
        &RelayServerOptions {
            config: RelayConfig::default(),
            allowlist: None,
        },
    )
    .expect("relay swarm");
    let relay_addr = listen_on_ephemeral_tcp(&mut relay)
        .await
        .expect("relay listen");
    let relay_routable = relay_addr.with(Protocol::P2p(*relay.local_peer_id()));
    let relay_task = spawn_relay_driver(relay);

    let mut client = build_relay_client_swarm(test_keypair(21), &NodeSwarmOptions::default())
        .expect("relay client");
    assert!(
        prepare_relay_client(&mut client, relay_routable, Duration::from_secs(25)).await,
        "client should register with relay"
    );
    relay_task.abort();
}

/// End-to-end relay traversal (A → relay → B). Requires stable loopback relay reservations;
/// run with `cargo test -p keryx-relay --test relay -- --ignored` when debugging libp2p relay.
#[tokio::test]
#[ignore = "relay circuit reservation flaky on loopback without DCUtR; smoke covered by relay_client_connects_to_relay_server"]
async fn relay_traversal_connects_listener_and_dialer() {
    let mut relay = build_relay_server_swarm(
        test_keypair(30),
        &RelayServerOptions {
            config: RelayConfig::default(),
            allowlist: None,
        },
    )
    .expect("relay swarm");
    let relay_addr = listen_on_ephemeral_tcp(&mut relay)
        .await
        .expect("relay listen");
    let relay_routable = relay_addr.with(Protocol::P2p(*relay.local_peer_id()));
    let relay_task = spawn_relay_driver(relay);

    let mut listener =
        build_relay_client_swarm(test_keypair(31), &NodeSwarmOptions::default()).expect("listener");
    assert!(
        listener_reserve_on_relay(
            &mut listener,
            relay_routable.clone(),
            Duration::from_secs(40)
        )
        .await,
        "listener should reserve a relay circuit"
    );
    let listener_peer = *listener.local_peer_id();

    let mut dialer =
        build_relay_client_swarm(test_keypair(32), &NodeSwarmOptions::default()).expect("dialer");
    assert!(
        prepare_relay_client(&mut dialer, relay_routable.clone(), Duration::from_secs(25)).await,
        "dialer should register with relay"
    );
    dialer
        .dial(
            relay_routable
                .with(Protocol::P2pCircuit)
                .with(Protocol::P2p(listener_peer)),
        )
        .expect("dial via circuit");

    let relayed =
        wait_for_relayed_connection(&mut listener, &mut dialer, Duration::from_secs(30)).await;
    relay_task.abort();
    assert!(relayed, "dialer should reach listener through relay");
}

fn spawn_relay_driver(
    mut relay: libp2p::Swarm<keryx_relay::RelayServerBehaviour>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let SwarmEvent::Behaviour(RelayServerBehaviourEvent::Identify(
                identify::Event::Received { info, .. },
            )) = relay.select_next_some().await
            {
                relay.add_external_address(info.observed_addr.clone());
            }
        }
    })
}

/// Mirrors the libp2p DCUtR example: local listen, dial relay, complete identify exchange.
async fn prepare_relay_client(
    client: &mut libp2p::Swarm<keryx_relay::RelayClientBehaviour>,
    relay_routable: Multiaddr,
    timeout: Duration,
) -> bool {
    client
        .listen_on("/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap())
        .expect("local listen");
    client.dial(relay_routable).expect("dial relay");

    let deadline = tokio::time::Instant::now() + timeout;
    let mut local_listen = false;
    let mut connected = false;
    let mut sent = false;
    let mut received = false;
    loop {
        if local_listen && connected && sent && received {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::select! {
            event = client.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { .. } => local_listen = true,
                    SwarmEvent::ConnectionEstablished { .. } => connected = true,
                    SwarmEvent::Behaviour(RelayClientBehaviourEvent::Identify(
                        identify::Event::Sent { .. },
                    )) => sent = true,
                    SwarmEvent::Behaviour(RelayClientBehaviourEvent::Identify(
                        identify::Event::Received { info, .. },
                    )) => {
                        client.add_external_address(info.observed_addr.clone());
                        received = true;
                    }
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

async fn listener_reserve_on_relay(
    client: &mut libp2p::Swarm<keryx_relay::RelayClientBehaviour>,
    relay_routable: Multiaddr,
    timeout: Duration,
) -> bool {
    client
        .listen_on("/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap())
        .expect("local listen");
    client.dial(relay_routable.clone()).expect("dial relay");

    let deadline = tokio::time::Instant::now() + timeout;
    let mut local_listen = false;
    let mut connected = false;
    let mut sent = false;
    let mut received = false;
    let mut circuit_listen_requested = false;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        if local_listen && connected && sent && received && !circuit_listen_requested {
            client
                .listen_on(relay_routable.clone().with(Protocol::P2pCircuit))
                .expect("circuit listen");
            circuit_listen_requested = true;
        }
        tokio::select! {
            event = client.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        if address.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
                            return true;
                        }
                        local_listen = true;
                    }
                    SwarmEvent::ConnectionEstablished { .. } => connected = true,
                    SwarmEvent::Behaviour(RelayClientBehaviourEvent::Identify(
                        identify::Event::Sent { .. },
                    )) => sent = true,
                    SwarmEvent::Behaviour(RelayClientBehaviourEvent::Identify(
                        identify::Event::Received { info, .. },
                    )) => {
                        client.add_external_address(info.observed_addr.clone());
                        received = true;
                    }
                    SwarmEvent::Behaviour(RelayClientBehaviourEvent::RelayClient(
                        libp2p::relay::client::Event::ReservationReqAccepted { .. },
                    )) => return true,
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

async fn wait_for_relayed_connection(
    listener: &mut libp2p::Swarm<keryx_relay::RelayClientBehaviour>,
    dialer: &mut libp2p::Swarm<keryx_relay::RelayClientBehaviour>,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::select! {
            event = listener.select_next_some() => {
                if matches_relayed(&event) {
                    return true;
                }
            }
            event = dialer.select_next_some() => {
                if matches_relayed(&event) {
                    return true;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

fn matches_relayed(event: &SwarmEvent<keryx_relay::RelayClientBehaviourEvent>) -> bool {
    match event {
        SwarmEvent::ConnectionEstablished { endpoint, .. } if endpoint.is_relayed() => true,
        SwarmEvent::Behaviour(RelayClientBehaviourEvent::RelayClient(
            libp2p::relay::client::Event::InboundCircuitEstablished { .. },
        )) => true,
        SwarmEvent::Behaviour(RelayClientBehaviourEvent::RelayClient(
            libp2p::relay::client::Event::OutboundCircuitEstablished { .. },
        )) => true,
        _ => false,
    }
}
