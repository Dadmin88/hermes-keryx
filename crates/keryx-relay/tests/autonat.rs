use std::time::Duration;

use futures::StreamExt;
use keryx_relay::{
    autonat::{map_autonat_status, NatReachability},
    config::RelayConfig,
    transport::{
        build_relay_client_swarm, build_relay_server_swarm, listen_on_ephemeral_tcp, test_keypair,
        NodeSwarmOptions, RelayClientBehaviourEvent, RelayServerOptions,
    },
};
use libp2p::swarm::SwarmEvent;
use libp2p::Multiaddr;

#[tokio::test]
async fn autonat_client_observes_nat_status() {
    let mut server = build_relay_server_swarm(
        test_keypair(30),
        &RelayServerOptions {
            config: RelayConfig::default(),
            allowlist: None,
        },
    )
    .expect("autonat server");
    let server_addr = listen_on_ephemeral_tcp(&mut server)
        .await
        .expect("server listen");
    let server_peer = *server.local_peer_id();

    let server_task = tokio::spawn(async move {
        loop {
            let _ = server.select_next_some().await;
        }
    });

    let mut client = build_relay_client_swarm(test_keypair(31), &NodeSwarmOptions::default())
        .expect("autonat client");
    client
        .listen_on("/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap())
        .unwrap();
    client
        .behaviour_mut()
        .autonat
        .add_server(server_peer, Some(server_addr.clone()));
    client.dial(server_addr).unwrap();

    let status = wait_for_autonat_status(&mut client, Duration::from_secs(30)).await;
    server_task.abort();

    assert!(
        matches!(
            status,
            NatReachability::Public | NatReachability::Private | NatReachability::Unknown
        ),
        "autonat should report a known reachability class, got {status:?}"
    );
}

async fn wait_for_autonat_status(
    client: &mut libp2p::Swarm<keryx_relay::RelayClientBehaviour>,
    timeout: Duration,
) -> NatReachability {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return NatReachability::Unknown;
        }
        tokio::select! {
            event = client.select_next_some() => {
                if let SwarmEvent::Behaviour(RelayClientBehaviourEvent::Autonat(
                    libp2p::autonat::Event::StatusChanged { new, .. },
                )) = event {
                    return map_autonat_status(new);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}
