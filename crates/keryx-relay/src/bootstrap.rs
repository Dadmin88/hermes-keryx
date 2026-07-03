//! Bootstrap dialing and optional mDNS discovery.

use std::time::Duration;

use libp2p::core::transport::TransportError;
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::Multiaddr;
use tracing::{info, warn};

/// Dial each configured bootstrap peer, logging failures but continuing.
pub fn dial_bootstrap_peers<B>(swarm: &mut Swarm<B>, peers: &[Multiaddr])
where
    B: NetworkBehaviour,
{
    for addr in peers {
        match swarm.dial(addr.clone()) {
            Ok(()) => info!(%addr, "dialing bootstrap peer"),
            Err(err) => warn!(%addr, error = %err, "failed to dial bootstrap peer"),
        }
    }
}

/// Block up to `timeout` waiting for at least one listen address on the swarm.
pub async fn wait_for_listen_addr<B>(swarm: &mut Swarm<B>, timeout: Duration) -> bool
where
    B: NetworkBehaviour,
{
    use futures::StreamExt;

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::select! {
            event = swarm.select_next_some() => {
                if matches!(event, SwarmEvent::NewListenAddr { .. }) {
                    return true;
                }
            }
            _ = tokio::time::sleep(remaining.min(Duration::from_millis(200))) => {
                if tokio::time::Instant::now() >= deadline {
                    return false;
                }
            }
        }
    }
}

/// Classify transport errors for bootstrap retries (used in tests).
pub fn is_transient_dial_error(err: &TransportError<std::io::Error>) -> bool {
    matches!(err, TransportError::MultiaddrNotSupported(_))
}
