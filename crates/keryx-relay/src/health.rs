//! Health report construction and HTTP `/health` handler.

use std::sync::Arc;

use keryx_observe::RelayMetricsSnapshot;
use serde::{Deserialize, Serialize};

use crate::runtime::RelayRuntime;

/// JSON-friendly health payload (also mapped to gRPC `HealthResponse`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayHealthReport {
    pub healthy: bool,
    pub connected_peers: u64,
    pub registry_size: u64,
    pub uptime_seconds: u64,
    pub transport_status: String,
    pub tasks_routed: u64,
    pub local_peer_id: String,
}

impl RelayHealthReport {
    #[must_use]
    pub fn from_runtime(runtime: &RelayRuntime) -> Self {
        let metrics = runtime.metrics().snapshot();
        Self::from_parts(runtime, metrics)
    }

    #[must_use]
    pub fn from_parts(runtime: &RelayRuntime, metrics: RelayMetricsSnapshot) -> Self {
        Self {
            healthy: runtime.is_healthy(),
            connected_peers: metrics.connected_peers,
            registry_size: metrics.registry_size,
            uptime_seconds: runtime.uptime_seconds(),
            transport_status: runtime.transport_status().to_string(),
            tasks_routed: metrics.tasks_routed,
            local_peer_id: runtime.local_peer_id().to_string(),
        }
    }
}

/// Serialize a health report as compact JSON.
#[must_use]
pub fn health_json(report: &RelayHealthReport) -> String {
    serde_json::to_string(report).expect("health report serializes")
}

/// Handle a single HTTP/1.1 request line (GET /health).
#[must_use]
pub fn http_health_response(report: &RelayHealthReport) -> Vec<u8> {
    let body = health_json(report);
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

/// Serve one client connection with `GET /health`.
pub async fn serve_http_health_once(
    runtime: Arc<RelayRuntime>,
    mut stream: tokio::net::TcpStream,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]);
    if !request.starts_with("GET /health") {
        let response = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        stream.write_all(response).await?;
        return Ok(());
    }
    let report = RelayHealthReport::from_runtime(&runtime);
    stream.write_all(&http_health_response(&report)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_report_reflects_metrics_and_transport() {
        let runtime = RelayRuntime::new("12D3KooWExample");
        runtime.mark_transport_listening();
        runtime.metrics().set_connected_peers(3);
        runtime.metrics().set_registry_size(7);
        runtime.metrics().increment_tasks_routed();

        let report = RelayHealthReport::from_runtime(&runtime);
        assert!(report.healthy);
        assert_eq!(report.connected_peers, 3);
        assert_eq!(report.registry_size, 7);
        assert_eq!(report.tasks_routed, 1);
        assert_eq!(report.transport_status, "listening");
        assert_eq!(report.local_peer_id, "12D3KooWExample");
    }

    #[test]
    fn http_response_is_valid_json_body() {
        let runtime = RelayRuntime::new("peer");
        runtime.mark_transport_listening();
        let report = RelayHealthReport::from_runtime(&runtime);
        let bytes = http_health_response(&report);
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("HTTP/1.1 200 OK"));
        let body = text.split("\r\n\r\n").nth(1).unwrap();
        let parsed: RelayHealthReport = serde_json::from_str(body).unwrap();
        assert_eq!(parsed, report);
    }
}
