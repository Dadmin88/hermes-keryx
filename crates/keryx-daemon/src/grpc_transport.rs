//! Shared TLS policy for outbound Keryx gRPC control and registry connections.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use thiserror::Error;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

/// Optional PEM CA file used for authenticated relay control and registry TLS.
pub const KERYX_CA_CERT_ENV: &str = "HERMES_KERYX_REGISTRY_CA_CERT";

#[derive(Debug, Error)]
pub enum GrpcTransportError {
    #[error("invalid Keryx gRPC endpoint {endpoint}: {reason}")]
    InvalidEndpoint { endpoint: String, reason: String },
    #[error("remote Keryx gRPC endpoints require TLS (https://): {endpoint}")]
    RemotePlaintext { endpoint: String },
    #[error("failed to read Keryx CA certificate {path}: {reason}")]
    ReadCa { path: PathBuf, reason: String },
    #[error("invalid Keryx gRPC TLS configuration for {endpoint}: {reason}")]
    InvalidTls { endpoint: String, reason: String },
}

/// Resolve the configured private CA path, when present.
#[must_use]
pub fn ca_cert_path_from_env() -> Option<PathBuf> {
    std::env::var_os(KERYX_CA_CERT_ENV)
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

/// Build a Tonic endpoint that permits plaintext only for loopback hosts.
pub fn secure_grpc_endpoint(
    endpoint: &str,
    ca_cert_path: Option<&Path>,
) -> Result<Endpoint, GrpcTransportError> {
    let mut builder = Endpoint::from_shared(endpoint.to_string()).map_err(|error| {
        GrpcTransportError::InvalidEndpoint {
            endpoint: endpoint.to_string(),
            reason: error.to_string(),
        }
    })?;
    let uri = builder.uri();
    let host = uri
        .host()
        .ok_or_else(|| GrpcTransportError::InvalidEndpoint {
            endpoint: endpoint.to_string(),
            reason: "endpoint must include a host".to_string(),
        })?;
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
    let secure = uri.scheme_str() == Some("https");
    if !secure && !loopback {
        return Err(GrpcTransportError::RemotePlaintext {
            endpoint: endpoint.to_string(),
        });
    }
    if secure {
        let mut tls = ClientTlsConfig::new().with_native_roots();
        if let Some(path) = ca_cert_path {
            let pem = std::fs::read(path).map_err(|error| GrpcTransportError::ReadCa {
                path: path.to_path_buf(),
                reason: error.to_string(),
            })?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        builder = builder
            .tls_config(tls)
            .map_err(|error| GrpcTransportError::InvalidTls {
                endpoint: endpoint.to_string(),
                reason: error.to_string(),
            })?;
    }
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_plaintext_endpoint_fails_closed() {
        let error = secure_grpc_endpoint("http://192.0.2.1:50052", None).unwrap_err();
        assert!(matches!(error, GrpcTransportError::RemotePlaintext { .. }));
    }

    #[test]
    fn loopback_plaintext_endpoint_is_allowed() {
        let endpoint = secure_grpc_endpoint("http://127.0.0.1:50052", None).unwrap();
        assert_eq!(endpoint.uri().scheme_str(), Some("http"));
    }

    #[test]
    fn https_endpoint_accepts_private_ca() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, b"test-ca-pem").unwrap();
        let endpoint = secure_grpc_endpoint("https://relay.example:50052", Some(&path)).unwrap();
        assert_eq!(endpoint.uri().scheme_str(), Some("https"));
    }
}
