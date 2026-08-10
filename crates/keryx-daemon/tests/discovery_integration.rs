use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use keryx_core::{NodeId, PeerId};
use keryx_daemon::{
    serve_daemon_rpc, ConfiguredSkill, DiscoverySettings, KeryxDaemonConfig, KeryxDaemonRuntime,
    RegistrationSettings, DEFAULT_REGISTRATION_TTL_SECONDS,
};
use keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient;
use keryx_proto::v1::{
    registry_service_client::RegistryServiceClient, DiscoverBySkillRequest, DiscoverSkillsRequest,
};
use keryx_relay::{
    security::NodeTokenAuth, serve_registry_rpc, RegistryRpcService, SkillRegistry,
    DEFAULT_REGISTRATION_TTL,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

async fn start_registry(
    registry: Arc<SkillRegistry>,
) -> (
    String,
    RegistryServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{addr}");
    let tokens = ["daemon-peer-a", "daemon-peer-b"]
        .into_iter()
        .map(|peer_id| {
            (
                NodeId::new(peer_id).unwrap(),
                format!("{peer_id}-test-token"),
            )
        })
        .collect::<HashMap<_, _>>();
    let server = tokio::spawn(serve_registry_rpc(
        RegistryRpcService::with_auth(
            registry,
            Arc::new(NodeTokenAuth::new(tokens, HashSet::new())),
        ),
        listener,
    ));
    let client = RegistryServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    (endpoint, client, server)
}

fn discovery_config(
    data_dir: std::path::PathBuf,
    registry_endpoint: String,
    peer_id: &str,
) -> KeryxDaemonConfig {
    let settings = DiscoverySettings {
        registry_endpoint,
        registry_ca_cert_path: None,
        registration: Some(RegistrationSettings {
            skills: vec![ConfiguredSkill {
                skill_id: "python".into(),
                description: "python tasks".into(),
                tags: vec!["backend".into()],
            }],
            name: "daemon-test".into(),
            description: "integration test daemon".into(),
            ttl_seconds: DEFAULT_REGISTRATION_TTL_SECONDS,
            refresh_interval: RegistrationSettings::refresh_interval_for_ttl(
                DEFAULT_REGISTRATION_TTL_SECONDS,
            ),
        }),
        node_token: Some(format!("{peer_id}-test-token")),
    };
    KeryxDaemonConfig::new(data_dir, 1)
        .with_local_peer_id(PeerId::new(peer_id).unwrap())
        .with_discovery(Some(settings))
        .with_daemon_rpc_token(Some("keryx-discovery-test-daemon-token".to_string()))
}

#[tokio::test]
async fn daemon_registers_and_discover_skills_returns_registration() {
    let registry = Arc::new(SkillRegistry::with_default_ttl(DEFAULT_REGISTRATION_TTL));
    let (registry_endpoint, mut registry_client, registry_server) =
        start_registry(Arc::clone(&registry)).await;

    let dir = TempDir::new().unwrap();
    let config = discovery_config(dir.path().join("data"), registry_endpoint, "daemon-peer-a");
    let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let daemon_addr = listener.local_addr().unwrap();
    let daemon_server = tokio::spawn(serve_daemon_rpc(
        runtime.as_ref().clone(),
        TcpListenerStream::new(listener),
    ));
    let mut daemon_client = KeryxDaemonClient::connect(format!("http://{daemon_addr}"))
        .await
        .unwrap();

    let direct = registry_client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "python".into(),
            tags: vec![],
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(direct.registrations.len(), 1);
    assert_eq!(direct.registrations[0].peer_id, "daemon-peer-a");

    let proxied = daemon_client
        .discover_skills(DiscoverSkillsRequest {
            skill_id: "python".into(),
            tags: vec![],
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(proxied.registrations.len(), 1);
    assert_eq!(proxied.registrations[0].peer_id, "daemon-peer-a");

    Arc::clone(&runtime).shutdown().await.unwrap();

    let after_shutdown = registry_client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "python".into(),
            tags: vec![],
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(after_shutdown.registrations.is_empty());

    daemon_server.abort();
    registry_server.abort();
}

#[tokio::test]
async fn daemon_startup_registers_configured_skills() {
    let registry = Arc::new(SkillRegistry::with_default_ttl(DEFAULT_REGISTRATION_TTL));
    let (registry_endpoint, mut registry_client, registry_server) =
        start_registry(Arc::clone(&registry)).await;

    let dir = TempDir::new().unwrap();
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(discovery_config(
            dir.path().join("data"),
            registry_endpoint,
            "daemon-peer-b",
        ))
        .await
        .unwrap(),
    );

    let found = registry_client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "python".into(),
            tags: vec!["backend".into()],
            limit: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(found.registrations.len(), 1);
    assert_eq!(found.registrations[0].peer_id, "daemon-peer-b");

    Arc::clone(&runtime).shutdown().await.unwrap();
    registry_server.abort();
}
