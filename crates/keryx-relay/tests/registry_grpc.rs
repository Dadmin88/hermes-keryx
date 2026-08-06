use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use keryx_core::NodeId;
use keryx_proto::v1::{
    registry_service_client::RegistryServiceClient, DiscoverBySkillRequest, RegisterSkillsRequest,
    SkillInfo, UnregisterSkillsRequest,
};
use keryx_relay::{
    health_server::{NODE_ID_METADATA_KEY, NODE_TOKEN_METADATA_KEY},
    security::NodeTokenAuth,
    serve_registry_rpc, serve_registry_rpc_with_tls, RegistryRpcService, SkillRegistry,
    DEFAULT_REGISTRATION_TTL,
};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request};

fn skill(id: &str, tags: &[&str]) -> SkillInfo {
    SkillInfo {
        skill_id: id.to_string(),
        description: format!("{id} skill"),
        tags: tags.iter().map(|t| (*t).to_string()).collect(),
    }
}

fn test_node_auth() -> Arc<NodeTokenAuth> {
    let tokens = ["peer-grpc", "p1", "p2", "p3", "peer-ttl-grpc", "peer-drop"]
        .into_iter()
        .map(|peer_id| {
            (
                NodeId::new(peer_id).unwrap(),
                format!("{peer_id}-test-token"),
            )
        })
        .collect::<HashMap<_, _>>();
    Arc::new(NodeTokenAuth::new(tokens, HashSet::new()))
}

#[tokio::test]
async fn plaintext_registry_helper_rejects_non_loopback_listener() {
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let service = RegistryRpcService::with_auth(
        Arc::new(SkillRegistry::with_default_ttl(Duration::from_secs(30))),
        test_node_auth(),
    );

    let error = serve_registry_rpc(service, listener).await.unwrap_err();

    assert!(error
        .to_string()
        .contains("non-loopback registry listeners require TLS"));
}

fn authenticated_request<T>(message: T, peer_id: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert(NODE_ID_METADATA_KEY, peer_id.parse().unwrap());
    request.metadata_mut().insert(
        NODE_TOKEN_METADATA_KEY,
        format!("{peer_id}-test-token").parse().unwrap(),
    );
    request
}

async fn start_test_server_with_service(
    service: RegistryRpcService,
) -> (
    RegistryServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_registry_rpc(service, listener));
    let client = RegistryServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    (client, server)
}

async fn start_test_server(
    registry: Arc<SkillRegistry>,
) -> (
    RegistryServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    start_test_server_with_service(RegistryRpcService::with_auth(registry, test_node_auth())).await
}

async fn start_unauthenticated_test_server(
    registry: Arc<SkillRegistry>,
) -> (
    RegistryServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    start_test_server_with_service(RegistryRpcService::new(registry)).await
}

#[tokio::test]
async fn grpc_registry_tls_accepts_authenticated_mutation() {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    let registry = Arc::new(SkillRegistry::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = RegistryRpcService::with_auth(Arc::clone(&registry), test_node_auth());
    let server = tokio::spawn(serve_registry_rpc_with_tls(
        service,
        listener,
        Some(Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes())),
    ));
    let endpoint = Endpoint::from_shared(format!("https://localhost:{}", addr.port()))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(cert_pem.as_bytes())),
        )
        .unwrap();
    let channel = endpoint.connect().await.unwrap();
    let mut client = RegistryServiceClient::new(channel);

    let response = client
        .register_skills(authenticated_request(
            RegisterSkillsRequest {
                peer_id: "p1".into(),
                skills: vec![skill("python", &["backend"])],
                name: "TLS worker".into(),
                description: "encrypted registry mutation".into(),
                ttl_seconds: 60,
                protocol_features: Vec::new(),
            },
            "p1",
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(response.accepted);
    assert_eq!(registry.registration_count().await, 1);
    server.abort();
}

#[tokio::test]
async fn grpc_registry_mutations_fail_closed_without_node_auth() {
    let registry = Arc::new(SkillRegistry::new());
    let (mut client, server) = start_unauthenticated_test_server(Arc::clone(&registry)).await;

    let register_error = client
        .register_skills(RegisterSkillsRequest {
            peer_id: "peer-unauthenticated".into(),
            skills: vec![skill("python", &[])],
            name: "Unauthenticated".into(),
            description: "must not be stored".into(),
            ttl_seconds: 60,
            protocol_features: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(register_error.code(), Code::Unauthenticated);

    let unregister_error = client
        .unregister_skills(UnregisterSkillsRequest {
            peer_id: "peer-unauthenticated".into(),
            skill_ids: vec!["python".into()],
        })
        .await
        .unwrap_err();
    assert_eq!(unregister_error.code(), Code::Unauthenticated);
    assert_eq!(registry.registration_count().await, 0);

    server.abort();
}

#[tokio::test]
async fn grpc_registry_mutations_require_authenticated_owner_identity() {
    let registry = Arc::new(SkillRegistry::new());
    let (mut client, server) = start_test_server(Arc::clone(&registry)).await;

    client
        .register_skills(authenticated_request(
            RegisterSkillsRequest {
                peer_id: "p2".into(),
                skills: vec![skill("owned", &[])],
                name: "owner".into(),
                description: "".into(),
                ttl_seconds: 300,
                protocol_features: Vec::new(),
            },
            "p2",
        ))
        .await
        .unwrap();

    let missing_metadata = client
        .register_skills(RegisterSkillsRequest {
            peer_id: "p1".into(),
            skills: vec![skill("missing-auth", &[])],
            name: "missing".into(),
            description: "".into(),
            ttl_seconds: 300,
            protocol_features: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(missing_metadata.code(), Code::Unauthenticated);

    let mut invalid_token = authenticated_request(
        RegisterSkillsRequest {
            peer_id: "p1".into(),
            skills: vec![skill("invalid-token", &[])],
            name: "invalid".into(),
            description: "".into(),
            ttl_seconds: 300,
            protocol_features: Vec::new(),
        },
        "p1",
    );
    invalid_token
        .metadata_mut()
        .insert(NODE_TOKEN_METADATA_KEY, "wrong-token".parse().unwrap());
    let invalid_token = client.register_skills(invalid_token).await.unwrap_err();
    assert_eq!(invalid_token.code(), Code::Unauthenticated);

    let mut malformed_register = Request::new(RegisterSkillsRequest {
        peer_id: "malformed peer id".into(),
        skills: vec![skill("malformed-auth", &[])],
        name: "malformed".into(),
        description: "".into(),
        ttl_seconds: 300,
        protocol_features: Vec::new(),
    });
    malformed_register
        .metadata_mut()
        .insert(NODE_ID_METADATA_KEY, "malformed peer id".parse().unwrap());
    malformed_register
        .metadata_mut()
        .insert(NODE_TOKEN_METADATA_KEY, "p1-test-token".parse().unwrap());
    let malformed_register = client
        .register_skills(malformed_register)
        .await
        .unwrap_err();
    assert_eq!(malformed_register.code(), Code::Unauthenticated);

    let mut malformed_unregister = Request::new(UnregisterSkillsRequest {
        peer_id: "malformed peer id".into(),
        skill_ids: vec!["owned".into()],
    });
    malformed_unregister
        .metadata_mut()
        .insert(NODE_ID_METADATA_KEY, "malformed peer id".parse().unwrap());
    malformed_unregister
        .metadata_mut()
        .insert(NODE_TOKEN_METADATA_KEY, "p1-test-token".parse().unwrap());
    let malformed_unregister = client
        .unregister_skills(malformed_unregister)
        .await
        .unwrap_err();
    assert_eq!(malformed_unregister.code(), Code::Unauthenticated);

    let mismatch = client
        .register_skills(authenticated_request(
            RegisterSkillsRequest {
                peer_id: "p2".into(),
                skills: vec![skill("forged", &[])],
                name: "forged".into(),
                description: "".into(),
                ttl_seconds: 300,
                protocol_features: Vec::new(),
            },
            "p1",
        ))
        .await
        .unwrap_err();
    assert_eq!(mismatch.code(), Code::PermissionDenied);

    let cross_peer_unregister = client
        .unregister_skills(authenticated_request(
            UnregisterSkillsRequest {
                peer_id: "p2".into(),
                skill_ids: vec!["owned".into()],
            },
            "p1",
        ))
        .await
        .unwrap_err();
    assert_eq!(cross_peer_unregister.code(), Code::PermissionDenied);

    let registration = client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "owned".into(),
            tags: vec![],
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(registration.registrations.len(), 1);
    assert_eq!(registration.registrations[0].peer_id, "p2");
    assert_eq!(registry.registration_count().await, 1);

    server.abort();
}

#[tokio::test]
async fn grpc_register_and_discover_skill() {
    let registry = Arc::new(SkillRegistry::with_default_ttl(DEFAULT_REGISTRATION_TTL));
    let (mut client, server) = start_test_server(registry).await;

    client
        .register_skills(authenticated_request(
            RegisterSkillsRequest {
                peer_id: "peer-grpc".into(),
                skills: vec![skill("python", &[])],
                name: "Py Agent".into(),
                description: "runs python".into(),
                ttl_seconds: 60,
                protocol_features: Vec::new(),
            },
            "peer-grpc",
        ))
        .await
        .unwrap();

    let resp = client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "python".into(),
            tags: vec![],
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.registrations.len(), 1);
    assert_eq!(resp.registrations[0].peer_id, "peer-grpc");

    server.abort();
}

#[tokio::test]
async fn grpc_discover_filters_tags_and_limit() {
    let registry = Arc::new(SkillRegistry::new());
    let (mut client, server) = start_test_server(registry).await;

    for (peer, tags) in [
        ("p1", vec!["ml"]),
        ("p2", vec!["ml", "gpu"]),
        ("p3", vec!["web"]),
    ] {
        client
            .register_skills(authenticated_request(
                RegisterSkillsRequest {
                    peer_id: peer.into(),
                    skills: vec![skill("infer", &tags)],
                    name: peer.into(),
                    description: "".into(),
                    ttl_seconds: 0,
                    protocol_features: Vec::new(),
                },
                peer,
            ))
            .await
            .unwrap();
    }

    let tagged = client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "infer".into(),
            tags: vec!["gpu".into()],
            limit: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(tagged.registrations.len(), 1);
    assert_eq!(tagged.registrations[0].peer_id, "p2");

    let limited = client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "infer".into(),
            tags: vec![],
            limit: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(limited.registrations.len(), 1);

    server.abort();
}

#[tokio::test]
async fn grpc_unregister_and_ttl() {
    let registry = Arc::new(SkillRegistry::new());
    let (mut client, server) = start_test_server(registry).await;

    client
        .register_skills(authenticated_request(
            RegisterSkillsRequest {
                peer_id: "peer-ttl-grpc".into(),
                skills: vec![skill("short", &[])],
                name: "tmp".into(),
                description: "".into(),
                ttl_seconds: 1,
                protocol_features: Vec::new(),
            },
            "peer-ttl-grpc",
        ))
        .await
        .unwrap();

    sleep(Duration::from_millis(1_200)).await;
    let expired = client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "short".into(),
            tags: vec![],
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(expired.registrations.is_empty());

    client
        .register_skills(authenticated_request(
            RegisterSkillsRequest {
                peer_id: "peer-drop".into(),
                skills: vec![skill("gone", &[]), skill("stay", &[])],
                name: "x".into(),
                description: "".into(),
                ttl_seconds: 300,
                protocol_features: Vec::new(),
            },
            "peer-drop",
        ))
        .await
        .unwrap();

    client
        .unregister_skills(authenticated_request(
            UnregisterSkillsRequest {
                peer_id: "peer-drop".into(),
                skill_ids: vec!["gone".into()],
            },
            "peer-drop",
        ))
        .await
        .unwrap();

    let left = client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: "".into(),
            tags: vec![],
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(left.registrations.len(), 1);
    assert_eq!(left.registrations[0].skills.len(), 1);
    assert_eq!(left.registrations[0].skills[0].skill_id, "stay");

    server.abort();
}
