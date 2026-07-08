use std::sync::Arc;
use std::time::Duration;

use keryx_proto::v1::{
    registry_service_client::RegistryServiceClient, DiscoverBySkillRequest, RegisterSkillsRequest,
    SkillInfo, UnregisterSkillsRequest,
};
use keryx_relay::{
    serve_registry_rpc, RegistryRpcService, SkillRegistry, DEFAULT_REGISTRATION_TTL,
};
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_stream::wrappers::TcpListenerStream;

fn skill(id: &str, tags: &[&str]) -> SkillInfo {
    SkillInfo {
        skill_id: id.to_string(),
        description: format!("{id} skill"),
        tags: tags.iter().map(|t| (*t).to_string()).collect(),
    }
}

async fn start_test_server(
    registry: Arc<SkillRegistry>,
) -> (
    RegistryServiceClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = RegistryRpcService::new(registry);
    let server = tokio::spawn(serve_registry_rpc(
        service,
        TcpListenerStream::new(listener),
    ));
    let client = RegistryServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    (client, server)
}

#[tokio::test]
async fn grpc_register_and_discover_skill() {
    let registry = Arc::new(SkillRegistry::with_default_ttl(DEFAULT_REGISTRATION_TTL));
    let (mut client, server) = start_test_server(registry).await;

    client
        .register_skills(RegisterSkillsRequest {
            peer_id: "peer-grpc".into(),
            skills: vec![skill("python", &[])],
            name: "Py Agent".into(),
            description: "runs python".into(),
            ttl_seconds: 60,
        })
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
            .register_skills(RegisterSkillsRequest {
                peer_id: peer.into(),
                skills: vec![skill("infer", &tags)],
                name: peer.into(),
                description: "".into(),
                ttl_seconds: 0,
            })
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
        .register_skills(RegisterSkillsRequest {
            peer_id: "peer-ttl-grpc".into(),
            skills: vec![skill("short", &[])],
            name: "tmp".into(),
            description: "".into(),
            ttl_seconds: 1,
        })
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
        .register_skills(RegisterSkillsRequest {
            peer_id: "peer-drop".into(),
            skills: vec![skill("gone", &[]), skill("stay", &[])],
            name: "x".into(),
            description: "".into(),
            ttl_seconds: 300,
        })
        .await
        .unwrap();

    client
        .unregister_skills(UnregisterSkillsRequest {
            peer_id: "peer-drop".into(),
            skill_ids: vec!["gone".into()],
        })
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
