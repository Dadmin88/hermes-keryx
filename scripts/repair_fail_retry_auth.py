#!/usr/bin/env python3
from pathlib import Path

path = Path("crates/keryx-daemon/tests/task_fail_retry.rs")
text = path.read_text(encoding="utf-8")

imports = "use tokio::net::TcpListener;\nuse tokio_stream::wrappers::TcpListenerStream;\n"
replacement = (
    imports
    + "use tonic::service::Interceptor;\n"
    + "use tonic::transport::Channel;\n"
    + "use tonic::Request;\n"
)
if text.count(imports) != 1:
    raise SystemExit(f"task-fail-retry import anchor drifted: {text.count(imports)}")
text = text.replace(imports, replacement, 1)

marker = "#[tokio::test]\nasync fn fail_task_via_rpc_requeues_with_retry_count_until_dead_lettered() {\n"
if text.count(marker) != 1:
    raise SystemExit("task-fail-retry test anchor drifted")
interceptor = r'''const TEST_DAEMON_TOKEN: &str = "keryx-fail-retry-test-daemon-token";

#[derive(Clone)]
struct TestDaemonTokenInterceptor;

impl Interceptor for TestDaemonTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {TEST_DAEMON_TOKEN}")
                .parse()
                .expect("static fail-retry token is valid metadata"),
        );
        Ok(request)
    }
}

type TestDaemonClient = keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient<
    tonic::service::interceptor::InterceptedService<Channel, TestDaemonTokenInterceptor>,
>;

async fn authenticated_client(addr: std::net::SocketAddr) -> TestDaemonClient {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient::with_interceptor(
        channel,
        TestDaemonTokenInterceptor,
    )
}

'''
text = text.replace(marker, interceptor + marker, 1)

old_config = '''    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(data_dir, 42).with_fail_retry_policy(policy),
    )
'''
new_config = '''    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(data_dir, 42)
            .with_fail_retry_policy(policy)
            .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string())),
    )
'''
if text.count(old_config) != 1:
    raise SystemExit(f"task-fail-retry config anchor drifted: {text.count(old_config)}")
text = text.replace(old_config, new_config, 1)

old_client = '''    let mut client =
        keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
'''
if text.count(old_client) != 1:
    raise SystemExit(f"task-fail-retry client anchor drifted: {text.count(old_client)}")
text = text.replace(old_client, "    let mut client = authenticated_client(addr).await;\n", 1)

path.write_text(text, encoding="utf-8")
print("task-fail-retry RPC fixture authenticated")
