#!/usr/bin/env python3
from pathlib import Path

path = Path("crates/keryx-daemon/tests/lease_recovery_loop.rs")
text = path.read_text(encoding="utf-8")

imports = "use tokio::net::TcpListener;\nuse tokio_stream::wrappers::TcpListenerStream;\n"
replacement_imports = (
    imports
    + "use tonic::service::Interceptor;\n"
    + "use tonic::transport::Channel;\n"
    + "use tonic::Request;\n"
)
if text.count(imports) != 1:
    raise SystemExit(f"lease-recovery import anchor drifted: {text.count(imports)}")
text = text.replace(imports, replacement_imports, 1)

marker = "fn task(id: &str, idem: &str) -> TaskRecord {\n"
if text.count(marker) != 1:
    raise SystemExit("lease-recovery helper anchor drifted")
interceptor = r'''const TEST_DAEMON_TOKEN: &str = "keryx-lease-recovery-test-daemon-token";

#[derive(Clone)]
struct TestDaemonTokenInterceptor;

impl Interceptor for TestDaemonTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {TEST_DAEMON_TOKEN}")
                .parse()
                .expect("static lease-recovery token is valid metadata"),
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

old_config = '''    let config = KeryxDaemonConfig::new(data_dir, 0).with_lease_recovery_interval_ms(20);
'''
new_config = '''    let config = KeryxDaemonConfig::new(data_dir, 0)
        .with_lease_recovery_interval_ms(20)
        .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string()));
'''
if text.count(old_config) != 1:
    raise SystemExit(f"lease-recovery RPC config anchor drifted: {text.count(old_config)}")
text = text.replace(old_config, new_config, 1)

old_client = '''    let mut client =
        keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
'''
new_client = '''    let mut client = authenticated_client(addr).await;
'''
if text.count(old_client) != 1:
    raise SystemExit(f"lease-recovery RPC client anchor drifted: {text.count(old_client)}")
text = text.replace(old_client, new_client, 1)

path.write_text(text, encoding="utf-8")
print("lease-recovery RPC fixture authenticated")
