#!/usr/bin/env python3
from pathlib import Path

# Authenticate the real network worker-loop fixture.
path = Path("crates/keryx-daemon/tests/e2e_worker_loop.rs")
text = path.read_text(encoding="utf-8")

imports = "use tokio::net::TcpListener;\nuse tokio_stream::wrappers::TcpListenerStream;\n"
replacement_imports = (
    imports
    + "use tonic::service::Interceptor;\n"
    + "use tonic::Request;\n"
)
if text.count(imports) != 1:
    raise SystemExit(f"worker-loop import anchor drifted: {text.count(imports)}")
text = text.replace(imports, replacement_imports, 1)

marker = "fn envelope(task_id: &str) -> TaskEnvelope {\n"
if text.count(marker) != 1:
    raise SystemExit("worker-loop envelope anchor drifted")
interceptor = r'''const TEST_DAEMON_TOKEN: &str = "keryx-worker-loop-test-daemon-token";

#[derive(Clone)]
struct TestDaemonTokenInterceptor;

impl Interceptor for TestDaemonTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {TEST_DAEMON_TOKEN}")
                .parse()
                .expect("static test daemon token is valid metadata"),
        );
        Ok(request)
    }
}

'''
text = text.replace(marker, interceptor + marker, 1)

old_config = '''    let config = KeryxDaemonConfig::new(data_dir, 0)
        .with_lease_recovery_interval_ms(25)
        .with_fail_retry_policy(keryx_core::RetryPolicy::no_retries());
'''
new_config = '''    let config = KeryxDaemonConfig::new(data_dir, 0)
        .with_lease_recovery_interval_ms(25)
        .with_fail_retry_policy(keryx_core::RetryPolicy::no_retries())
        .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string()));
'''
if text.count(old_config) != 1:
    raise SystemExit(f"worker-loop config anchor drifted: {text.count(old_config)}")
text = text.replace(old_config, new_config, 1)

old_client = '''    let mut client = KeryxDaemonClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
'''
new_client = '''    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = KeryxDaemonClient::with_interceptor(channel, TestDaemonTokenInterceptor);
'''
if text.count(old_client) != 1:
    raise SystemExit(f"worker-loop client anchor drifted: {text.count(old_client)}")
text = text.replace(old_client, new_client, 1)

path.write_text(text, encoding="utf-8")

# Graceful-shutdown tests exercise shutdown ordering, not authentication. Give
# every real listener a test credential and use an authenticated client so the
# sensitive result-delivery calls still reach the shutdown gate first.
graceful_path = Path("crates/keryx-daemon/tests/graceful_shutdown.rs")
graceful = graceful_path.read_text(encoding="utf-8")

graceful_imports = "use tokio_stream::wrappers::TcpListenerStream;\nuse tonic::Code;\n"
graceful_import_replacement = (
    "use tokio_stream::wrappers::TcpListenerStream;\n"
    "use tonic::service::Interceptor;\n"
    "use tonic::transport::Channel;\n"
    "use tonic::{Code, Request};\n"
)
if graceful.count(graceful_imports) != 1:
    raise SystemExit(
        f"graceful-shutdown import anchor drifted: {graceful.count(graceful_imports)}"
    )
graceful = graceful.replace(graceful_imports, graceful_import_replacement, 1)

spawn_marker = "async fn spawn_rpc_server(\n"
if graceful.count(spawn_marker) != 1:
    raise SystemExit("graceful-shutdown server helper anchor drifted")
graceful_auth = r'''const TEST_DAEMON_TOKEN: &str = "keryx-graceful-shutdown-daemon-token";

#[derive(Clone)]
struct TestDaemonTokenInterceptor;

impl Interceptor for TestDaemonTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {TEST_DAEMON_TOKEN}")
                .parse()
                .expect("static graceful-shutdown token is valid metadata"),
        );
        Ok(request)
    }
}

type TestDaemonClient = KeryxDaemonClient<
    tonic::service::interceptor::InterceptedService<Channel, TestDaemonTokenInterceptor>,
>;

async fn authenticated_client(endpoint: String) -> TestDaemonClient {
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    KeryxDaemonClient::with_interceptor(channel, TestDaemonTokenInterceptor)
}

'''
graceful = graceful.replace(spawn_marker, graceful_auth + spawn_marker, 1)

old_runtime = "        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir, 42))\n"
new_runtime = (
    "        KeryxDaemonRuntime::startup(\n"
    "            KeryxDaemonConfig::new(data_dir, 42)\n"
    "                .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string())),\n"
    "        )\n"
)
if graceful.count(old_runtime) != 3:
    raise SystemExit(
        f"expected three graceful-shutdown runtime fixtures, found {graceful.count(old_runtime)}"
    )
graceful = graceful.replace(old_runtime, new_runtime)

old_graceful_client = "    let mut client = KeryxDaemonClient::connect(endpoint).await.unwrap();\n"
new_graceful_client = "    let mut client = authenticated_client(endpoint).await;\n"
if graceful.count(old_graceful_client) != 3:
    raise SystemExit(
        f"expected three graceful-shutdown clients, found {graceful.count(old_graceful_client)}"
    )
graceful = graceful.replace(old_graceful_client, new_graceful_client)
graceful_path.write_text(graceful, encoding="utf-8")

print("worker-loop and graceful-shutdown daemon fixtures authenticated")
