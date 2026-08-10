#!/usr/bin/env python3
from pathlib import Path

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
print("worker-loop daemon fixture authenticated")
