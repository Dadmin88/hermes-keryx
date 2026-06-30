use anyhow::Result;
use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir(), now_ms())).await?;
    tracing::info!(
        component = "keryxd",
        db_path = %runtime.report().db_path.display(),
        schema_version = runtime.report().schema_version,
        recovered_tasks = runtime.report().recovered_tasks,
        "Hermes Keryx daemon runtime ready"
    );

    if let Some(addr) = daemon_addr() {
        let listener = TcpListener::bind(&addr).await?;
        let local_addr = listener.local_addr()?;
        tracing::info!(
            component = "keryxd",
            listen_addr = %local_addr,
            "Hermes Keryx daemon RPC service listening"
        );
        serve_daemon_rpc(runtime, TcpListenerStream::new(listener)).await?;
    }

    Ok(())
}

fn data_dir() -> std::path::PathBuf {
    std::env::var_os("HERMES_KERYX_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(".keryx"))
}

fn daemon_addr() -> Option<String> {
    std::env::var("HERMES_KERYX_DAEMON_ADDR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
