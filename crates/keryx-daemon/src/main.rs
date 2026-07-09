use anyhow::Result;
use keryx_daemon::{
    discovery_settings_from_env, relay_endpoint_from_env, serve_daemon_rpc, KeryxDaemonConfig,
    KeryxDaemonRuntime,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let mut config = KeryxDaemonConfig::new(data_dir(), now_ms());
    if let Some(discovery) = discovery_settings_from_env() {
        config = config.with_discovery(Some(discovery));
    }
    if let Some(relay_endpoint) = relay_endpoint_from_env() {
        config = config.with_relay_endpoint(Some(relay_endpoint));
    }
    let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await?);
    tracing::info!(
        component = "keryxd",
        db_path = %runtime.report().db_path.display(),
        schema_version = runtime.report().schema_version,
        recovered_tasks = runtime.report().recovery.recovered_task_count(),
        cleaned_terminal_leases = runtime.report().recovery.cleaned_terminal_leases,
        corruption_count = runtime.report().recovery.corruption_count(),
        "Hermes Keryx daemon runtime ready"
    );

    if let Some(addr) = daemon_addr()? {
        let lease_recovery_loop = runtime.spawn_lease_recovery_loop();
        let deadline_enforcement_loop = runtime.spawn_deadline_enforcement_loop();
        let health_loop = runtime.spawn_health_loop();
        tracing::info!(
            component = "keryxd",
            lease_recovery_interval_ms = runtime.config().lease_recovery_interval_ms(),
            deadline_enforcement_interval_ms = runtime.config().deadline_enforcement_interval_ms(),
            health_check_interval_ms = runtime.config().health_check_interval_ms(),
            "Hermes Keryx background loops started"
        );

        let listener = TcpListener::bind(&addr).await?;
        let local_addr = listener.local_addr()?;
        tracing::info!(
            component = "keryxd",
            listen_addr = %local_addr,
            "Hermes Keryx daemon RPC service listening"
        );

        let rpc_runtime = (*runtime).clone();
        let serve_handle = tokio::spawn(serve_daemon_rpc(
            rpc_runtime,
            TcpListenerStream::new(listener),
        ));

        tokio::signal::ctrl_c().await?;
        tracing::info!(component = "keryxd", "shutdown signal received");

        lease_recovery_loop.shutdown().await;
        deadline_enforcement_loop.shutdown().await;
        health_loop.shutdown().await;
        Arc::clone(&runtime).shutdown().await?;
        serve_handle.await??;
    }

    Ok(())
}

fn data_dir() -> std::path::PathBuf {
    std::env::var_os("HERMES_KERYX_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(".keryx"))
}

fn daemon_addr() -> Result<Option<SocketAddr>> {
    std::env::var("HERMES_KERYX_DAEMON_ADDR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| {
            let addr: SocketAddr = value.parse()?;
            anyhow::ensure!(
                addr.ip().is_loopback(),
                "HERMES_KERYX_DAEMON_ADDR must be a loopback address"
            );
            Ok(addr)
        })
        .transpose()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::daemon_addr;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_daemon_addr(value: Option<&str>, test: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().unwrap();
        let previous = std::env::var_os("HERMES_KERYX_DAEMON_ADDR");
        match value {
            Some(value) => std::env::set_var("HERMES_KERYX_DAEMON_ADDR", value),
            None => std::env::remove_var("HERMES_KERYX_DAEMON_ADDR"),
        }
        test();
        match previous {
            Some(previous) => std::env::set_var("HERMES_KERYX_DAEMON_ADDR", previous),
            None => std::env::remove_var("HERMES_KERYX_DAEMON_ADDR"),
        }
    }

    #[test]
    fn daemon_addr_accepts_loopback_addresses() {
        with_daemon_addr(Some("127.0.0.1:50051"), || {
            assert_eq!(
                daemon_addr().unwrap().unwrap().to_string(),
                "127.0.0.1:50051"
            );
        });

        with_daemon_addr(Some("[::1]:50051"), || {
            assert_eq!(daemon_addr().unwrap().unwrap().to_string(), "[::1]:50051");
        });
    }

    #[test]
    fn daemon_addr_rejects_wildcard_and_non_loopback_addresses() {
        with_daemon_addr(Some("0.0.0.0:50051"), || {
            assert!(daemon_addr().unwrap_err().to_string().contains("loopback"));
        });

        with_daemon_addr(Some("[::]:50051"), || {
            assert!(daemon_addr().unwrap_err().to_string().contains("loopback"));
        });

        with_daemon_addr(Some("192.0.2.1:50051"), || {
            assert!(daemon_addr().unwrap_err().to_string().contains("loopback"));
        });
    }

    #[test]
    fn daemon_addr_ignores_empty_values() {
        with_daemon_addr(Some("  "), || {
            assert!(daemon_addr().unwrap().is_none());
        });

        with_daemon_addr(None, || {
            assert!(daemon_addr().unwrap().is_none());
        });
    }
}
