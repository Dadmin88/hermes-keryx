use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    tracing::info!(
        component = "keryx-relay",
        "Hermes Keryx relay skeleton starting"
    );
    Ok(())
}
