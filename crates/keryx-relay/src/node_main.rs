use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    keryx_relay::node::run_edge_node().await
}
