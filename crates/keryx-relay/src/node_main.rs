use anyhow::Result;
use keryx_relay::{
    fleet_observation_bridge::FleetObservationUdsBridge,
    node::{run_edge_node, run_edge_node_with_direct_control_handlers, DirectControlHandlers},
};
use std::{path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() -> Result<()> {
    let Some(socket) = std::env::var_os("HERMES_FLEET_REMOTE_OBSERVATION_SOCKET") else {
        return run_edge_node().await;
    };
    let bridge = FleetObservationUdsBridge::new(PathBuf::from(socket))?;
    run_edge_node_with_direct_control_handlers(DirectControlHandlers {
        fleet_observation_publish: Some(Arc::new(bridge)),
        ..DirectControlHandlers::default()
    })
    .await
}
