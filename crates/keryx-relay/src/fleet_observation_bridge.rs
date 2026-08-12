use std::{
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};
use keryx_proto::v1::{
    fleet_observation_publish_v1, FleetObservationAuthorityEpochV1,
    FleetObservationPublishDisposition, FleetObservationPublishResultV1, FleetObservationPublishV1,
};
use serde::{
    de::{Error as _, MapAccess, SeqAccess, Visitor},
    Deserialize, Serialize,
};
use serde_json::{json, Map, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::timeout,
};

use crate::node::{AuthenticatedDirectContext, FleetObservationPublishHandler};

pub(crate) const MAX_FLEET_OBSERVATION_JSON_BYTES: usize = 64 * 1024;
pub(crate) const MAX_FLEET_OBSERVATION_BRIDGE_FRAME_BYTES: usize = 128 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(3);
const SCHEMA: &str = "fleet.remote-observation-internal.v1";

struct UniqueJson(Value);

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJson;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("JSON without duplicate object members")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueJson)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<UniqueJson>()? {
            values.push(value.0);
        }
        Ok(UniqueJson(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(A::Error::custom("duplicate JSON object member"));
            }
            let value = object.next_value::<UniqueJson>()?;
            values.insert(key, value.0);
        }
        Ok(UniqueJson(Value::Object(values)))
    }
}

fn parse_unique_json(input: &[u8]) -> serde_json::Result<Value> {
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let value = UniqueJson::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value.0)
}

#[derive(Clone, Debug)]
pub struct FleetObservationUdsBridge {
    socket_path: PathBuf,
}

impl FleetObservationUdsBridge {
    pub fn new(socket_path: PathBuf) -> Result<Self> {
        anyhow::ensure!(
            socket_path.is_absolute(),
            "Fleet observation socket must be absolute"
        );
        Ok(Self { socket_path })
    }

    fn validate_socket_path(&self) -> Result<()> {
        let metadata = std::fs::symlink_metadata(&self.socket_path)
            .context("Fleet observation socket metadata is unavailable")?;
        validate_socket_metadata(&metadata, effective_uid())
    }

    fn validate_connected_peer(stream: &UnixStream) -> Result<()> {
        let credentials = stream
            .peer_cred()
            .context("Fleet observation peer credentials are unavailable")?;
        anyhow::ensure!(
            credentials.uid() == effective_uid(),
            "Fleet observation peer must run as the current user"
        );
        Ok(())
    }

    async fn transact(&self, request: Value) -> Result<Value> {
        let payload = serde_json::to_vec(&request)?;
        anyhow::ensure!(
            !payload.is_empty() && payload.len() <= MAX_FLEET_OBSERVATION_BRIDGE_FRAME_BYTES,
            "Fleet bridge request is outside bounds"
        );
        timeout(IO_TIMEOUT, async {
            self.validate_socket_path()?;
            let mut stream = UnixStream::connect(&self.socket_path).await?;
            Self::validate_connected_peer(&stream)?;
            stream
                .write_all(&(payload.len() as u32).to_be_bytes())
                .await?;
            stream.write_all(&payload).await?;
            stream.shutdown().await?;
            let mut header = [0_u8; 4];
            stream.read_exact(&mut header).await?;
            let length = u32::from_be_bytes(header) as usize;
            anyhow::ensure!(
                length > 0 && length <= MAX_FLEET_OBSERVATION_BRIDGE_FRAME_BYTES,
                "Fleet bridge response is outside bounds"
            );
            let mut response = vec![0_u8; length];
            stream.read_exact(&mut response).await?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&response)?)
        })
        .await
        .context("Fleet bridge timed out")?
    }
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and only reads process credentials.
    unsafe { libc::geteuid() }
}

fn validate_socket_metadata(metadata: &std::fs::Metadata, expected_uid: u32) -> Result<()> {
    anyhow::ensure!(
        metadata.file_type().is_socket(),
        "Fleet observation path must be a Unix socket"
    );
    anyhow::ensure!(
        metadata.uid() == expected_uid,
        "Fleet observation socket must be owned by the current user"
    );
    anyhow::ensure!(
        metadata.mode() & 0o077 == 0,
        "Fleet observation socket must not grant group or other access"
    );
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BridgeResponse {
    schema: String,
    kind: String,
    ok: bool,
    #[serde(default)]
    result: Value,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthorityEpoch {
    binding_id: String,
    authenticated_peer_id: String,
    binding_generation: u64,
    projection_generation: u64,
}

impl From<AuthorityEpoch> for FleetObservationAuthorityEpochV1 {
    fn from(value: AuthorityEpoch) -> Self {
        Self {
            binding_id: value.binding_id,
            authenticated_peer_id: value.authenticated_peer_id,
            binding_generation: value.binding_generation,
            projection_generation: value.projection_generation,
        }
    }
}

#[tonic::async_trait]
impl FleetObservationPublishHandler for FleetObservationUdsBridge {
    async fn handle_fleet_observation_publish(
        &self,
        context: AuthenticatedDirectContext,
        operation: FleetObservationPublishV1,
    ) -> Result<FleetObservationPublishResultV1> {
        let request = match operation
            .request
            .context("Fleet observation request is required")?
        {
            fleet_observation_publish_v1::Request::Acquire(acquire) => {
                let selector = acquire.selector.context("selector is required")?;
                json!({"schema":SCHEMA,"kind":"acquire","selector":{
                    "source":selector.source,"network_id":selector.network_id,"device_id":selector.device_id
                }})
            }
            fleet_observation_publish_v1::Request::Publish(publish) => {
                let selector = publish.selector.context("selector is required")?;
                let epoch = publish
                    .authority_epoch
                    .context("authority epoch is required")?;
                anyhow::ensure!(
                    !publish.observation_json.is_empty()
                        && publish.observation_json.len() <= MAX_FLEET_OBSERVATION_JSON_BYTES,
                    "observation JSON is outside the semantic bound"
                );
                let observation = parse_unique_json(&publish.observation_json)?;
                json!({
                    "authenticated_context": {
                        "sender_peer_id": context.authenticated_source_node_id()
                    },
                    "request": {"schema":SCHEMA,"kind":"publish","selector":{
                        "source":selector.source,"network_id":selector.network_id,"device_id":selector.device_id
                    },"authority_epoch":{
                        "binding_id":epoch.binding_id,"authenticated_peer_id":epoch.authenticated_peer_id,
                        "binding_generation":epoch.binding_generation,"projection_generation":epoch.projection_generation
                    },"observation":observation}
                })
            }
        };
        let response: BridgeResponse = serde_json::from_value(self.transact(request).await?)?;
        anyhow::ensure!(response.schema == SCHEMA, "Fleet bridge schema mismatch");
        if !response.ok {
            return Ok(FleetObservationPublishResultV1 {
                disposition: FleetObservationPublishDisposition::Rejected as i32,
                accepted: false,
                authority_epoch: None,
                reason: "request rejected".into(),
                code: "authority_rejected".into(),
            });
        }
        match response.kind.as_str() {
            "acquire" => {
                let epoch: AuthorityEpoch = serde_json::from_value(response.result)?;
                Ok(FleetObservationPublishResultV1 {
                    disposition: FleetObservationPublishDisposition::Acquired as i32,
                    accepted: true,
                    authority_epoch: Some(epoch.into()),
                    reason: String::new(),
                    code: String::new(),
                })
            }
            "publish" => {
                let outcome = response.result.get("outcome").and_then(Value::as_str);
                let disposition = match outcome {
                    Some("published") => FleetObservationPublishDisposition::Published,
                    Some("already_recorded") => FleetObservationPublishDisposition::AlreadyRecorded,
                    _ => FleetObservationPublishDisposition::Rejected,
                };
                Ok(FleetObservationPublishResultV1 {
                    accepted: disposition != FleetObservationPublishDisposition::Rejected,
                    disposition: disposition as i32,
                    authority_epoch: None,
                    reason: if disposition == FleetObservationPublishDisposition::Rejected {
                        "request rejected".into()
                    } else {
                        String::new()
                    },
                    code: if disposition == FleetObservationPublishDisposition::Rejected {
                        "authority_rejected".into()
                    } else {
                        String::new()
                    },
                })
            }
            _ => anyhow::bail!("Fleet bridge response kind is invalid"),
        }
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use keryx_proto::v1::{
        fleet_observation_publish_v1, FleetObservationAuthorityEpochV1,
        FleetObservationPublishDisposition, FleetObservationSampleV1, FleetObservationSelectorV1,
    };
    use serde_json::Value;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    #[test]
    fn bridge_rejects_non_socket_and_group_accessible_socket_paths() {
        let temporary = tempdir().unwrap();
        let plain_file = temporary.path().join("plain");
        std::fs::write(&plain_file, b"not a socket").unwrap();
        let bridge = FleetObservationUdsBridge::new(plain_file).unwrap();
        assert!(bridge.validate_socket_path().is_err());

        let socket = temporary.path().join("fleet.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o660)).unwrap();
        let bridge = FleetObservationUdsBridge::new(socket).unwrap();
        assert!(bridge.validate_socket_path().is_err());
        let metadata = std::fs::symlink_metadata(&bridge.socket_path).unwrap();
        assert!(validate_socket_metadata(&metadata, effective_uid().wrapping_add(1)).is_err());
        drop(listener);
    }

    #[tokio::test]
    async fn connected_owner_peer_credentials_are_required() {
        let (client, _server) = UnixStream::pair().unwrap();
        FleetObservationUdsBridge::validate_connected_peer(&client).unwrap();
        assert_eq!(client.peer_cred().unwrap().uid(), effective_uid());
    }

    #[tokio::test]
    async fn oversized_complete_local_frame_rejects_before_connect() {
        let temporary = tempdir().unwrap();
        let bridge = FleetObservationUdsBridge::new(temporary.path().join("absent.sock")).unwrap();
        let oversized = json!({"payload":"x".repeat(MAX_FLEET_OBSERVATION_BRIDGE_FRAME_BYTES)});
        let error = bridge.transact(oversized).await.unwrap_err();
        assert!(error.to_string().contains("request is outside bounds"));
    }

    #[tokio::test]
    async fn duplicate_observation_members_reject_before_connect() {
        for observation_json in [
            br#"{"observed_at_ms":1,"observed_at_ms":2}"#.as_slice(),
            br#"{"resources":{"cpu":1,"cpu":2}}"#.as_slice(),
        ] {
            let temporary = tempdir().unwrap();
            let bridge =
                FleetObservationUdsBridge::new(temporary.path().join("absent.sock")).unwrap();
            let error = bridge
                .handle_fleet_observation_publish(
                    AuthenticatedDirectContext::new(
                        "peer-authenticated",
                        "peer-katana",
                        "frame-duplicate",
                    ),
                    FleetObservationPublishV1 {
                        request: Some(fleet_observation_publish_v1::Request::Publish(
                            FleetObservationSampleV1 {
                                selector: Some(FleetObservationSelectorV1 {
                                    source: "nodescale".into(),
                                    network_id: "network-1".into(),
                                    device_id: "device-1".into(),
                                }),
                                authority_epoch: Some(FleetObservationAuthorityEpochV1 {
                                    binding_id: "binding-1".into(),
                                    authenticated_peer_id: "peer-authenticated".into(),
                                    binding_generation: 7,
                                    projection_generation: 9,
                                }),
                                observation_json: observation_json.to_vec(),
                                sample_bytes: Vec::new(),
                            },
                        )),
                    },
                )
                .await
                .unwrap_err();
            assert!(error.to_string().contains("duplicate JSON object member"));
        }
    }

    #[tokio::test]
    async fn oversized_semantic_observation_rejects_before_connect() {
        let temporary = tempdir().unwrap();
        let bridge = FleetObservationUdsBridge::new(temporary.path().join("absent.sock")).unwrap();
        let error = bridge
            .handle_fleet_observation_publish(
                AuthenticatedDirectContext::new(
                    "peer-authenticated",
                    "peer-katana",
                    "frame-oversized",
                ),
                FleetObservationPublishV1 {
                    request: Some(fleet_observation_publish_v1::Request::Publish(
                        FleetObservationSampleV1 {
                            selector: Some(FleetObservationSelectorV1 {
                                source: "nodescale".into(),
                                network_id: "network-1".into(),
                                device_id: "device-1".into(),
                            }),
                            authority_epoch: Some(FleetObservationAuthorityEpochV1 {
                                binding_id: "binding-1".into(),
                                authenticated_peer_id: "peer-authenticated".into(),
                                binding_generation: 7,
                                projection_generation: 9,
                            }),
                            observation_json: vec![b'0'; MAX_FLEET_OBSERVATION_JSON_BYTES + 1],
                            sample_bytes: Vec::new(),
                        },
                    )),
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("semantic bound"));
    }

    #[tokio::test]
    async fn bridge_accepts_maximum_legal_observation_with_framing_overhead() {
        let temporary = tempdir().unwrap();
        let socket = temporary.path().join("fleet.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0_u8; 4];
            stream.read_exact(&mut header).await.unwrap();
            let length = u32::from_be_bytes(header) as usize;
            assert!(length > 32_768);
            let mut payload = vec![0_u8; length];
            stream.read_exact(&mut payload).await.unwrap();
            let response = serde_json::to_vec(&json!({
                "schema": SCHEMA, "kind": "publish", "ok": true,
                "result": {"outcome":"published"}
            }))
            .unwrap();
            stream
                .write_all(&(response.len() as u32).to_be_bytes())
                .await
                .unwrap();
            stream.write_all(&response).await.unwrap();
        });
        let bridge = FleetObservationUdsBridge::new(socket).unwrap();
        let mut observation = Vec::with_capacity(MAX_FLEET_OBSERVATION_JSON_BYTES);
        observation.push(b'"');
        observation.extend(std::iter::repeat_n(
            b'x',
            MAX_FLEET_OBSERVATION_JSON_BYTES - 2,
        ));
        observation.push(b'"');
        let result = bridge
            .handle_fleet_observation_publish(
                AuthenticatedDirectContext::new("peer-authenticated", "peer-katana", "frame-large"),
                FleetObservationPublishV1 {
                    request: Some(fleet_observation_publish_v1::Request::Publish(
                        FleetObservationSampleV1 {
                            selector: Some(FleetObservationSelectorV1 {
                                source: "nodescale".into(),
                                network_id: "network-1".into(),
                                device_id: "device-1".into(),
                            }),
                            authority_epoch: Some(FleetObservationAuthorityEpochV1 {
                                binding_id: "binding-1".into(),
                                authenticated_peer_id: "peer-authenticated".into(),
                                binding_generation: 7,
                                projection_generation: 9,
                            }),
                            observation_json: observation,
                            sample_bytes: Vec::new(),
                        },
                    )),
                },
            )
            .await
            .unwrap();
        assert!(result.accepted);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn publish_injects_authenticated_context_sender_into_owner_only_bridge() {
        let temporary = tempdir().unwrap();
        let socket = temporary.path().join("fleet.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0_u8; 4];
            stream.read_exact(&mut header).await.unwrap();
            let mut payload = vec![0_u8; u32::from_be_bytes(header) as usize];
            stream.read_exact(&mut payload).await.unwrap();
            let request: Value = serde_json::from_slice(&payload).unwrap();
            assert_eq!(
                request["authenticated_context"]["sender_peer_id"],
                "peer-authenticated"
            );
            assert!(request["request"].get("authenticated_sender").is_none());
            assert!(request["request"]["observation"]
                .get("authenticated_sender")
                .is_none());
            let response = serde_json::to_vec(&json!({
                "schema": SCHEMA,
                "kind": "publish",
                "ok": true,
                "result": {"outcome":"published"}
            }))
            .unwrap();
            stream
                .write_all(&(response.len() as u32).to_be_bytes())
                .await
                .unwrap();
            stream.write_all(&response).await.unwrap();
        });
        let bridge = FleetObservationUdsBridge::new(socket).unwrap();
        let result = bridge
            .handle_fleet_observation_publish(
                AuthenticatedDirectContext::new("peer-authenticated", "peer-katana", "frame-1"),
                FleetObservationPublishV1 {
                    request: Some(fleet_observation_publish_v1::Request::Publish(
                        FleetObservationSampleV1 {
                            selector: Some(FleetObservationSelectorV1 {
                                source: "nodescale".into(),
                                network_id: "network-1".into(),
                                device_id: "device-1".into(),
                            }),
                            authority_epoch: Some(FleetObservationAuthorityEpochV1 {
                                binding_id: "binding-1".into(),
                                authenticated_peer_id: "peer-authenticated".into(),
                                binding_generation: 7,
                                projection_generation: 9,
                            }),
                            observation_json: br#"{"observed_at_ms":1}"#.to_vec(),
                            sample_bytes: Vec::new(),
                        },
                    )),
                },
            )
            .await
            .unwrap();
        assert!(result.accepted);
        assert_eq!(
            result.disposition,
            FleetObservationPublishDisposition::Published as i32
        );
        server.await.unwrap();
    }
}
