#!/usr/bin/env python3
"""Apply the Phase 17 daemon, relay, edge, and result transport slice."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKER = "// PHASE17_RESULT_RUNTIME"


def read(path: str) -> str:
    return (ROOT / path).read_text()


def write(path: str, value: str) -> None:
    (ROOT / path).write_text(value)


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    if new in text:
        return
    if old not in text:
        raise RuntimeError(f"anchor missing in {path}: {old[:100]!r}")
    write(path, text.replace(old, new, 1))


RELAY_PROTO = '''syntax = "proto3";
package hermes.keryx.v1;

import "hermes/keryx/v1/common.proto";
import "hermes/keryx/v1/task.proto";
import "hermes/keryx/v1/result.proto";

service KeryxRelay {
  rpc ConnectNode(stream NodeFrame) returns (stream RelayFrame);
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  rpc PublishTask(PublishTaskRequest) returns (PublishTaskResponse);
  rpc PublishResult(PublishResultRequest) returns (PublishResultResponse);
  rpc AckTask(AckTaskRequest) returns (AckTaskResponse);
  rpc AckFrame(AckFrameRequest) returns (AckFrameResponse);
  rpc Health(HealthRequest) returns (HealthResponse);
}

message HealthRequest {}
message HealthResponse {
  bool healthy = 1;
  uint64 connected_peers = 2;
  uint64 registry_size = 3;
  uint64 uptime_seconds = 4;
  string transport_status = 5;
  uint64 tasks_routed = 6;
  string local_peer_id = 7;
}

message NodeFrame {
  string frame_id = 1;
  TaskEnvelope task = 2;
  TaskResultEnvelope result = 3;
  string target_node_id = 4;
}
message RelayFrame {
  string frame_id = 1;
  TaskEnvelope task = 2;
  TaskResultEnvelope result = 3;
  string authenticated_source_node_id = 4;
  string destination_node_id = 5;
}
message RegisterNodeRequest { NodeId node_id = 1; string token = 2; }
message RegisterNodeResponse { bool accepted = 1; }
message PublishTaskRequest {
  TaskEnvelope task = 1;
  string target_node_id = 2;
  string source_node_id = 3;
}
message PublishTaskResponse { TaskId task_id = 1; string frame_id = 2; }
message PublishResultRequest {
  TaskResultEnvelope result = 1;
  string target_node_id = 2;
  string source_node_id = 3;
  string frame_id = 4;
}
message PublishResultResponse { bool accepted = 1; string frame_id = 2; }
message AckTaskRequest { TaskId task_id = 1; }
message AckTaskResponse { bool accepted = 1; }
message AckFrameRequest { string frame_id = 1; }
message AckFrameResponse { bool accepted = 1; }
'''
write("proto/hermes/keryx/v1/relay.proto", RELAY_PROTO)

# Daemon protocol additions.
replace_once(
    "proto/hermes/keryx/v1/daemon.proto",
    'import "hermes/keryx/v1/registry.proto";\n',
    'import "hermes/keryx/v1/registry.proto";\nimport "hermes/keryx/v1/result.proto";\n',
)
replace_once(
    "proto/hermes/keryx/v1/daemon.proto",
    "  rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse);\n",
    "  rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse);\n"
    "  rpc SubmitRemoteTask(SubmitRemoteTaskRequest) returns (SubmitTaskResponse);\n"
    "  rpc GetTaskResult(GetTaskResultRequest) returns (GetTaskResultResponse);\n"
    "  rpc ClaimNextResultDelivery(ClaimNextResultDeliveryRequest) returns (ClaimNextResultDeliveryResponse);\n"
    "  rpc AckResultDelivery(AckResultDeliveryRequest) returns (AckResultDeliveryResponse);\n"
    "  rpc FailResultDelivery(FailResultDeliveryRequest) returns (FailResultDeliveryResponse);\n"
    "  rpc IngestRemoteResult(IngestRemoteResultRequest) returns (IngestRemoteResultResponse);\n",
)
replace_once(
    "proto/hermes/keryx/v1/daemon.proto",
    "message SubmitTaskRequest { TaskEnvelope envelope = 1; }\n",
    '''message SubmitTaskRequest { TaskEnvelope envelope = 1; }
message SubmitRemoteTaskRequest {
  TaskEnvelope envelope = 1;
  string authenticated_sender_peer_id = 2;
  string destination_peer_id = 3;
  string relay_frame_id = 4;
}
message GetTaskResultRequest { TaskId task_id = 1; }
message GetTaskResultResponse {
  bool found = 1;
  string status = 2;
  TaskResultEnvelope result = 3;
  uint64 update_sequence = 4;
}
message ClaimNextResultDeliveryRequest {
  string worker_id = 1;
  int64 lease_duration_ms = 2;
}
message ClaimNextResultDeliveryResponse {
  bool has_delivery = 1;
  string delivery_id = 2;
  string target_peer_id = 3;
  TaskResultEnvelope result = 4;
  uint32 attempt_count = 5;
  int64 lease_expires_at_ms = 6;
}
message AckResultDeliveryRequest {
  string delivery_id = 1;
  string worker_id = 2;
}
message AckResultDeliveryResponse { bool accepted = 1; }
message FailResultDeliveryRequest {
  string delivery_id = 1;
  string worker_id = 2;
  string error_reason = 3;
  int64 retry_delay_ms = 4;
  bool dead_letter = 5;
}
message FailResultDeliveryResponse { bool accepted = 1; }
message IngestRemoteResultRequest {
  TaskResultEnvelope result = 1;
  string authenticated_executor_peer_id = 2;
  string destination_peer_id = 3;
  string relay_frame_id = 4;
}
message IngestRemoteResultResponse {
  TaskId task_id = 1;
  string status = 2;
}
''',
)

# Generalize relay mailbox acknowledgements by frame id.
runtime_path = "crates/keryx-relay/src/runtime.rs"
text = read(runtime_path)
text = text.replace("acked_task_ids", "acked_frame_ids")
text = text.replace("is_acked(frame, &guard.acked_frame_ids)", "is_acked(frame, &guard.acked_frame_ids)")
text = text.replace(
    '''    /// Acknowledge a task and remove any undelivered mailbox copies.
    pub fn ack_task(&self, task_id: &str) -> bool {
        if task_id.trim().is_empty() {
            return false;
        }
        let mut guard = self.lock_peers();
        guard.acked_frame_ids.insert(task_id.to_string());
        for mailbox in guard.mailboxes.values_mut() {
            mailbox.retain(|frame| frame_task_id(frame).as_deref() != Some(task_id));
        }
        true
    }
''',
    '''    /// Acknowledge a frame and remove any undelivered mailbox copies.
    pub fn ack_frame(&self, frame_id: &str) -> bool {
        let frame_id = frame_id.trim();
        if frame_id.is_empty() {
            return false;
        }
        let mut guard = self.lock_peers();
        guard.acked_frame_ids.insert(frame_id.to_string());
        for mailbox in guard.mailboxes.values_mut() {
            mailbox.retain(|frame| frame.frame_id.trim() != frame_id);
        }
        true
    }

    /// Compatibility acknowledgement for older task-id callers.
    pub fn ack_task(&self, task_id: &str) -> bool {
        let task_id = task_id.trim();
        if task_id.is_empty() {
            return false;
        }
        let mut guard = self.lock_peers();
        let mut matched = false;
        for mailbox in guard.mailboxes.values_mut() {
            mailbox.retain(|frame| {
                let remove = frame_task_id(frame).as_deref() == Some(task_id);
                matched |= remove;
                !remove
            });
        }
        matched || true
    }
''',
)
text = text.replace(
    '''fn is_acked(frame: &RelayFrame, acked: &HashSet<String>) -> bool {
    frame_task_id(frame).is_some_and(|task_id| acked.contains(&task_id))
}
''',
    '''fn is_acked(frame: &RelayFrame, acked: &HashSet<String>) -> bool {
    !frame.frame_id.trim().is_empty() && acked.contains(frame.frame_id.trim())
}
''',
)
write(runtime_path, text)

# Relay service result routing and source stamping.
health_path = "crates/keryx-relay/src/health_server.rs"
text = read(health_path)
text = text.replace(
    '''    AckTaskRequest, AckTaskResponse, HealthRequest, HealthResponse, NodeFrame, PublishTaskRequest,
    PublishTaskResponse, RegisterNodeRequest, RegisterNodeResponse, RelayFrame, TaskEnvelope,
''',
    '''    AckFrameRequest, AckFrameResponse, AckTaskRequest, AckTaskResponse, HealthRequest,
    HealthResponse, NodeFrame, PublishResultRequest, PublishResultResponse, PublishTaskRequest,
    PublishTaskResponse, RegisterNodeRequest, RegisterNodeResponse, RelayFrame, TaskEnvelope,
''',
)
text = text.replace("if let Err(err) = route_node_frame(&runtime, frame)", "if let Err(err) = route_node_frame(&runtime, &source_node_id, frame)")
old_publish = '''        let task = request
            .into_inner()
            .task
            .ok_or_else(|| Status::invalid_argument("PublishTask requires task"))?;
        let target_node_id = target_node_id_from_task(&task)?;
'''
new_publish = '''        let inner = request.into_inner();
        let task = inner
            .task
            .ok_or_else(|| Status::invalid_argument("PublishTask requires task"))?;
        let target_node_id = if inner.target_node_id.trim().is_empty() {
            target_node_id_from_task(&task)?
        } else {
            inner.target_node_id.trim().to_string()
        };
        let source_node_id = required_node_value(&inner.source_node_id, "source_node_id")?;
'''
text = text.replace(old_publish, new_publish)
text = text.replace(
    '''        let frame = RelayFrame {
            frame_id: frame_id_for_task(&task),
            task: Some(task),
        };
        self.runtime.route_frame(target_node_id, frame);
        Ok(Response::new(PublishTaskResponse {
            task_id: Some(task_id),
        }))
''',
    '''        let frame_id = frame_id_for_task(&task);
        let frame = RelayFrame {
            frame_id: frame_id.clone(),
            task: Some(task),
            result: None,
            authenticated_source_node_id: source_node_id,
            destination_node_id: target_node_id.clone(),
        };
        self.runtime.route_frame(target_node_id, frame);
        Ok(Response::new(PublishTaskResponse {
            task_id: Some(task_id),
            frame_id,
        }))
''',
)
ack_anchor = '''    async fn ack_task(
        &self,
        request: Request<AckTaskRequest>,
    ) -> Result<Response<AckTaskResponse>, Status> {
'''
result_methods = '''    async fn publish_result(
        &self,
        request: Request<PublishResultRequest>,
    ) -> Result<Response<PublishResultResponse>, Status> {
        let inner = request.into_inner();
        let result = inner
            .result
            .ok_or_else(|| Status::invalid_argument("PublishResult requires result"))?;
        let target_node_id = required_node_value(&inner.target_node_id, "target_node_id")?;
        let source_node_id = required_node_value(&inner.source_node_id, "source_node_id")?;
        let task_id = result
            .task_id
            .as_ref()
            .map(|value| value.value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::invalid_argument("PublishResult requires result.task_id"))?;
        let frame_id = if inner.frame_id.trim().is_empty() {
            format!("result-{task_id}")
        } else {
            inner.frame_id.trim().to_string()
        };
        self.runtime.route_frame(
            target_node_id.clone(),
            RelayFrame {
                frame_id: frame_id.clone(),
                task: None,
                result: Some(result),
                authenticated_source_node_id: source_node_id,
                destination_node_id: target_node_id,
            },
        );
        Ok(Response::new(PublishResultResponse {
            accepted: true,
            frame_id,
        }))
    }

    async fn ack_frame(
        &self,
        request: Request<AckFrameRequest>,
    ) -> Result<Response<AckFrameResponse>, Status> {
        let accepted = self.runtime.ack_frame(&request.into_inner().frame_id);
        Ok(Response::new(AckFrameResponse { accepted }))
    }

'''
if result_methods not in text:
    text = text.replace(ack_anchor, result_methods + ack_anchor)
text = text.replace(
    "fn route_node_frame(runtime: &RelayRuntime, frame: NodeFrame) -> Result<(), Status> {",
    "fn route_node_frame(runtime: &RelayRuntime, source_node_id: &str, frame: NodeFrame) -> Result<(), Status> {",
)
old_route = '''    let task = frame
        .task
        .ok_or_else(|| Status::invalid_argument("NodeFrame requires task"))?;
    let target_node_id = target_node_id_from_task(&task)?;
    let relay_frame = RelayFrame {
        frame_id: if frame.frame_id.trim().is_empty() {
            frame_id_for_task(&task)
        } else {
            frame.frame_id
        },
        task: Some(task),
    };
    runtime.route_frame(target_node_id, relay_frame);
    Ok(())
'''
new_route = '''    let target_node_id = required_node_value(&frame.target_node_id, "target_node_id")?;
    let has_task = frame.task.is_some();
    let has_result = frame.result.is_some();
    if has_task == has_result {
        return Err(Status::invalid_argument(
            "NodeFrame must contain exactly one of task or result",
        ));
    }
    let frame_id = if frame.frame_id.trim().is_empty() {
        if let Some(task) = frame.task.as_ref() {
            frame_id_for_task(task)
        } else {
            let task_id = frame
                .result
                .as_ref()
                .and_then(|result| result.task_id.as_ref())
                .map(|task_id| task_id.value.trim())
                .unwrap_or("unknown");
            format!("result-{task_id}")
        }
    } else {
        frame.frame_id
    };
    runtime.route_frame(
        target_node_id.clone(),
        RelayFrame {
            frame_id,
            task: frame.task,
            result: frame.result,
            authenticated_source_node_id: source_node_id.to_string(),
            destination_node_id: target_node_id,
        },
    );
    Ok(())
'''
text = text.replace(old_route, new_route)
helper_anchor = "fn parse_registry_peer_id(value: &str) -> Result<PeerId, Status> {"
helper = '''fn required_node_value(value: &str, field: &str) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        Err(Status::invalid_argument(format!("{field} is required")))
    } else {
        Ok(value.to_string())
    }
}

'''
if helper not in text:
    text = text.replace(helper_anchor, helper + helper_anchor)
write(health_path, text)

# Daemon routing retains outbound tasks and identifies the source peer explicitly.
routing_path = "crates/keryx-daemon/src/routing.rs"
text = read(routing_path)
text = text.replace(
    "use keryx_proto::v1::{keryx_relay_client::KeryxRelayClient, PublishTaskRequest, TaskEnvelope};",
    "use keryx_proto::v1::{keryx_relay_client::KeryxRelayClient, PublishTaskRequest, TaskEnvelope};\nuse prost::Message;\nuse tonic::Request;",
)
text = text.replace(
    "use keryx_store::{SqliteStore, StoreError, StoreResult, TaskRecord};",
    "use keryx_store::{SqliteStore, StoreError, StoreResult, TaskEnvelopeRecord, TaskRecord, TaskTransportContextRecord};",
)
text = text.replace(
    '''pub struct GrpcRelayTaskPublisher {
    endpoint: String,
}
''',
    '''pub struct GrpcRelayTaskPublisher {
    endpoint: String,
    source_peer_id: PeerId,
}
''',
)
text = text.replace(
    '''    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
''',
    '''    pub fn new(endpoint: impl Into<String>, source_peer_id: PeerId) -> Self {
        Self {
            endpoint: endpoint.into(),
            source_peer_id,
        }
    }
''',
)
text = text.replace(
    '''        client
            .publish_task(PublishTaskRequest {
                task: Some(envelope),
            })
''',
    '''        client
            .publish_task(Request::new(PublishTaskRequest {
                task: Some(envelope),
                target_node_id: target_peer_id.as_str().to_string(),
                source_node_id: self.source_peer_id.as_str().to_string(),
            }))
''',
)
retain_anchor = '''        let timeout = normalize_timeout(timeout_ms, self.default_timeout_ms);
        let delivery = tokio::time::timeout(
'''
retain_code = '''        let encoded_envelope = envelope.encode_to_vec();
        let idempotency_key = parse_envelope_idempotency_key(&envelope)?;
        let record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);
        let now_ms = unix_ms_now();
        let envelope_record = TaskEnvelopeRecord::new(task_id.clone(), encoded_envelope, now_ms);
        let context = TaskTransportContextRecord {
            task_id: task_id.clone(),
            authenticated_sender_peer_id: None,
            expected_executor_peer_id: Some(target_peer_id.clone()),
            destination_peer_id: self.peers.local_peer_id().clone(),
            relay_frame_id: Some(format!("relay-{}", task_id.as_str())),
            received_at_ms: now_ms,
        };
        store
            .accept_task_with_envelope_and_context(record, envelope_record, context)
            .await?;

        let timeout = normalize_timeout(timeout_ms, self.default_timeout_ms);
        let delivery = tokio::time::timeout(
'''
if retain_code not in text:
    text = text.replace(retain_anchor, retain_code)
# Add local clock helper.
if "fn unix_ms_now() -> i64" not in text:
    text += '''
fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
'''
write(routing_path, text)

# Daemon implementation and result RPCs.
daemon_path = "crates/keryx-daemon/src/lib.rs"
text = read(daemon_path)
text = text.replace(
    "Arc::new(GrpcRelayTaskPublisher::new(endpoint)) as Arc<dyn RelayTaskPublisher>",
    "Arc::new(GrpcRelayTaskPublisher::new(endpoint, config.local_peer_id().clone())) as Arc<dyn RelayTaskPublisher>",
)
text = text.replace(
    "    TaskRecord, CURRENT_SCHEMA_VERSION,\n",
    "    TaskRecord, TaskTransportContextRecord, TerminalResultRecord, CURRENT_SCHEMA_VERSION,\n",
)
extra_import = '''use keryx_proto::v1::{
    AckResultDeliveryRequest, AckResultDeliveryResponse, ClaimNextResultDeliveryRequest,
    ClaimNextResultDeliveryResponse, FailResultDeliveryRequest, FailResultDeliveryResponse,
    GetTaskResultRequest, GetTaskResultResponse, IngestRemoteResultRequest,
    IngestRemoteResultResponse, ResultArtifact, SubmitRemoteTaskRequest, TaskResultEnvelope,
    TerminalOutcome,
};
'''
if extra_import not in text:
    text = text.replace("use prost::Message;\n", "use prost::Message;\n" + extra_import)
# Runtime remote accept method.
runtime_anchor = '''    #[must_use]
    pub const fn report(&self) -> &StartupReport {
'''
runtime_method = '''    pub async fn accept_pending_remote_task_with_backpressure(
        &self,
        record: TaskRecord,
        envelope: TaskEnvelopeRecord,
        context: TaskTransportContextRecord,
    ) -> StoreResult<TaskRecord> {
        self.config
            .limits()
            .check_envelope_bytes(envelope.encoded_envelope.len() as u64)
            .map_err(|error| StoreError::Validation(error.into()))?;
        let _guard = self.submit_backpressure_lock.lock().await;
        let pending_count = self.store.count_tasks_by_status(TaskStatus::Pending).await?;
        self.config
            .limits()
            .check_pending_tasks(pending_count)
            .map_err(|error| StoreError::Validation(error.into()))?;
        let accepted = self
            .store
            .accept_task_with_envelope_and_context(record, envelope, context)
            .await?;
        self.task_available.notify_waiters();
        Ok(accepted)
    }

'''
if runtime_method not in text:
    text = text.replace(runtime_anchor, runtime_method + runtime_anchor)
# Sender identity on ClaimNextTask.
text = text.replace(
    '''                    return Ok(Some(ClaimNextTaskResponse {
                        has_task: true,
''',
    '''                    let sender_peer_id = self
                        .runtime
                        .store()
                        .get_transport_context(task.task_id())
                        .await
                        .ok()
                        .and_then(|context| context.authenticated_sender_peer_id)
                        .map(|peer| peer.as_str().to_string())
                        .unwrap_or_default();
                    return Ok(Some(ClaimNextTaskResponse {
                        has_task: true,
''',
)
text = text.replace("                        sender_peer_id: String::new(),\n", "                        sender_peer_id,\n", 1)
# SubmitRemoteTask method.
submit_anchor = '''    #[instrument(
        name = "keryx::rpc::claim_task",
'''
submit_remote = '''    #[instrument(name = "keryx::rpc::submit_remote_task", skip(self, request))]
    async fn submit_remote_task(
        &self,
        request: Request<SubmitRemoteTaskRequest>,
    ) -> Result<Response<SubmitTaskResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let envelope = inner
            .envelope
            .ok_or_else(|| Status::invalid_argument("envelope is required"))?;
        let task_id = parse_required_task_id(envelope.task_id.as_ref())?;
        let sender = PeerId::new(inner.authenticated_sender_peer_id.trim())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let destination = PeerId::new(inner.destination_peer_id.trim())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if destination != *self.runtime.config().local_peer_id() {
            return Err(Status::permission_denied("remote task destination does not match local peer"));
        }
        let idempotency_key = parse_optional_idempotency_key(envelope.idempotency_key.as_ref())?;
        let encoded = envelope.encode_to_vec();
        let now_ms = unix_ms_now();
        let record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);
        let accepted = self
            .runtime
            .accept_pending_remote_task_with_backpressure(
                record,
                TaskEnvelopeRecord::new(task_id.clone(), encoded, now_ms),
                TaskTransportContextRecord {
                    task_id: task_id.clone(),
                    authenticated_sender_peer_id: Some(sender),
                    expected_executor_peer_id: None,
                    destination_peer_id: destination,
                    relay_frame_id: Some(inner.relay_frame_id),
                    received_at_ms: now_ms,
                },
            )
            .await
            .map_err(store_error_to_status)?;
        self.runtime.metrics().increment_tasks_submitted();
        Ok(Response::new(SubmitTaskResponse {
            task_id: Some(proto_task_id(accepted.task_id())),
            status: task_status_label(accepted.status).to_string(),
        }))
    }

'''
if submit_remote not in text:
    text = text.replace(submit_anchor, submit_remote + submit_anchor)
# Complete task durable result.
old_complete = '''        let task = self
            .runtime
            .store()
            .complete_task(&task_id, &lease_id, &worker_id)
            .await
            .map_err(store_error_to_status)?;
'''
new_complete = '''        let result = build_terminal_result(
            self.runtime.store(),
            self.runtime.config().local_peer_id(),
            &task_id,
            TerminalOutcome::Completed,
            inner.duration_ms,
            String::new(),
            inner.result_metadata.clone(),
            inner.output_artifacts.clone(),
        )
        .await?;
        let stored = TerminalResultRecord {
            task_id: task_id.clone(),
            encoded_result: result.encode_to_vec(),
            terminal_status: TaskStatus::Completed,
            return_peer_id: return_peer_for_task(self.runtime.store(), &task_id).await,
            executor_peer_id: self.runtime.config().local_peer_id().clone(),
            created_at_ms: result.completed_at_ms,
        };
        let task = self
            .runtime
            .store()
            .complete_task_with_result(&task_id, &lease_id, &worker_id, stored)
            .await
            .map_err(store_error_to_status)?;
'''
text = text.replace(old_complete, new_complete)
old_fail = '''        let task = self
            .runtime
            .store()
            .fail_task(&task_id, &lease_id, &worker_id, &error_reason, &policy)
            .await
            .map_err(store_error_to_status)?;
'''
new_fail = '''        let result = build_terminal_result(
            self.runtime.store(),
            self.runtime.config().local_peer_id(),
            &task_id,
            TerminalOutcome::Failed,
            inner.duration_ms,
            error_reason.clone(),
            inner.failure_metadata.clone(),
            Vec::new(),
        )
        .await?;
        let stored = TerminalResultRecord {
            task_id: task_id.clone(),
            encoded_result: result.encode_to_vec(),
            terminal_status: TaskStatus::Failed,
            return_peer_id: return_peer_for_task(self.runtime.store(), &task_id).await,
            executor_peer_id: self.runtime.config().local_peer_id().clone(),
            created_at_ms: result.completed_at_ms,
        };
        let task = self
            .runtime
            .store()
            .fail_task_with_result(
                &task_id,
                &lease_id,
                &worker_id,
                &error_reason,
                &policy,
                stored,
            )
            .await
            .map_err(store_error_to_status)?;
'''
text = text.replace(old_fail, new_fail)
# Result RPC methods before SendTask.
send_anchor = '''    #[instrument(name = "keryx::rpc::send_task", skip(self, request))]
'''
result_rpcs = '''    async fn get_task_result(
        &self,
        request: Request<GetTaskResultRequest>,
    ) -> Result<Response<GetTaskResultResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let task_id = parse_required_task_id(request.into_inner().task_id.as_ref())?;
        let task = self.runtime.store().get_task(&task_id).await.map_err(store_error_to_status)?;
        let events = self
            .runtime
            .store()
            .events_for_task(&task_id)
            .await
            .map_err(store_error_to_status)?;
        match self.runtime.store().get_terminal_result(&task_id).await {
            Ok(record) => {
                let result = TaskResultEnvelope::decode(record.encoded_result.as_slice())
                    .map_err(|error| Status::data_loss(format!("invalid stored terminal result: {error}")))?;
                Ok(Response::new(GetTaskResultResponse {
                    found: true,
                    status: task_status_label(task.status).to_string(),
                    result: Some(result),
                    update_sequence: events.len() as u64,
                }))
            }
            Err(StoreError::TerminalResultNotFound(_)) => Ok(Response::new(GetTaskResultResponse {
                found: false,
                status: task_status_label(task.status).to_string(),
                result: None,
                update_sequence: events.len() as u64,
            })),
            Err(error) => Err(store_error_to_status(error)),
        }
    }

    async fn claim_next_result_delivery(
        &self,
        request: Request<ClaimNextResultDeliveryRequest>,
    ) -> Result<Response<ClaimNextResultDeliveryResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let worker_id = inner.worker_id.trim();
        if worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        let lease_ms = inner.lease_duration_ms.max(1_000);
        let now_ms = unix_ms_now();
        match self
            .runtime
            .store()
            .claim_next_result_delivery(worker_id, now_ms, lease_ms)
            .await
            .map_err(store_error_to_status)?
        {
            Some((outbox, record)) => {
                let result = TaskResultEnvelope::decode(record.encoded_result.as_slice())
                    .map_err(|error| Status::data_loss(format!("invalid stored result: {error}")))?;
                Ok(Response::new(ClaimNextResultDeliveryResponse {
                    has_delivery: true,
                    delivery_id: outbox.delivery_id,
                    target_peer_id: outbox.target_peer_id.as_str().to_string(),
                    result: Some(result),
                    attempt_count: outbox.attempt_count,
                    lease_expires_at_ms: outbox.lease_expires_at_ms.unwrap_or_default(),
                }))
            }
            None => Ok(Response::new(ClaimNextResultDeliveryResponse {
                has_delivery: false,
                delivery_id: String::new(),
                target_peer_id: String::new(),
                result: None,
                attempt_count: 0,
                lease_expires_at_ms: 0,
            })),
        }
    }

    async fn ack_result_delivery(
        &self,
        request: Request<AckResultDeliveryRequest>,
    ) -> Result<Response<AckResultDeliveryResponse>, Status> {
        let inner = request.into_inner();
        self.runtime
            .store()
            .ack_result_delivery(&inner.delivery_id, &inner.worker_id, unix_ms_now())
            .await
            .map_err(store_error_to_status)?;
        Ok(Response::new(AckResultDeliveryResponse { accepted: true }))
    }

    async fn fail_result_delivery(
        &self,
        request: Request<FailResultDeliveryRequest>,
    ) -> Result<Response<FailResultDeliveryResponse>, Status> {
        let inner = request.into_inner();
        let now_ms = unix_ms_now();
        self.runtime
            .store()
            .fail_result_delivery(
                &inner.delivery_id,
                &inner.worker_id,
                now_ms,
                now_ms.saturating_add(inner.retry_delay_ms.max(1_000)),
                &inner.error_reason,
                inner.dead_letter,
            )
            .await
            .map_err(store_error_to_status)?;
        Ok(Response::new(FailResultDeliveryResponse { accepted: true }))
    }

    async fn ingest_remote_result(
        &self,
        request: Request<IngestRemoteResultRequest>,
    ) -> Result<Response<IngestRemoteResultResponse>, Status> {
        let inner = request.into_inner();
        let result = inner
            .result
            .ok_or_else(|| Status::invalid_argument("result is required"))?;
        let task_id = parse_required_task_id(result.task_id.as_ref())?;
        let executor = PeerId::new(inner.authenticated_executor_peer_id.trim())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if inner.destination_peer_id.trim() != self.runtime.config().local_peer_id().as_str() {
            return Err(Status::permission_denied("result destination does not match local peer"));
        }
        let terminal_status = terminal_outcome_status(result.outcome)?;
        let record = TerminalResultRecord {
            task_id: task_id.clone(),
            encoded_result: result.encode_to_vec(),
            terminal_status,
            return_peer_id: None,
            executor_peer_id: executor.clone(),
            created_at_ms: result.completed_at_ms,
        };
        let task = self
            .runtime
            .store()
            .apply_remote_result(record, &executor)
            .await
            .map_err(store_error_to_status)?;
        Ok(Response::new(IngestRemoteResultResponse {
            task_id: Some(proto_task_id(&task_id)),
            status: task_status_label(task.status).to_string(),
        }))
    }

'''
if result_rpcs not in text:
    text = text.replace(send_anchor, result_rpcs + send_anchor)
# Helpers before normalized_filter_set.
helper_anchor = "fn normalized_filter_set(values: Vec<String>) -> HashSet<String> {"
helpers = '''async fn return_peer_for_task(store: &SqliteStore, task_id: &TaskId) -> Option<PeerId> {
    store
        .get_transport_context(task_id)
        .await
        .ok()
        .and_then(|context| context.authenticated_sender_peer_id)
}

async fn build_terminal_result(
    store: &SqliteStore,
    executor: &PeerId,
    task_id: &TaskId,
    outcome: TerminalOutcome,
    duration_ms: i64,
    error_reason: String,
    result_metadata: std::collections::HashMap<String, String>,
    artifacts: Vec<keryx_proto::v1::TaskArtifact>,
) -> Result<TaskResultEnvelope, Status> {
    let correlation_id = store
        .get_task_envelope(task_id)
        .await
        .ok()
        .and_then(|record| TaskEnvelope::decode(record.encoded_envelope.as_slice()).ok())
        .and_then(|envelope| envelope.correlation_id);
    Ok(TaskResultEnvelope {
        protocol_version: 1,
        task_id: Some(proto_task_id(task_id)),
        correlation_id,
        outcome: outcome as i32,
        executor_peer_id: executor.as_str().to_string(),
        duration_ms,
        completed_at_ms: unix_ms_now(),
        error_reason,
        result_metadata,
        output_artifacts: artifacts
            .into_iter()
            .map(|artifact| ResultArtifact {
                path: artifact.path,
                media_type: artifact.media_type,
                metadata: artifact.metadata,
            })
            .collect(),
    })
}

fn terminal_outcome_status(outcome: i32) -> Result<TaskStatus, Status> {
    match TerminalOutcome::try_from(outcome).unwrap_or(TerminalOutcome::Unspecified) {
        TerminalOutcome::Completed => Ok(TaskStatus::Completed),
        TerminalOutcome::Failed
        | TerminalOutcome::Canceled
        | TerminalOutcome::TimedOut
        | TerminalOutcome::Rejected => Ok(TaskStatus::Failed),
        TerminalOutcome::Unspecified => Err(Status::invalid_argument("terminal outcome is required")),
    }
}

'''
if helpers not in text:
    text = text.replace(helper_anchor, helpers + helper_anchor)
write(daemon_path, text)

# Edge receives task/result frames and drains the result outbox.
node_path = "crates/keryx-relay/src/node.rs"
text = read(node_path)
text = text.replace(
    "use keryx_proto::v1::{AckTaskRequest, NodeFrame, SubmitTaskRequest};",
    "use keryx_proto::v1::{AckFrameRequest, AckResultDeliveryRequest, ClaimNextResultDeliveryRequest, FailResultDeliveryRequest, IngestRemoteResultRequest, NodeFrame, PublishResultRequest, SubmitRemoteTaskRequest};",
)
old_loop = '''    let (_tx, rx) = mpsc::channel::<NodeFrame>(8);
    let mut request = Request::new(ReceiverStream::new(rx));
'''
new_loop = '''    let (_tx, rx) = mpsc::channel::<NodeFrame>(8);
    let mut request = Request::new(ReceiverStream::new(rx));
'''
text = text.replace(old_loop, new_loop)
start = text.index("    while let Some(frame) = stream.next().await {")
end = text.index("    Ok(())\n}", start)
replacement = '''    let delivery_worker = format!("edge-{registry_peer_id}");
    let mut delivery_tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            next = stream.next() => {
                let Some(frame) = next else { break; };
                let frame = frame.context("keryx node stream: relay frame failed")?;
                let mut daemon = KeryxDaemonClient::connect(daemon_endpoint.clone())
                    .await
                    .with_context(|| format!("keryx node stream: daemon unavailable at {daemon_endpoint}"))?;
                if let Some(task) = frame.task {
                    daemon
                        .submit_remote_task(SubmitRemoteTaskRequest {
                            envelope: Some(task),
                            authenticated_sender_peer_id: frame.authenticated_source_node_id.clone(),
                            destination_peer_id: frame.destination_node_id.clone(),
                            relay_frame_id: frame.frame_id.clone(),
                        })
                        .await
                        .context("keryx node stream: daemon SubmitRemoteTask failed")?;
                } else if let Some(result) = frame.result {
                    daemon
                        .ingest_remote_result(IngestRemoteResultRequest {
                            result: Some(result),
                            authenticated_executor_peer_id: frame.authenticated_source_node_id.clone(),
                            destination_peer_id: frame.destination_node_id.clone(),
                            relay_frame_id: frame.frame_id.clone(),
                        })
                        .await
                        .context("keryx node stream: daemon IngestRemoteResult failed")?;
                } else {
                    tracing::warn!(frame_id = %frame.frame_id, "dropping empty relay frame");
                    continue;
                }
                let mut ack_client = KeryxRelayClient::connect(relay_endpoint.clone()).await?;
                ack_client
                    .ack_frame(AckFrameRequest { frame_id: frame.frame_id })
                    .await
                    .context("keryx node stream: relay AckFrame failed")?;
            }
            _ = delivery_tick.tick() => {
                let mut daemon = KeryxDaemonClient::connect(daemon_endpoint.clone()).await?;
                let delivery = daemon
                    .claim_next_result_delivery(ClaimNextResultDeliveryRequest {
                        worker_id: delivery_worker.clone(),
                        lease_duration_ms: 30_000,
                    })
                    .await?
                    .into_inner();
                if !delivery.has_delivery {
                    continue;
                }
                let publish = relay
                    .publish_result(PublishResultRequest {
                        result: delivery.result,
                        target_node_id: delivery.target_peer_id,
                        source_node_id: registry_peer_id.clone(),
                        frame_id: delivery.delivery_id.clone(),
                    })
                    .await;
                match publish {
                    Ok(_) => {
                        daemon
                            .ack_result_delivery(AckResultDeliveryRequest {
                                delivery_id: delivery.delivery_id,
                                worker_id: delivery_worker.clone(),
                            })
                            .await?;
                    }
                    Err(error) => {
                        daemon
                            .fail_result_delivery(FailResultDeliveryRequest {
                                delivery_id: delivery.delivery_id,
                                worker_id: delivery_worker.clone(),
                                error_reason: error.message().to_string(),
                                retry_delay_ms: 1_000,
                                dead_letter: false,
                            })
                            .await?;
                    }
                }
            }
        }
    }
'''
text = text[:start] + replacement + text[end:]
write(node_path, text)

print("Phase 17 daemon/relay result runtime applied")
