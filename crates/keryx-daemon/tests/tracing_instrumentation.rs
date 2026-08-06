mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use common::RpcTestHarness;
use keryx_proto::v1::{
    AgentId, ClaimTaskRequest, CompleteTaskRequest, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use tracing::field::{Field, Visit};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::Registry;

#[derive(Clone, Default)]
struct CapturedSpan {
    name: String,
    fields: HashMap<String, String>,
}

#[derive(Clone)]
struct SpanCapture {
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

struct FieldVisitor<'a> {
    fields: &'a mut HashMap<String, String>,
}

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

impl<S> Layer<S> for SpanCapture
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: Context<'_, S>,
    ) {
        let mut fields = HashMap::new();
        attrs.record(&mut FieldVisitor {
            fields: &mut fields,
        });
        self.spans.lock().unwrap().push(CapturedSpan {
            name: attrs.metadata().name().to_string(),
            fields,
        });
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let name = span.metadata().name().to_string();
        let mut recorded = HashMap::new();
        values.record(&mut FieldVisitor {
            fields: &mut recorded,
        });
        let mut guard = self.spans.lock().unwrap();
        if let Some(captured) = guard.iter_mut().rev().find(|s| s.name == name) {
            for (key, value) in recorded {
                captured.fields.insert(key, value);
            }
        }
    }
}

fn install_span_capture() -> Arc<Mutex<Vec<CapturedSpan>>> {
    let spans: Arc<Mutex<Vec<CapturedSpan>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = SpanCapture {
        spans: Arc::clone(&spans),
    };
    let _ = Registry::default().with(layer).try_init();
    spans
}

fn has_span(spans: &[CapturedSpan], name: &str) -> bool {
    spans.iter().any(|s| s.name == name)
}

fn span_fields<'a>(spans: &'a [CapturedSpan], name: &str) -> Option<&'a HashMap<String, String>> {
    spans
        .iter()
        .rev()
        .find(|s| s.name == name)
        .map(|s| &s.fields)
}

#[tokio::test]
async fn rpc_handlers_emit_named_tracing_spans() {
    let captured = install_span_capture();
    let mut harness = RpcTestHarness::start().await;

    let task_id = TaskId {
        value: "task-trace-rpc".to_string(),
    };

    harness
        .client
        .submit_task(SubmitTaskRequest {
            envelope: Some(TaskEnvelope {
                task_id: Some(task_id.clone()),
                correlation_id: None,
                idempotency_key: None,
                status: 0,
                messages: vec![],
                metadata: Default::default(),
                deadline_ms: 0,
            }),
        })
        .await
        .unwrap();

    let claim = harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(task_id.clone()),
            worker_id: Some(AgentId {
                value: "worker-trace".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();

    harness
        .client
        .complete_task(CompleteTaskRequest {
            task_id: Some(task_id),
            lease_id: claim.lease_id,
            worker_id: Some(AgentId {
                value: "worker-trace".to_string(),
            }),
            duration_ms: 100,
            result_metadata: Default::default(),
            output_artifacts: vec![],
        })
        .await
        .unwrap();

    let spans = captured.lock().unwrap();
    assert!(
        has_span(&spans, "keryx::rpc::submit_task"),
        "expected submit_task span, got {:?}",
        spans.iter().map(|s| &s.name).collect::<Vec<_>>()
    );
    assert!(
        has_span(&spans, "keryx::rpc::claim_task"),
        "expected claim_task span"
    );
    assert!(
        has_span(&spans, "keryx::rpc::complete_task"),
        "expected complete_task span"
    );

    let claim_fields = span_fields(&spans, "keryx::rpc::claim_task").expect("claim span fields");
    assert_eq!(
        claim_fields.get("task_id").map(String::as_str),
        Some("task-trace-rpc")
    );
    assert_eq!(
        claim_fields.get("worker_id").map(String::as_str),
        Some("worker-trace")
    );
}
