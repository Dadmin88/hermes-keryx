mod common;

use common::RpcTestHarness;
use keryx_core::MAX_BLOB_BYTES;
use keryx_proto::v1::{
    ArtifactId, GetArtifactRequest, ListArtifactsRequest, PutArtifactRequest, SubmitTaskRequest,
    TaskEnvelope, TaskId,
};
use tonic::Code;

async fn submit_pending_task(harness: &mut RpcTestHarness, task_id: &TaskId) {
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
            }),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn artifact_rpc_round_trips_inline_content_and_lists_then_deletes() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "artifact-rpc-inline".to_string(),
    };
    submit_pending_task(&mut harness, &task_id).await;

    let content = b"artifact bytes from rpc".to_vec();
    let put = harness
        .client
        .put_artifact(PutArtifactRequest {
            task_id: Some(task_id.clone()),
            artifact_id: Some(ArtifactId {
                value: "artifact-rpc-inline-1".to_string(),
            }),
            media_type: "text/plain".to_string(),
            content: content.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(put.task_id.as_ref().unwrap().value, task_id.value);
    assert_eq!(
        put.artifact_id.as_ref().unwrap().value,
        "artifact-rpc-inline-1"
    );
    assert_eq!(put.byte_len, content.len() as u64);
    assert!(put.inline);
    assert_eq!(put.media_type, "text/plain");
    assert!(!put.digest.is_empty());

    let get = harness
        .client
        .get_artifact(GetArtifactRequest {
            artifact_id: put.artifact_id.clone(),
            metadata_only: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get.task_id.as_ref().unwrap().value, task_id.value);
    assert_eq!(get.content, content);
    assert_eq!(get.digest, put.digest);

    let metadata_only = harness
        .client
        .get_artifact(GetArtifactRequest {
            artifact_id: put.artifact_id.clone(),
            metadata_only: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(metadata_only.content.is_empty());
    assert_eq!(metadata_only.digest, put.digest);

    let listed = harness
        .client
        .list_artifacts(ListArtifactsRequest {
            task_id: Some(task_id.clone()),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.artifacts.len(), 1);
    assert_eq!(
        listed.artifacts[0].artifact_id.as_ref().unwrap().value,
        "artifact-rpc-inline-1"
    );
    assert_eq!(listed.artifacts[0].digest, put.digest);

    let deleted = harness
        .client
        .delete_artifact(keryx_proto::v1::DeleteArtifactRequest {
            artifact_id: put.artifact_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.deleted);

    let missing_after_delete = harness
        .client
        .get_artifact(GetArtifactRequest {
            artifact_id: put.artifact_id,
            metadata_only: false,
        })
        .await
        .unwrap_err();
    assert_eq!(missing_after_delete.code(), Code::NotFound);
}

#[tokio::test]
async fn artifact_rpc_put_rejects_oversize_uploads_with_resource_exhausted() {
    let mut harness = RpcTestHarness::start().await;
    let task_id = TaskId {
        value: "artifact-rpc-oversize".to_string(),
    };
    submit_pending_task(&mut harness, &task_id).await;

    let oversize = harness
        .client
        .put_artifact(PutArtifactRequest {
            task_id: Some(task_id),
            artifact_id: Some(ArtifactId {
                value: "artifact-rpc-oversize-1".to_string(),
            }),
            media_type: "application/octet-stream".to_string(),
            content: vec![0; MAX_BLOB_BYTES + 1],
        })
        .await
        .unwrap_err();

    assert_eq!(oversize.code(), Code::ResourceExhausted);
}

#[tokio::test]
async fn artifact_rpc_lists_only_artifacts_for_the_requested_task() {
    let mut harness = RpcTestHarness::start().await;
    let alpha_task = TaskId {
        value: "artifact-rpc-list-alpha".to_string(),
    };
    let beta_task = TaskId {
        value: "artifact-rpc-list-beta".to_string(),
    };
    submit_pending_task(&mut harness, &alpha_task).await;
    submit_pending_task(&mut harness, &beta_task).await;

    for (task_id, artifact_id, content) in [
        (
            alpha_task.clone(),
            "artifact-rpc-alpha-1",
            b"alpha one".to_vec(),
        ),
        (
            alpha_task.clone(),
            "artifact-rpc-alpha-2",
            b"alpha two".to_vec(),
        ),
        (beta_task.clone(), "artifact-rpc-beta-1", b"beta".to_vec()),
    ] {
        harness
            .client
            .put_artifact(PutArtifactRequest {
                task_id: Some(task_id),
                artifact_id: Some(ArtifactId {
                    value: artifact_id.to_string(),
                }),
                media_type: "text/plain".to_string(),
                content,
            })
            .await
            .unwrap();
    }

    let alpha_list = harness
        .client
        .list_artifacts(ListArtifactsRequest {
            task_id: Some(alpha_task),
        })
        .await
        .unwrap()
        .into_inner();
    let beta_list = harness
        .client
        .list_artifacts(ListArtifactsRequest {
            task_id: Some(beta_task),
        })
        .await
        .unwrap()
        .into_inner();

    let alpha_ids = alpha_list
        .artifacts
        .iter()
        .map(|artifact| artifact.artifact_id.as_ref().unwrap().value.as_str())
        .collect::<Vec<_>>();
    let beta_ids = beta_list
        .artifacts
        .iter()
        .map(|artifact| artifact.artifact_id.as_ref().unwrap().value.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        alpha_ids,
        vec!["artifact-rpc-alpha-1", "artifact-rpc-alpha-2"]
    );
    assert_eq!(beta_ids, vec!["artifact-rpc-beta-1"]);
}

#[tokio::test]
async fn artifact_rpc_validates_missing_task_and_bad_identifier_inputs() {
    let mut harness = RpcTestHarness::start().await;

    let missing_task = harness
        .client
        .put_artifact(PutArtifactRequest {
            task_id: Some(TaskId {
                value: "artifact-missing-task".to_string(),
            }),
            artifact_id: Some(ArtifactId {
                value: "artifact-rpc-missing-task".to_string(),
            }),
            media_type: "text/plain".to_string(),
            content: b"content".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(missing_task.code(), Code::NotFound);

    let missing_task_id = harness
        .client
        .put_artifact(PutArtifactRequest {
            task_id: None,
            artifact_id: Some(ArtifactId {
                value: "artifact-rpc-no-task".to_string(),
            }),
            media_type: "text/plain".to_string(),
            content: b"content".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(missing_task_id.code(), Code::InvalidArgument);

    let bad_artifact_id = harness
        .client
        .get_artifact(GetArtifactRequest {
            artifact_id: Some(ArtifactId {
                value: "bad artifact id with spaces".to_string(),
            }),
            metadata_only: false,
        })
        .await
        .unwrap_err();
    assert_eq!(bad_artifact_id.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn artifact_rpc_delete_missing_artifact_returns_deleted_false() {
    let mut harness = RpcTestHarness::start().await;
    let deleted = harness
        .client
        .delete_artifact(keryx_proto::v1::DeleteArtifactRequest {
            artifact_id: Some(ArtifactId {
                value: "artifact-rpc-missing".to_string(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!deleted.deleted);
}
