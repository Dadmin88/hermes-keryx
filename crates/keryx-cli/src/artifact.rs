use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::Subcommand;
use keryx_proto::v1::{
    ArtifactId, DeleteArtifactRequest, GetArtifactRequest, ListArtifactsRequest,
    PutArtifactRequest, TaskId,
};

use crate::{authorized_request, connect_daemon, require_daemon_endpoint};

#[derive(Debug, Subcommand)]
pub enum ArtifactCommand {
    /// Store an artifact for a task.
    Put {
        task_id: String,
        file_path: PathBuf,
        #[arg(long)]
        media_type: Option<String>,
        #[arg(long)]
        id: Option<String>,
    },
    /// Retrieve an artifact by id.
    Get {
        artifact_id: String,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        metadata_only: bool,
    },
    /// List artifacts for a task.
    Ls { task_id: String },
    /// Delete an artifact by id.
    Rm { artifact_id: String },
}

pub async fn run(command: ArtifactCommand) -> Result<()> {
    let endpoint = require_daemon_endpoint()?;
    let mut client = connect_daemon(&endpoint, "keryx artifact").await?;
    match command {
        ArtifactCommand::Put {
            task_id,
            file_path,
            media_type,
            id,
        } => {
            let content = std::fs::read(&file_path).with_context(|| {
                format!("keryx artifact put: failed to read {}", file_path.display())
            })?;
            let media_type = media_type.unwrap_or_else(|| infer_media_type(&file_path));
            let response = client
                .put_artifact(authorized_request(PutArtifactRequest {
                    task_id: Some(TaskId {
                        value: task_id.clone(),
                    }),
                    artifact_id: id.map(|value| ArtifactId { value }),
                    media_type: media_type.clone(),
                    content,
                }))
                .await
                .with_context(|| {
                    format!(
                        "keryx artifact put: RPC failed for task {} from {}",
                        task_id,
                        file_path.display()
                    )
                })?
                .into_inner();
            let artifact_id = response
                .artifact_id
                .ok_or_else(|| anyhow!("keryx artifact put: daemon omitted artifact_id"))?;
            let response_task_id = response
                .task_id
                .ok_or_else(|| anyhow!("keryx artifact put: daemon omitted task_id"))?;
            println!(
                "Stored artifact {} for task {}: {} ({} bytes, {})",
                artifact_id.value,
                response_task_id.value,
                response.digest,
                response.byte_len,
                response.media_type
            );
        }
        ArtifactCommand::Get {
            artifact_id,
            output,
            metadata_only,
        } => {
            let response = client
                .get_artifact(authorized_request(GetArtifactRequest {
                    artifact_id: Some(ArtifactId {
                        value: artifact_id.clone(),
                    }),
                    metadata_only,
                }))
                .await
                .with_context(|| {
                    format!("keryx artifact get: RPC failed for artifact {artifact_id}")
                })?
                .into_inner();
            let response_artifact_id = response
                .artifact_id
                .ok_or_else(|| anyhow!("keryx artifact get: daemon omitted artifact_id"))?;
            let task_id = response
                .task_id
                .ok_or_else(|| anyhow!("keryx artifact get: daemon omitted task_id"))?;

            if metadata_only {
                print_metadata(
                    &response_artifact_id.value,
                    &task_id.value,
                    &response.digest,
                    &response.media_type,
                    response.byte_len,
                    response.inline,
                    &response.created_at,
                );
                return Ok(());
            }

            if let Some(output_path) = output {
                std::fs::write(&output_path, &response.content).with_context(|| {
                    format!(
                        "keryx artifact get: failed to write {}",
                        output_path.display()
                    )
                })?;
                println!(
                    "Wrote {} bytes to {}",
                    response.content.len(),
                    output_path.display()
                );
                return Ok(());
            }

            if is_textual_media_type(&response.media_type) {
                print_metadata(
                    &response_artifact_id.value,
                    &task_id.value,
                    &response.digest,
                    &response.media_type,
                    response.byte_len,
                    response.inline,
                    &response.created_at,
                );
                if !response.content.is_empty() {
                    match String::from_utf8(response.content) {
                        Ok(text) => print!("{text}"),
                        Err(error) => print!("{}", String::from_utf8_lossy(error.as_bytes())),
                    }
                }
                return Ok(());
            }

            eprintln!(
                "warning: artifact {} has media_type={} and may be binary; writing raw bytes to stdout",
                response_artifact_id.value,
                response.media_type
            );
            io::stdout()
                .write_all(&response.content)
                .context("keryx artifact get: failed to write raw bytes to stdout")?;
            io::stdout().flush().ok();
        }
        ArtifactCommand::Ls { task_id } => {
            let response = client
                .list_artifacts(authorized_request(ListArtifactsRequest {
                    task_id: Some(TaskId {
                        value: task_id.clone(),
                    }),
                }))
                .await
                .with_context(|| format!("keryx artifact ls: RPC failed for task {task_id}"))?
                .into_inner();
            if response.artifacts.is_empty() {
                println!("No artifacts for task {task_id}");
                return Ok(());
            }
            println!("ARTIFACT_ID\tDIGEST\tMEDIA_TYPE\tBYTE_LEN\tINLINE\tCREATED_AT");
            for artifact in response.artifacts {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}",
                    required_artifact_id(artifact.artifact_id)?.value,
                    artifact.digest,
                    artifact.media_type,
                    artifact.byte_len,
                    artifact.inline,
                    artifact.created_at
                );
            }
        }
        ArtifactCommand::Rm { artifact_id } => {
            let response = client
                .delete_artifact(authorized_request(DeleteArtifactRequest {
                    artifact_id: Some(ArtifactId {
                        value: artifact_id.clone(),
                    }),
                }))
                .await
                .with_context(|| {
                    format!("keryx artifact rm: RPC failed for artifact {artifact_id}")
                })?
                .into_inner();
            if response.deleted {
                println!("Deleted artifact {artifact_id}");
            } else {
                println!("Artifact {artifact_id} not found (already deleted?)");
            }
        }
    }
    Ok(())
}

fn required_artifact_id(value: Option<ArtifactId>) -> Result<ArtifactId> {
    value.ok_or_else(|| anyhow!("artifact summary missing artifact_id"))
}

fn infer_media_type(path: &Path) -> String {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
        .as_deref()
    {
        Some("json") => "application/json",
        Some("txt") | Some("log") | Some("md") => "text/plain",
        Some("csv") => "text/csv",
        Some("html") => "text/html",
        Some("xml") => "application/xml",
        Some("yaml") | Some("yml") => "application/yaml",
        _ => "application/octet-stream",
    }
    .to_string()
}

fn is_textual_media_type(media_type: &str) -> bool {
    let media_type = media_type.to_ascii_lowercase();
    media_type.starts_with("text/")
        || matches!(
            media_type.as_str(),
            "application/json"
                | "application/xml"
                | "application/yaml"
                | "application/javascript"
                | "application/x-yaml"
        )
        || media_type.ends_with("+json")
        || media_type.ends_with("+xml")
}

fn print_metadata(
    artifact_id: &str,
    task_id: &str,
    digest: &str,
    media_type: &str,
    byte_len: u64,
    inline: bool,
    created_at: &str,
) {
    println!("artifact_id: {artifact_id}");
    println!("task_id: {task_id}");
    println!("digest: {digest}");
    println!("media_type: {media_type}");
    println!("byte_len: {byte_len}");
    println!("inline: {inline}");
    println!("created_at: {created_at}");
}
