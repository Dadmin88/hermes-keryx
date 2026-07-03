use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{
    error::{validate_identifier, ValidationError},
    task::TaskId,
};

pub const MAX_INLINE_ARTIFACT_BYTES: usize = 64 * 1024;
pub const MAX_BLOB_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactId(String);

impl ArtifactId {
    pub fn new(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        validate_identifier("ArtifactId", value).map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ArtifactId {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Digest(String);

impl Digest {
    pub fn new(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let raw = value.as_ref();
        let normalized = raw.trim().to_ascii_lowercase();
        if normalized.len() != 64 || !normalized.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(ValidationError::InvalidDigest(raw.to_owned()));
        }
        Ok(Self(normalized))
    }

    #[must_use]
    pub fn compute(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        Self(format!("{digest:x}"))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Digest {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MediaType(String);

impl MediaType {
    #[must_use]
    pub fn new(value: impl AsRef<str>) -> Self {
        let normalized = value.as_ref().trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Self::default();
        }
        Self(normalized)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn octet_stream() -> Self {
        Self("application/octet-stream".to_owned())
    }

    #[must_use]
    pub fn application_json() -> Self {
        Self("application/json".to_owned())
    }

    #[must_use]
    pub fn text_plain() -> Self {
        Self("text/plain".to_owned())
    }
}

impl Default for MediaType {
    fn default() -> Self {
        Self::octet_stream()
    }
}

impl fmt::Display for MediaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MediaType {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMeta {
    pub artifact_id: ArtifactId,
    pub task_id: TaskId,
    pub digest: Digest,
    pub media_type: MediaType,
    pub byte_len: u64,
    pub inline: bool,
    pub created_at: String,
}

pub fn validate_artifact_size(byte_len: u64) -> Result<(), ValidationError> {
    if byte_len > MAX_BLOB_BYTES as u64 {
        return Err(ValidationError::ArtifactTooLarge {
            byte_len,
            limit_bytes: MAX_BLOB_BYTES as u64,
        });
    }
    Ok(())
}

#[must_use]
pub fn should_inline(byte_len: u64) -> bool {
    byte_len <= MAX_INLINE_ARTIFACT_BYTES as u64
}

#[cfg(test)]
mod tests {
    use super::{
        should_inline, validate_artifact_size, Digest, MediaType, ValidationError, MAX_BLOB_BYTES,
        MAX_INLINE_ARTIFACT_BYTES,
    };

    #[test]
    fn digest_compute_matches_sha256_reference() {
        let digest = Digest::compute(b"hello world");
        assert_eq!(
            digest.as_str(),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn digest_rejects_invalid_hex() {
        assert_eq!(
            Digest::new("not-a-digest").unwrap_err(),
            ValidationError::InvalidDigest("not-a-digest".to_owned())
        );
    }

    #[test]
    fn media_type_defaults_when_empty() {
        assert_eq!(
            MediaType::new("   ").as_str(),
            MediaType::octet_stream().as_str()
        );
    }

    #[test]
    fn size_validation_and_inline_thresholds_are_explicit() {
        validate_artifact_size(MAX_BLOB_BYTES as u64).unwrap();
        assert!(should_inline(MAX_INLINE_ARTIFACT_BYTES as u64));
        assert!(!should_inline((MAX_INLINE_ARTIFACT_BYTES + 1) as u64));
        assert!(validate_artifact_size((MAX_BLOB_BYTES + 1) as u64).is_err());
    }
}
