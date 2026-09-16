//! Result storage — D2 §4.3's `chunks.rs` row and §5.5's size math.
//!
//! # The commit point
//!
//! **Every chunk is written first; the status patch that indexes them is the
//! commit.** That ordering is what makes a controller restart safe: chunks
//! that exist but are not indexed are invisible and are collected with their
//! owner, while an index that named a chunk which was never written would be a
//! status pointing at nothing. A restart between the two re-decodes the relay
//! (the Job still exists, because no TTL is set until after the commit) and
//! re-writes byte-identical chunks, which the 409 rule accepts.
//!
//! # The bounds
//!
//! D2 §5.5: at most [`MAX_CHUNK_LINES`] entries **and** [`MAX_CHUNK_BYTES`]
//! per chunk. Both, because either alone is insufficient: 2,500 worst-case
//! 283-byte lines is ~690 KiB, comfortably under the 1 MiB `ConfigMap` limit,
//! but 2,500 lines says nothing about a run of pathological names, and a byte
//! bound alone would let a chunk carry 10,000 short lines that the API's page
//! size then has to split anyway.
//!
//! # Immutable and owned, exactly as the plan is
//!
//! Same reasoning as [`super::plan`], and the same 409 rule with a different
//! code: [`CheckCode::ResultStorageConflict`].

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, ObjectMeta, PostParams};
use kube::ResourceExt as _;

use logweir_core::check_contract::{CheckCode, TopicEntry, TOPIC_INVENTORY_FORMAT};

use crate::job::RunnerOwner;

/// At most this many TSV lines per chunk (D2 §5.5).
pub const MAX_CHUNK_LINES: usize = 2_500;

/// At most this many bytes of TSV per chunk (D2 §5.5).
///
/// 768 KiB, under the API server's 1 MiB `ConfigMap` limit with headroom for
/// `metadata`, the annotations below and the object's own encoding.
pub const MAX_CHUNK_BYTES: usize = 768 * 1024;

/// The one key inside a topic chunk.
pub const CHUNK_KEY: &str = "topics.tsv";

/// The one key inside a details `ConfigMap`.
pub const DETAILS_KEY: &str = "details.jsonl";

/// The format the chunk carries — `logweir.dev/topic-inventory/v1` for topics.
pub const FORMAT_ANNOTATION: &str = "logweir.dev/result-format";
/// `sha256:<hex>` over this chunk's own data.
pub const SHA256_ANNOTATION: &str = "logweir.dev/result-sha256";
/// `"<i>/<n>"`, one-based — D2 §4.3.
pub const CHUNK_ANNOTATION: &str = "logweir.dev/chunk";

/// The details format: one JSON object per line.
pub const DETAILS_FORMAT: &str = "logweir.dev/check-details/v1";

/// `<job>-r<3-digit index>`, zero-based — D2 §4.3.
///
/// THREE DIGITS, which is the shape D2 names and which orders lexically for
/// the 20 chunks the 50,000-topic hard maximum produces. An index past 999
/// cannot arise: `MAX_CHUNK_LINES` × 1,000 is 2.5 M entries, fifty times the
/// contract's `MAX_TOPICS_CEILING`.
#[must_use]
pub fn chunk_name(job_name: &str, index: usize) -> String {
    format!("{job_name}-r{index:03}")
}

/// `<job>-details` — D2 §4.3.
#[must_use]
pub fn details_name(job_name: &str) -> String {
    format!("{job_name}-details")
}

/// One chunk's content, before it becomes an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    /// Zero-based, and the number in [`chunk_name`].
    pub index: usize,
    /// The TSV lines, concatenated.
    pub data: String,
    /// How many entries it holds.
    pub lines: usize,
    /// `sha256:<hex>` over [`Chunk::data`].
    pub sha256: String,
}

/// Split an inventory into chunks — **pure**, and the whole of D2 §5.5's
/// bound.
///
/// A single entry longer than [`MAX_CHUNK_BYTES`] cannot exist (a Kafka name is
/// at most 249 characters, so a line is at most ~308 bytes), but the loop is
/// written so that one would land alone in its own chunk rather than loop
/// forever or be dropped.
#[must_use]
pub fn split(entries: &[TopicEntry]) -> Vec<Chunk> {
    let mut out: Vec<Chunk> = Vec::new();
    let mut data = String::new();
    let mut lines = 0usize;
    for entry in entries {
        let line = entry.tsv_line();
        let would_overflow = lines >= MAX_CHUNK_LINES
            || (!data.is_empty() && data.len() + line.len() > MAX_CHUNK_BYTES);
        if would_overflow {
            out.push(finish(out.len(), std::mem::take(&mut data), lines));
            lines = 0;
        }
        data.push_str(&line);
        lines += 1;
    }
    if lines > 0 {
        out.push(finish(out.len(), data, lines));
    }
    out
}

fn finish(index: usize, data: String, lines: usize) -> Chunk {
    let sha256 = logweir_core::ids::sha256_prefixed(data.as_bytes());
    Chunk {
        index,
        data,
        lines,
        sha256,
    }
}

/// One chunk, as an immutable owned `ConfigMap`.
#[must_use]
pub fn build_chunk(
    job_name: &str,
    namespace: &str,
    owner: &RunnerOwner,
    chunk: &Chunk,
    total: usize,
) -> ConfigMap {
    object(
        &chunk_name(job_name, chunk.index),
        namespace,
        owner,
        TOPIC_INVENTORY_FORMAT,
        &chunk.sha256,
        Some(format!("{}/{total}", chunk.index + 1)),
        CHUNK_KEY,
        chunk.data.clone(),
    )
}

/// The details document, as an immutable owned `ConfigMap`.
#[must_use]
pub fn build_details(
    job_name: &str,
    namespace: &str,
    owner: &RunnerOwner,
    details: &str,
) -> ConfigMap {
    object(
        &details_name(job_name),
        namespace,
        owner,
        DETAILS_FORMAT,
        &logweir_core::ids::sha256_prefixed(details.as_bytes()),
        None,
        DETAILS_KEY,
        details.to_string(),
    )
}

#[allow(clippy::too_many_arguments)]
fn object(
    name: &str,
    namespace: &str,
    owner: &RunnerOwner,
    format: &str,
    sha256: &str,
    chunk: Option<String>,
    key: &str,
    data: String,
) -> ConfigMap {
    let mut annotations = BTreeMap::from([
        (FORMAT_ANNOTATION.to_string(), format.to_string()),
        (SHA256_ANNOTATION.to_string(), sha256.to_string()),
    ]);
    if let Some(chunk) = chunk {
        annotations.insert(CHUNK_ANNOTATION.to_string(), chunk);
    }
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            owner_references: Some(vec![OwnerReference {
                api_version: owner.api_version.clone(),
                kind: owner.kind.clone(),
                name: owner.name.clone(),
                uid: owner.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..ObjectMeta::default()
        },
        immutable: Some(true),
        data: Some(BTreeMap::from([(key.to_string(), data)])),
        binary_data: None,
    }
}

/// Whether an object already at this name IS this chunk — the same three-part
/// rule as [`super::plan::accepts_existing`], with
/// [`CheckCode::ResultStorageConflict`] instead.
///
/// # Errors
///
/// [`ChunkConflict`] naming which of the three failed.
pub fn accepts_existing(
    existing: &ConfigMap,
    owner_uid: &str,
    sha256: &str,
) -> Result<(), ChunkConflict> {
    let owned = existing
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.uid == owner_uid && o.controller == Some(true));
    if !owned {
        return Err(ChunkConflict(format!(
            "ConfigMap {} exists and is not controlled by this check's subject",
            existing.name_any()
        )));
    }
    let found = existing
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(SHA256_ANNOTATION))
        .map(String::as_str);
    if found != Some(sha256) {
        return Err(ChunkConflict(format!(
            "ConfigMap {} exists with result digest {} and this pass produced {sha256}",
            existing.name_any(),
            found.unwrap_or("<none>")
        )));
    }
    if existing.immutable != Some(true) {
        return Err(ChunkConflict(format!(
            "ConfigMap {} exists without `immutable: true`, so a result an API response already \
             served could still change; it is not adopted",
            existing.name_any()
        )));
    }
    Ok(())
}

/// A terminal result-storage refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkConflict(pub String);

impl ChunkConflict {
    /// The closed code a status carries for this.
    #[must_use]
    pub fn code(&self) -> CheckCode {
        CheckCode::ResultStorageConflict
    }
}

impl std::fmt::Display for ChunkConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ChunkConflict {}

/// Write every chunk, accepting identical ones that are already there.
///
/// **Returns the names in index order, and the caller's NEXT act is the status
/// patch that indexes them** — that patch is the commit point, and nothing
/// before it is visible to a reader of the custom resource.
///
/// # Errors
///
/// [`super::plan::EnsureError::Api`] for a transport failure (requeue);
/// [`WriteError::Conflict`] for a terminal
/// [`CheckCode::ResultStorageConflict`].
pub async fn write_all(
    client: &kube::Client,
    namespace: &str,
    owner_uid: &str,
    objects: &[ConfigMap],
) -> Result<Vec<String>, WriteError> {
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let mut written = Vec::with_capacity(objects.len());
    for object in objects {
        let name = object.name_any();
        let sha256 = object
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(SHA256_ANNOTATION))
            .cloned()
            .unwrap_or_default();
        match maps.create(&PostParams::default(), object).await {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 409 => {
                let existing = maps
                    .get_opt(&name)
                    .await
                    .map_err(WriteError::Api)?
                    .ok_or_else(|| WriteError::Api(kube::Error::Api(response.clone())))?;
                accepts_existing(&existing, owner_uid, &sha256).map_err(WriteError::Conflict)?;
            }
            Err(e) => return Err(WriteError::Api(e)),
        }
        written.push(name);
    }
    Ok(written)
}

/// Why [`write_all`] did not finish.
#[derive(Debug)]
pub enum WriteError {
    /// The API server could not be talked to. **Requeue.**
    Api(kube::Error),
    /// A terminal refusal.
    Conflict(ChunkConflict),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "the Kubernetes API returned an error: {e}"),
            Self::Conflict(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for WriteError {}
