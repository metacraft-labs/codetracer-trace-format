use serde::{Deserialize, Serialize};

pub const TRACE_STORAGE_SCHEMA: &str = "codetracer.trace-storage.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TraceStorageConfig {
    pub schema: String,
    pub service: ServiceIdentity,
    pub mode: StorageMode,
    #[serde(default)]
    pub storage_servers: Vec<StorageServer>,
    #[serde(default)]
    pub pools: Vec<StoragePool>,
    pub split_policy: SplitPolicy,
    pub shard_policy: CtfsShardPolicy,
    pub materialized_artifact_policy: MaterializedArtifactPolicy,
    pub replication: ReplicationPolicy,
    pub retention: RetentionPolicy,
}

impl TraceStorageConfig {
    pub fn from_json(input: &str) -> serde_json::Result<Self> {
        serde_json::from_str(input)
    }

    pub fn to_json_pretty(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMode {
    ManagedUpload { control_plane_url: String },
    DirectStorage { control_plane_url: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ServiceIdentity {
    pub service_name: String,
    pub environment: String,
    pub instance_id: String,
    pub tenant_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StorageServer {
    pub id: String,
    pub pool: String,
    pub endpoint: StorageEndpoint,
    pub credential_ref: CredentialRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StorageEndpoint {
    pub scheme: String,
    pub base_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CredentialRef {
    pub provider: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StoragePool {
    pub id: String,
    pub purpose: StoragePoolPurpose,
    pub server_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoragePoolPurpose {
    Ctfs,
    MaterializedArtifact,
    Manifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SplitPolicy {
    pub enabled: bool,
    pub max_segment_bytes: u64,
    pub checkpoint_aligned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CtfsShardPolicy {
    pub enabled: bool,
    pub shard_count: u16,
    pub block_range_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MaterializedArtifactPolicy {
    pub pool: String,
    pub max_artifact_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReplicationPolicy {
    pub min_replicas: u8,
    pub target_replicas: u8,
    pub placement: ReplicationPlacement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationPlacement {
    SamePool,
    DistinctServers,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RetentionPolicy {
    pub retained_for_days: u32,
    pub delete_after_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TraceStorageManifest {
    pub schema: String,
    pub recording_id: String,
    pub service: ServiceIdentity,
    pub source: TraceSource,
    pub lifecycle: LifecycleState,
    pub retry: RetryState,
    pub finalize: FinalizeState,
    pub retention: DataState,
    pub replication: ReplicationState,
}

impl TraceStorageManifest {
    pub fn from_json(input: &str) -> serde_json::Result<Self> {
        serde_json::from_str(input)
    }

    pub fn to_json_pretty(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TraceSource {
    SingleCtfs {
        file: PlacedObject,
    },
    SplitCtfs {
        segments: Vec<CtfsSegment>,
    },
    ShardedSplitCtfs {
        segments: Vec<ShardedCtfsSegment>,
    },
    MaterializedArtifact {
        language: MaterializedLanguage,
        artifact: PlacedObject,
        replay_start: ReplayStart,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializedLanguage {
    Python,
    Ruby,
    Javascript,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CtfsSegment {
    pub index: u32,
    pub geid_start: u64,
    pub geid_end: u64,
    pub file: PlacedObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ShardedCtfsSegment {
    pub index: u32,
    pub geid_start: u64,
    pub geid_end: u64,
    pub shards: Vec<CtfsShard>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CtfsShard {
    pub shard_index: u16,
    pub block_start: u64,
    pub block_end: u64,
    pub replicas: Vec<PlacedObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PlacedObject {
    pub object_id: String,
    pub uri: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub placement: Placement,
    pub upload: UploadState,
    pub data_state: DataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Placement {
    pub pool: String,
    pub server_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadState {
    Pending,
    Uploading,
    Uploaded,
    RetryableFailure,
    FatalFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Pending,
    Uploading,
    Uploaded,
    Finalized,
    RetryableFailure,
    FatalFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataState {
    Retained,
    Expired,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RetryState {
    pub attempt: u32,
    pub next_retry_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FinalizeState {
    pub finalized: bool,
    pub finalized_at: Option<String>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReplicationState {
    pub target_replicas: u8,
    pub completed_replicas: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReplayStart {
    pub trace_id: String,
    pub span_id: String,
    pub geid: Option<u64>,
    pub timestamp_unix_nanos: Option<u64>,
}
