use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedUploadKind {
    McrSlice { slice_index: u32 },
    MaterializedArtifact { artifact_kind: String },
    Manifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedUploadObject {
    pub object_key: String,
    pub local_path: String,
    pub content_length: u64,
    pub sha256: String,
    pub kind: ManagedUploadKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedUploadReceipt {
    pub object_key: String,
    pub storage_pool_id: String,
    pub storage_server_id: String,
    pub storage_endpoint_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedFinalizeRequest {
    pub total_slices: u32,
    pub total_events: u64,
    pub manifest: TraceStorageManifest,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderHealth {
    pub healthy: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderError {
    pub retryable: bool,
    pub message: String,
}

impl SenderError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            message: message.into(),
        }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            message: message.into(),
        }
    }
}

pub trait SharedSenderBackend {
    fn upload_slice(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError>;
    fn upload_materialized_artifact(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError>;
    fn upload_manifest(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError>;
    fn finalize(&mut self, request: &ManagedFinalizeRequest) -> Result<(), SenderError>;
    fn health(&self) -> SenderHealth;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderObjectState {
    pub object: ManagedUploadObject,
    pub receipt: Option<ManagedUploadReceipt>,
    pub upload: UploadState,
    pub retry: RetryState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderFinalizeState {
    pub finalized: bool,
    pub idempotency_key: String,
    pub attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedSenderState {
    pub objects: BTreeMap<String, SenderObjectState>,
    pub finalize: SenderFinalizeState,
}

pub struct ManagedTraceSender<B: SharedSenderBackend> {
    backend: B,
    state: ManagedSenderState,
    finalized_keys: BTreeSet<String>,
}

impl<B: SharedSenderBackend> ManagedTraceSender<B> {
    pub fn new(backend: B, idempotency_key: impl Into<String>) -> Self {
        Self {
            backend,
            state: ManagedSenderState {
                objects: BTreeMap::new(),
                finalize: SenderFinalizeState {
                    finalized: false,
                    idempotency_key: idempotency_key.into(),
                    attempts: 0,
                },
            },
            finalized_keys: BTreeSet::new(),
        }
    }

    pub fn state(&self) -> &ManagedSenderState {
        &self.state
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    pub fn health(&self) -> SenderHealth {
        self.backend.health()
    }

    pub fn upload_slice(&mut self, object: ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload_object(object, |backend, object| backend.upload_slice(object))
    }

    pub fn upload_materialized_artifact(&mut self, object: ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload_object(object, |backend, object| backend.upload_materialized_artifact(object))
    }

    pub fn upload_manifest(&mut self, object: ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload_object(object, |backend, object| backend.upload_manifest(object))
    }

    pub fn retry_pending(&mut self) -> Result<Vec<ManagedUploadReceipt>, SenderError> {
        let pending: Vec<ManagedUploadObject> = self
            .state
            .objects
            .values()
            .filter(|entry| entry.upload == UploadState::RetryableFailure)
            .map(|entry| entry.object.clone())
            .collect();
        let mut receipts = Vec::with_capacity(pending.len());
        for object in pending {
            let receipt = match object.kind {
                ManagedUploadKind::McrSlice { .. } => self.upload_slice(object)?,
                ManagedUploadKind::MaterializedArtifact { .. } => self.upload_materialized_artifact(object)?,
                ManagedUploadKind::Manifest => self.upload_manifest(object)?,
            };
            receipts.push(receipt);
        }
        Ok(receipts)
    }

    pub fn finalize(&mut self, request: ManagedFinalizeRequest) -> Result<(), SenderError> {
        self.state.finalize.attempts += 1;
        if self.state.finalize.finalized && self.finalized_keys.contains(&request.idempotency_key) {
            return Ok(());
        }
        self.backend.finalize(&request)?;
        self.state.finalize.finalized = true;
        self.state.finalize.idempotency_key = request.idempotency_key.clone();
        self.finalized_keys.insert(request.idempotency_key);
        Ok(())
    }

    fn upload_object(
        &mut self,
        object: ManagedUploadObject,
        upload: impl FnOnce(&mut B, &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError>,
    ) -> Result<ManagedUploadReceipt, SenderError> {
        if let Some(existing) = self.state.objects.get(&object.object_key) {
            if existing.upload == UploadState::Uploaded {
                return Ok(existing.receipt.clone().expect("uploaded object has receipt"));
            }
        }

        self.state.objects.entry(object.object_key.clone()).or_insert_with(|| SenderObjectState {
            object: object.clone(),
            receipt: None,
            upload: UploadState::Pending,
            retry: RetryState {
                attempt: 0,
                next_retry_at: None,
                last_error: None,
            },
        });
        self.state.objects.get_mut(&object.object_key).expect("object state exists").upload = UploadState::Uploading;

        match upload(&mut self.backend, &object) {
            Ok(receipt) => {
                let entry = self.state.objects.get_mut(&object.object_key).expect("object state exists");
                entry.receipt = Some(receipt.clone());
                entry.upload = UploadState::Uploaded;
                entry.retry.last_error = None;
                Ok(receipt)
            }
            Err(error) => {
                let entry = self.state.objects.get_mut(&object.object_key).expect("object state exists");
                entry.retry.attempt += 1;
                entry.retry.last_error = Some(error.message.clone());
                entry.upload = if error.retryable {
                    UploadState::RetryableFailure
                } else {
                    UploadState::FatalFailure
                };
                Err(error)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReplayStart {
    pub trace_id: String,
    pub span_id: String,
    pub geid: Option<u64>,
    pub timestamp_unix_nanos: Option<u64>,
}
