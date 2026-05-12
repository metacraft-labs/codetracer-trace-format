use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

pub const TRACE_STORAGE_SCHEMA: &str = "codetracer.trace-storage.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TraceStorageConfig {
    pub schema: String,
    pub service: ServiceIdentity,
    pub mode: StorageMode,
    #[serde(default)]
    pub organization_id: Option<String>,
    #[serde(default)]
    pub enterprise_lease: Option<EnterpriseLeaseConfig>,
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

    pub fn validate(&self) -> Result<(), SenderError> {
        if self.schema != TRACE_STORAGE_SCHEMA {
            return Err(SenderError::fatal(format!("unsupported trace-storage schema: {}", self.schema)));
        }
        if self.service.tenant_id.trim().is_empty() {
            return Err(SenderError::fatal("trace-storage config requires service.tenant_id"));
        }
        if matches!(self.mode, StorageMode::DirectStorage { .. }) {
            self.validate_direct_storage()?;
        }
        validate_positive("split_policy.max_segment_bytes", self.split_policy.max_segment_bytes)?;
        validate_positive("shard_policy.block_range_bytes", self.shard_policy.block_range_bytes)?;
        if self.shard_policy.enabled && self.shard_policy.shard_count == 0 {
            return Err(SenderError::fatal("enabled shard_policy requires shard_count > 0"));
        }
        if self.replication.min_replicas == 0 || self.replication.target_replicas < self.replication.min_replicas {
            return Err(SenderError::fatal("replication requires 0 < min_replicas <= target_replicas"));
        }
        if self.retention.delete_after_days < self.retention.retained_for_days {
            return Err(SenderError::fatal(
                "retention.delete_after_days must be greater than or equal to retained_for_days",
            ));
        }
        Ok(())
    }

    fn validate_direct_storage(&self) -> Result<(), SenderError> {
        let Some(lease) = &self.enterprise_lease else {
            return Err(SenderError::fatal("direct_storage requires enterprise_lease configuration"));
        };
        lease.validate()?;
        if self.storage_servers.is_empty() {
            return Err(SenderError::fatal("direct_storage requires at least one storage server"));
        }
        let server_ids = self
            .storage_servers
            .iter()
            .map(|server| {
                if server.id.trim().is_empty() {
                    return Err(SenderError::fatal("storage server id must not be empty"));
                }
                if server.endpoint.base_url.trim().is_empty() {
                    return Err(SenderError::fatal(format!("storage server {} endpoint is empty", server.id)));
                }
                Ok(server.id.as_str())
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        for pool in &self.pools {
            if pool.server_ids.is_empty() {
                return Err(SenderError::fatal(format!("storage pool {} must contain server_ids", pool.id)));
            }
            for server_id in &pool.server_ids {
                if !server_ids.contains(server_id.as_str()) {
                    return Err(SenderError::fatal(format!(
                        "storage pool {} references unknown server {}",
                        pool.id, server_id
                    )));
                }
            }
        }
        self.require_pool(StoragePoolPurpose::Ctfs, "ctfs")?;
        self.require_pool(StoragePoolPurpose::MaterializedArtifact, "materialized artifact")?;
        self.require_pool(StoragePoolPurpose::Manifest, "manifest")?;
        if !self.pools.iter().any(|pool| pool.id == self.materialized_artifact_policy.pool) {
            return Err(SenderError::fatal(format!(
                "materialized artifact policy references unknown pool {}",
                self.materialized_artifact_policy.pool
            )));
        }
        Ok(())
    }

    fn require_pool(&self, purpose: StoragePoolPurpose, label: &str) -> Result<(), SenderError> {
        if self.pools.iter().any(|pool| pool.purpose == purpose) {
            Ok(())
        } else {
            Err(SenderError::fatal(format!("direct_storage requires a {label} storage pool")))
        }
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
    #[serde(default)]
    pub organization_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EnterpriseLeaseConfig {
    pub endpoint_url: String,
    pub credential_ref: CredentialRef,
    pub organization_id: String,
    pub client_session_id: String,
    #[serde(default = "default_recording_session_kind")]
    pub session_kind: String,
    #[serde(default)]
    pub product_name: Option<String>,
    #[serde(default)]
    pub workload_identity: Option<String>,
    #[serde(default)]
    pub trace_identity: Option<String>,
}

impl EnterpriseLeaseConfig {
    pub fn validate(&self) -> Result<(), SenderError> {
        if self.endpoint_url.trim().is_empty() {
            return Err(SenderError::fatal("enterprise_lease.endpoint_url is required"));
        }
        if self.organization_id.trim().is_empty() {
            return Err(SenderError::fatal("enterprise_lease.organization_id is required"));
        }
        if self.client_session_id.trim().is_empty() {
            return Err(SenderError::fatal("enterprise_lease.client_session_id is required"));
        }
        if !matches!(self.session_kind.as_str(), "recording" | "replay") {
            return Err(SenderError::fatal(format!(
                "unsupported enterprise_lease.session_kind: {}",
                self.session_kind
            )));
        }
        if self.credential_ref.key.trim().is_empty() {
            return Err(SenderError::fatal("enterprise_lease.credential_ref.key is required"));
        }
        Ok(())
    }
}

fn default_recording_session_kind() -> String {
    "recording".to_string()
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
        #[serde(default)]
        artifacts: Vec<PlacedObject>,
        replay_start: ReplayStart,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalCtfsBlockLocation<'a> {
    pub segment_index: u32,
    pub block_id: u64,
    pub replicas: Vec<&'a PlacedObject>,
}

impl TraceSource {
    pub fn segment_count(&self) -> usize {
        match self {
            TraceSource::SingleCtfs { .. } => 1,
            TraceSource::SplitCtfs { segments } => segments.len(),
            TraceSource::ShardedSplitCtfs { segments } => segments.len(),
            TraceSource::MaterializedArtifact { .. } => 0,
        }
    }

    pub fn resolve_logical_ctfs_block(&self, segment_index: u32, block_id: u64) -> Result<LogicalCtfsBlockLocation<'_>, SenderError> {
        match self {
            TraceSource::SingleCtfs { file } => {
                if segment_index != 0 {
                    return Err(SenderError::fatal(format!("single CTFS trace has no segment {segment_index}")));
                }
                Ok(LogicalCtfsBlockLocation {
                    segment_index,
                    block_id,
                    replicas: vec![file],
                })
            }
            TraceSource::SplitCtfs { segments } => {
                let segment = segments
                    .iter()
                    .find(|segment| segment.index == segment_index)
                    .ok_or_else(|| SenderError::fatal(format!("split CTFS trace has no segment {segment_index}")))?;
                Ok(LogicalCtfsBlockLocation {
                    segment_index,
                    block_id,
                    replicas: vec![&segment.file],
                })
            }
            TraceSource::ShardedSplitCtfs { segments } => {
                let segment = segments
                    .iter()
                    .find(|segment| segment.index == segment_index)
                    .ok_or_else(|| SenderError::fatal(format!("sharded CTFS trace has no segment {segment_index}")))?;
                let shard = segment
                    .shards
                    .iter()
                    .find(|shard| block_id >= shard.block_start && block_id <= shard.block_end)
                    .ok_or_else(|| SenderError::fatal(format!("sharded CTFS segment {segment_index} has no shard for block {block_id}")))?;
                if shard.replicas.is_empty() {
                    return Err(SenderError::fatal(format!(
                        "sharded CTFS segment {segment_index} block {block_id} has no replicas"
                    )));
                }
                Ok(LogicalCtfsBlockLocation {
                    segment_index,
                    block_id,
                    replicas: shard.replicas.iter().collect(),
                })
            }
            TraceSource::MaterializedArtifact { .. } => Err(SenderError::fatal("materialized trace artifacts do not expose CTFS block locations")),
        }
    }
}

pub trait LogicalCtfsBlockReader {
    fn read_block(&mut self, object: &PlacedObject, block_id: u64) -> Result<Vec<u8>, SenderError>;
}

impl TraceStorageManifest {
    pub fn read_logical_ctfs_block(
        &self,
        segment_index: u32,
        block_id: u64,
        reader: &mut impl LogicalCtfsBlockReader,
    ) -> Result<Vec<u8>, SenderError> {
        let location = self.source.resolve_logical_ctfs_block(segment_index, block_id)?;
        let mut last_error = None;
        for replica in location.replicas {
            match reader.read_block(replica, block_id) {
                Ok(bytes) => return Ok(bytes),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| SenderError::retryable("no CTFS block replicas were readable")))
    }
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
pub struct ManagedMaterializedUpload {
    pub receipt: ManagedUploadReceipt,
    pub content_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedMaterializedUploadSet {
    pub uploads: Vec<ManagedMaterializedUpload>,
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

fn validate_positive(label: &str, value: u64) -> Result<(), SenderError> {
    if value == 0 {
        Err(SenderError::fatal(format!("{label} must be greater than zero")))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnterpriseLeaseGrant {
    pub lease_id: String,
    pub expires_at: String,
}

pub trait EnterpriseLeaseChecker {
    fn checkout(&mut self, lease: &EnterpriseLeaseConfig) -> Result<EnterpriseLeaseGrant, SenderError>;
}

#[derive(Debug)]
pub struct HttpEnterpriseLeaseChecker {
    agent: ureq::Agent,
}

impl Default for HttpEnterpriseLeaseChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpEnterpriseLeaseChecker {
    pub fn new() -> Self {
        Self {
            agent: ureq::AgentBuilder::new().timeout(Duration::from_secs(10)).build(),
        }
    }
}

impl EnterpriseLeaseChecker for HttpEnterpriseLeaseChecker {
    fn checkout(&mut self, lease: &EnterpriseLeaseConfig) -> Result<EnterpriseLeaseGrant, SenderError> {
        lease.validate()?;
        let token = resolve_credential(&lease.credential_ref)?;
        let body = serde_json::json!({
            "organizationId": lease.organization_id,
            "clientSessionId": lease.client_session_id,
            "sessionKind": lease.session_kind,
            "productName": lease.product_name,
            "workloadIdentity": lease.workload_identity,
            "traceIdentity": lease.trace_identity,
        });
        let response = self
            .agent
            .post(&lease.endpoint_url)
            .set("Authorization", &format!("Bearer {token}"))
            .send_json(body)
            .map_err(lease_error_from_ureq)?
            .into_json::<serde_json::Value>()
            .map_err(|error| SenderError::retryable(format!("invalid Enterprise lease checkout response: {error}")))?;
        Ok(EnterpriseLeaseGrant {
            lease_id: json_string(&response, "leaseId")?,
            expires_at: json_string(&response, "expiresAt")?,
        })
    }
}

fn resolve_credential(credential_ref: &CredentialRef) -> Result<String, SenderError> {
    match credential_ref.provider.as_str() {
        "env" => std::env::var(&credential_ref.key)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| SenderError::fatal(format!("{} is required for Enterprise lease checkout", credential_ref.key))),
        other => Err(SenderError::fatal(format!(
            "unsupported credential provider for Enterprise lease checkout: {other}"
        ))),
    }
}

fn lease_error_from_ureq(error: ureq::Error) -> SenderError {
    match &error {
        ureq::Error::Status(code, response) if *code >= 400 && *code < 500 && *code != 408 && *code != 429 => {
            SenderError::fatal(format!("Enterprise lease checkout rejected: HTTP {code} {}", response.status_text()))
        }
        ureq::Error::Status(code, response) => SenderError::retryable(format!(
            "Enterprise lease checkout transient failure: HTTP {code} {}",
            response.status_text()
        )),
        ureq::Error::Transport(_) => SenderError::retryable(format!("Enterprise lease checkout transport failure: {error}")),
    }
}

pub trait DirectStorageTransport {
    fn upload_direct(&mut self, server: &StorageServer, object: &ManagedUploadObject, bytes: &[u8]) -> Result<ManagedUploadReceipt, SenderError>;

    fn report_direct_finalize(&mut self, request: &ManagedFinalizeRequest, lease: &EnterpriseLeaseGrant) -> Result<(), SenderError>;

    fn health(&self) -> SenderHealth;
}

#[derive(Debug)]
pub struct HttpDirectStorageTransport {
    control_plane_base_url: String,
    tenant_id: String,
    organization_id: Option<String>,
    agent: ureq::Agent,
}

impl HttpDirectStorageTransport {
    pub fn new(control_plane_base_url: impl Into<String>, tenant_id: impl Into<String>, organization_id: Option<String>) -> Self {
        Self {
            control_plane_base_url: control_plane_base_url.into().trim_end_matches('/').to_string(),
            tenant_id: tenant_id.into(),
            organization_id,
            agent: ureq::AgentBuilder::new().timeout(Duration::from_secs(10)).build(),
        }
    }

    pub fn from_trace_storage_config(config: &TraceStorageConfig) -> Result<Self, SenderError> {
        let control_plane_base_url = match &config.mode {
            StorageMode::DirectStorage { control_plane_url } => control_plane_url.clone(),
            StorageMode::ManagedUpload { .. } => {
                return Err(SenderError::fatal("HttpDirectStorageTransport requires direct_storage mode"));
            }
        };
        Ok(Self::new(
            control_plane_base_url,
            config.service.tenant_id.clone(),
            config.organization_id.clone().or_else(|| config.service.organization_id.clone()),
        ))
    }
}

impl DirectStorageTransport for HttpDirectStorageTransport {
    fn upload_direct(&mut self, server: &StorageServer, object: &ManagedUploadObject, bytes: &[u8]) -> Result<ManagedUploadReceipt, SenderError> {
        let url = format!(
            "{}/objects/{}",
            server.endpoint.base_url.trim_end_matches('/'),
            urlencoding::encode(&object.object_key)
        );
        self.agent
            .put(&url)
            .set("Content-Type", content_type_for_upload_kind(&object.kind))
            .set("Content-Length", &bytes.len().to_string())
            .send_bytes(bytes)
            .map_err(sender_error_from_ureq)?;

        Ok(ManagedUploadReceipt {
            object_key: object.object_key.clone(),
            storage_pool_id: server.pool.clone(),
            storage_server_id: server.id.clone(),
            storage_endpoint_uri: server.endpoint.base_url.trim_end_matches('/').to_string(),
        })
    }

    fn report_direct_finalize(&mut self, request: &ManagedFinalizeRequest, lease: &EnterpriseLeaseGrant) -> Result<(), SenderError> {
        let session = UploadSession {
            session_id: request.manifest.recording_id.clone(),
            s3_key_prefix: format!("traces/{}/{}", self.tenant_id, request.manifest.recording_id),
            storage_pool_id: None,
            storage_server_id: None,
            storage_endpoint_uri: None,
        };
        let config = CodetracerCiSenderConfig {
            base_url: self.control_plane_base_url.clone(),
            tenant_id: self.tenant_id.clone(),
            bearer_token: String::new(),
            platform: "native".to_string(),
            recording_mode: Some("observability".to_string()),
            service_name: request.manifest.service.service_name.clone(),
            instance_id: Some(request.manifest.service.instance_id.clone()),
        };
        let recording_manifest = monolith_recording_manifest(request, &config, &session);
        let manifest_s3_key = recording_manifest.get("manifestS3Key").and_then(|value| value.as_str()).unwrap_or("");
        let body = serde_json::json!({
            "tenantId": self.tenant_id,
            "organizationId": self.organization_id,
            "leaseId": lease.lease_id,
            "traceIdentity": request.manifest.recording_id,
            "totalSlices": request.total_slices,
            "totalEvents": request.total_events,
            "manifestS3Key": manifest_s3_key,
            "recordingManifest": recording_manifest,
        });
        let url = format!(
            "{}/api/v1/direct-storage/traces/{}/finalize",
            self.control_plane_base_url, request.manifest.recording_id
        );
        // M39 / M40 NixOS tests call this from inside Incus and only see
        // the helper's stderr. Dump the JSON we're about to POST so a
        // 400 with an empty body (model-binder rejection) is debuggable
        // from the test log without an additional pcap run. Gated on
        // CODETRACER_MANAGED_UPLOAD_DEBUG so the chatty diagnostic
        // doesn't appear in production runs.
        if std::env::var("CODETRACER_MANAGED_UPLOAD_DEBUG").is_ok() {
            eprintln!(
                "[codetracer-managed-upload] POST {url} body={}",
                serde_json::to_string(&body).unwrap_or_else(|_| "<unserializable>".to_string())
            );
        }
        self.agent
            .post(&url)
            .set("X-CodeTracer-Enterprise-Lease-Id", &lease.lease_id)
            .send_json(body)
            .map(|_| ())
            .map_err(sender_error_from_ureq)
    }

    fn health(&self) -> SenderHealth {
        match self.agent.get(&format!("{}/healthz", self.control_plane_base_url)).call() {
            Ok(_) => SenderHealth {
                healthy: true,
                message: "direct storage control plane reachable".to_string(),
            },
            Err(error) => SenderHealth {
                healthy: false,
                message: error.to_string(),
            },
        }
    }
}

fn content_type_for_upload_kind(kind: &ManagedUploadKind) -> &'static str {
    match kind {
        ManagedUploadKind::McrSlice { .. } => "application/vnd.codetracer.ctfs",
        ManagedUploadKind::MaterializedArtifact { .. } => "application/vnd.codetracer.materialized-trace+json",
        ManagedUploadKind::Manifest => "application/vnd.codetracer.recording-manifest+json",
    }
}

pub struct DirectStorageSenderBackend<T: DirectStorageTransport, L: EnterpriseLeaseChecker> {
    config: TraceStorageConfig,
    transport: T,
    lease_checker: L,
    lease: Option<EnterpriseLeaseGrant>,
}

impl<T: DirectStorageTransport, L: EnterpriseLeaseChecker> DirectStorageSenderBackend<T, L> {
    pub fn new(config: TraceStorageConfig, transport: T, lease_checker: L) -> Result<Self, SenderError> {
        config.validate()?;
        if !matches!(config.mode, StorageMode::DirectStorage { .. }) {
            return Err(SenderError::fatal("DirectStorageSenderBackend requires direct_storage mode"));
        }
        Ok(Self {
            config,
            transport,
            lease_checker,
            lease: None,
        })
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    fn ensure_enterprise_lease(&mut self) -> Result<EnterpriseLeaseGrant, SenderError> {
        if let Some(lease) = &self.lease {
            return Ok(lease.clone());
        }
        let lease_config = self
            .config
            .enterprise_lease
            .as_ref()
            .ok_or_else(|| SenderError::fatal("direct_storage requires enterprise_lease configuration"))?;
        let grant = self.lease_checker.checkout(lease_config)?;
        self.lease = Some(grant.clone());
        Ok(grant)
    }

    fn upload(&mut self, object: &ManagedUploadObject, purpose: StoragePoolPurpose) -> Result<ManagedUploadReceipt, SenderError> {
        self.ensure_enterprise_lease()?;
        let bytes = fs::read(&object.local_path).map_err(|error| SenderError::retryable(format!("failed to read {}: {error}", object.local_path)))?;
        if object.content_length != bytes.len() as u64 {
            return Err(SenderError::fatal(format!(
                "content length mismatch for {}: manifest={} actual={}",
                object.local_path,
                object.content_length,
                bytes.len()
            )));
        }
        let server = self.select_server(purpose)?.clone();
        self.transport.upload_direct(&server, object, &bytes)
    }

    fn select_server(&self, purpose: StoragePoolPurpose) -> Result<&StorageServer, SenderError> {
        let pool = self
            .config
            .pools
            .iter()
            .find(|pool| pool.purpose == purpose)
            .ok_or_else(|| SenderError::fatal(format!("no storage pool configured for {:?}", purpose)))?;
        let server_id = pool
            .server_ids
            .first()
            .ok_or_else(|| SenderError::fatal(format!("storage pool {} has no servers", pool.id)))?;
        self.config
            .storage_servers
            .iter()
            .find(|server| &server.id == server_id)
            .ok_or_else(|| SenderError::fatal(format!("storage pool {} references unknown server {}", pool.id, server_id)))
    }
}

impl<T: DirectStorageTransport, L: EnterpriseLeaseChecker> SharedSenderBackend for DirectStorageSenderBackend<T, L> {
    fn upload_slice(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload(object, StoragePoolPurpose::Ctfs)
    }

    fn upload_materialized_artifact(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload(object, StoragePoolPurpose::MaterializedArtifact)
    }

    fn upload_manifest(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload(object, StoragePoolPurpose::Manifest)
    }

    fn finalize(&mut self, request: &ManagedFinalizeRequest) -> Result<(), SenderError> {
        validate_complete_finalize_request(request)?;
        let lease = self.ensure_enterprise_lease()?;
        self.transport.report_direct_finalize(request, &lease)
    }

    fn health(&self) -> SenderHealth {
        self.transport.health()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodetracerCiSenderConfig {
    pub base_url: String,
    pub tenant_id: String,
    pub bearer_token: String,
    pub platform: String,
    pub recording_mode: Option<String>,
    pub service_name: String,
    pub instance_id: Option<String>,
}

impl CodetracerCiSenderConfig {
    pub fn from_env() -> Result<Self, SenderError> {
        let base_url = env_required("CODETRACER_MANAGED_UPLOAD_URL")?;
        let tenant_id = env_required("CODETRACER_MANAGED_UPLOAD_TENANT")?;
        let bearer_token = env_required("CODETRACER_MANAGED_UPLOAD_TOKEN")?;
        let service_name = std::env::var("CODETRACER_MANAGED_UPLOAD_SERVICE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "unknown-service".to_string());
        Ok(Self {
            base_url,
            tenant_id,
            bearer_token,
            platform: std::env::var("CODETRACER_MANAGED_UPLOAD_PLATFORM").unwrap_or_else(|_| "native".to_string()),
            recording_mode: std::env::var("CODETRACER_MANAGED_UPLOAD_RECORDING_MODE").ok(),
            service_name,
            instance_id: std::env::var("CODETRACER_MANAGED_UPLOAD_INSTANCE").ok(),
        })
    }

    pub fn from_env_for_platform(platform: impl Into<String>) -> Result<Self, SenderError> {
        let mut config = Self::from_env()?;
        config.platform = platform.into();
        config.recording_mode = Some("materialized".to_string());
        Ok(config)
    }
}

fn env_required(key: &str) -> Result<String, SenderError> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SenderError::fatal(format!("{key} is required for managed upload")))
}

#[derive(Debug, Clone)]
struct UploadSession {
    session_id: String,
    s3_key_prefix: String,
    storage_pool_id: Option<String>,
    storage_server_id: Option<String>,
    storage_endpoint_uri: Option<String>,
}

#[derive(Debug)]
pub struct CodetracerCiSenderBackend {
    config: CodetracerCiSenderConfig,
    agent: ureq::Agent,
    session: Option<UploadSession>,
}

impl CodetracerCiSenderBackend {
    pub fn new(config: CodetracerCiSenderConfig) -> Self {
        Self {
            config,
            agent: ureq::AgentBuilder::new().timeout(Duration::from_secs(10)).build(),
            session: None,
        }
    }

    fn ensure_session(&mut self) -> Result<UploadSession, SenderError> {
        if let Some(session) = &self.session {
            return Ok(session.clone());
        }

        let service_identity = serde_json::json!({
            "serviceName": self.config.service_name,
        });
        let mut body = serde_json::json!({
            "platform": self.config.platform,
            "serviceIdentity": service_identity,
        });
        if let Some(instance_id) = &self.config.instance_id {
            body["instanceIdentity"] = serde_json::json!({
                "instanceId": instance_id,
            });
        }
        if let Some(recording_mode) = &self.config.recording_mode {
            body["recordingMode"] = serde_json::json!(recording_mode);
        }

        let url = format!("{}/api/v1/tenants/{}/traces/upload-session", self.base_url(), self.config.tenant_id);
        let value = self
            .authed_post(&url)
            .send_json(body)
            .map_err(sender_error_from_ureq)?
            .into_json::<serde_json::Value>()
            .map_err(|error| SenderError::retryable(format!("invalid upload-session response: {error}")))?;

        let session = UploadSession {
            session_id: json_string(&value, "sessionId")?,
            s3_key_prefix: json_string(&value, "s3KeyPrefix")?,
            storage_pool_id: json_opt_string(&value, "storagePoolId"),
            storage_server_id: json_opt_string(&value, "storageServerId"),
            storage_endpoint_uri: json_opt_string(&value, "storageEndpointUri"),
        };
        self.session = Some(session.clone());
        Ok(session)
    }

    fn upload(&mut self, object: &ManagedUploadObject, content_type: &str) -> Result<ManagedUploadReceipt, SenderError> {
        let session = self.ensure_session()?;
        let object_key = self.object_key_for_session(&session, object);
        let bytes = fs::read(&object.local_path).map_err(|error| SenderError::retryable(format!("failed to read {}: {error}", object.local_path)))?;
        if object.content_length != bytes.len() as u64 {
            return Err(SenderError::fatal(format!(
                "content length mismatch for {}: manifest={} actual={}",
                object.local_path,
                object.content_length,
                bytes.len()
            )));
        }

        let put_url = format!(
            "{}/api/v1/observability/storage-policy/tenants/{}/local-storage/objects/{}",
            self.base_url(),
            self.config.tenant_id,
            urlencoding::encode(&object_key)
        );
        self.authed_put(&put_url)
            .set("Content-Type", content_type)
            .set("Content-Length", &bytes.len().to_string())
            .send_bytes(&bytes)
            .map_err(sender_error_from_ureq)?;

        Ok(ManagedUploadReceipt {
            object_key,
            storage_pool_id: session.storage_pool_id.unwrap_or_default(),
            storage_server_id: session.storage_server_id.unwrap_or_default(),
            storage_endpoint_uri: session.storage_endpoint_uri.unwrap_or_default(),
        })
    }

    fn object_key_for_session(&self, session: &UploadSession, object: &ManagedUploadObject) -> String {
        if object.object_key.starts_with(&session.s3_key_prefix) {
            return object.object_key.clone();
        }
        let name = Path::new(&object.object_key)
            .file_name()
            .or_else(|| Path::new(&object.local_path).file_name())
            .and_then(|value| value.to_str())
            .unwrap_or("artifact.bin");
        format!("{}/{}", session.s3_key_prefix.trim_end_matches('/'), name)
    }

    fn base_url(&self) -> String {
        self.config.base_url.trim_end_matches('/').to_string()
    }

    fn authed_post(&self, url: &str) -> ureq::Request {
        self.agent.post(url).set("Authorization", &format!("Bearer {}", self.config.bearer_token))
    }

    fn authed_put(&self, url: &str) -> ureq::Request {
        self.agent.put(url).set("Authorization", &format!("Bearer {}", self.config.bearer_token))
    }
}

impl SharedSenderBackend for CodetracerCiSenderBackend {
    fn upload_slice(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload(object, "application/vnd.codetracer.ctfs")
    }

    fn upload_materialized_artifact(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload(object, "application/vnd.codetracer.materialized-trace+json")
    }

    fn upload_manifest(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload(object, "application/vnd.codetracer.recording-manifest+json")
    }

    fn finalize(&mut self, request: &ManagedFinalizeRequest) -> Result<(), SenderError> {
        validate_complete_finalize_request(request)?;
        let session = self.ensure_session()?;
        let manifest = monolith_recording_manifest(request, &self.config, &session);
        let manifest_s3_key = manifest.get("manifestS3Key").and_then(|value| value.as_str()).unwrap_or("");
        let body = serde_json::json!({
            "totalSlices": request.total_slices,
            "totalEvents": request.total_events,
            "manifestS3Key": manifest_s3_key,
            "recordingManifest": manifest,
        });
        let url = format!("{}/api/v1/traces/{}/finalize", self.base_url(), session.session_id);
        self.authed_post(&url).send_json(body).map(|_| ()).map_err(sender_error_from_ureq)
    }

    fn health(&self) -> SenderHealth {
        let url = format!("{}/healthz", self.base_url());
        match self.agent.get(&url).call() {
            Ok(_) => SenderHealth {
                healthy: true,
                message: "codetracer-ci reachable".to_string(),
            },
            Err(error) => SenderHealth {
                healthy: false,
                message: error.to_string(),
            },
        }
    }
}

pub fn upload_materialized_artifact_from_env(trace_dir: impl AsRef<Path>, language: &str) -> Result<ManagedMaterializedUpload, SenderError> {
    let uploads = upload_materialized_artifacts_from_env(trace_dir, language)?.uploads;
    uploads
        .into_iter()
        .next()
        .ok_or_else(|| SenderError::fatal("materialized upload set was empty"))
}

pub fn upload_materialized_artifacts_from_env(trace_dir: impl AsRef<Path>, language: &str) -> Result<ManagedMaterializedUploadSet, SenderError> {
    let trace_dir = trace_dir.as_ref();
    let config = CodetracerCiSenderConfig::from_env_for_platform(language)?;
    let service = if config.service_name.trim().is_empty() {
        language.to_string()
    } else {
        config.service_name.clone()
    };
    let artifacts = materialized_artifact_set(trace_dir, language, &service)?;
    let mut sender = ManagedTraceSender::new(CodetracerCiSenderBackend::new(config), format!("{language}-materialized-finalize"));
    let upload_objects = artifacts
        .iter()
        .map(|artifact| ManagedUploadObject {
            object_key: artifact.object_key.clone(),
            local_path: artifact.local_path.to_string_lossy().to_string(),
            content_length: artifact.content_length,
            sha256: artifact.sha256.clone(),
            kind: ManagedUploadKind::MaterializedArtifact {
                artifact_kind: artifact.artifact_kind.clone(),
            },
        })
        .collect::<Vec<_>>();
    let receipts = sender.upload_materialized_artifacts(upload_objects)?;
    let mut uploads = Vec::with_capacity(receipts.len());
    let mut placed = Vec::with_capacity(receipts.len());

    for (artifact, receipt) in artifacts.into_iter().zip(receipts.into_iter()) {
        placed.push(placed_object_from_materialized_upload(
            &receipt,
            artifact.content_length,
            &artifact.sha256,
        ));
        uploads.push(ManagedMaterializedUpload {
            receipt,
            content_length: artifact.content_length,
            sha256: artifact.sha256,
        });
    }

    let primary = placed
        .first()
        .cloned()
        .ok_or_else(|| SenderError::fatal("refusing to finalize empty materialized artifact set"))?;
    let idempotency_key = format!("{language}-materialized-finalize");
    let manifest = TraceStorageManifest {
        schema: TRACE_STORAGE_SCHEMA.to_string(),
        recording_id: service.clone(),
        service: ServiceIdentity {
            service_name: service,
            environment: "managed-upload".to_string(),
            instance_id: std::env::var("CODETRACER_MANAGED_UPLOAD_INSTANCE").unwrap_or_else(|_| format!("{language}-recorder")),
            tenant_id: std::env::var("CODETRACER_MANAGED_UPLOAD_TENANT").unwrap_or_default(),
            organization_id: None,
        },
        source: TraceSource::MaterializedArtifact {
            language: parse_materialized_language(language)?,
            artifact: primary,
            artifacts: placed,
            replay_start: ReplayStart {
                trace_id: String::new(),
                span_id: String::new(),
                geid: None,
                timestamp_unix_nanos: None,
            },
        },
        lifecycle: LifecycleState::Finalized,
        retry: RetryState {
            attempt: 0,
            next_retry_at: None,
            last_error: None,
        },
        finalize: FinalizeState {
            finalized: true,
            finalized_at: Some("1970-01-01T00:00:00Z".to_string()),
            idempotency_key: idempotency_key.clone(),
        },
        retention: DataState::Retained,
        replication: ReplicationState {
            target_replicas: 1,
            completed_replicas: 1,
        },
    };
    sender.finalize(ManagedFinalizeRequest {
        total_slices: 0,
        total_events: 0,
        manifest,
        idempotency_key,
    })?;

    Ok(ManagedMaterializedUploadSet { uploads })
}

/// One materialized artifact to upload, located on the local filesystem and
/// already SHA-256 hashed. Used both by the managed-upload helper and by the
/// shared `codetracer-managed-upload direct-materialized-finalize` subcommand
/// so the live recorder and the direct-storage helper agree on which files
/// belong to a materialized trace and how they should be keyed.
#[derive(Debug, Clone)]
pub struct MaterializedLocalArtifact {
    pub object_key: String,
    pub local_path: std::path::PathBuf,
    pub content_length: u64,
    pub sha256: String,
    pub artifact_kind: String,
}

/// Scan `trace_dir` for the materialized recorder's artifact set (primary
/// `trace.{json,ct}`, optional correlation index, plus a generated
/// `artifact-set.json` manifest pointing at both) and hash each entry.
///
/// Exposed so the `direct-materialized-finalize` helper subcommand can reuse
/// the exact same artifact discovery the managed-upload path uses; the live
/// Python/Ruby/JS recorders therefore stream the same logical bytes whether
/// the deployment is configured for managed-upload or direct-storage.
pub fn materialized_artifact_set(trace_dir: &Path, language: &str, service: &str) -> Result<Vec<MaterializedLocalArtifact>, SenderError> {
    let primary_path = materialized_artifact_path(trace_dir)?;
    let mut files = vec![(
        "materialized-trace-v1.json".to_string(),
        primary_path,
        "materialized_trace_v1".to_string(),
    )];

    for name in ["correlation-index.json", "correlation-index-v1.json"] {
        let path = trace_dir.join(name);
        if path.exists() {
            files.push(("correlation-index.json".to_string(), path, "correlation_index_v1".to_string()));
            break;
        }
    }

    let artifact_set_path = ensure_artifact_set(trace_dir, language, &files)?;
    files.push((
        "artifact-set.json".to_string(),
        artifact_set_path,
        "materialized_artifact_set_v1".to_string(),
    ));

    files
        .into_iter()
        .map(|(object_name, local_path, artifact_kind)| {
            let bytes = fs::read(&local_path)
                .map_err(|error| SenderError::retryable(format!("failed to read materialized artifact {}: {error}", local_path.display())))?;
            if bytes.is_empty() {
                return Err(SenderError::fatal(format!(
                    "refusing to upload empty materialized artifact: {}",
                    local_path.display()
                )));
            }
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            Ok(MaterializedLocalArtifact {
                object_key: format!("{service}/{object_name}"),
                local_path,
                content_length: bytes.len() as u64,
                sha256: format!("{:x}", hasher.finalize()),
                artifact_kind,
            })
        })
        .collect()
}

fn ensure_artifact_set(trace_dir: &Path, language: &str, files: &[(String, std::path::PathBuf, String)]) -> Result<std::path::PathBuf, SenderError> {
    let path = trace_dir.join("artifact-set.json");
    let artifacts = files
        .iter()
        .map(|(name, local_path, kind)| {
            serde_json::json!({
                "artifactKey": name,
                "artifactKind": kind,
                "localPath": local_path.file_name().and_then(|value| value.to_str()).unwrap_or(name),
            })
        })
        .collect::<Vec<_>>();
    let body = serde_json::json!({
        "kind": "materialized_artifact_set",
        "schema": "codetracer.materialized-artifact-set.v1",
        "language": language,
        "artifacts": artifacts,
    });
    let bytes =
        serde_json::to_vec_pretty(&body).map_err(|error| SenderError::fatal(format!("failed to encode materialized artifact set: {error}")))?;
    fs::write(&path, bytes).map_err(|error| SenderError::retryable(format!("failed to write {}: {error}", path.display())))?;
    Ok(path)
}

fn placed_object_from_materialized_upload(receipt: &ManagedUploadReceipt, content_length: u64, sha256: &str) -> PlacedObject {
    PlacedObject {
        object_id: receipt.object_key.clone(),
        uri: format!("{}/{}", receipt.storage_endpoint_uri, receipt.object_key),
        size_bytes: content_length,
        sha256: sha256.to_string(),
        placement: Placement {
            pool: receipt.storage_pool_id.clone(),
            server_id: receipt.storage_server_id.clone(),
        },
        upload: UploadState::Uploaded,
        data_state: DataState::Retained,
    }
}

/// Map the human-readable recorder language tag onto the
/// `MaterializedLanguage` enum embedded in the shared trace-storage manifest.
///
/// Public so the direct-storage finalize helper accepts the same language
/// vocabulary as the managed-upload helper.
pub fn parse_materialized_language(language: &str) -> Result<MaterializedLanguage, SenderError> {
    match language {
        "python" => Ok(MaterializedLanguage::Python),
        "ruby" => Ok(MaterializedLanguage::Ruby),
        "javascript" | "js" => Ok(MaterializedLanguage::Javascript),
        other => Err(SenderError::fatal(format!(
            "unsupported materialized language for managed upload: {other}"
        ))),
    }
}

fn materialized_artifact_path(trace_dir: &Path) -> Result<std::path::PathBuf, SenderError> {
    let preferred = trace_dir.join("trace.json");
    if preferred.exists() {
        return Ok(preferred);
    }
    let ctfs = trace_dir.join("trace.ct");
    if ctfs.exists() {
        return Ok(ctfs);
    }
    let entries = fs::read_dir(trace_dir)
        .map_err(|error| SenderError::retryable(format!("failed to inspect materialized trace directory {}: {error}", trace_dir.display())))?;
    for entry in entries {
        let path = entry
            .map_err(|error| SenderError::retryable(format!("failed to inspect trace entry: {error}")))?
            .path();
        if path.extension().and_then(|value| value.to_str()) == Some("ct") {
            return Ok(path);
        }
    }
    Ok(preferred)
}

fn validate_complete_finalize_request(request: &ManagedFinalizeRequest) -> Result<(), SenderError> {
    match &request.manifest.source {
        TraceSource::MaterializedArtifact { artifact, artifacts, .. } => {
            validate_complete_object(artifact, "materialized trace artifact")?;
            if artifacts.is_empty() {
                return Err(SenderError::fatal(
                    "refusing complete finalize without materialized artifact set metadata",
                ));
            }
            for entry in artifacts {
                validate_complete_object(entry, "materialized artifact set entry")?;
            }
            Ok(())
        }
        TraceSource::SingleCtfs { file } => {
            if request.total_slices != 1 {
                return Err(SenderError::fatal("single CTFS finalize must report exactly one slice"));
            }
            validate_complete_object(file, "single CTFS file")
        }
        TraceSource::SplitCtfs { segments } => {
            if request.total_slices == 0 || segments.is_empty() {
                return Err(SenderError::fatal("refusing complete finalize without MCR slice metadata"));
            }
            if segments.len() != request.total_slices as usize {
                return Err(SenderError::fatal(format!(
                    "refusing complete finalize with incomplete MCR slice metadata: expected {} slices, got {}",
                    request.total_slices,
                    segments.len()
                )));
            }
            for (expected_index, segment) in segments.iter().enumerate() {
                if segment.index != expected_index as u32 {
                    return Err(SenderError::fatal(format!(
                        "refusing complete finalize with out-of-order MCR slice metadata: expected index {}, got {}",
                        expected_index, segment.index
                    )));
                }
                validate_complete_object(&segment.file, "MCR slice")?;
            }
            Ok(())
        }
        TraceSource::ShardedSplitCtfs { segments } => {
            if request.total_slices == 0 || segments.is_empty() {
                return Err(SenderError::fatal("refusing complete finalize without sharded MCR segment metadata"));
            }
            if segments.len() != request.total_slices as usize {
                return Err(SenderError::fatal(format!(
                    "refusing complete finalize with incomplete sharded MCR segment metadata: expected {} segments, got {}",
                    request.total_slices,
                    segments.len()
                )));
            }
            for (expected_index, segment) in segments.iter().enumerate() {
                if segment.index != expected_index as u32 {
                    return Err(SenderError::fatal(format!(
                        "refusing complete finalize with out-of-order sharded MCR segment metadata: expected index {}, got {}",
                        expected_index, segment.index
                    )));
                }
                if segment.shards.is_empty() {
                    return Err(SenderError::fatal(format!(
                        "refusing complete finalize with sharded MCR segment {} missing shard metadata",
                        segment.index
                    )));
                }
                for shard in &segment.shards {
                    if shard.block_end < shard.block_start {
                        return Err(SenderError::fatal(format!(
                            "refusing complete finalize with invalid shard block range {}..{}",
                            shard.block_start, shard.block_end
                        )));
                    }
                    if shard.replicas.is_empty() {
                        return Err(SenderError::fatal(format!(
                            "refusing complete finalize with segment {} shard {} missing replicas",
                            segment.index, shard.shard_index
                        )));
                    }
                    for replica in &shard.replicas {
                        validate_complete_object(replica, "MCR shard replica")?;
                    }
                }
            }
            Ok(())
        }
    }
}

fn validate_complete_object(object: &PlacedObject, label: &str) -> Result<(), SenderError> {
    if object.object_id.trim().is_empty() {
        return Err(SenderError::fatal(format!("refusing complete finalize with {label} missing object key")));
    }
    if object.uri.trim().is_empty() {
        return Err(SenderError::fatal(format!("refusing complete finalize with {label} missing storage key")));
    }
    if object.size_bytes == 0 {
        return Err(SenderError::fatal(format!(
            "refusing complete finalize with {label} missing content length"
        )));
    }
    if object.sha256.trim().is_empty() {
        return Err(SenderError::fatal(format!(
            "refusing complete finalize with {label} missing content hash"
        )));
    }
    if object.upload != UploadState::Uploaded {
        return Err(SenderError::fatal(format!("refusing complete finalize with {label} not uploaded")));
    }
    Ok(())
}

fn sender_error_from_ureq(error: ureq::Error) -> SenderError {
    match error {
        ureq::Error::Status(code, response) if (400..500).contains(&code) && code != 408 && code != 429 => {
            let message = response.status_text().to_string();
            // Always log the body-read attempt so M39 / M40 NixOS tests
            // can distinguish "endpoint returned 400 with no body" from
            // "ureq couldn't read the body" from "body was a problem+json
            // envelope" — important for diagnosing upload-rejection
            // reasons inside the codetracer-managed-upload subprocess.
            let body_excerpt = match response.into_string() {
                Ok(text) if text.is_empty() => " body=<empty>".to_string(),
                Ok(text) => format!(" body={text}"),
                Err(read_err) => format!(" body=<read-error: {read_err}>"),
            };
            SenderError::fatal(format!("codetracer-ci rejected upload: HTTP {code} {message}{body_excerpt}"))
        }
        ureq::Error::Status(code, response) => {
            let message = response.status_text().to_string();
            let body_excerpt = match response.into_string() {
                Ok(text) if text.is_empty() => " body=<empty>".to_string(),
                Ok(text) => format!(" body={text}"),
                Err(read_err) => format!(" body=<read-error: {read_err}>"),
            };
            SenderError::retryable(format!("codetracer-ci transient upload failure: HTTP {code} {message}{body_excerpt}"))
        }
        ureq::Error::Transport(transport) => SenderError::retryable(format!("codetracer-ci transport failure: {transport}")),
    }
}

fn json_string(value: &serde_json::Value, key: &str) -> Result<String, SenderError> {
    value
        .get(key)
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .ok_or_else(|| SenderError::retryable(format!("upload-session response missing {key}")))
}

fn json_opt_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|value| value.as_str()).map(ToString::to_string)
}

fn materialized_artifact_kind(object_id: &str) -> &'static str {
    match Path::new(object_id).file_name().and_then(|value| value.to_str()) {
        Some("correlation-index.json" | "correlation-index-v1.json") => "correlation_index_v1",
        Some("artifact-set.json" | "artifact-set-v1.json") => "materialized_artifact_set_v1",
        _ => "materialized_trace_v1",
    }
}

fn object_key_for_placed_object(object: &PlacedObject) -> String {
    if object.object_id.trim().is_empty() {
        object.uri.trim_start_matches("local://").to_string()
    } else {
        object.object_id.clone()
    }
}

fn storage_endpoint_uri_for_placed_object(object: &PlacedObject) -> String {
    let uri = object.uri.trim_end_matches('/');
    let object_key = object_key_for_placed_object(object);
    let object_key = object_key.trim_start_matches('/');
    let suffix = format!("/{object_key}");

    uri.strip_suffix(&suffix).unwrap_or(uri).to_string()
}

fn monolith_recording_manifest(request: &ManagedFinalizeRequest, config: &CodetracerCiSenderConfig, session: &UploadSession) -> serde_json::Value {
    let now = "1970-01-01T00:00:00Z";
    let service = serde_json::json!({
        "serviceName": request.manifest.service.service_name,
        "serviceNamespace": serde_json::Value::Null,
        "serviceVersion": serde_json::Value::Null,
    });
    let instance = serde_json::json!({
        "instanceId": request.manifest.service.instance_id,
        "instanceName": serde_json::Value::Null,
        "hostName": serde_json::Value::Null,
    });
    let manifest_key = format!("{}/manifest.json", session.s3_key_prefix.trim_end_matches('/'));

    match &request.manifest.source {
        TraceSource::MaterializedArtifact {
            language: _,
            artifact,
            artifacts,
            replay_start,
        } => {
            let manifest_artifacts = if artifacts.is_empty() {
                vec![artifact.clone()]
            } else {
                artifacts.clone()
            };
            serde_json::json!({
                "kind": "materialized_trace",
                "uploadCompletionState": "complete",
                "serviceIdentity": service,
                "instanceIdentity": instance,
                "timeRange": {},
                "retentionStatus": "available",
                "missingSliceKeys": [],
                "mcrSlices": [],
                "materializedTraceArtifacts": manifest_artifacts.iter().map(|entry| {
                    // Prefer the raw object key (object_id) for `artifactKey` because
                    // codetracer-ci's direct-storage finalize validator pairs the
                    // reported placement (storage pool/server/endpoint) with the same
                    // object key it expects to find on the storage node. When the
                    // placement is empty (e.g. legacy managed-upload receipts that
                    // only populated `uri`), fall back to the URI minus the
                    // `local://` shim so older fixtures keep working.
                    let artifact_key = if entry.object_id.trim().is_empty() {
                        entry.uri.trim_start_matches("local://").to_string()
                    } else {
                        entry.object_id.clone()
                    };
                    let endpoint_uri = storage_endpoint_uri_for_placed_object(entry);
                    serde_json::json!({
                        "artifactKey": artifact_key,
                        "artifactKind": materialized_artifact_kind(&entry.object_id),
                        "uploadCompletionState": "complete",
                        "retentionStatus": "available",
                        "contentLength": entry.size_bytes,
                        "contentHash": entry.sha256,
                        "storagePoolId": entry.placement.pool,
                        "storageServerId": entry.placement.server_id,
                        "storageEndpointUri": endpoint_uri,
                        "replayStart": {
                            "geid": replay_start.geid,
                            "traceId": replay_start.trace_id,
                            "spanId": replay_start.span_id,
                            "wallTimeUnixNs": replay_start.timestamp_unix_nanos
                        }
                    })
                }).collect::<Vec<_>>(),
                "totalSlices": 0,
                "totalEvents": request.total_events,
                "createdAt": now,
                "finalizedAt": now,
                "manifestS3Key": manifest_key,
                "recordingMode": config.recording_mode,
            })
        }
        TraceSource::SplitCtfs { segments } => serde_json::json!({
            "kind": "mcr_slices",
            "uploadCompletionState": "complete",
            "serviceIdentity": service,
            "instanceIdentity": instance,
            "timeRange": {},
            "retentionStatus": "available",
            "missingSliceKeys": [],
            "mcrSlices": segments.iter().map(|segment| serde_json::json!({
                "key": segment.file.object_id,
                "objectKey": segment.file.object_id,
                "index": segment.index,
                "order": segment.index,
                "sliceIndex": segment.index,
                "sliceKey": segment.file.uri.trim_start_matches("local://"),
                "uploadCompletionState": "complete",
                "retentionStatus": "available",
                "eventCount": segment.geid_end.saturating_sub(segment.geid_start),
                "sizeBytes": segment.file.size_bytes,
                "contentLength": segment.file.size_bytes,
                "sha256": segment.file.sha256,
                "contentHash": segment.file.sha256,
            })).collect::<Vec<_>>(),
            "materializedTraceArtifacts": [],
            "totalSlices": request.total_slices,
            "totalEvents": request.total_events,
            "createdAt": now,
            "finalizedAt": now,
            "manifestS3Key": manifest_key,
            "recordingMode": config.recording_mode,
        }),
        TraceSource::ShardedSplitCtfs { segments } => serde_json::json!({
            "kind": "mcr_slices",
            "uploadCompletionState": "complete",
            "serviceIdentity": service,
            "instanceIdentity": instance,
            "timeRange": {
                "geidStart": segments.iter().map(|segment| segment.geid_start).min(),
                "geidEnd": segments.iter().map(|segment| segment.geid_end).max(),
            },
            "retentionStatus": "available",
            "missingSliceKeys": [],
            "mcrSlices": [],
            "shardedMcrSegments": segments.iter().map(|segment| serde_json::json!({
                "segmentIndex": segment.index,
                "order": segment.index,
                "geidStart": segment.geid_start,
                "geidEnd": segment.geid_end,
                "shards": segment.shards.iter().map(|shard| serde_json::json!({
                    "shardIndex": shard.shard_index,
                    "blockStart": shard.block_start,
                    "blockEnd": shard.block_end,
                    "replicas": shard.replicas.iter().enumerate().map(|(replica_index, replica)| serde_json::json!({
                        "replicaIndex": replica_index,
                        "objectKey": object_key_for_placed_object(replica),
                        "storagePoolId": replica.placement.pool,
                        "storageServerId": replica.placement.server_id,
                        "storageEndpointUri": storage_endpoint_uri_for_placed_object(replica),
                        "contentLength": replica.size_bytes,
                        "contentHash": replica.sha256,
                        "uploadCompletionState": "complete",
                        "retentionStatus": "available",
                    })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "materializedTraceArtifacts": [],
            "totalSlices": request.total_slices,
            "totalEvents": request.total_events,
            "createdAt": now,
            "finalizedAt": now,
            "manifestS3Key": manifest_key,
            "recordingMode": config.recording_mode,
        }),
        _ => serde_json::json!({
            "kind": "mcr_slices",
            "uploadCompletionState": "complete",
            "serviceIdentity": service,
            "instanceIdentity": instance,
            "timeRange": {},
            "retentionStatus": "available",
            "missingSliceKeys": [],
            "mcrSlices": [],
            "materializedTraceArtifacts": [],
            "totalSlices": request.total_slices,
            "totalEvents": request.total_events,
            "createdAt": now,
            "finalizedAt": now,
            "manifestS3Key": manifest_key,
            "recordingMode": config.recording_mode,
        }),
    }
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

    pub fn upload_materialized_artifacts(&mut self, objects: Vec<ManagedUploadObject>) -> Result<Vec<ManagedUploadReceipt>, SenderError> {
        let mut receipts = Vec::with_capacity(objects.len());
        for object in objects {
            receipts.push(self.upload_materialized_artifact(object)?);
        }
        Ok(receipts)
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
