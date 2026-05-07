use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::Path,
    thread,
};

use codetracer_ctfs::trace_storage::LogicalCtfsBlockReader;
use codetracer_ctfs::trace_storage::{
    CodetracerCiSenderBackend, CodetracerCiSenderConfig, DataState, DirectStorageSenderBackend, DirectStorageTransport, EnterpriseLeaseGrant,
    FinalizeState, HttpEnterpriseLeaseChecker, LifecycleState, ManagedFinalizeRequest, ManagedTraceSender, ManagedUploadKind, ManagedUploadObject,
    ManagedUploadReceipt, MaterializedLanguage, PlacedObject, Placement, ReplayStart, ReplicationState, RetryState, SenderError, SenderHealth,
    ServiceIdentity, SharedSenderBackend, StorageMode, StorageServer, TraceSource, TraceStorageConfig, TraceStorageManifest, UploadState,
    TRACE_STORAGE_SCHEMA,
};
use serde_json::json;

fn fixture(name: &str) -> String {
    fs::read_to_string(Path::new("tests/fixtures/trace_storage").join(name)).unwrap()
}

#[test]
fn test_shared_trace_storage_config_roundtrip() {
    let config = TraceStorageConfig::from_json(&fixture("storage_config.full.json")).unwrap();
    assert_eq!(config.schema, TRACE_STORAGE_SCHEMA);
    config.validate().unwrap();
    assert_eq!(config.organization_id.as_deref(), Some("org-enterprise-a"));
    let lease = config.enterprise_lease.as_ref().unwrap();
    assert_eq!(lease.organization_id, "org-enterprise-a");
    assert_eq!(lease.credential_ref.key, "CODETRACER_ENTERPRISE_LEASE_TOKEN");
    match &config.mode {
        StorageMode::DirectStorage { control_plane_url } => {
            assert_eq!(control_plane_url, "https://ci.example.test/trace-storage")
        }
        StorageMode::ManagedUpload { .. } => panic!("expected direct storage fixture"),
    }
    assert_eq!(config.storage_servers[0].endpoint.base_url, "https://store-a.example.test");
    assert_eq!(config.storage_servers[0].credential_ref.key, "CODETRACER_STORE_A_TOKEN");
    assert_eq!(config.storage_servers.len(), 2);
    assert_eq!(config.pools[0].server_ids, ["store-a", "store-b"]);
    assert!(config.split_policy.enabled);
    assert_eq!(config.split_policy.max_segment_bytes, 67_108_864);
    assert!(config.shard_policy.enabled);
    assert_eq!(config.shard_policy.block_range_bytes, 8_388_608);
    assert_eq!(config.materialized_artifact_policy.pool, "artifacts-hot");
    assert_eq!(config.replication.target_replicas, 2);
    assert_eq!(config.retention.retained_for_days, 30);

    let reparsed = TraceStorageConfig::from_json(&config.to_json_pretty().unwrap()).unwrap();
    assert_eq!(reparsed, config);
}

#[test]
fn test_direct_storage_config_requires_enterprise_lease() {
    let mut config = TraceStorageConfig::from_json(&fixture("storage_config.full.json")).unwrap();
    config.enterprise_lease = None;

    let error = config.validate().unwrap_err();
    assert!(!error.retryable);
    assert!(error.message.contains("enterprise_lease"));
}

#[test]
fn test_shared_trace_storage_config_roundtrip_managed_upload() {
    let config = TraceStorageConfig::from_json(&fixture("storage_config.managed_upload.json")).unwrap();
    assert_eq!(config.schema, TRACE_STORAGE_SCHEMA);
    config.validate().unwrap();
    match &config.mode {
        StorageMode::ManagedUpload { control_plane_url } => {
            assert_eq!(control_plane_url, "https://ci.example.test/managed-upload")
        }
        StorageMode::DirectStorage { .. } => panic!("expected managed upload fixture"),
    }
    assert_eq!(config.service.environment, "production");
    assert_eq!(config.storage_servers[1].endpoint.base_url, "https://managed-store-b.example.test");
    assert_eq!(config.storage_servers[1].credential_ref.provider, "vault");
    assert_eq!(config.pools[0].server_ids, ["managed-store-a", "managed-store-b"]);
    assert_eq!(config.split_policy.max_segment_bytes, 33_554_432);
    assert_eq!(config.shard_policy.shard_count, 8);
    assert_eq!(config.shard_policy.block_range_bytes, 4_194_304);
    assert_eq!(config.replication.min_replicas, 2);
    assert_eq!(config.replication.target_replicas, 3);
    assert_eq!(config.retention.delete_after_days, 45);

    let reparsed = TraceStorageConfig::from_json(&config.to_json_pretty().unwrap()).unwrap();
    assert_eq!(reparsed, config);
}

#[test]
fn test_shared_manifest_models_all_trace_source_variants() {
    let cases = [
        ("manifest.single_ctfs.json", "single"),
        ("manifest.split_ctfs.json", "split"),
        ("manifest.sharded_split_ctfs.json", "sharded"),
        ("manifest.python_materialized.json", "python"),
        ("manifest.ruby_materialized.json", "ruby"),
        ("manifest.javascript_materialized.json", "javascript"),
    ];

    for (file, label) in cases {
        let manifest = TraceStorageManifest::from_json(&fixture(file)).unwrap();
        assert_eq!(manifest.schema, TRACE_STORAGE_SCHEMA, "{label}");
        assert_eq!(manifest.lifecycle, LifecycleState::Finalized, "{label}");

        match (&manifest.source, label) {
            (TraceSource::SingleCtfs { file }, "single") => assert_eq!(file.placement.pool, "ctfs-hot"),
            (TraceSource::SplitCtfs { segments }, "split") => assert_eq!(segments.len(), 2),
            (TraceSource::ShardedSplitCtfs { segments }, "sharded") => {
                assert_eq!(segments[0].shards[0].replicas.len(), 2)
            }
            (TraceSource::MaterializedArtifact { language, artifact, .. }, "python") => {
                assert_eq!(*language, MaterializedLanguage::Python);
                assert_eq!(artifact.placement.pool, "artifacts-hot");
            }
            (TraceSource::MaterializedArtifact { language, .. }, "ruby") => {
                assert_eq!(*language, MaterializedLanguage::Ruby)
            }
            (TraceSource::MaterializedArtifact { language, .. }, "javascript") => {
                assert_eq!(*language, MaterializedLanguage::Javascript)
            }
            _ => panic!("unexpected source variant for {label}"),
        }

        let reparsed = TraceStorageManifest::from_json(&manifest.to_json_pretty().unwrap()).unwrap();
        assert_eq!(reparsed, manifest, "{label}");
    }
}

#[test]
fn test_manifest_models_split_files_and_block_shards_orthogonally() {
    let single = TraceStorageManifest::from_json(&fixture("manifest.single_ctfs.json")).unwrap();
    let split = TraceStorageManifest::from_json(&fixture("manifest.split_ctfs.json")).unwrap();
    let sharded_single = sharded_manifest("single-sharded", 1);
    let sharded_split = sharded_manifest("split-sharded", 2);

    assert_eq!(single.source.segment_count(), 1);
    assert_eq!(split.source.segment_count(), 2);
    assert_eq!(sharded_single.source.segment_count(), 1);
    assert_eq!(sharded_split.source.segment_count(), 2);

    let cases = [
        (&single, 0, 7, "ctfs-single"),
        (&split, 1, 7, "ctfs-split-1"),
        (&sharded_single, 0, 7, "single-sharded-segment-0-shard-low-a"),
        (&sharded_split, 1, 70, "split-sharded-segment-1-shard-high-a"),
    ];
    for (manifest, segment, block, expected_object) in cases {
        let location = manifest.source.resolve_logical_ctfs_block(segment, block).unwrap();
        assert_eq!(location.segment_index, segment);
        assert_eq!(location.block_id, block);
        assert_eq!(location.replicas[0].object_id, expected_object);
    }

    let mut reader = TestLogicalReader {
        fail_first_object_id: Some("split-sharded-segment-1-shard-high-a".to_string()),
        calls: Vec::new(),
    };
    let block = sharded_split.read_logical_ctfs_block(1, 70, &mut reader).unwrap();
    assert_eq!(block, logical_block_bytes("split-sharded-segment-1-shard-high-b", 70));
    assert_eq!(
        reader.calls,
        [
            ("split-sharded-segment-1-shard-high-a".to_string(), 70),
            ("split-sharded-segment-1-shard-high-b".to_string(), 70)
        ]
    );

    let materialized = TraceStorageManifest::from_json(&fixture("manifest.python_materialized.json")).unwrap();
    assert!(materialized.source.resolve_logical_ctfs_block(0, 0).is_err());
}

#[test]
fn test_shared_manifest_models_materialized_artifacts_without_ctfs_shards() {
    for (file, language) in [
        ("manifest.python_materialized.json", MaterializedLanguage::Python),
        ("manifest.ruby_materialized.json", MaterializedLanguage::Ruby),
        ("manifest.javascript_materialized.json", MaterializedLanguage::Javascript),
    ] {
        let manifest = TraceStorageManifest::from_json(&fixture(file)).unwrap();
        assert_eq!(manifest.source.segment_count(), 0);
        match &manifest.source {
            TraceSource::MaterializedArtifact {
                language: parsed,
                artifact,
                artifacts,
                ..
            } => {
                assert_eq!(*parsed, language);
                assert_eq!(artifact.placement.pool, "artifacts-hot");
                assert_eq!(artifact.data_state, DataState::Retained);
                assert!(artifacts.is_empty(), "{file} fixture intentionally has no CTFS or shard artifact list");
            }
            _ => panic!("{file} should be a materialized source"),
        }
        let json = manifest.to_json_pretty().unwrap();
        assert!(!json.contains("shards"));
        assert!(!json.contains("geid_start"));
        assert!(!json.contains("block_start"));
        let reparsed = TraceStorageManifest::from_json(&json).unwrap();
        assert_eq!(reparsed.replication.target_replicas, manifest.replication.target_replicas);
        assert_eq!(reparsed.retention, DataState::Retained);
    }
}

#[test]
fn test_shared_manifest_roundtrips_upload_lifecycle_data_retry_and_finalize_states() {
    let cases = [
        (
            "pending",
            LifecycleState::Pending,
            "pending",
            UploadState::Pending,
            "retained",
            DataState::Retained,
            0,
            None,
            None,
            FinalizeState {
                finalized: false,
                finalized_at: None,
                idempotency_key: "finalize-pending".to_string(),
            },
        ),
        (
            "uploading",
            LifecycleState::Uploading,
            "uploading",
            UploadState::Uploading,
            "retained",
            DataState::Retained,
            1,
            Some("2026-05-06T10:10:00Z"),
            Some("in-flight retry lease"),
            FinalizeState {
                finalized: false,
                finalized_at: None,
                idempotency_key: "finalize-uploading".to_string(),
            },
        ),
        (
            "uploaded",
            LifecycleState::Uploaded,
            "uploaded",
            UploadState::Uploaded,
            "retained",
            DataState::Retained,
            0,
            None,
            None,
            FinalizeState {
                finalized: false,
                finalized_at: None,
                idempotency_key: "finalize-uploaded".to_string(),
            },
        ),
        (
            "finalized",
            LifecycleState::Finalized,
            "uploaded",
            UploadState::Uploaded,
            "retained",
            DataState::Retained,
            0,
            None,
            None,
            FinalizeState {
                finalized: true,
                finalized_at: Some("2026-05-06T10:11:00Z".to_string()),
                idempotency_key: "finalize-finalized".to_string(),
            },
        ),
        (
            "retryable_failure",
            LifecycleState::RetryableFailure,
            "retryable_failure",
            UploadState::RetryableFailure,
            "expired",
            DataState::Expired,
            3,
            Some("2026-05-06T10:12:00Z"),
            Some("temporary storage server outage"),
            FinalizeState {
                finalized: false,
                finalized_at: None,
                idempotency_key: "finalize-retryable".to_string(),
            },
        ),
        (
            "fatal_failure",
            LifecycleState::FatalFailure,
            "fatal_failure",
            UploadState::FatalFailure,
            "deleted",
            DataState::Deleted,
            4,
            None,
            Some("sha256 mismatch after upload"),
            FinalizeState {
                finalized: false,
                finalized_at: None,
                idempotency_key: "finalize-fatal".to_string(),
            },
        ),
    ];

    for (
        label,
        expected_lifecycle,
        upload_json,
        expected_upload,
        data_json,
        expected_data,
        retry_attempt,
        next_retry_at,
        last_error,
        expected_finalize,
    ) in cases
    {
        let manifest_json = json!({
            "schema": TRACE_STORAGE_SCHEMA,
            "recording_id": format!("rec-{label}"),
            "service": {
                "service_name": "checkout-api",
                "environment": "staging",
                "instance_id": "checkout-api-7f8d",
                "tenant_id": "tenant-a"
            },
            "source": {
                "kind": "single_ctfs",
                "file": {
                    "object_id": format!("ctfs-{label}"),
                    "uri": format!("ctfs://store-a/rec-{label}/trace.ct"),
                    "size_bytes": 4096,
                    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "placement": { "pool": "ctfs-hot", "server_id": "store-a" },
                    "upload": upload_json,
                    "data_state": data_json
                }
            },
            "lifecycle": label,
            "retry": {
                "attempt": retry_attempt,
                "next_retry_at": next_retry_at,
                "last_error": last_error
            },
            "finalize": {
                "finalized": expected_finalize.finalized,
                "finalized_at": expected_finalize.finalized_at,
                "idempotency_key": expected_finalize.idempotency_key
            },
            "retention": data_json,
            "replication": { "target_replicas": 2, "completed_replicas": 1 }
        })
        .to_string();

        let manifest = TraceStorageManifest::from_json(&manifest_json).unwrap();
        assert_eq!(manifest.lifecycle, expected_lifecycle, "{label}");
        assert_eq!(manifest.retention, expected_data, "{label}");
        assert_eq!(manifest.retry.attempt, retry_attempt, "{label}");
        assert_eq!(manifest.retry.next_retry_at.as_deref(), next_retry_at, "{label}");
        assert_eq!(manifest.retry.last_error.as_deref(), last_error, "{label}");
        assert_eq!(manifest.finalize, expected_finalize, "{label}");
        match &manifest.source {
            TraceSource::SingleCtfs { file } => {
                assert_eq!(file.upload, expected_upload, "{label}");
                assert_eq!(file.data_state, expected_data, "{label}");
            }
            _ => panic!("expected single CTFS lifecycle fixture for {label}"),
        }

        let reparsed = TraceStorageManifest::from_json(&manifest.to_json_pretty().unwrap()).unwrap();
        assert_eq!(reparsed.lifecycle, expected_lifecycle, "{label}");
        assert_eq!(reparsed.retry.attempt, retry_attempt, "{label}");
        assert_eq!(reparsed.finalize, expected_finalize, "{label}");
    }
}

#[test]
fn test_recorders_use_shared_storage_config_without_private_parsers() {
    let workspace = Path::new("..");
    let recorder_dependency_files = [
        "../codetracer-python-recorder/codetracer-python-recorder/Cargo.toml",
        "../codetracer-ruby-recorder/gems/codetracer-ruby-recorder/ext/native_tracer/Cargo.toml",
        "../codetracer-js-recorder/crates/recorder_native/Cargo.toml",
    ];
    for file in recorder_dependency_files {
        let text = fs::read_to_string(workspace.join(file)).unwrap();
        assert!(text.contains("codetracer_ctfs"), "{file} must depend on the shared CTFS storage contract");
    }

    let native_test =
        fs::read_to_string(workspace.join("../codetracer-native-recorder/ct_recorder/tests/test_shared_storage_config_adapter.nim")).unwrap();
    assert!(native_test.contains("codetracer_ctfs/trace_storage_config"));

    let forbidden = [
        "struct StorageServer",
        "struct StoragePool",
        "struct CtfsShardPolicy",
        "struct ShardPlacement",
        "type StorageServer",
        "type StoragePool",
        "type CtfsShardPolicy",
        "type ShardPlacement",
        "class StorageServer",
        "interface StorageServer",
    ];
    let recorder_roots = [
        "../codetracer-native-recorder",
        "../codetracer-python-recorder",
        "../codetracer-ruby-recorder",
        "../codetracer-js-recorder",
    ];
    for root in recorder_roots {
        for entry in walk_files(workspace.join(root)) {
            let Some(ext) = entry.extension().and_then(|value| value.to_str()) else {
                continue;
            };
            if !matches!(ext, "rs" | "nim" | "py" | "rb" | "ts" | "js") {
                continue;
            }
            let text = fs::read_to_string(&entry).unwrap();
            for token in forbidden {
                assert!(
                    !text.contains(token),
                    "{} must not define private storage placement parser/schema token {token}",
                    entry.display()
                );
            }
        }
    }
}

#[derive(Default)]
struct TestManagedBackend {
    fail_next_uploads: usize,
    fail_next_finalize: usize,
    uploads: Vec<String>,
    finalized: Vec<String>,
}

impl SharedSenderBackend for TestManagedBackend {
    fn upload_slice(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload("slice", object)
    }

    fn upload_materialized_artifact(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload("materialized", object)
    }

    fn upload_manifest(&mut self, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        self.upload("manifest", object)
    }

    fn finalize(&mut self, request: &ManagedFinalizeRequest) -> Result<(), SenderError> {
        if self.fail_next_finalize > 0 {
            self.fail_next_finalize -= 1;
            return Err(SenderError::retryable("transient finalize failure"));
        }
        self.finalized.push(request.idempotency_key.clone());
        Ok(())
    }

    fn health(&self) -> SenderHealth {
        SenderHealth {
            healthy: true,
            message: "test backend healthy".to_string(),
        }
    }
}

impl TestManagedBackend {
    fn upload(&mut self, label: &str, object: &ManagedUploadObject) -> Result<ManagedUploadReceipt, SenderError> {
        if self.fail_next_uploads > 0 {
            self.fail_next_uploads -= 1;
            return Err(SenderError::retryable(format!("transient {label} failure")));
        }
        self.uploads.push(object.object_key.clone());
        Ok(ManagedUploadReceipt {
            object_key: object.object_key.clone(),
            storage_pool_id: "shared-local".to_string(),
            storage_server_id: "local-storage-1".to_string(),
            storage_endpoint_uri: "local://codetracer-ci/storage-service".to_string(),
        })
    }
}

#[derive(Default)]
struct TestDirectTransport {
    uploaded: Vec<(String, String, Vec<u8>)>,
    finalized_with_lease: Vec<String>,
}

impl DirectStorageTransport for TestDirectTransport {
    fn upload_direct(&mut self, server: &StorageServer, object: &ManagedUploadObject, bytes: &[u8]) -> Result<ManagedUploadReceipt, SenderError> {
        self.uploaded.push((server.id.clone(), object.object_key.clone(), bytes.to_vec()));
        Ok(ManagedUploadReceipt {
            object_key: object.object_key.clone(),
            storage_pool_id: server.pool.clone(),
            storage_server_id: server.id.clone(),
            storage_endpoint_uri: server.endpoint.base_url.clone(),
        })
    }

    fn report_direct_finalize(&mut self, _request: &ManagedFinalizeRequest, lease: &EnterpriseLeaseGrant) -> Result<(), SenderError> {
        self.finalized_with_lease.push(lease.lease_id.clone());
        Ok(())
    }

    fn health(&self) -> SenderHealth {
        SenderHealth {
            healthy: true,
            message: "direct transport healthy".to_string(),
        }
    }
}

#[test]
fn test_direct_storage_requires_enterprise_lease_before_any_storage_write() {
    let (lease_url, server) = start_fake_lease_server(403);
    std::env::set_var("CODETRACER_ENTERPRISE_LEASE_TOKEN", "test-token");

    let mut config = TraceStorageConfig::from_json(&fixture("storage_config.full.json")).unwrap();
    config.enterprise_lease.as_mut().unwrap().endpoint_url = lease_url;
    let mut sender = DirectStorageSenderBackend::new(config, TestDirectTransport::default(), HttpEnterpriseLeaseChecker::new()).unwrap();
    let temp = tempfile::NamedTempFile::new().unwrap();
    fs::write(temp.path(), b"trace-bytes").unwrap();
    let object = ManagedUploadObject {
        object_key: "recording-a/slice_0000.ct".to_string(),
        local_path: temp.path().to_string_lossy().to_string(),
        content_length: 11,
        sha256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_string(),
        kind: ManagedUploadKind::McrSlice { slice_index: 0 },
    };

    let error = sender.upload_slice(&object).unwrap_err();
    assert!(!error.retryable);
    assert!(error.message.contains("Enterprise lease checkout rejected"));
    assert!(
        sender.transport().uploaded.is_empty(),
        "storage writes must not happen before a valid lease"
    );
    server.join().unwrap();
}

#[test]
fn test_direct_storage_valid_enterprise_lease_allows_mcr_and_materialized_uploads() {
    let (lease_url, server) = start_fake_lease_server(200);
    std::env::set_var("CODETRACER_ENTERPRISE_LEASE_TOKEN", "test-token");

    let mut config = TraceStorageConfig::from_json(&fixture("storage_config.full.json")).unwrap();
    config.enterprise_lease.as_mut().unwrap().endpoint_url = lease_url;
    let mut sender = DirectStorageSenderBackend::new(config, TestDirectTransport::default(), HttpEnterpriseLeaseChecker::new()).unwrap();

    let mcr = tempfile::NamedTempFile::new().unwrap();
    fs::write(mcr.path(), b"mcr-bytes").unwrap();
    let materialized = tempfile::NamedTempFile::new().unwrap();
    fs::write(materialized.path(), b"materialized-bytes").unwrap();

    sender
        .upload_slice(&ManagedUploadObject {
            object_key: "recording-a/slice_0000.ct".to_string(),
            local_path: mcr.path().to_string_lossy().to_string(),
            content_length: 9,
            sha256: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_string(),
            kind: ManagedUploadKind::McrSlice { slice_index: 0 },
        })
        .unwrap();
    sender
        .upload_materialized_artifact(&ManagedUploadObject {
            object_key: "recording-a/materialized-trace-v1.json".to_string(),
            local_path: materialized.path().to_string_lossy().to_string(),
            content_length: 18,
            sha256: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
            kind: ManagedUploadKind::MaterializedArtifact {
                artifact_kind: "materialized_trace_v1".to_string(),
            },
        })
        .unwrap();

    assert_eq!(sender.transport().uploaded.len(), 2);
    assert_eq!(sender.transport().uploaded[0].0, "store-a");
    assert_eq!(sender.transport().uploaded[1].0, "store-b");
    server.join().unwrap();
}

fn start_fake_lease_server(status: u16) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/api/v1/license/sessions/checkout", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).unwrap();
        let request_text = String::from_utf8_lossy(&request[..read]);
        assert!(request_text.contains("POST /api/v1/license/sessions/checkout"));
        assert!(request_text.contains("Authorization: Bearer test-token"));
        let (status_line, body) = if status == 200 {
            ("HTTP/1.1 200 OK", r#"{"leaseId":"lease-m38","expiresAt":"2026-05-07T12:00:00Z"}"#)
        } else {
            ("HTTP/1.1 403 Forbidden", r#"{"code":"not_enterprise"}"#)
        };
        let response = format!(
            "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    });
    (url, handle)
}

#[test]
fn test_shared_sender_retries_and_finalize_is_idempotent() {
    let mut sender = ManagedTraceSender::new(
        TestManagedBackend {
            fail_next_uploads: 2,
            fail_next_finalize: 1,
            ..TestManagedBackend::default()
        },
        "finalize-m32",
    );

    let slice = ManagedUploadObject {
        object_key: "traces/tenant-a/recording-a/slice_0000.ct".to_string(),
        local_path: "/tmp/slice_0000.ct".to_string(),
        content_length: 128,
        sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        kind: ManagedUploadKind::McrSlice { slice_index: 0 },
    };
    let materialized = ManagedUploadObject {
        object_key: "traces/tenant-a/recording-b/python-materialized-trace-v1.json".to_string(),
        local_path: "/tmp/python/materialized-trace-v1.json".to_string(),
        content_length: 256,
        sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        kind: ManagedUploadKind::MaterializedArtifact {
            artifact_kind: "materialized_trace_v1".to_string(),
        },
    };

    assert!(sender.upload_slice(slice.clone()).unwrap_err().retryable);
    assert_eq!(sender.state().objects[&slice.object_key].upload, UploadState::RetryableFailure);
    assert!(Path::new(&sender.state().objects[&slice.object_key].object.local_path).is_absolute());

    assert!(sender.upload_materialized_artifact(materialized.clone()).unwrap_err().retryable);
    assert_eq!(sender.state().objects[&materialized.object_key].upload, UploadState::RetryableFailure);

    let receipts = sender.retry_pending().unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(sender.state().objects[&slice.object_key].upload, UploadState::Uploaded);
    assert_eq!(sender.state().objects[&materialized.object_key].upload, UploadState::Uploaded);

    let manifest = TraceStorageManifest::from_json(&fixture("manifest.split_ctfs.json")).unwrap();
    let finalize = ManagedFinalizeRequest {
        total_slices: 1,
        total_events: 10,
        manifest,
        idempotency_key: "finalize-m32".to_string(),
    };
    assert!(sender.finalize(finalize.clone()).unwrap_err().retryable);
    sender.finalize(finalize.clone()).unwrap();
    sender.finalize(finalize).unwrap();

    assert!(sender.state().finalize.finalized);
    assert_eq!(sender.backend().uploads.len(), 2);
    assert_eq!(sender.backend().finalized, ["finalize-m32"]);
}

#[test]
fn test_shared_sender_uploads_complete_materialized_artifact_set_before_finalize() {
    let mut sender = ManagedTraceSender::new(TestManagedBackend::default(), "materialized-finalize");
    let objects = vec![
        materialized_upload_object("checkout/materialized-trace-v1.json", "materialized_trace_v1"),
        materialized_upload_object("checkout/correlation-index.json", "correlation_index_v1"),
        materialized_upload_object("checkout/artifact-set.json", "materialized_artifact_set_v1"),
    ];
    let receipts = sender.upload_materialized_artifacts(objects).unwrap();
    assert_eq!(receipts.len(), 3);

    let placed = receipts
        .iter()
        .map(|receipt| PlacedObject {
            object_id: receipt.object_key.clone(),
            uri: format!("{}/{}", receipt.storage_endpoint_uri, receipt.object_key),
            size_bytes: 128,
            sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
            placement: Placement {
                pool: receipt.storage_pool_id.clone(),
                server_id: receipt.storage_server_id.clone(),
            },
            upload: UploadState::Uploaded,
            data_state: DataState::Retained,
        })
        .collect::<Vec<_>>();
    let manifest = TraceStorageManifest {
        schema: TRACE_STORAGE_SCHEMA.to_string(),
        recording_id: "checkout".to_string(),
        service: ServiceIdentity {
            service_name: "checkout".to_string(),
            environment: "test".to_string(),
            instance_id: "checkout-1".to_string(),
            tenant_id: "tenant-a".to_string(),
            organization_id: None,
        },
        source: TraceSource::MaterializedArtifact {
            language: MaterializedLanguage::Python,
            artifact: placed[0].clone(),
            artifacts: placed,
            replay_start: ReplayStart {
                trace_id: "trace".to_string(),
                span_id: "span".to_string(),
                geid: Some(1),
                timestamp_unix_nanos: Some(2),
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
            finalized_at: Some("2026-05-06T00:00:00Z".to_string()),
            idempotency_key: "materialized-finalize".to_string(),
        },
        retention: DataState::Retained,
        replication: ReplicationState {
            target_replicas: 1,
            completed_replicas: 1,
        },
    };

    sender
        .finalize(ManagedFinalizeRequest {
            total_slices: 0,
            total_events: 0,
            manifest,
            idempotency_key: "materialized-finalize".to_string(),
        })
        .unwrap();
    assert_eq!(
        sender.backend().uploads,
        [
            "checkout/materialized-trace-v1.json",
            "checkout/correlation-index.json",
            "checkout/artifact-set.json"
        ]
    );
    assert_eq!(sender.backend().finalized, ["materialized-finalize"]);
}

fn materialized_upload_object(object_key: &str, artifact_kind: &str) -> ManagedUploadObject {
    ManagedUploadObject {
        object_key: object_key.to_string(),
        local_path: format!("/tmp/{object_key}"),
        content_length: 128,
        sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
        kind: ManagedUploadKind::MaterializedArtifact {
            artifact_kind: artifact_kind.to_string(),
        },
    }
}

struct TestLogicalReader {
    fail_first_object_id: Option<String>,
    calls: Vec<(String, u64)>,
}

impl LogicalCtfsBlockReader for TestLogicalReader {
    fn read_block(&mut self, object: &PlacedObject, block_id: u64) -> Result<Vec<u8>, SenderError> {
        self.calls.push((object.object_id.clone(), block_id));
        if self.fail_first_object_id.as_deref() == Some(object.object_id.as_str()) {
            self.fail_first_object_id = None;
            return Err(SenderError::retryable("replica unavailable"));
        }
        Ok(logical_block_bytes(&object.object_id, block_id))
    }
}

fn logical_block_bytes(object_id: &str, block_id: u64) -> Vec<u8> {
    format!("{object_id}:{block_id}").into_bytes()
}

fn sharded_manifest(recording_id: &str, segment_count: u32) -> TraceStorageManifest {
    let segments = (0..segment_count)
        .map(|segment_index| codetracer_ctfs::trace_storage::ShardedCtfsSegment {
            index: segment_index,
            geid_start: 1 + u64::from(segment_index) * 100,
            geid_end: 100 + u64::from(segment_index) * 100,
            shards: vec![
                codetracer_ctfs::trace_storage::CtfsShard {
                    shard_index: 0,
                    block_start: 0,
                    block_end: 63,
                    replicas: vec![
                        placed_shard(recording_id, segment_index, "low", "a"),
                        placed_shard(recording_id, segment_index, "low", "b"),
                    ],
                },
                codetracer_ctfs::trace_storage::CtfsShard {
                    shard_index: 1,
                    block_start: 64,
                    block_end: 127,
                    replicas: vec![
                        placed_shard(recording_id, segment_index, "high", "a"),
                        placed_shard(recording_id, segment_index, "high", "b"),
                    ],
                },
            ],
        })
        .collect();
    TraceStorageManifest {
        schema: TRACE_STORAGE_SCHEMA.to_string(),
        recording_id: recording_id.to_string(),
        service: ServiceIdentity {
            service_name: "checkout-api".to_string(),
            environment: "test".to_string(),
            instance_id: "checkout-1".to_string(),
            tenant_id: "tenant-a".to_string(),
            organization_id: None,
        },
        source: TraceSource::ShardedSplitCtfs { segments },
        lifecycle: LifecycleState::Finalized,
        retry: RetryState {
            attempt: 0,
            next_retry_at: None,
            last_error: None,
        },
        finalize: FinalizeState {
            finalized: true,
            finalized_at: Some("2026-05-06T00:00:00Z".to_string()),
            idempotency_key: format!("finalize-{recording_id}"),
        },
        retention: DataState::Retained,
        replication: ReplicationState {
            target_replicas: 2,
            completed_replicas: 2,
        },
    }
}

fn placed_shard(recording_id: &str, segment_index: u32, range: &str, replica: &str) -> PlacedObject {
    let object_id = format!("{recording_id}-segment-{segment_index}-shard-{range}-{replica}");
    PlacedObject {
        object_id: object_id.clone(),
        uri: format!("ctfs://store-{replica}/{recording_id}/{object_id}.cts"),
        size_bytes: 4096,
        sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        placement: Placement {
            pool: "ctfs-hot".to_string(),
            server_id: format!("store-{replica}"),
        },
        upload: UploadState::Uploaded,
        data_state: DataState::Retained,
    }
}

#[test]
fn test_codetracer_ci_backend_refuses_complete_finalize_without_complete_mcr_slices() {
    let config = CodetracerCiSenderConfig {
        base_url: "http://127.0.0.1:9".to_string(),
        tenant_id: "tenant-a".to_string(),
        bearer_token: "token".to_string(),
        platform: "native".to_string(),
        recording_mode: None,
        service_name: "checkout".to_string(),
        instance_id: Some("ct-mcr".to_string()),
    };
    let mut backend = CodetracerCiSenderBackend::new(config);

    let single_manifest = TraceStorageManifest::from_json(&fixture("manifest.single_ctfs.json")).unwrap();
    let single_request = ManagedFinalizeRequest {
        total_slices: 2,
        total_events: 10,
        manifest: single_manifest,
        idempotency_key: "finalize-m32-single".to_string(),
    };
    let error = backend.finalize(&single_request).unwrap_err();
    assert!(!error.retryable);
    assert!(error.message.contains("exactly one slice"));

    let mut split_manifest = TraceStorageManifest::from_json(&fixture("manifest.split_ctfs.json")).unwrap();
    let TraceSource::SplitCtfs { segments } = &mut split_manifest.source else {
        panic!("fixture should be split_ctfs");
    };
    segments.pop();
    let incomplete_request = ManagedFinalizeRequest {
        total_slices: 2,
        total_events: 200,
        manifest: split_manifest,
        idempotency_key: "finalize-m32-incomplete".to_string(),
    };
    let error = backend.finalize(&incomplete_request).unwrap_err();
    assert!(!error.retryable);
    assert!(error.message.contains("expected 2 slices, got 1"));

    let mut missing_hash_manifest = TraceStorageManifest::from_json(&fixture("manifest.split_ctfs.json")).unwrap();
    let TraceSource::SplitCtfs { segments } = &mut missing_hash_manifest.source else {
        panic!("fixture should be split_ctfs");
    };
    segments[0].file.sha256.clear();
    let missing_hash_request = ManagedFinalizeRequest {
        total_slices: 2,
        total_events: 200,
        manifest: missing_hash_manifest,
        idempotency_key: "finalize-m32-missing-hash".to_string(),
    };
    let error = backend.finalize(&missing_hash_request).unwrap_err();
    assert!(!error.retryable);
    assert!(error.message.contains("missing content hash"));
}

#[test]
fn test_no_recorder_private_sender_or_static_config_parser() {
    test_recorders_use_shared_storage_config_without_private_parsers();

    let workspace = Path::new("..");
    let recorder_roots = [
        "../codetracer-native-recorder",
        "../codetracer-python-recorder",
        "../codetracer-ruby-recorder",
        "../codetracer-js-recorder",
    ];
    let forbidden = [
        "struct ManagedTraceSender",
        "class ManagedTraceSender",
        "type ManagedTraceSender",
        "trait SharedSenderBackend",
        "interface SharedSenderBackend",
        "requestSliceUploadUrl(",
        "finalizeUploadSession(",
    ];
    for root in recorder_roots {
        for entry in walk_files(workspace.join(root)) {
            let Some(ext) = entry.extension().and_then(|value| value.to_str()) else {
                continue;
            };
            if !matches!(ext, "rs" | "nim" | "py" | "rb" | "ts" | "js") {
                continue;
            }
            let text = fs::read_to_string(&entry).unwrap();
            for token in forbidden {
                assert!(
                    !text.contains(token),
                    "{} must not define private managed sender lifecycle/static API parser token {token}",
                    entry.display()
                );
            }
        }
    }

    let shared = fs::read_to_string(workspace.join("codetracer_ctfs/src/trace_storage.rs")).unwrap();
    assert!(shared.contains("trait SharedSenderBackend"));
    assert!(shared.contains("ManagedTraceSender"));
}

fn walk_files(root: impl AsRef<Path>) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.as_ref().to_path_buf()];
    while let Some(path) = stack.pop() {
        if path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| matches!(name, ".git" | "target" | "node_modules" | ".direnv"))
        {
            continue;
        }
        if path.is_dir() {
            for child in fs::read_dir(path).unwrap() {
                stack.push(child.unwrap().path());
            }
        } else {
            files.push(path);
        }
    }
    files
}
