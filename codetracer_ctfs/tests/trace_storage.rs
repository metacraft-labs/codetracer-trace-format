use std::{fs, path::Path};

use codetracer_ctfs::trace_storage::{
    CodetracerCiSenderBackend, CodetracerCiSenderConfig, DataState, FinalizeState, LifecycleState, ManagedFinalizeRequest, ManagedTraceSender,
    ManagedUploadKind, ManagedUploadObject, ManagedUploadReceipt, MaterializedLanguage, PlacedObject, Placement, ReplayStart, ReplicationState,
    RetryState, SenderError, SenderHealth, ServiceIdentity, SharedSenderBackend, StorageMode, TraceSource, TraceStorageConfig, TraceStorageManifest,
    UploadState, TRACE_STORAGE_SCHEMA,
};
use serde_json::json;

fn fixture(name: &str) -> String {
    fs::read_to_string(Path::new("tests/fixtures/trace_storage").join(name)).unwrap()
}

#[test]
fn test_shared_trace_storage_config_roundtrip() {
    let config = TraceStorageConfig::from_json(&fixture("storage_config.full.json")).unwrap();
    assert_eq!(config.schema, TRACE_STORAGE_SCHEMA);
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
fn test_shared_trace_storage_config_roundtrip_managed_upload() {
    let config = TraceStorageConfig::from_json(&fixture("storage_config.managed_upload.json")).unwrap();
    assert_eq!(config.schema, TRACE_STORAGE_SCHEMA);
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
        total_slices: 1,
        total_events: 10,
        manifest: single_manifest,
        idempotency_key: "finalize-m32-single".to_string(),
    };
    let error = backend.finalize(&single_request).unwrap_err();
    assert!(!error.retryable);
    assert!(error.message.contains("complete MCR slice metadata"));

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
