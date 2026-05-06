use std::{fs, path::Path};

use codetracer_ctfs::trace_storage::{
    DataState, FinalizeState, LifecycleState, MaterializedLanguage, StorageMode, TraceSource, TraceStorageConfig, TraceStorageManifest, UploadState,
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
