use std::env;
use std::fs;
use std::path::Path;
use std::process;

use codetracer_ctfs::trace_storage::{
    materialized_artifact_set, parse_materialized_language, CodetracerCiSenderBackend, CodetracerCiSenderConfig, CtfsShard, DataState,
    DirectStorageTransport, EnterpriseLeaseChecker, FinalizeState, HttpDirectStorageTransport, HttpEnterpriseLeaseChecker, LifecycleState,
    ManagedFinalizeRequest, ManagedTraceSender, ManagedUploadKind, ManagedUploadObject, PlacedObject, Placement, ReplayStart, ReplicationState,
    RetryState, ServiceIdentity, ShardedCtfsSegment, StorageMode, StoragePool, StoragePoolPurpose, StorageServer, TraceSource, TraceStorageConfig,
    TraceStorageManifest, UploadState, TRACE_STORAGE_SCHEMA,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut argv: Vec<String> = env::args().skip(1).collect();
    if argv.is_empty() {
        return Err(usage());
    }
    let command = argv.remove(0);
    match command.as_str() {
        "upload-materialized" => run_upload_materialized(argv),
        "direct-mcr-finalize" => run_direct_mcr_finalize(argv),
        "direct-materialized-finalize" => run_direct_materialized_finalize(argv),
        _ => Err(usage()),
    }
}

fn run_upload_materialized(argv: Vec<String>) -> Result<(), String> {
    let mut iter = argv.into_iter();
    let mut local_path = String::new();
    let mut object_key = String::new();
    let mut artifact_kind = String::from("materialized_trace_v1");
    let mut sha256 = String::new();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--path" => local_path = iter.next().ok_or_else(usage)?,
            "--object-key" => object_key = iter.next().ok_or_else(usage)?,
            "--artifact-kind" => artifact_kind = iter.next().ok_or_else(usage)?,
            "--sha256" => sha256 = iter.next().ok_or_else(usage)?,
            _ => return Err(format!("unknown argument: {arg}\n{}", usage())),
        }
    }

    if local_path.is_empty() || object_key.is_empty() || sha256.is_empty() {
        return Err(usage());
    }

    let content_length = fs::metadata(&local_path)
        .map_err(|error| format!("failed to stat {local_path}: {error}"))?
        .len();
    if content_length == 0 {
        return Err(format!("refusing to upload empty materialized artifact: {local_path}"));
    }

    let config = CodetracerCiSenderConfig::from_env().map_err(|error| error.message)?;
    let backend = CodetracerCiSenderBackend::new(config);
    let mut sender = ManagedTraceSender::new(backend, format!("materialized-{object_key}"));
    let receipt = sender
        .upload_materialized_artifact(ManagedUploadObject {
            object_key,
            local_path,
            content_length,
            sha256,
            kind: ManagedUploadKind::MaterializedArtifact { artifact_kind },
        })
        .map_err(|error| error.message)?;

    println!("{}", receipt.object_key);
    Ok(())
}

/// `direct-mcr-finalize`: load a static `TraceStorageConfig`, check out an
/// Enterprise lease, PUT each split MCR slice file to every storage server
/// in the configured ctfs pool (replication target), then submit a
/// metadata-only `direct-storage` finalize to codetracer-ci.
///
/// Invoked by `ct_cli record --storage-config <path>` after the post-record
/// split completes. Exists so the Nim recorder does not duplicate Rust
/// direct-storage logic — it reuses the shared CTFS sender, transport, and
/// lease-checker primitives wholesale.
fn run_direct_mcr_finalize(argv: Vec<String>) -> Result<(), String> {
    let mut iter = argv.into_iter();
    let mut storage_config_path = String::new();
    let mut recording_id = String::new();
    let mut object_key_prefix = String::new();
    let mut idempotency_key = String::new();
    let mut slice_paths: Vec<String> = Vec::new();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--storage-config" => storage_config_path = iter.next().ok_or_else(usage)?,
            "--recording-id" => recording_id = iter.next().ok_or_else(usage)?,
            "--object-key-prefix" => object_key_prefix = iter.next().ok_or_else(usage)?,
            "--idempotency-key" => idempotency_key = iter.next().ok_or_else(usage)?,
            "--slice" => slice_paths.push(iter.next().ok_or_else(usage)?),
            _ => return Err(format!("unknown argument: {arg}\n{}", usage())),
        }
    }

    if storage_config_path.is_empty() {
        return Err("--storage-config is required".to_string());
    }
    if recording_id.is_empty() {
        return Err("--recording-id is required".to_string());
    }
    if object_key_prefix.is_empty() {
        return Err("--object-key-prefix is required".to_string());
    }
    if idempotency_key.is_empty() {
        return Err("--idempotency-key is required".to_string());
    }
    if slice_paths.is_empty() {
        return Err("at least one --slice <path> is required".to_string());
    }

    let config_text =
        fs::read_to_string(&storage_config_path).map_err(|error| format!("failed to read storage config {storage_config_path}: {error}"))?;
    let config = TraceStorageConfig::from_json(&config_text).map_err(|error| format!("failed to parse storage config: {error}"))?;
    if config.schema != TRACE_STORAGE_SCHEMA {
        return Err(format!("unsupported trace-storage schema: {}", config.schema));
    }
    config.validate().map_err(|error| error.message)?;
    if !matches!(config.mode, StorageMode::DirectStorage { .. }) {
        return Err("direct-mcr-finalize requires storage_config.mode=direct_storage".to_string());
    }

    let mut transport = HttpDirectStorageTransport::from_trace_storage_config(&config).map_err(|error| error.message)?;

    // Resolve which storage servers the ctfs-purpose pool maps to. Each slice
    // is replicated to *every* server in that pool: replication=N requires N
    // pool members.
    let ctfs_pool: &StoragePool = config
        .pools
        .iter()
        .find(|pool| pool.purpose == StoragePoolPurpose::Ctfs)
        .ok_or_else(|| "storage config has no ctfs-purpose storage pool".to_string())?;
    let pool_servers: Vec<StorageServer> = ctfs_pool
        .server_ids
        .iter()
        .map(|server_id| {
            config
                .storage_servers
                .iter()
                .find(|server| &server.id == server_id)
                .cloned()
                .ok_or_else(|| format!("ctfs storage pool references unknown server {server_id}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if (pool_servers.len() as u8) < config.replication.target_replicas {
        return Err(format!(
            "ctfs storage pool has {} server(s) but target_replicas is {}",
            pool_servers.len(),
            config.replication.target_replicas
        ));
    }

    let mut lease_checker = HttpEnterpriseLeaseChecker::new();
    let lease_config = config
        .enterprise_lease
        .clone()
        .ok_or_else(|| "direct_storage requires enterprise_lease configuration".to_string())?;
    let lease = lease_checker.checkout(&lease_config).map_err(|error| error.message)?;

    let prefix = object_key_prefix.trim_end_matches('/').to_string();
    let mut segments: Vec<ShardedCtfsSegment> = Vec::with_capacity(slice_paths.len());
    let mut bytes_written: u64 = 0;
    for (slice_index, slice_path) in slice_paths.iter().enumerate() {
        let bytes = fs::read(slice_path).map_err(|error| format!("failed to read slice {slice_path}: {error}"))?;
        if bytes.is_empty() {
            return Err(format!("refusing to upload empty MCR slice: {slice_path}"));
        }
        let sha256 = hex_sha256(&bytes);

        let _file_name = Path::new(slice_path)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("invalid slice path (no file name): {slice_path}"))?;

        let mut replicas: Vec<PlacedObject> = Vec::with_capacity(pool_servers.len());
        for server in &pool_servers {
            let object_key = format!("{prefix}/segment_{slice_index:04}_replica_{server_id}.ct", server_id = server.id);
            let object = ManagedUploadObject {
                object_key: object_key.clone(),
                local_path: slice_path.clone(),
                content_length: bytes.len() as u64,
                sha256: sha256.clone(),
                kind: ManagedUploadKind::McrSlice {
                    slice_index: slice_index as u32,
                },
            };
            transport.upload_direct(server, &object, &bytes).map_err(|error| error.message)?;
            bytes_written = bytes_written.saturating_add(bytes.len() as u64);
            let endpoint = server.endpoint.base_url.trim_end_matches('/').to_string();
            replicas.push(PlacedObject {
                object_id: object_key.clone(),
                uri: format!("{endpoint}/{object_key}"),
                size_bytes: bytes.len() as u64,
                sha256: sha256.clone(),
                placement: Placement {
                    pool: ctfs_pool.id.clone(),
                    server_id: server.id.clone(),
                },
                upload: UploadState::Uploaded,
                data_state: DataState::Retained,
            });
        }

        let block_end = (bytes.len() as u64).saturating_sub(1);
        segments.push(ShardedCtfsSegment {
            index: slice_index as u32,
            geid_start: (slice_index as u64).saturating_add(1),
            geid_end: (slice_index as u64).saturating_add(2),
            shards: vec![CtfsShard {
                shard_index: 0,
                block_start: 0,
                block_end,
                replicas,
            }],
        });
    }

    let manifest = TraceStorageManifest {
        schema: TRACE_STORAGE_SCHEMA.to_string(),
        recording_id: recording_id.clone(),
        service: config.service.clone(),
        source: TraceSource::ShardedSplitCtfs { segments: segments.clone() },
        lifecycle: LifecycleState::Finalized,
        retry: RetryState {
            attempt: 0,
            next_retry_at: None,
            last_error: None,
        },
        finalize: FinalizeState {
            finalized: true,
            finalized_at: None,
            idempotency_key: idempotency_key.clone(),
        },
        retention: DataState::Retained,
        replication: ReplicationState {
            target_replicas: config.replication.target_replicas,
            completed_replicas: config.replication.target_replicas,
        },
    };

    transport
        .report_direct_finalize(
            &ManagedFinalizeRequest {
                total_slices: segments.len() as u32,
                total_events: 1, // recorder does not yet pipe per-slice event counts to the helper
                manifest,
                idempotency_key,
            },
            &lease,
        )
        .map_err(|error| error.message)?;

    println!(
        "direct-mcr-finalize ok recording_id={recording_id} segments={segments} replicas_per_segment={replicas} bytes_written={bytes_written}",
        segments = segments.len(),
        replicas = pool_servers.len()
    );
    Ok(())
}

fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(digest.len() * 2);
    for byte in digest.iter() {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// `direct-materialized-finalize`: load a static `TraceStorageConfig`, check
/// out an Enterprise lease, PUT every materialized artifact (`trace.json` /
/// `trace.ct`, optional correlation index, generated `artifact-set.json`)
/// produced by the live Python/Ruby/JavaScript recorder to the configured
/// materialized-artifact storage server, then submit a metadata-only
/// `direct-storage` finalize to codetracer-ci.
///
/// Symmetric with `direct-mcr-finalize` for the MCR recorder: the live
/// recorder process writes its materialized artifacts to a local trace
/// directory; this helper is then invoked with `--artifact-dir <trace_dir>`
/// and pipes the same bytes to real distributed storage nodes via the
/// shared CTFS sender (`HttpDirectStorageTransport`) and reports a metadata
/// finalize through the shared `report_direct_finalize` endpoint. No
/// recorder ever embeds direct-storage HTTP, lease, or finalize logic — they
/// all converge here so a single static config drives recorder + uploader.
fn run_direct_materialized_finalize(argv: Vec<String>) -> Result<(), String> {
    let mut iter = argv.into_iter();
    let mut storage_config_path = String::new();
    let mut recording_id = String::new();
    let mut object_key_prefix = String::new();
    let mut idempotency_key = String::new();
    let mut artifact_dir = String::new();
    let mut language = String::from("python");

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--storage-config" => storage_config_path = iter.next().ok_or_else(usage)?,
            "--recording-id" => recording_id = iter.next().ok_or_else(usage)?,
            "--object-key-prefix" => object_key_prefix = iter.next().ok_or_else(usage)?,
            "--idempotency-key" => idempotency_key = iter.next().ok_or_else(usage)?,
            "--artifact-dir" => artifact_dir = iter.next().ok_or_else(usage)?,
            "--language" => language = iter.next().ok_or_else(usage)?,
            _ => return Err(format!("unknown argument: {arg}\n{}", usage())),
        }
    }

    if storage_config_path.is_empty() {
        return Err("--storage-config is required".to_string());
    }
    if recording_id.is_empty() {
        return Err("--recording-id is required".to_string());
    }
    if object_key_prefix.is_empty() {
        return Err("--object-key-prefix is required".to_string());
    }
    if idempotency_key.is_empty() {
        return Err("--idempotency-key is required".to_string());
    }
    if artifact_dir.is_empty() {
        return Err("--artifact-dir is required".to_string());
    }

    // Validate the config strictly *before* touching the filesystem so a
    // malformed-config negative test path cannot accidentally leak bytes to
    // a storage node.
    let config_text =
        fs::read_to_string(&storage_config_path).map_err(|error| format!("failed to read storage config {storage_config_path}: {error}"))?;
    let config = TraceStorageConfig::from_json(&config_text).map_err(|error| format!("failed to parse storage config: {error}"))?;
    if config.schema != TRACE_STORAGE_SCHEMA {
        return Err(format!("unsupported trace-storage schema: {}", config.schema));
    }
    config.validate().map_err(|error| error.message)?;
    if !matches!(config.mode, StorageMode::DirectStorage { .. }) {
        return Err("direct-materialized-finalize requires storage_config.mode=direct_storage".to_string());
    }

    let parsed_language = parse_materialized_language(&language).map_err(|error| error.message)?;
    let _ = parsed_language; // language tagging happens later via `parse_materialized_language` again

    let artifact_dir_path = Path::new(&artifact_dir);
    if !artifact_dir_path.is_dir() {
        return Err(format!("--artifact-dir {} is not a directory", artifact_dir));
    }

    // Resolve the storage server that owns the materialized-artifact pool.
    // The static config's `materialized_artifact_policy.pool` names the pool;
    // each direct-materialized upload targets the FIRST server in that pool.
    // Replication for materialized artifacts is left to the storage layer
    // (these JSON blobs are tiny compared to MCR slices), and matches the
    // existing `DirectStorageSenderBackend::upload_materialized_artifact`
    // semantics so on-prem deployments behave identically.
    let materialized_pool: &StoragePool = config
        .pools
        .iter()
        .find(|pool| pool.id == config.materialized_artifact_policy.pool)
        .ok_or_else(|| {
            format!(
                "materialized artifact policy references unknown pool {}",
                config.materialized_artifact_policy.pool
            )
        })?;
    if materialized_pool.purpose != StoragePoolPurpose::MaterializedArtifact {
        return Err(format!("pool {} is not configured for materialized artifacts", materialized_pool.id));
    }
    let server_id = materialized_pool
        .server_ids
        .first()
        .ok_or_else(|| format!("materialized storage pool {} has no servers", materialized_pool.id))?;
    let server: StorageServer = config
        .storage_servers
        .iter()
        .find(|server| &server.id == server_id)
        .cloned()
        .ok_or_else(|| format!("materialized storage pool {} references unknown server {server_id}", materialized_pool.id))?;

    let mut transport = HttpDirectStorageTransport::from_trace_storage_config(&config).map_err(|error| error.message)?;

    // Discover the artifact set the recorder produced. This reuses
    // `materialized_artifact_set` so the live recorder + helper agree on
    // file selection, hashing, and the auto-generated `artifact-set.json`.
    let service_name = config.service.service_name.clone();
    let local_artifacts = materialized_artifact_set(artifact_dir_path, &language, &service_name).map_err(|error| error.message)?;
    if local_artifacts.is_empty() {
        return Err("recorder produced no materialized artifacts to upload".to_string());
    }

    // Acquire the Enterprise lease *after* artifact discovery so a recorder
    // run that produced no usable artifacts does not consume a license slot.
    let mut lease_checker = HttpEnterpriseLeaseChecker::new();
    let lease_config = config
        .enterprise_lease
        .clone()
        .ok_or_else(|| "direct_storage requires enterprise_lease configuration".to_string())?;
    let lease = lease_checker.checkout(&lease_config).map_err(|error| error.message)?;

    let prefix = object_key_prefix.trim_end_matches('/').to_string();
    let mut placed_artifacts: Vec<PlacedObject> = Vec::with_capacity(local_artifacts.len());
    let mut bytes_written: u64 = 0;
    for local in &local_artifacts {
        let bytes =
            fs::read(&local.local_path).map_err(|error| format!("failed to read materialized artifact {}: {error}", local.local_path.display()))?;
        if bytes.is_empty() {
            return Err(format!("refusing to upload empty materialized artifact: {}", local.local_path.display()));
        }
        // Bind the recorder-supplied object suffix under the caller's
        // per-recording prefix; this guarantees the on-disk object key is
        // unique per-tenant/per-session and matches the URL the helper
        // reports to codetracer-ci.
        let suffix = local
            .local_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| format!("invalid materialized artifact path: {}", local.local_path.display()))?;
        let object_key = format!("{prefix}/{suffix}");
        let computed_sha = hex_sha256(&bytes);
        if computed_sha != local.sha256 {
            return Err(format!(
                "sha256 mismatch for {}: discovered={} re-read={}",
                local.local_path.display(),
                local.sha256,
                computed_sha
            ));
        }

        let object = ManagedUploadObject {
            object_key: object_key.clone(),
            local_path: local.local_path.to_string_lossy().to_string(),
            content_length: bytes.len() as u64,
            sha256: computed_sha.clone(),
            kind: ManagedUploadKind::MaterializedArtifact {
                artifact_kind: local.artifact_kind.clone(),
            },
        };
        transport.upload_direct(&server, &object, &bytes).map_err(|error| error.message)?;
        bytes_written = bytes_written.saturating_add(bytes.len() as u64);

        let endpoint = server.endpoint.base_url.trim_end_matches('/').to_string();
        placed_artifacts.push(PlacedObject {
            object_id: object_key.clone(),
            uri: format!("{endpoint}/{object_key}"),
            size_bytes: bytes.len() as u64,
            sha256: computed_sha,
            placement: Placement {
                pool: materialized_pool.id.clone(),
                server_id: server.id.clone(),
            },
            upload: UploadState::Uploaded,
            data_state: DataState::Retained,
        });
    }

    let primary = placed_artifacts
        .first()
        .cloned()
        .ok_or_else(|| "internal error: placed artifact list became empty".to_string())?;

    let manifest = TraceStorageManifest {
        schema: TRACE_STORAGE_SCHEMA.to_string(),
        recording_id: recording_id.clone(),
        service: ServiceIdentity {
            service_name: service_name.clone(),
            environment: config.service.environment.clone(),
            instance_id: config.service.instance_id.clone(),
            tenant_id: config.service.tenant_id.clone(),
            organization_id: config.service.organization_id.clone(),
        },
        source: TraceSource::MaterializedArtifact {
            language: parse_materialized_language(&language).map_err(|error| error.message)?,
            artifact: primary,
            artifacts: placed_artifacts.clone(),
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
            finalized_at: None,
            idempotency_key: idempotency_key.clone(),
        },
        retention: DataState::Retained,
        replication: ReplicationState {
            target_replicas: 1,
            completed_replicas: 1,
        },
    };

    transport
        .report_direct_finalize(
            &ManagedFinalizeRequest {
                total_slices: 0,
                total_events: 1,
                manifest,
                idempotency_key,
            },
            &lease,
        )
        .map_err(|error| error.message)?;

    println!(
        "direct-materialized-finalize ok recording_id={recording_id} artifacts={artifacts} bytes_written={bytes_written}",
        artifacts = placed_artifacts.len()
    );
    Ok(())
}

fn usage() -> String {
    "usage: codetracer-managed-upload <subcommand> [args]\n  upload-materialized --path <file> --object-key <key> --sha256 <hex> [--artifact-kind <kind>]\n  direct-mcr-finalize --storage-config <path> --recording-id <id> --object-key-prefix <prefix> --idempotency-key <key> --slice <path> [--slice <path>]...\n  direct-materialized-finalize --storage-config <path> --recording-id <id> --object-key-prefix <prefix> --idempotency-key <key> --artifact-dir <dir> [--language python|ruby|javascript]".to_string()
}
