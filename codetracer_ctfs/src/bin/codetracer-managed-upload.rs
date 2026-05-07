use std::env;
use std::fs;
use std::path::Path;
use std::process;

use codetracer_ctfs::trace_storage::{
    CodetracerCiSenderBackend, CodetracerCiSenderConfig, CtfsShard, DataState, DirectStorageTransport, EnterpriseLeaseChecker, FinalizeState,
    HttpDirectStorageTransport, HttpEnterpriseLeaseChecker, LifecycleState, ManagedFinalizeRequest, ManagedTraceSender, ManagedUploadKind,
    ManagedUploadObject, PlacedObject, Placement, ReplicationState, RetryState, ShardedCtfsSegment, StorageMode, StoragePool, StoragePoolPurpose,
    StorageServer, TraceSource, TraceStorageConfig, TraceStorageManifest, UploadState, TRACE_STORAGE_SCHEMA,
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

fn usage() -> String {
    "usage: codetracer-managed-upload <subcommand> [args]\n  upload-materialized --path <file> --object-key <key> --sha256 <hex> [--artifact-kind <kind>]\n  direct-mcr-finalize --storage-config <path> --recording-id <id> --object-key-prefix <prefix> --idempotency-key <key> --slice <path> [--slice <path>]...".to_string()
}
