use std::env;
use std::fs;
use std::process;

use codetracer_ctfs::trace_storage::{
    CodetracerCiSenderBackend, CodetracerCiSenderConfig, ManagedTraceSender, ManagedUploadKind,
    ManagedUploadObject,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let command = args.next().ok_or_else(usage)?;
    if command != "upload-materialized" {
        return Err(usage());
    }

    let mut local_path = String::new();
    let mut object_key = String::new();
    let mut artifact_kind = String::from("materialized_trace_v1");
    let mut sha256 = String::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => local_path = args.next().ok_or_else(usage)?,
            "--object-key" => object_key = args.next().ok_or_else(usage)?,
            "--artifact-kind" => artifact_kind = args.next().ok_or_else(usage)?,
            "--sha256" => sha256 = args.next().ok_or_else(usage)?,
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

fn usage() -> String {
    "usage: codetracer-managed-upload upload-materialized --path <file> --object-key <key> --sha256 <hex> [--artifact-kind <kind>]".to_string()
}
