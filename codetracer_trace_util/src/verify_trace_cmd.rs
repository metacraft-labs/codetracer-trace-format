//! `verify-trace`: open each `.ct` file through this repository's Rust
//! readers the way a consumer does, and report whether every one of them
//! accepts it.
//!
//! The Nim implementation's `ct-print` is the other half: a committed
//! recording is checked by both, because a check through one reader alone
//! passes whatever that reader tolerates and the other refuses.

use std::path::Path;

use clap::Args;
use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::{
    call_stream_reader::CallStreamReader, interning_tables_reader::InterningTablesReader, io_event_stream_reader::IoEventStreamReader,
    retired_streams::refuse_retired_members, span_stream_reader::SpanStreamReader, step_map_reader::StepMapReader,
    step_stream_reader::StepStreamReader, value_stream_reader::ValueStreamReader,
};
use codetracer_trace_writer::meta_dat::decode_meta_dat;

#[derive(Debug, Clone, Args)]
pub(crate) struct VerifyTraceCommand {
    /// The `.ct` files to verify
    #[arg(required = true)]
    files: Vec<String>,
}

/// Every check, in order; the first refusal is the file's verdict.
fn verify(path: &Path) -> Result<String, String> {
    let mut reader = CtfsReader::open(path).map_err(|e| format!("container: {e}"))?;
    refuse_retired_members(&reader)?;
    let members = reader.list_files();
    for name in &members {
        reader.read_file(name).map_err(|e| format!("member {name:?}: {e}"))?;
    }
    let meta = reader.read_file("meta.dat").map_err(|e| format!("meta.dat: {e}"))?;
    decode_meta_dat(&meta)?;
    let tables = InterningTablesReader::open(&mut reader)?;
    let mut streams = Vec::new();
    if StepStreamReader::open(&mut reader)?.is_some() {
        streams.push("steps");
    }
    if CallStreamReader::open(&mut reader)?.is_some() {
        streams.push("calls");
    }
    if ValueStreamReader::open(&mut reader)?.is_some() {
        streams.push("values");
    }
    if IoEventStreamReader::open(&mut reader)?.is_some() {
        streams.push("events");
    }
    if StepMapReader::open(&mut reader)?.is_some() {
        streams.push("step-map");
    }
    if SpanStreamReader::open(&mut reader)?.is_some() {
        streams.push("spans");
    }
    Ok(format!(
        "{} members, interning tables {}, streams [{}]",
        members.len(),
        if tables.is_some() { "all four" } else { "none" },
        streams.join(" ")
    ))
}

/// Prints `OK   <file>: …` or `FAIL <file>: <refusal>` per file; exits 1 if
/// any file failed.
pub(crate) fn run(cmd: VerifyTraceCommand) {
    let mut failed = false;
    for file in &cmd.files {
        match verify(Path::new(file)) {
            Ok(summary) => println!("OK   {file}: {summary}"),
            Err(refusal) => {
                println!("FAIL {file}: {refusal}");
                failed = true;
            }
        }
    }
    if failed {
        std::process::exit(1);
    }
}
