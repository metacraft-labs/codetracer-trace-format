//! The `Binary` (CBOR + seekable Zstandard) writer reports a write the file
//! system refuses at `finish_writing_trace_events`, instead of panicking in
//! the record call that hit it. Record calls return nothing, so the first
//! failure is held by the writer and the finishing call — the one that
//! returns a `Result` — fails with it.
//!
//! No mocks. The refused write is a real `ulimit -f` file-size limit on a
//! real file, in a child process that is this test binary re-executed. With
//! `SIGXFSZ` ignored the kernel refuses the write with `EFBIG` instead of
//! killing the process.

use std::path::{Path, PathBuf};
use std::process::Command;

use codetracer_trace_types::{EventLogKind, Line, TypeKind, ValueRecord};
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer::{TraceEventsFileFormat, create_trace_writer};

const CHILD_ENV: &str = "CT_BINARY_WRITE_FAILURE_CHILD";

/// Exit code the child uses when finishing reported an error.
const FINISH_FAILED: i32 = 3;

/// Enough poorly-compressible event content that the encoder writes
/// compressed blocks to the file while recording, well past the limit.
fn record(w: &mut dyn TraceWriter) {
    let a = Path::new("/src/a.py");
    TraceWriter::start(w, a, Line(1));
    let int = TraceWriter::ensure_type_id(w, TypeKind::Int, "int");
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..20_000i64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        TraceWriter::register_step(w, a, Line(i % 500 + 1));
        TraceWriter::register_variable_with_full_value(w, "v", ValueRecord::Int { i: x as i64, type_id: int });
        TraceWriter::register_special_event(w, EventLogKind::Write, "stdout", &format!("{x:016x}{:016x}\n", x.rotate_left(29)));
    }
}

/// The child half: record into `$CHILD_ENV`, finish, and report the result
/// through the exit code.
#[test]
fn child_records_and_finishes() {
    let Ok(path) = std::env::var(CHILD_ENV) else {
        return; // Only meaningful when re-executed by the test below.
    };
    let mut w = create_trace_writer("refused", &[], TraceEventsFileFormat::Binary);
    TraceWriter::begin_writing_trace_events(&mut *w, Path::new(&path)).expect("begin");
    record(&mut *w);
    let r = TraceWriter::finish_writing_trace_events(&mut *w);
    eprintln!("FINISH: {r:?}");
    std::process::exit(if r.is_ok() { 0 } else { FINISH_FAILED });
}

fn run_child(path: &Path, pre: &str) -> (std::process::ExitStatus, String) {
    let exe = std::env::current_exe().unwrap();
    let out = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "{pre} exec \"$0\" --exact child_records_and_finishes --nocapture --test-threads=1"
        ))
        .arg(exe)
        .env(CHILD_ENV, path)
        .output()
        .expect("spawn child");
    (out.status, String::from_utf8_lossy(&out.stderr).into_owned())
}

/// Without a limit the same recording finishes, so the failure below is the
/// limit's doing and not the recording's.
#[test]
fn the_recording_finishes_without_a_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("trace.bin");
    let (status, stderr) = run_child(&path, "");
    assert!(status.success(), "{status:?}\n{stderr}");
    assert!(
        std::fs::metadata(&path).unwrap().len() > 64 * 1024,
        "the recording is larger than the limit used below"
    );
}

/// A write refused while recording fails the recording at finish, naming the
/// refused event write and the I/O error — it neither panics in the record
/// call nor succeeds.
#[test]
fn a_write_refused_while_recording_fails_the_finish() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("trace.bin");
    // 16 blocks of 512 bytes (or 1024, depending on the shell): the header
    // and the first compressed blocks fit, the recording does not.
    let (status, stderr) = run_child(&path, "trap '' XFSZ; ulimit -f 16;");
    assert_eq!(
        status.code(),
        Some(FINISH_FAILED),
        "the recording must fail at finish, not succeed or panic: {status:?}\n{stderr}"
    );
    assert!(!stderr.contains("panicked"), "no record call panics: {stderr}");
    assert!(stderr.contains("FINISH: Err"), "{stderr}");
    assert!(
        stderr.contains("writing an event"),
        "the failure is the event write that hit the limit: {stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("file too large") || stderr.contains("EFBIG"),
        "the I/O error is named: {stderr}"
    );
}
