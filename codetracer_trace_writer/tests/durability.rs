//! A writer that writes its container to a file keeps it readable while it
//! records (`ctfs-container.md` §6, "Durability: a writer publishes every
//! sealed chunk"): a recording whose process dies leaves a container a reader
//! opens and reads up to the last chunk each stream sealed. `meta.dat` is
//! written once, complete, by the first record, and a call that would change
//! it afterwards fails (`internal-files.md` §"Extended flags"). A write the
//! file system refuses is reported by the writer, not dropped.
//!
//! No mocks. The killed recording is a real child process — this test binary
//! re-executed — that aborts mid-recording; the refused write is a real
//! `ulimit -f` file-size limit on a real file.

use std::path::{Path, PathBuf};
use std::process::Command;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;
use codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader;
use codetracer_trace_reader::step_stream_reader::StepStreamReader;
use codetracer_trace_reader::value_stream_reader::ValueStreamReader;
use codetracer_trace_types::{EventLogKind, Line, TypeKind, ValueRecord};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::meta_dat::{
    FLAG_HAS_CALL_STREAM, FLAG_HAS_INTERNING_TABLES, FLAG_HAS_IO_EVENT_STREAM, FLAG_HAS_STEP_STREAM, FLAG_HAS_VALUE_STREAM, decode_meta_dat,
};
use codetracer_trace_writer::trace_writer::TraceWriter;

const CHILD_ENV: &str = "CT_DURABILITY_CHILD";
const STEPS: i64 = 22;
const WORKDIR: &str = "/work/durable";

/// A writer with small chunks, so a short recording seals several of each.
fn writer() -> CtfsTraceWriter {
    CtfsTraceWriter::new("durable", &["--x".to_string()])
        .with_steps_chunk_size(4)
        .with_values_chunk_size(4)
        .with_events_chunk_size(2)
        .with_calls_chunk_size(4)
}

/// `STEPS` steps over two files inside one call, each with a value and an
/// I/O event. Exec records 0..22: steps.dat seals 5 chunks (20 records);
/// values.dat's last record can still gain values until the next step, so it
/// seals 5 chunks (20 records); events.dat seals all 11 of its chunks;
/// calls.dat seals none, because its call is still open.
fn record(w: &mut CtfsTraceWriter) {
    let (a, b) = (Path::new("/src/a.py"), Path::new("/src/b.py"));
    let int = AbstractTraceWriter::ensure_type_id(w, TypeKind::Int, "int");
    let f = AbstractTraceWriter::ensure_function_id(w, "main", a, Line(1));
    AbstractTraceWriter::register_call(w, f, vec![]);
    for i in 0..STEPS {
        AbstractTraceWriter::register_step(w, if i % 2 == 0 { a } else { b }, Line(i + 1));
        AbstractTraceWriter::register_variable_with_full_value(w, &format!("v{}", i % 3), ValueRecord::Int { i, type_id: int });
        AbstractTraceWriter::register_special_event(w, EventLogKind::Write, "stdout", &format!("line {i}\n"));
    }
}

/// The child half: record, then die without finishing.
#[test]
fn child_records_then_aborts() {
    let Ok(base) = std::env::var(CHILD_ENV) else {
        return; // Only meaningful when re-executed by the tests below.
    };
    let mut w = writer();
    AbstractTraceWriter::set_workdir(&mut w, Path::new(WORKDIR));
    TraceWriter::begin_writing_trace_events(&mut w, Path::new(&base)).expect("begin");
    record(&mut w);
    if std::env::var("CT_DURABILITY_MODE").as_deref() == Ok("finish") {
        let r = TraceWriter::finish_writing_trace_events(&mut w);
        eprintln!("FINISH: {r:?}");
        std::process::exit(if r.is_ok() { 0 } else { 3 });
    }
    std::process::abort();
}

/// Re-execute this test binary as the child, with `pre` run by the shell
/// first. Returns the child's status and stderr.
fn run_child(base: &Path, pre: &str, mode: &str) -> (std::process::ExitStatus, String) {
    let exe = std::env::current_exe().unwrap();
    let out = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "{pre} exec \"$0\" --exact child_records_then_aborts --nocapture --test-threads=1"
        ))
        .arg(exe)
        .env(CHILD_ENV, base)
        .env("CT_DURABILITY_MODE", mode)
        .output()
        .expect("spawn child");
    (out.status, String::from_utf8_lossy(&out.stderr).into_owned())
}

fn check_crashed_container(ct: &Path) {
    let mut r = CtfsReader::open(ct).expect("the crashed container opens");

    let meta = decode_meta_dat(&r.read_file("meta.dat").expect("meta.dat was written by the first record")).unwrap();
    assert_eq!(meta.workdir, WORKDIR);
    assert_eq!(meta.args, vec!["--x".to_string()]);

    let mut steps = StepStreamReader::open(&mut r).unwrap().expect("steps.dat");
    assert_eq!(steps.count(), 20, "every sealed steps.dat chunk is readable");
    assert_eq!(steps.read_all().unwrap().len(), 20);

    let mut values = ValueStreamReader::open(&mut r).unwrap().expect("values.dat");
    assert_eq!(values.count(), 20, "every sealed values.dat chunk is readable");
    values.read_all().unwrap();

    let mut io = IoEventStreamReader::open(&mut r).unwrap().expect("events.dat");
    assert_eq!(io.count(), STEPS as u64, "every sealed events.dat chunk is readable");
    let last = io.read(STEPS as u64 - 1).unwrap();
    assert_eq!(last.content, format!("line {}\n", STEPS - 1).into_bytes());

    // What the sealed chunks refer to was published with them.
    let tables = InterningTablesReader::open(&mut r).unwrap().expect("interning tables");
    assert_eq!(tables.path_count(), 2);
    assert_eq!(tables.path_str(1).unwrap(), "/src/b.py");
    assert_eq!(tables.type_count(), 1);
    assert_eq!(tables.varname_count(), 3);

    assert_eq!(r.file_size("step-map.ns"), None, "the close-time index is not there");
}

#[test]
fn a_killed_recording_reads_up_to_its_last_sealed_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("killed");
    let (status, stderr) = run_child(&base, "", "abort");
    assert!(!status.success(), "the child must have died: {status:?}\n{stderr}");
    assert!(stderr.contains("1 passed") || !stderr.contains("test result"), "{stderr}");
    check_crashed_container(&base.with_extension("ct"));
}

#[test]
fn a_writer_dropped_without_finishing_reads_up_to_its_last_sealed_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("dropped");
    {
        let mut w = writer();
        AbstractTraceWriter::set_workdir(&mut w, Path::new(WORKDIR));
        TraceWriter::begin_writing_trace_events(&mut w, &base).unwrap();
        record(&mut w);
    }
    check_crashed_container(&base.with_extension("ct"));
}

#[test]
fn a_finished_recording_has_everything() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("whole");
    let mut w = writer();
    AbstractTraceWriter::set_workdir(&mut w, Path::new(WORKDIR));
    TraceWriter::begin_writing_trace_events(&mut w, &base).unwrap();
    record(&mut w);
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
    let mut r = CtfsReader::open(&base.with_extension("ct")).unwrap();
    assert_eq!(StepStreamReader::open(&mut r).unwrap().unwrap().count(), STEPS as u64);
    assert_eq!(ValueStreamReader::open(&mut r).unwrap().unwrap().count(), STEPS as u64);
    assert!(r.file_size("step-map.ns").is_some());
    let meta = decode_meta_dat(&r.read_file("meta.dat").unwrap()).unwrap();
    let streams = FLAG_HAS_CALL_STREAM | FLAG_HAS_STEP_STREAM | FLAG_HAS_VALUE_STREAM | FLAG_HAS_IO_EVENT_STREAM | FLAG_HAS_INTERNING_TABLES;
    assert_eq!(meta.flags & 0xff00, streams, "bits 8-12, and no lazily created member's bit");
}

/// `meta.dat` is committed by the first record; a call that would change it
/// afterwards is refused, and the recording fails rather than carrying a
/// `meta.dat` that disagrees with what the recorder said.
#[test]
fn meta_dat_is_fixed_by_the_first_record() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("committed");
    let mut w = writer();
    TraceWriter::begin_writing_trace_events(&mut w, &base).unwrap();
    // Before the first record, declarations and the workdir may still change.
    AbstractTraceWriter::set_workdir(&mut w, Path::new(WORKDIR));
    w.declare_source_reload().expect("declared before the first record");
    AbstractTraceWriter::register_step(&mut w, Path::new("/src/a.py"), Line(1));
    assert!(w.declare_source_reload().is_err(), "after the first record the declaration is refused");
    assert!(w.enable_line_count_table().is_err(), "after the first record a capability is refused");
    AbstractTraceWriter::set_workdir(&mut w, Path::new("/elsewhere"));
    let err = TraceWriter::finish_writing_trace_events(&mut w).expect_err("a refused set_workdir fails the recording");
    assert!(err.to_string().contains("workdir"), "{err}");
    let mut r = CtfsReader::open(&base.with_extension("ct")).unwrap();
    let meta = decode_meta_dat(&r.read_file("meta.dat").unwrap()).unwrap();
    assert_eq!(meta.workdir, WORKDIR, "meta.dat keeps what was committed");
    assert_eq!(meta.ext_flags, codetracer_trace_writer::meta_dat::FLAG_EXT_HAS_SOURCE_RELOAD);
}

/// A write the file system refuses — here, past a `ulimit -f` file-size
/// limit — fails the recording, naming the failure. With `SIGXFSZ` ignored,
/// the kernel refuses the write with `EFBIG` instead of killing the process.
#[test]
fn a_refused_write_fails_the_recording() {
    let dir = tempfile::tempdir().unwrap();
    let base: PathBuf = dir.path().join("full");
    // 24 blocks of 512 bytes: block 0 and a few members fit, the recording
    // does not.
    let (status, stderr) = run_child(&base, "trap '' XFSZ; ulimit -f 24;", "finish");
    assert_eq!(
        status.code(),
        Some(3),
        "the recording must fail, not succeed or crash: {status:?}\n{stderr}"
    );
    assert!(stderr.contains("FINISH: Err"), "{stderr}");
    assert!(
        stderr.to_lowercase().contains("file too large") || stderr.contains("EFBIG"),
        "the I/O error is named: {stderr}"
    );
}
