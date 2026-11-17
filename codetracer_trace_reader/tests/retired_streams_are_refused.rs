//! `events.log` and `events.fmt` are not part of the trace format: a container
//! that carries either is refused, by every entry point of this reader, with an
//! error that names the member.
//!
//! Each container here is a real recording written by the shipped writer, with
//! the retired member added beside its streams, so the refusal cannot be
//! explained by anything else the container lacks. A reader that ignored the
//! member would read the recording successfully, and each test fails then.

use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter};
use codetracer_trace_reader::call_stream_reader::CallStreamReader;
use codetracer_trace_reader::ctfs_reader::{read_trace_from_ctfs, seek_events_in_ctfs};
use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;
use codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader;
use codetracer_trace_reader::split_stream_reader::{read_trace_with_details, read_window};
use codetracer_trace_reader::step_map_reader::StepMapReader;
use codetracer_trace_reader::step_stream_reader::StepStreamReader;
use codetracer_trace_reader::value_stream_reader::ValueStreamReader;
use codetracer_trace_types::{Line, TypeKind, ValueRecord};
use codetracer_trace_writer::ctfs_writer::{CtfsOutput, CtfsTraceWriter};
use codetracer_trace_writer::trace_writer::TraceWriter;

/// A small real recording, as container bytes.
fn recording() -> Vec<u8> {
    let mut w = CtfsTraceWriter::new("retired", &[]).with_output(CtfsOutput::Memory);
    TraceWriter::begin_writing_trace_events(&mut w, std::path::Path::new("retired")).unwrap();
    let src = std::path::PathBuf::from("/src/r.py");
    TraceWriter::start(&mut w, &src, Line(1));
    let int = TraceWriter::ensure_type_id(&mut w, TypeKind::Int, "Int");
    TraceWriter::register_step(&mut w, &src, Line(2));
    TraceWriter::register_variable_with_full_value(&mut w, "x", ValueRecord::Int { i: 7, type_id: int });
    TraceWriter::register_step(&mut w, &src, Line(3));
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
    w.take_container_bytes().unwrap()
}

/// `bytes` with one more member, `name`, holding `content`.
fn with_member(bytes: Vec<u8>, name: &str, content: &[u8]) -> Vec<u8> {
    let mut reader = CtfsReader::from_bytes(bytes).unwrap();
    let mut out = CtfsWriter::create_in_memory(4096, 31, CompressionMethod::None).unwrap();
    for (member, data) in reader.members().unwrap() {
        let h = out.add_file(&member).unwrap();
        out.write(h, &data).unwrap();
    }
    let h = out.add_file(name).unwrap();
    out.write(h, content).unwrap();
    out.finish_to_bytes().unwrap()
}

fn assert_names(result: Result<(), String>, member: &str, entry: &str) {
    match result {
        Ok(()) => panic!("{entry} read a container carrying `{member}`"),
        Err(e) => assert!(e.contains(member), "{entry} refused, but without naming `{member}`: {e}"),
    }
}

/// Every entry point, against one container.
fn refused_everywhere(bytes: &[u8], member: &str) {
    let open = || CtfsReader::from_bytes(bytes.to_vec()).unwrap();
    assert_names(StepStreamReader::open(&mut open()).map(|_| ()), member, "StepStreamReader::open");
    assert_names(ValueStreamReader::open(&mut open()).map(|_| ()), member, "ValueStreamReader::open");
    assert_names(CallStreamReader::open(&mut open()).map(|_| ()), member, "CallStreamReader::open");
    assert_names(IoEventStreamReader::open(&mut open()).map(|_| ()), member, "IoEventStreamReader::open");
    assert_names(
        InterningTablesReader::open(&mut open()).map(|_| ()),
        member,
        "InterningTablesReader::open",
    );
    assert_names(StepMapReader::open(&mut open()).map(|_| ()), member, "StepMapReader::open");
    assert_names(read_trace_with_details(&mut open()).map(|_| ()), member, "read_trace_with_details");
    assert_names(read_window(&mut open(), 0, 1).map(|_| ()), member, "read_window");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("retired.ct");
    std::fs::write(&path, bytes).unwrap();
    assert_names(
        read_trace_from_ctfs(&path).map(|_| ()).map_err(|e| e.to_string()),
        member,
        "read_trace_from_ctfs",
    );
    assert_names(
        seek_events_in_ctfs(&path, 0, 1).map(|_| ()).map_err(|e| e.to_string()),
        member,
        "seek_events_in_ctfs",
    );
}

#[test]
fn the_recording_itself_reads() {
    // The control: without the retired member the same recording is read, so
    // a refusal below is the member's doing.
    let mut reader = CtfsReader::from_bytes(recording()).unwrap();
    let trace = read_trace_with_details(&mut reader).unwrap();
    assert!(!trace.events.is_empty());
}

#[test]
fn a_container_carrying_events_log_is_refused_by_name() {
    refused_everywhere(&with_member(recording(), "events.log", b"\x00"), "events.log");
}

#[test]
fn a_container_carrying_events_fmt_is_refused_by_name() {
    refused_everywhere(&with_member(recording(), "events.fmt", b"split-binary"), "events.fmt");
}

#[test]
fn an_empty_events_log_is_refused_too() {
    // The presence of the entry is what is refused, not its content.
    refused_everywhere(&with_member(recording(), "events.log", b""), "events.log");
}
