//! The in-memory CTFS writer must produce the same container as the
//! file-backed one.
//!
//! `CtfsTraceWriter::new_in_memory` exists so a recorder can build a `.ct`
//! where there is no filesystem — a wasm sandbox, most immediately. That is
//! only worth anything if what comes out is a real container rather than a
//! lookalike, so these tests pin the strong property: given the same events
//! and the same pinned `recording_id`, the in-memory bytes are **identical**
//! to the bytes the on-disk writer emits, and the result reads back through
//! the ordinary CTFS reader.
//!
//! No mocks: both writers here are the production writer, and the reader is
//! the production reader operating on a real file.

use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_types::*;
use std::path::{Path, PathBuf};

/// A pinned id so the two writers agree on `meta.json` / `meta.dat`, which
/// otherwise carry a freshly minted UUIDv7 and would differ by construction.
const RECORDING_ID: &str = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb";

/// Drive a small but representative trace: paths, a function, a call with
/// arguments, several steps, variable values, an I/O event and a return — so
/// every one of the split streams (`steps`, `calls`, `values`, `events`) and
/// all four interning tables have real content.
fn write_sample_trace(writer: &mut dyn TraceWriter) {
    // Every call is written `TraceWriter::method(writer, ..)`: `TraceWriter`
    // redeclares each of `AbstractTraceWriter`'s methods, so an inherent-style
    // `writer.method(..)` is ambiguous between the two traits.
    let path = PathBuf::from("/src/example.rs");
    TraceWriter::set_workdir(writer, Path::new("/src"));
    TraceWriter::start(writer, &path, Line(1));

    let function_id = TraceWriter::ensure_function_id(writer, "main", &path, Line(1));
    let int_type = TraceWriter::ensure_type_id(writer, TypeKind::Int, "Int");

    let args = vec![TraceWriter::arg(writer, "n", ValueRecord::Int { i: 7, type_id: int_type })];
    TraceWriter::register_call(writer, function_id, args);

    for line in 1..=40 {
        TraceWriter::register_step(writer, &path, Line(line));
        TraceWriter::register_variable_with_full_value(writer, "acc", ValueRecord::Int { i: line * 2, type_id: int_type });
    }

    TraceWriter::register_special_event(writer, EventLogKind::Write, "", "hello from the sample trace\n");
    TraceWriter::register_return(writer, ValueRecord::Int { i: 80, type_id: int_type });
}

#[test]
fn in_memory_container_is_byte_identical_to_the_file_written_one() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("trace");

    let mut on_disk = CtfsTraceWriter::new("example", &["--flag".to_string()]);
    on_disk.set_recording_id(RECORDING_ID);
    on_disk.begin_writing_trace_events(&base).unwrap();
    write_sample_trace(&mut on_disk);
    on_disk.finish_writing_trace_events().unwrap();
    assert!(on_disk.take_container_bytes().is_none(), "a file-backed writer must not report in-memory bytes");

    let mut in_memory = CtfsTraceWriter::new_in_memory("example", &["--flag".to_string()]);
    in_memory.set_recording_id(RECORDING_ID);
    in_memory.begin_writing_trace_events(Path::new("ignored")).unwrap();
    write_sample_trace(&mut in_memory);
    in_memory.finish_writing_trace_events().unwrap();
    let memory_bytes = in_memory.take_container_bytes().expect("an in-memory writer must yield its container");

    let file_bytes = std::fs::read(base.with_extension("ct")).unwrap();

    assert_eq!(
        memory_bytes.len(),
        file_bytes.len(),
        "in-memory container is {} bytes, on-disk container is {}",
        memory_bytes.len(),
        file_bytes.len()
    );
    assert!(
        memory_bytes == file_bytes,
        "in-memory and on-disk containers diverge at byte {}",
        memory_bytes.iter().zip(&file_bytes).position(|(a, b)| a != b).unwrap_or(0)
    );
    assert!(!memory_bytes.is_empty());
}

#[test]
fn the_in_memory_container_reads_back_through_the_ctfs_reader() {
    let mut writer = CtfsTraceWriter::new_in_memory("example", &[]);
    writer.set_recording_id(RECORDING_ID);
    writer.begin_writing_trace_events(Path::new("ignored")).unwrap();
    write_sample_trace(&mut writer);
    writer.finish_writing_trace_events().unwrap();
    let bytes = writer.take_container_bytes().unwrap();

    // The reader takes a path, so land the in-memory bytes on disk first —
    // exactly what a host does with what a wasm module hands back.
    let dir = tempfile::tempdir().unwrap();
    let ct_path = dir.path().join("from-memory.ct");
    std::fs::write(&ct_path, &bytes).unwrap();

    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct_path).unwrap();

    let steps = events.iter().filter(|e| matches!(e, TraceLowLevelEvent::Step(_))).count();
    assert!(steps >= 40, "expected at least the 40 registered steps, read back {steps}");
    assert!(
        events.iter().any(|e| matches!(e, TraceLowLevelEvent::Event(_))),
        "the I/O event did not survive the round trip"
    );
}

/// `take_container_bytes` moves the bytes out, so a second call is empty —
/// callers that need to keep them should use `container_bytes()`.
#[test]
fn taking_the_container_bytes_consumes_them() {
    let mut writer = CtfsTraceWriter::new_in_memory("example", &[]);
    writer.begin_writing_trace_events(Path::new("ignored")).unwrap();
    write_sample_trace(&mut writer);
    writer.finish_writing_trace_events().unwrap();

    assert!(writer.container_bytes().is_some());
    assert!(writer.take_container_bytes().is_some());
    assert!(writer.take_container_bytes().is_none());
}
