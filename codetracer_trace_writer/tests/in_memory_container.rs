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

use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
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
        TraceWriter::register_variable_with_full_value(
            writer,
            "acc",
            ValueRecord::Int {
                i: line * 2,
                type_id: int_type,
            },
        );
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
    assert!(
        on_disk.take_container_bytes().is_none(),
        "a file-backed writer must not report in-memory bytes"
    );

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

/// A recorder written against `codetracer_trace_writer_nim`'s `TraceWriter`
/// must type-check against this one, and the extra methods must be callable
/// without disturbing the container.
///
/// The column-aware family is accepted and ignored here (this writer has no
/// column-bearing step encoder), so the assertion is that calling it is
/// harmless — not that columns survive. `register_path_with_line_lengths` is
/// the exception: it must still register the path and hand back its id.
#[test]
fn the_nim_writer_trait_surface_is_callable_and_harmless() {
    let mut writer = CtfsTraceWriter::new_in_memory("example", &[]);
    writer.set_recording_id(RECORDING_ID);
    writer.begin_writing_trace_events(Path::new("ignored")).unwrap();
    // Before the first record: `meta.dat` is written by it.
    TraceWriter::set_workdir(&mut writer, Path::new("/src"));

    assert!(!writer.dropped_column_awareness(), "nothing has asked for columns yet");

    {
        let w: &mut dyn TraceWriter = &mut writer;
        w.enable_column_aware_steps();
        w.enable_column_breakpoints_support();
        w.enable_column_motions_support();
        w.write_delta_column(4);

        let path = PathBuf::from("/src/columns.rs");
        let id = w.register_path_with_line_lengths(&path, &[10, 20, 30]).unwrap();
        assert_eq!(id, TraceWriter::ensure_path_id(w, &path), "the returned id must be the path's own id");

        TraceWriter::register_step_with_column(w, &path, Line(1), Some(Line(7)));
    }

    write_sample_trace(&mut writer);
    writer.finish_writing_trace_events().unwrap();
    TraceWriter::close(&mut writer).unwrap();

    let bytes = writer.take_container_bytes().expect("the container must still be produced");
    assert_eq!(&bytes[..5], &[0xC0, 0xDE, 0x72, 0xAC, 0xE2]);

    // The loss of column data must be *detectable*: a container that silently
    // drops columns still reads back and still matches on every step count, so
    // a recorder that depends on column-aware replay has nothing else to check.
    assert!(
        writer.dropped_column_awareness(),
        "asking for column-aware output that cannot be produced must be visible to the caller"
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

/// The sample trace's container from a writer given `threshold`, in memory
/// or through a file.
fn sample_container(in_memory: bool, threshold: u64) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("trace");
    let mut writer = if in_memory {
        CtfsTraceWriter::new_in_memory("example", &[])
    } else {
        CtfsTraceWriter::new("example", &[])
    }
    .with_compact_threshold(threshold);
    writer.set_recording_id(RECORDING_ID);
    writer.begin_writing_trace_events(&base).unwrap();
    write_sample_trace(&mut writer);
    writer.finish_writing_trace_events().unwrap();
    if in_memory {
        writer.take_container_bytes().expect("in-memory bytes")
    } else {
        let bytes = std::fs::read(base.with_extension("ct")).unwrap();
        let left: Vec<_> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(left, ["trace.ct"], "the conversion leaves the container and nothing else");
        bytes
    }
}

/// `with_compact_threshold` replaces the in-memory bytes and the file alike
/// with the compact container when the compact members total fewer than the
/// threshold, and leaves the full container byte for byte as it is when they
/// do not (`ctfs-container.md` §1e).
#[test]
fn the_profile_is_chosen_at_close_in_memory_and_on_disk_alike() {
    use codetracer_ctfs::CtfsReader;
    use codetracer_ctfs::compact::Profile;
    use codetracer_trace_writer::compact_profile::{compact_members_of, raw_member_bytes, select_profile};

    let full = sample_container(true, 0);
    let raw = raw_member_bytes(&compact_members_of(&full).unwrap());
    let (_, compact, _) = select_profile(full.clone(), raw + 1).unwrap();
    let profile = |c: &[u8]| CtfsReader::from_bytes(c.to_vec()).unwrap().profile();
    assert_eq!(profile(&full), Profile::Full);
    assert_eq!(profile(&compact), Profile::Compact);

    for in_memory in [true, false] {
        assert!(
            sample_container(in_memory, raw + 1) == compact,
            "in memory: {in_memory}: compact at R + 1"
        );
        assert!(sample_container(in_memory, raw) == full, "in memory: {in_memory}: full at R");
    }
}
