//! M17a round-trip tests for the dedicated `calls.dat` call stream.
//!
//! A trace is written with the `has_call_stream` flag on; the call records are
//! then read back two ways and compared:
//!   1. directly from `calls.dat` via the seekable `CallStreamReader`, and
//!   2. re-derived from the unchanged `events.log` (read with the normal reader)
//!      by replaying the same `CallStreamBuilder`.
//! The two MUST agree — proving `calls.dat` is consistent with the unified
//! stream it was split from. A flag-off (legacy) trace is also exercised to
//! confirm the call stream is absent and old readers are unaffected.

use std::path::Path;

use codetracer_trace_types::*;
use codetracer_trace_writer::call_stream::CallStreamBuilder;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;

/// Write a small trace exercising nested calls. `start()` emits an implicit
/// `<toplevel>` call (function_id 0) which is the true root; the user calls
/// nest beneath it. Layout (call records, by call_key):
///   <toplevel>()                 -> call_key 0 (root, depth 0)
///     main()                     -> call_key 1 (child of 0, depth 1)
///       used_a() returns 1       -> call_key 2 (child of 1, depth 2)
///       used_b() calls leaf()    -> call_key 3 (child of 1, depth 2)
///         leaf()  returns        -> call_key 4 (child of 3, depth 3)
///   (unused_c is defined but never called)
fn write_trace(dir: &tempfile::TempDir, with_call_stream: bool) -> std::path::PathBuf {
    let path_buf = dir.path().join("trace");
    let mut writer = CtfsTraceWriter::new("test_program", &[]).with_call_stream(with_call_stream);
    // small chunk size so seeking crosses a chunk boundary in the test
    writer = writer.with_calls_chunk_size(2);
    TraceWriter::begin_writing_trace_events(&mut writer, &path_buf).unwrap();

    let src = Path::new("/test/prog.rs");
    TraceWriter::start(&mut writer, src, Line(1));

    let int_type = TraceWriter::ensure_type_id(&mut writer, TypeKind::Int, "Int");
    let main_fn = TraceWriter::ensure_function_id(&mut writer, "main", src, Line(1));
    let used_a = TraceWriter::ensure_function_id(&mut writer, "used_a", src, Line(10));
    let used_b = TraceWriter::ensure_function_id(&mut writer, "used_b", src, Line(20));
    let leaf = TraceWriter::ensure_function_id(&mut writer, "leaf", src, Line(30));
    // unused_c is interned (defined) but never called.
    let _unused_c = TraceWriter::ensure_function_id(&mut writer, "unused_c", src, Line(40));

    // main()
    TraceWriter::register_call(&mut writer, main_fn, vec![]);
    TraceWriter::register_step(&mut writer, src, Line(2));

    // used_a() -> 1
    let arg_a = TraceWriter::arg(&mut writer, "x", ValueRecord::Int { i: 5, type_id: int_type });
    TraceWriter::register_call(&mut writer, used_a, vec![arg_a]);
    TraceWriter::register_step(&mut writer, src, Line(11));
    TraceWriter::register_return(&mut writer, ValueRecord::Int { i: 1, type_id: int_type });

    // used_b() -> calls leaf()
    TraceWriter::register_call(&mut writer, used_b, vec![]);
    TraceWriter::register_step(&mut writer, src, Line(21));
    TraceWriter::register_call(&mut writer, leaf, vec![]);
    TraceWriter::register_step(&mut writer, src, Line(31));
    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::register_return(&mut writer, ValueRecord::Int { i: 2, type_id: int_type });

    // main returns
    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });

    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
    path_buf.with_extension("ct")
}

/// Re-derive the expected call records straight from `events.log` (read with
/// the unchanged unified-stream reader) by replaying the same builder.
fn expected_from_events(ct_path: &Path) -> Vec<codetracer_trace_writer::call_stream::CallStreamRecord> {
    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(ct_path).unwrap();
    let mut builder = CallStreamBuilder::new();
    for ev in &events {
        builder.observe(ev);
    }
    builder.finish()
}

#[test]
fn calls_dat_matches_events_log() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true);

    // Read the dedicated stream.
    let mut cs = codetracer_trace_reader::call_stream_reader::open_call_stream(&ct_path)
        .expect("open_call_stream ok")
        .expect("call stream present when has_call_stream flag is set");
    let from_dat = cs.read_all().unwrap();

    // Re-derive from the unchanged events.log.
    let from_events = expected_from_events(&ct_path);

    assert_eq!(from_dat, from_events, "calls.dat records must equal the events.log-derived records");

    // Sanity on the tree shape: <toplevel> root + 4 user calls = 5 records.
    assert_eq!(from_dat.len(), 5);
    assert_eq!(from_dat[0].function_id, 0); // <toplevel>
    assert_eq!(from_dat[0].parent_key, -1);
    assert_eq!(from_dat[0].depth, 0);
    assert_eq!(from_dat[0].children, vec![1]);

    assert_eq!(from_dat[1].function_id, 1); // main
    assert_eq!(from_dat[1].parent_key, 0);
    assert_eq!(from_dat[1].depth, 1);
    assert_eq!(from_dat[1].children, vec![2, 3]);

    assert_eq!(from_dat[2].function_id, 2); // used_a
    assert_eq!(from_dat[2].parent_key, 1);
    assert_eq!(from_dat[2].depth, 2);
    assert!(from_dat[2].children.is_empty());
    assert!(!from_dat[2].args.is_empty(), "used_a was called with an arg");

    assert_eq!(from_dat[3].function_id, 3); // used_b
    assert_eq!(from_dat[3].parent_key, 1);
    assert_eq!(from_dat[3].depth, 2);
    assert_eq!(from_dat[3].children, vec![4]);

    assert_eq!(from_dat[4].function_id, 4); // leaf
    assert_eq!(from_dat[4].parent_key, 3);
    assert_eq!(from_dat[4].depth, 3);
}

#[test]
fn seek_to_call_by_key() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true);

    let mut cs = codetracer_trace_reader::call_stream_reader::open_call_stream(&ct_path).unwrap().unwrap();
    assert_eq!(cs.count(), 5);

    // chunk_size is 2, so call_key 4 lives in chunk 2 — seeking must decompress
    // only that chunk and still return the right record.
    let rec4 = cs.read(4).unwrap();
    assert_eq!(rec4.call_key, 4);
    assert_eq!(rec4.function_id, 4); // leaf
    assert_eq!(rec4.parent_key, 3);

    // A random earlier key also resolves (crosses back into chunk 0).
    let rec0 = cs.read(0).unwrap();
    assert_eq!(rec0.function_id, 0); // <toplevel>
    assert_eq!(rec0.children, vec![1]);

    // Out-of-range key errors, never panics.
    assert!(cs.read(99).is_err());
}

#[test]
fn legacy_trace_has_no_call_stream() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, false);

    // No dedicated call stream when the flag is off.
    let cs = codetracer_trace_reader::call_stream_reader::open_call_stream(&ct_path).unwrap();
    assert!(cs.is_none(), "a flag-off trace must not expose a call stream");

    // ...and the unified events.log still reads exactly as before, with the
    // same call tree derivable from it.
    let from_events = expected_from_events(&ct_path);
    assert_eq!(from_events.len(), 5);
    assert_eq!(from_events[0].function_id, 0); // <toplevel> root
}
