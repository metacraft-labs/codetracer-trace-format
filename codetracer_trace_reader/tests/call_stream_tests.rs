//! Round-trip tests for the dedicated `calls.dat` call stream: the records are
//! read back through the seekable `CallStreamReader` and addressed by call_key,
//! including across a chunk boundary.
//!
//! The combined `events.log` stream is not part of the trace format spec and is
//! no longer written, so the tests that cross-checked `calls.dat` against an
//! `events.log`-derived call tree, and the one asserting a legacy (flag-off)
//! bundle exposes no call stream, were removed: they exercised a writer mode
//! that no longer exists.

use std::path::Path;

use codetracer_trace_types::*;
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
fn write_trace(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let path_buf = dir.path().join("trace");
    let mut writer = CtfsTraceWriter::new("test_program", &[]).with_call_stream(true);
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

#[test]
fn seek_to_call_by_key() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir);

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
