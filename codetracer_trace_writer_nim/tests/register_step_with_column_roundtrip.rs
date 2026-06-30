//! C1 — `register_step_with_column` round-trip test.
//!
//! Verifies the Nim-backed `NimTraceWriter::register_step_with_column`
//! actually carries the column through to the trace (prior to C1 the
//! method silently dropped it, so every non-JS recorder using this
//! wrapper lost column information).
//!
//! Strategy: write a single column-aware Step via
//! `register_step_with_column(path, line=1, column=Some(12))`, close
//! the trace, re-open it via `NimTraceReaderHandle`, and assert the
//! Nim canonical column-aware decoder returns the same `(path, line,
//! column)` triple.  The wrapper still uses the FFI's
//! `register_step` + `write_delta_column` call sequence, but the
//! split-stream writer folds that delta into the pending line step
//! before it is flushed.

use std::path::Path;
use std::sync::Mutex;

use codetracer_trace_types::Line;
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is **not** thread-safe — its global state lives
/// behind a single lock.  Serialize the test binary's writers/readers
/// through this mutex to match the other Nim-backed test suites in
/// this crate (see `tests/column_decode_cross_reader.rs:38`).
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn register_step_with_column_round_trips_column_value() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().expect("tempdir");
    let program = "ctfs_register_step_with_column_roundtrip";
    let events_path = dir.path().join("trace.json");
    let metadata_path = dir.path().join("trace_metadata.json");
    let paths_path = dir.path().join("trace_paths.json");

    let mut writer = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    writer.begin_writing_trace_events(&events_path).expect("begin_events");
    writer.begin_writing_trace_metadata(&metadata_path).expect("begin_metadata");
    writer.begin_writing_trace_paths(&paths_path).expect("begin_paths");

    // The column-aware flag must be flipped BEFORE the first event so
    // the writer's column-aware path is open and the path registration
    // carries per-line lengths.  This matches the spec contract codified
    // in P6.3.
    writer.enable_column_aware_steps();

    // Register a path with line lengths big enough that column 12 is
    // a valid in-line position (line 1 has 32 addressable columns).
    // Without the per-line counts the reader's `step_locations_with_columns`
    // surfaces column 0 / None because GLI resolution can't bound-check.
    let source_path = Path::new("/tmp/ctfs_register_step_with_column_roundtrip.py");
    writer
        .register_path_with_line_lengths(source_path, &[32])
        .expect("register_path_with_line_lengths");

    // The actual exercise: emit a single column-aware step at (line 1,
    // column 12).  Pre-C1 this silently dropped the column and the
    // reader would surface column 1; now the writer annotates the
    // pending line step with a column delta so the single flushed step
    // lands on column 12.
    writer.register_step_with_column(source_path, Line(1), Some(Line(12)));

    writer.finish_writing_trace_events().expect("finish_events");
    writer.finish_writing_trace_metadata().expect("finish_metadata");
    writer.finish_writing_trace_paths().expect("finish_paths");
    writer.close().expect("close");
    drop(writer);

    let ct_path = dir.path().join(format!("{program}.ct"));
    // Persist the trace beyond the tempdir guard — the reader needs
    // the file to exist for the duration of the assertions.
    #[allow(deprecated)]
    let _dir_path = dir.into_path();
    assert!(ct_path.exists(), ".ct trace file was not created at {}", ct_path.display());

    let reader = NimTraceReaderHandle::open(ct_path.to_str().unwrap()).expect("reader open");
    assert!(
        reader.has_column_aware_steps(),
        "writer.enable_column_aware_steps() should set FlagHasColumnAwareSteps in meta.dat"
    );

    // The FFI folds the column delta into the pending line step before
    // flushing it, so the split execution stream contains exactly one
    // step at the column-bearing position the caller requested.
    let step_count = reader.step_count();
    assert_eq!(
        step_count, 1,
        "register_step_with_column on a column-aware writer must flush one folded exec-stream entry"
    );

    let mut path_ids = [0u64; 1];
    let mut lines = [0u64; 1];
    let mut columns = [0u64; 1];
    let written = reader
        .step_locations_with_columns(0, 1, &mut path_ids, &mut lines, &mut columns)
        .expect("step_locations_with_columns");
    assert_eq!(written, 1, "expected one decoded folded-step location");

    assert_eq!(path_ids[0], 0, "step 0 should reference the only registered path");
    assert_eq!(lines[0], 1, "step 0 should be on line 1");
    assert_eq!(
        columns[0], 12,
        "column round-trip: register_step_with_column(..., Some(12)) MUST land at column 12 \
         (pre-C1 the wrapper dropped the column silently)"
    );
}

/// Back-compat: when the writer has NOT opted into column-aware mode,
/// `register_step_with_column` MUST behave like `register_step` and
/// emit a column-less Step.  This guards against regressions where a
/// future refactor might attempt to call `write_delta_column` on a
/// non-column-aware writer (which sets the thread-local error string
/// and silently corrupts the next `last_error()` query).
#[test]
fn register_step_with_column_falls_back_when_not_column_aware() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().expect("tempdir");
    let program = "ctfs_register_step_with_column_legacy";
    let events_path = dir.path().join("trace.json");
    let metadata_path = dir.path().join("trace_metadata.json");
    let paths_path = dir.path().join("trace_paths.json");

    let mut writer = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    writer.begin_writing_trace_events(&events_path).expect("begin_events");
    writer.begin_writing_trace_metadata(&metadata_path).expect("begin_metadata");
    writer.begin_writing_trace_paths(&paths_path).expect("begin_paths");

    // Deliberately do NOT call enable_column_aware_steps; the trace
    // must still encode the step (no panic, no spurious last_error).
    let source_path = Path::new("/tmp/ctfs_register_step_with_column_legacy.py");
    writer.register_step_with_column(source_path, Line(5), Some(Line(9)));

    writer.finish_writing_trace_events().expect("finish_events");
    writer.finish_writing_trace_metadata().expect("finish_metadata");
    writer.finish_writing_trace_paths().expect("finish_paths");
    writer.close().expect("close");
    drop(writer);

    let ct_path = dir.path().join(format!("{program}.ct"));
    #[allow(deprecated)]
    let _dir_path = dir.into_path();
    assert!(ct_path.exists(), ".ct trace file was not created at {}", ct_path.display());

    let reader = NimTraceReaderHandle::open(ct_path.to_str().unwrap()).expect("reader open");
    assert!(
        !reader.has_column_aware_steps(),
        "without enable_column_aware_steps(), the trace must NOT carry the column-aware flag"
    );
    assert_eq!(reader.step_count(), 1, "the legacy fallback still emits exactly one Step event");
}
