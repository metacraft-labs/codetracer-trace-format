//! A failure inside the Nim writer reaches the Rust caller as an `Err`.
//!
//! Most `NimTraceWriter` calls mirror `void` C entry points, so a failure in
//! one has no return value to travel through. The Nim side latches it on the
//! handle and `trace_writer_close` fails with it; the Rust side's own
//! un-returnable failures (a value the streaming encoder could not encode)
//! are held the same way and returned by [`NimTraceWriter::close`]. Before
//! either existed, these recordings closed `Ok` while missing events — the
//! BEAM recorder's empty traces.
//!
//! The failures forced here are real ones the writer refuses today, not
//! injected faults (the C-level tests in `codetracer-trace-format-nim`
//! cover every entry-point class with injection):
//!
//! * a step on a path with no recorded line count under the line-count table;
//! * a step past its file's recorded line count;
//! * an encoder call that is invalid in its state.
//!
//! Each is paired with its control — the same writer without the failure
//! closes `Ok` — so a close that always failed could not pass.
//!
//! No mocks: the real Nim writer through its C ABI.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_trace_types::Line;
use codetracer_trace_writer_nim::{NimTraceWriter, StreamingValueEncoder, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Record with the line-count table on, `/a.ex` sized 10; `drive` adds steps.
fn record(drive: impl FnOnce(&mut NimTraceWriter)) -> Result<(), String> {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut w = NimTraceWriter::new("failures", &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.path().join("e.json")).expect("begin_events");
    w.begin_writing_trace_metadata(&dir.path().join("m.json")).expect("begin_metadata");
    w.begin_writing_trace_paths(&dir.path().join("p.json")).expect("begin_paths");
    w.enable_line_count_table().expect("table");
    w.register_path_with_line_count(Path::new("/a.ex"), 10).expect("path");
    w.register_step(Path::new("/a.ex"), Line(1));
    drive(&mut w);
    w.finish_writing_trace_events().map_err(|e| e.to_string())?;
    w.finish_writing_trace_metadata().map_err(|e| e.to_string())?;
    w.finish_writing_trace_paths().map_err(|e| e.to_string())?;
    w.close().map_err(|e| e.to_string())
}

#[test]
fn the_control_closes_ok() {
    let _g = nim_lock();
    record(|w| w.register_step(Path::new("/a.ex"), Line(10))).expect("a legal recording closes Ok");
}

#[test]
fn a_step_on_an_uncounted_path_fails_the_close() {
    let _g = nim_lock();
    let err = record(|w| {
        w.register_step(Path::new("/uncounted.ex"), Line(1));
        w.register_step(Path::new("/a.ex"), Line(2));
    })
    .expect_err("a refused step must fail the close, not vanish");
    assert!(err.contains("/uncounted.ex"), "the error must name the path; got: {err}");
}

#[test]
fn a_step_past_its_file_fails_the_close() {
    let _g = nim_lock();
    let err = record(|w| {
        w.register_step(Path::new("/a.ex"), Line(11));
        w.register_step(Path::new("/a.ex"), Line(2));
    })
    .expect_err("a step past the file's line count must fail the close");
    assert!(err.contains("/a.ex"), "the error must name the file; got: {err}");
}

#[test]
fn an_encoder_failure_is_reported() {
    let _g = nim_lock();
    let mut e = StreamingValueEncoder::new();
    e.end_compound();
    let failure = e.take_failure();
    assert!(failure.is_some(), "closing a compound that was never opened must be reported");
    assert!(e.take_failure().is_none(), "take_failure clears the failure");
}
