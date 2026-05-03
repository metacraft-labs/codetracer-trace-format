//! Headless test for thread lifecycle events on the Nim multi-stream writer.
//!
//! Background — until this test was added, `NimTraceWriter::add_event` was
//! a silent no-op for every variant of [`TraceLowLevelEvent`].  Recorders
//! (notably the Ruby native tracer) routed `ThreadStart`, `ThreadExit`, and
//! `ThreadSwitch` through `TraceWriter::add_event` and the events vanished.
//! Three separate prior incidents (1.21 / 1.22 / 1.27 in the IsoNim
//! migration log) were ultimately rooted in this footgun.
//!
//! This test exercises both the new dedicated entry points
//! ([`NimTraceWriter::register_thread_start`] / `_exit` / `_switch`) and the
//! `add_event` dispatch path that recorders without a thread-aware migration
//! still rely on, then reads the trace back via the Nim trace reader and
//! asserts the events show up in the resulting exec stream.
//!
//! Per the headless-first policy in
//! `codetracer-specs/Testing/Testing-Guidelines.md`, this is the lowest-layer
//! verification — no Electron, no DAP, no recorder process.  If this test
//! fails, the regression is clearly localized to the Rust↔Nim FFI boundary.

use std::path::Path;
use std::sync::Mutex;

use codetracer_trace_types::{Line, ThreadId, TraceLowLevelEvent};
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is **not** thread-safe — its global state (interning
/// tables, heap, trace-writer registry) lives behind a single lock.  Multiple
/// `cargo test` threads creating writers concurrently corrupt the state and
/// segfault.  Serialize every test in this binary through this mutex.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn make_writer(program_basename: &str) -> (tempfile::TempDir, NimTraceWriter) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut writer = NimTraceWriter::new(program_basename, TraceEventsFileFormat::Binary);

    // Start the trace lifecycle exactly the way recorders do.  The
    // multi-stream backend defers .ct creation until `begin_writing_trace_events`,
    // and the trace reader needs the metadata + paths phases for .ct
    // post-processing to complete.
    let metadata_path = dir.path().join("trace_metadata.json");
    writer.begin_writing_trace_metadata(&metadata_path).expect("begin_metadata");
    writer.finish_writing_trace_metadata().expect("finish_metadata");

    let events_path = dir.path().join("trace.json");
    writer.begin_writing_trace_events(&events_path).expect("begin_events");

    let paths_path = dir.path().join("trace_paths.json");
    writer.begin_writing_trace_paths(&paths_path).expect("begin_paths");
    writer.finish_writing_trace_paths().expect("finish_paths");

    (dir, writer)
}

fn close_writer(dir: tempfile::TempDir, mut writer: NimTraceWriter, program_basename: &str) -> std::path::PathBuf {
    writer.finish_writing_trace_events().expect("finish_events");
    writer.close().expect("close");
    drop(writer);
    let ct_path = dir.path().join(format!("{program_basename}.ct"));
    // Persist by leaking the tempdir — the reader needs the file to outlive
    // this scope, and TempDir would otherwise delete it.
    #[allow(deprecated)]
    let _dir_path = dir.into_path();
    assert!(ct_path.exists(), ".ct trace file was not created at {}", ct_path.display());
    ct_path
}

#[test]
fn add_event_thread_switch_is_captured() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let program = "thread_switch_dispatch_test";
    let (dir, mut writer) = make_writer(program);

    // Anchor the trace with a single explicit step so the multi-stream
    // backend has at least one path registered (which the reader's
    // metadata layer requires) before we emit a thread-switch.
    writer.start(Path::new("/tmp/thread_switch_dispatch_test.rb"), Line(1));
    writer.register_step(Path::new("/tmp/thread_switch_dispatch_test.rb"), Line(2));

    // Dispatch ThreadSwitch through the legacy `add_event` path — this is the
    // exact API the Ruby recorder used (and that used to silently drop).
    writer.add_event(TraceLowLevelEvent::ThreadSwitch(ThreadId(0xDEADBEEF)));

    writer.register_step(Path::new("/tmp/thread_switch_dispatch_test.rb"), Line(3));

    let ct_path = close_writer(dir, writer, program);

    // Read the trace back.  We expect 4 step-stream entries: the start step,
    // the explicit step at line 2, the thread-switch event (which writes to
    // the exec stream and bumps stepCount), and the explicit step at line 3.
    let reader = NimTraceReaderHandle::open(ct_path.to_str().unwrap()).expect("reader open");

    let step_count = reader.step_count();
    assert_eq!(step_count, 4, "expected 4 exec-stream entries, got {step_count}");

    // The thread switch is the third entry (index 2).  Its JSON should
    // identify it as the new `thread_switch` step-event kind with thread_id
    // matching what we sent.
    let json = reader.step_json(2).expect("step_json[2]");
    assert!(json.contains("\"kind\":\"thread_switch\""), "expected thread_switch event, got: {json}");
    assert!(
        json.contains("\"thread_id\":3735928559"),
        "expected thread_id 3735928559 (0xDEADBEEF), got: {json}"
    );
}

#[test]
fn register_thread_lifecycle_round_trip() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let program = "thread_lifecycle_round_trip";
    let (dir, mut writer) = make_writer(program);

    writer.start(Path::new("/tmp/thread_lifecycle_round_trip.rb"), Line(1));

    // Use the dedicated entry points — these are what the Ruby recorder is
    // being migrated to.  Pre-fix this code would have compiled but lost
    // every event.
    writer.register_thread_start(7);
    writer.register_step(Path::new("/tmp/thread_lifecycle_round_trip.rb"), Line(2));
    writer.register_thread_switch(7);
    writer.register_step(Path::new("/tmp/thread_lifecycle_round_trip.rb"), Line(3));
    writer.register_thread_exit(7);

    let ct_path = close_writer(dir, writer, program);

    let reader = NimTraceReaderHandle::open(ct_path.to_str().unwrap()).expect("reader open");

    // Layout: start (idx 0) | thread_start (1) | step line 2 (2) |
    //         thread_switch (3) | step line 3 (4) | thread_exit (5)
    let step_count = reader.step_count();
    assert_eq!(step_count, 6, "expected 6 exec-stream entries, got {step_count}");

    let kinds: Vec<String> = (0..step_count).map(|i| reader.step_json(i).expect("step_json")).collect();

    // Verify each thread event is at the expected position with the right kind.
    assert!(
        kinds[1].contains("\"kind\":\"thread_start\"") && kinds[1].contains("\"thread_id\":7"),
        "expected thread_start at index 1, got: {}",
        kinds[1]
    );
    assert!(
        kinds[3].contains("\"kind\":\"thread_switch\"") && kinds[3].contains("\"thread_id\":7"),
        "expected thread_switch at index 3, got: {}",
        kinds[3]
    );
    assert!(
        kinds[5].contains("\"kind\":\"thread_exit\"") && kinds[5].contains("\"thread_id\":7"),
        "expected thread_exit at index 5, got: {}",
        kinds[5]
    );
}

#[test]
fn add_event_thread_start_and_exit_round_trip() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let program = "thread_start_exit_dispatch";
    let (dir, mut writer) = make_writer(program);

    writer.start(Path::new("/tmp/thread_start_exit_dispatch.rb"), Line(1));

    // Both ThreadStart and ThreadExit must round-trip through `add_event` —
    // the Ruby recorder routes its `RUBY_INTERNAL_THREAD_EVENT_STARTED` and
    // `RUBY_INTERNAL_THREAD_EVENT_EXITED` callbacks through this path.
    writer.add_event(TraceLowLevelEvent::ThreadStart(ThreadId(42)));
    writer.register_step(Path::new("/tmp/thread_start_exit_dispatch.rb"), Line(2));
    writer.add_event(TraceLowLevelEvent::ThreadExit(ThreadId(42)));

    let ct_path = close_writer(dir, writer, program);
    let reader = NimTraceReaderHandle::open(ct_path.to_str().unwrap()).expect("reader open");

    let step_count = reader.step_count();
    assert_eq!(step_count, 4, "expected 4 exec-stream entries, got {step_count}");

    let json_start = reader.step_json(1).expect("step_json[1]");
    assert!(
        json_start.contains("\"kind\":\"thread_start\"") && json_start.contains("\"thread_id\":42"),
        "expected thread_start at index 1, got: {json_start}"
    );

    let json_exit = reader.step_json(3).expect("step_json[3]");
    assert!(
        json_exit.contains("\"kind\":\"thread_exit\"") && json_exit.contains("\"thread_id\":42"),
        "expected thread_exit at index 3, got: {json_exit}"
    );
}

#[test]
fn append_events_drains_and_dispatches() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let program = "append_events_drain";
    let (dir, mut writer) = make_writer(program);

    writer.start(Path::new("/tmp/append_events_drain.rb"), Line(1));

    let mut buffered = vec![
        TraceLowLevelEvent::ThreadStart(ThreadId(1)),
        TraceLowLevelEvent::ThreadSwitch(ThreadId(2)),
        TraceLowLevelEvent::ThreadExit(ThreadId(1)),
    ];
    writer.append_events(&mut buffered);

    // append_events is documented to drain the input vec — so callers can
    // reuse it as a scratch buffer.  Verify that contract.
    assert!(
        buffered.is_empty(),
        "append_events did not drain the input vec; remaining: {}",
        buffered.len()
    );

    let ct_path = close_writer(dir, writer, program);
    let reader = NimTraceReaderHandle::open(ct_path.to_str().unwrap()).expect("reader open");

    let step_count = reader.step_count();
    assert_eq!(step_count, 4, "expected 4 exec-stream entries, got {step_count}");

    assert!(reader.step_json(1).unwrap().contains("\"kind\":\"thread_start\""), "missing thread_start");
    assert!(
        reader.step_json(2).unwrap().contains("\"kind\":\"thread_switch\""),
        "missing thread_switch"
    );
    assert!(reader.step_json(3).unwrap().contains("\"kind\":\"thread_exit\""), "missing thread_exit");
}
