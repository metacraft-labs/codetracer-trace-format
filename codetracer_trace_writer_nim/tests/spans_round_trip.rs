//! Headless round trip for the RS-M1 span API on the Nim multi-stream writer.
//!
//! This is the lowest layer at which the request-span feature can be checked —
//! no recorder, no server, no GUI: a writer, real container bytes on disk, and
//! the canonical Nim span reader (`initSpanStreamReader` via `ct_spans_json`).
//! Per the headless-first policy in
//! `codetracer-specs/Testing/Testing-Guidelines.md`, a failure here localises
//! the regression to the Rust↔Nim FFI boundary rather than to whichever
//! recorder noticed it.
//!
//! Three things are asserted, each of which a language recorder depends on
//! (`codetracer-specs/Trace-Files/CTFS-Request-Span-Streams.md`):
//!
//! 1. **`next_step_index` tracks the WRITER's exec-event counter**, not a count
//!    of `register_step` calls. Thread events and column deltas are exec-stream
//!    events too, so a recorder that counted its own step registrations would
//!    bind spans to the wrong step ids. The test emits a thread event between
//!    steps precisely to pin that difference.
//! 2. **An open record and its completion collapse to one span** under
//!    last-record-wins, while the raw read still shows both in append order —
//!    the mechanism a live Request Panel uses to show an in-flight row.
//! 3. **Metadata order survives the round trip**, since consumers render the
//!    well-known `http.*` keys in emission order.
//!
//! No mocks: the only test-owned values are the span fields themselves.

use std::path::Path;
use std::sync::Mutex;

use codetracer_trace_types::Line;
use codetracer_trace_writer_nim::{
    read_span_stream_json, NimTraceWriter, SpanRecord, TraceEventsFileFormat, SPAN_STATUS_OK,
};

/// The Nim runtime is not thread-safe — its global state lives behind a single
/// lock — so every test in this binary is serialised, as in `thread_events.rs`.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn make_writer(program_basename: &str) -> (tempfile::TempDir, NimTraceWriter) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut writer = NimTraceWriter::new(program_basename, &[], TraceEventsFileFormat::Binary);
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

fn close_writer(
    dir: tempfile::TempDir,
    mut writer: NimTraceWriter,
    program_basename: &str,
) -> std::path::PathBuf {
    writer.finish_writing_trace_events().expect("finish_events");
    writer.close().expect("close");
    drop(writer);
    let ct_path = dir.path().join(format!("{program_basename}.ct"));
    // The reader needs the file to outlive this scope, so the tempdir is leaked.
    #[allow(deprecated)]
    let _dir_path = dir.into_path();
    assert!(ct_path.exists(), ".ct trace file was not created at {}", ct_path.display());
    ct_path
}

fn web_request_span(span_id: u64, start_step: u64, end_step: u64, open: bool) -> SpanRecord {
    SpanRecord {
        span_id,
        is_open: open,
        status: if open { 0 } else { SPAN_STATUS_OK },
        start_wall_ns: 1_764_500_000_000_000_000,
        end_wall_ns: if open { 0 } else { 1_764_500_000_012_000_000 },
        thread_id: 1,
        start_step,
        end_step: if open { 0 } else { end_step },
        span_type: "web-request".to_string(),
        label: "GET /api/users".to_string(),
        contiguous_on_one_thread: true,
        shares_timeline: true,
        // Deliberately NOT alphabetical: the reader must hand these back in
        // exactly this order.
        metadata: vec![
            ("http.method".to_string(), "GET".to_string()),
            ("http.url".to_string(), "/api/users".to_string()),
            ("http.status_code".to_string(), if open { "0" } else { "200" }.to_string()),
            ("http.duration_ms".to_string(), if open { "0" } else { "12" }.to_string()),
            ("framework".to_string(), "test".to_string()),
        ],
        ..SpanRecord::default()
    }
}

#[test]
fn next_step_index_follows_the_writers_exec_counter() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let program = "span_next_step_index";
    let (dir, mut writer) = make_writer(program);
    let source = Path::new("/tmp/span_next_step_index.py");

    // Nothing recorded yet: the next event will be step 0.
    assert_eq!(writer.next_step_index(), 0, "a fresh writer's next step is 0");

    writer.start(source, Line(1));
    // `start` buffers a pending step, which will take index 0 — so the next NEW
    // event takes index 1.
    assert_eq!(writer.next_step_index(), 1);

    writer.register_step(source, Line(2));
    assert_eq!(writer.next_step_index(), 2);

    // A thread event is an exec-stream event too, and the writer's counter
    // advances for it.  This is the case a recorder that counted its own
    // `register_step` calls would get wrong — it would still say 2.
    writer.register_thread_start(7);
    let after_thread_event = writer.next_step_index();
    assert_eq!(
        after_thread_event, 3,
        "a thread event occupies a step id; next_step_index must account for it"
    );

    writer.register_step(source, Line(3));
    assert_eq!(writer.next_step_index(), 4);

    let ct_path = close_writer(dir, writer, program);

    // The recorded stream is exactly as long as the last reported index, which
    // is what makes `[start_step, end_step]` a coordinate inside the container.
    let reader = codetracer_trace_writer_nim::NimTraceReaderHandle::open(
        ct_path.to_str().expect("utf-8 path"),
    )
    .expect("reader open");
    assert_eq!(reader.step_count(), 4);
}

#[test]
fn open_span_and_completion_collapse_to_one_settled_span() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let program = "span_open_then_settled";
    let (dir, mut writer) = make_writer(program);
    let source = Path::new("/tmp/span_open_then_settled.py");

    writer.start(source, Line(1));
    let start_step = writer.next_step_index();
    for line in 2..=5 {
        writer.register_step(source, Line(line));
    }
    let end_step = writer.next_step_index() - 1;

    // Publish in flight, then settled — the same span id twice, append-only.
    writer
        .register_span(&web_request_span(1, start_step, end_step, true))
        .expect("register open span");
    writer
        .register_span(&web_request_span(1, start_step, end_step, false))
        .expect("register settled span");
    // A second request, completion-only, so the settled view has two spans.
    let mut second = web_request_span(2, start_step, end_step, false);
    second.label = "GET /health".to_string();
    second.metadata[1] = ("http.url".to_string(), "/health".to_string());
    writer.register_span(&second).expect("register second span");

    // Sealing mid-session must not lose or duplicate anything.
    writer.flush_spans().expect("flush spans");

    let ct_path = close_writer(dir, writer, program);

    // --- settled view: last record wins per span id ----------------------
    let settled = read_span_stream_json(&ct_path, true).expect("settled spans");
    assert_eq!(
        settled.matches("\"span_id\"").count(),
        2,
        "an open record and its completion must settle into ONE span: {settled}"
    );
    assert!(settled.contains("\"is_open\":false"));
    assert!(!settled.contains("\"is_open\":true"), "settled view kept an open record: {settled}");
    assert!(settled.contains("\"end_step\":") && settled.contains(&end_step.to_string()));

    // Metadata order is part of the wire contract, so the array must come back
    // in emission order rather than sorted.
    let metadata_start = settled.find("\"metadata\":").expect("metadata key");
    let metadata = &settled[metadata_start..];
    let method_at = metadata.find("http.method").expect("http.method");
    let url_at = metadata.find("http.url").expect("http.url");
    let status_at = metadata.find("http.status_code").expect("http.status_code");
    let framework_at = metadata.find("framework").expect("framework");
    assert!(
        method_at < url_at && url_at < status_at && status_at < framework_at,
        "metadata order was not preserved: {metadata}"
    );

    // --- raw view: every record, in append order -------------------------
    let raw = read_span_stream_json(&ct_path, false).expect("raw spans");
    assert_eq!(
        raw.matches("\"span_id\"").count(),
        3,
        "the raw view must show all three appended records: {raw}"
    );
    let first_open = raw.find("\"is_open\":true").expect("the open record");
    let first_settled = raw.find("\"is_open\":false").expect("a settled record");
    assert!(
        first_open < first_settled,
        "append order lost: the open record must precede its completion"
    );
}

#[test]
fn spans_are_rejected_by_a_backend_that_cannot_store_them() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // The JSON backend has no span stream.  A middleware must be told so rather
    // than believing a request was recorded — hence an error, not a no-op.
    let mut writer = NimTraceWriter::new("span_unsupported", &[], TraceEventsFileFormat::Json);
    let err = writer
        .register_span(&web_request_span(1, 0, 1, false))
        .expect_err("the JSON backend must reject spans");
    assert!(
        err.to_string().contains("spans"),
        "the error should name the missing capability: {err}"
    );
}
