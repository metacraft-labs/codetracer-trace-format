//! The span stream (`spans.dat`, `spans.idx`, `spantype.ns`), native-to-VM
//! crossings and a call's exception, through both writers and both readers.
//!
//! What is asserted, and why each can fail:
//!
//! 1. One recording — open and settled span pairs, an external span, ordered
//!    metadata, a short chunk sealed by a flush, several span types, nested
//!    crossings, enough spans to cross a chunk boundary, a call that exits by
//!    an exception — written by the Nim writer (through its C ABI) and by
//!    this repository's writer gives the same container: the same members in
//!    the same order, every member byte for byte, and the same file size. A
//!    writer that seals at a different point, interns span types in another
//!    order, mints crossing ids differently or encodes a field otherwise
//!    differs somewhere here.
//! 2. This repository's reader reads the Nim-written container and finds the
//!    spans the recording declared, record by record and settled, addressed
//!    through the index's cumulative column, and the span-type index.
//! 3. The Nim reader (its C ABI) reads the Rust-written container and reports
//!    the same records, settled spans and span-type index as this
//!    repository's reader does.
//! 4. Both readers refuse the same malformed span streams and span-type
//!    indexes, and both accept the well-formed control built the same way, so
//!    the refusals are the readers' and not the harness's.
//!
//! No mocks: both writers and both readers are the shipped ones. The malformed
//! containers are real containers, written by the CTFS writer from a
//! well-formed recording's members with one member replaced.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::{CtfsReader, CtfsWriter};
use codetracer_trace_reader::span_stream_reader::{read_span_type_namespace, SpanStreamReader};
use codetracer_trace_reader::split_stream_reader::read_trace_with_details;
use codetracer_trace_types::{FunctionId, Line, TypeId, TypeKind, ValueRecord};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::span_stream::{
    encode_span_record, encode_span_type_namespace, SpanRecord, SpanTypeEntry, SPAN_STATUS_ERROR, SPAN_STATUS_OK,
};
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{read_span_stream_json, read_span_types_json, NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "spans";
const SRC: &str = "/src/app.py";
/// Spans written without a flush between them, to fill whole 64-record chunks.
const BULK: u64 = 70;

/// What the recording needs from a writer. Both writers are driven through
/// the same sequence of calls.
trait Recorder {
    fn step(&mut self, line: i64);
    fn call(&mut self, function: usize);
    fn ret(&mut self);
    fn ret_exception(&mut self, exception: &ValueRecord);
    fn raise(&mut self, type_id: u64);
    fn catch(&mut self, type_id: u64);
    fn span(&mut self, span: &SpanRecord);
    fn flush(&mut self);
    fn begin_crossing(&mut self, span_type: &str) -> u64;
    fn end_crossing(&mut self, span_id: u64);
}

struct Nim(NimTraceWriter, TypeId);
struct Rust(CtfsTraceWriter, TypeId);

impl Recorder for Nim {
    fn step(&mut self, line: i64) {
        self.0.register_step(Path::new(SRC), Line(line));
    }
    fn call(&mut self, function: usize) {
        self.0.register_call(FunctionId(function), vec![]);
    }
    fn ret(&mut self) {
        self.0.register_return(ValueRecord::None { type_id: self.1 });
    }
    fn ret_exception(&mut self, exception: &ValueRecord) {
        self.0.register_return_exception(exception);
    }
    fn raise(&mut self, type_id: u64) {
        self.0.register_raise(type_id, b"boom");
    }
    fn catch(&mut self, type_id: u64) {
        self.0.register_catch(type_id);
    }
    fn span(&mut self, span: &SpanRecord) {
        self.0.register_span(&to_nim(span)).expect("nim register_span");
    }
    fn flush(&mut self) {
        self.0.flush_spans().expect("nim flush_spans");
    }
    fn begin_crossing(&mut self, span_type: &str) -> u64 {
        self.0.begin_crossing(span_type).expect("nim begin_crossing")
    }
    fn end_crossing(&mut self, span_id: u64) {
        self.0.end_crossing(span_id).expect("nim end_crossing");
    }
}

impl Recorder for Rust {
    fn step(&mut self, line: i64) {
        AbstractTraceWriter::register_step(&mut self.0, Path::new(SRC), Line(line));
    }
    fn call(&mut self, function: usize) {
        AbstractTraceWriter::register_call(&mut self.0, FunctionId(function), vec![]);
    }
    fn ret(&mut self) {
        AbstractTraceWriter::register_return(&mut self.0, ValueRecord::None { type_id: self.1 });
    }
    fn ret_exception(&mut self, exception: &ValueRecord) {
        self.0.register_return_exception(exception).expect("rust register_return_exception");
    }
    fn raise(&mut self, type_id: u64) {
        self.0.register_raise(type_id, b"boom").expect("rust raise");
    }
    fn catch(&mut self, type_id: u64) {
        self.0.register_catch(type_id).expect("rust catch");
    }
    fn span(&mut self, span: &SpanRecord) {
        self.0.register_span(span).expect("rust register_span");
    }
    fn flush(&mut self) {
        self.0.flush_spans().expect("rust flush_spans");
    }
    fn begin_crossing(&mut self, span_type: &str) -> u64 {
        self.0.begin_crossing(span_type).expect("rust begin_crossing")
    }
    fn end_crossing(&mut self, span_id: u64) {
        self.0.end_crossing(span_id).expect("rust end_crossing");
    }
}

/// The binding's span type, field for field.
fn to_nim(s: &SpanRecord) -> codetracer_trace_writer_nim::SpanRecord {
    codetracer_trace_writer_nim::SpanRecord {
        span_id: s.span_id,
        parent_span_id: s.parent_span_id,
        is_open: s.is_open,
        is_external: s.is_external,
        status: s.status,
        start_wall_ns: s.start_wall_ns,
        end_wall_ns: s.end_wall_ns,
        process_ord: s.process_ord,
        thread_id: s.thread_id,
        start_step: s.start_step,
        end_step: s.end_step,
        external_recording: s.external_recording.clone(),
        external_path: s.external_path.clone(),
        span_type: s.span_type.clone(),
        label: s.label.clone(),
        contiguous_on_one_thread: s.contiguous_on_one_thread,
        shares_timeline: s.shares_timeline,
        concurrent_with_siblings: s.concurrent_with_siblings,
        metadata: s.metadata.clone(),
    }
}

fn web_request_open() -> SpanRecord {
    SpanRecord {
        span_id: 10,
        is_open: true,
        start_wall_ns: 100,
        start_step: 1,
        span_type: "web-request".into(),
        label: "GET /a".into(),
        contiguous_on_one_thread: true,
        metadata: vec![("http.method".into(), "GET".into()), ("http.path".into(), "/a".into())],
        ..SpanRecord::default()
    }
}

fn web_request_settled() -> SpanRecord {
    SpanRecord {
        is_open: false,
        status: SPAN_STATUS_OK,
        end_wall_ns: 500,
        end_step: 3,
        metadata: vec![
            ("http.method".into(), "GET".into()),
            ("http.path".into(), "/a".into()),
            ("http.status".into(), "200".into()),
        ],
        ..web_request_open()
    }
}

fn external_process() -> SpanRecord {
    SpanRecord {
        span_id: 11,
        is_external: true,
        status: SPAN_STATUS_OK,
        start_wall_ns: 200,
        end_wall_ns: 300,
        process_ord: 1,
        thread_id: 7,
        external_recording: "01900000-0000-7000-8000-000000000001".into(),
        external_path: "child/p.ct".into(),
        span_type: "process".into(),
        label: "/usr/bin/child".into(),
        concurrent_with_siblings: true,
        ..SpanRecord::default()
    }
}

fn failed_test() -> SpanRecord {
    SpanRecord {
        span_id: 12,
        status: SPAN_STATUS_ERROR,
        start_wall_ns: 600,
        end_wall_ns: 700,
        start_step: 2,
        end_step: 9,
        span_type: "test".into(),
        label: "tést ✓".into(),
        shares_timeline: true,
        metadata: vec![
            (String::new(), String::new()),
            ("z".into(), "first".into()),
            ("a".into(), "second".into()),
        ],
        ..SpanRecord::default()
    }
}

fn bulk(i: u64) -> SpanRecord {
    SpanRecord {
        span_id: 100 + i,
        status: SPAN_STATUS_OK,
        start_wall_ns: 1_000 + i,
        end_wall_ns: 2_000 + i,
        start_step: i,
        end_step: i + 1,
        span_type: if i.is_multiple_of(3) { "bulk-a".into() } else { "bulk-b".into() },
        label: format!("job {i}"),
        ..SpanRecord::default()
    }
}

fn exception_value() -> ValueRecord {
    ValueRecord::Error {
        msg: "boom".to_string(),
        type_id: TypeId(0),
    }
}

/// The recording. Returns the ids the two crossings were given.
fn record(r: &mut dyn Recorder) -> (u64, u64) {
    r.call(0);
    r.step(2);
    // A crossing opened right after the first step: the step is the caller's,
    // so the crossing starts at the next one.
    let outer = r.begin_crossing("vm");
    r.span(&web_request_open());
    r.span(&external_process());
    r.flush();
    r.step(3);
    let inner = r.begin_crossing("vm");
    r.step(4);
    r.step(5);
    r.end_crossing(inner);
    r.span(&web_request_settled());
    r.step(6);
    r.end_crossing(outer);
    r.span(&failed_test());
    for i in 0..BULK {
        r.span(&bulk(i));
    }
    r.call(1);
    r.step(7);
    r.raise(0);
    r.ret_exception(&exception_value());
    r.catch(0);
    r.step(8);
    r.ret();
    (outer, inner)
}

fn write_nim(dir: &Path, compact_threshold: u64) -> (PathBuf, (u64, u64)) {
    let src = PathBuf::from(SRC);
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.set_recording_id("01900000-0000-7000-8000-0000000000aa").expect("nim recording id");
    w.set_workdir(Path::new("/work"));
    w.set_compact_threshold(compact_threshold).expect("nim compact threshold");
    w.begin_writing_trace_events(&dir.join("e.json")).expect("nim begin_events");
    w.begin_writing_trace_metadata(&dir.join("m.json")).expect("nim begin_metadata");
    w.begin_writing_trace_paths(&dir.join("p.json")).expect("nim begin_paths");
    w.enable_line_count_table().expect("nim enable_line_count_table");
    w.register_path_with_line_count(&src, 20).expect("nim path");
    w.register_function("main", &src, Line(1));
    w.register_function("g", &src, Line(7));
    let tid = w.ensure_type_id(TypeKind::Error, "ValueError");
    let mut rec = Nim(w, tid);
    let ids = record(&mut rec);
    let mut w = rec.0;
    w.finish_writing_trace_events().expect("nim finish_events");
    w.finish_writing_trace_metadata().expect("nim finish_metadata");
    w.finish_writing_trace_paths().expect("nim finish_paths");
    w.close().expect("nim close");
    drop(w);
    (dir.join(format!("{PROGRAM}.ct")), ids)
}

fn write_rust(dir: &Path, compact_threshold: u64) -> (PathBuf, (u64, u64)) {
    let src = PathBuf::from(SRC);
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    w.set_recording_id("01900000-0000-7000-8000-0000000000aa");
    AbstractTraceWriter::set_workdir(&mut w, Path::new("/work"));
    w.set_compact_threshold(compact_threshold);
    let out = dir.join(PROGRAM);
    TraceWriter::begin_writing_trace_events(&mut w, &out).expect("rust begin_events");
    w.enable_line_count_table().expect("rust enable_line_count_table");
    w.register_path_with_line_count(&src, 20).expect("rust path");
    AbstractTraceWriter::register_function(&mut w, "main", &src, Line(1));
    AbstractTraceWriter::register_function(&mut w, "g", &src, Line(7));
    let tid = AbstractTraceWriter::ensure_type_id(&mut w, TypeKind::Error, "ValueError");
    let mut rec = Rust(w, tid);
    let ids = record(&mut rec);
    let mut w = rec.0;
    TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish_events");
    assert!(w.refusals().is_empty(), "the recording is legal; refused: {:?}", w.refusals());
    (out.with_extension("ct"), ids)
}

fn members(ct: &Path) -> Vec<(String, Vec<u8>)> {
    let mut r = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()));
    r.members().unwrap_or_else(|e| panic!("members of {}: {e:?}", ct.display()))
}

struct Pair {
    _dir: tempfile::TempDir,
    nim: PathBuf,
    rust: PathBuf,
    nim_ids: (u64, u64),
    rust_ids: (u64, u64),
}

fn write_both() -> Pair {
    write_both_with(0)
}

/// Both writers, converting to the compact profile below `compact_threshold`
/// raw bytes (0: never).
fn write_both_with(compact_threshold: u64) -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let (nd, rd) = (dir.path().join("nim"), dir.path().join("rust"));
    std::fs::create_dir_all(&nd).unwrap();
    std::fs::create_dir_all(&rd).unwrap();
    let (nim, nim_ids) = write_nim(&nd, compact_threshold);
    let (rust, rust_ids) = write_rust(&rd, compact_threshold);
    Pair {
        _dir: dir,
        nim,
        rust,
        nim_ids,
        rust_ids,
    }
}

fn open_spans(ct: &Path) -> SpanStreamReader {
    let mut r = CtfsReader::open(ct).expect("open");
    SpanStreamReader::open(&mut r)
        .expect("the span stream opens")
        .expect("the container has a span stream")
}

/// A span as `ct_spans_json` renders it.
fn span_json(s: &SpanRecord) -> serde_json::Value {
    serde_json::json!({
        "span_id": s.span_id,
        "parent_span_id": s.parent_span_id,
        "is_open": s.is_open,
        "is_external": s.is_external,
        "status": s.status,
        "start_wall_ns": s.start_wall_ns,
        "end_wall_ns": s.end_wall_ns,
        "process_ord": s.process_ord,
        "thread_id": s.thread_id,
        "start_step": s.start_step,
        "end_step": s.end_step,
        "external_recording": s.external_recording,
        "external_path": s.external_path,
        "span_type": s.span_type,
        "label": s.label,
        "contiguous_on_one_thread": s.contiguous_on_one_thread,
        "shares_timeline": s.shares_timeline,
        "concurrent_with_siblings": s.concurrent_with_siblings,
        "metadata": s.metadata.iter().map(|(k, v)| serde_json::json!([k, v])).collect::<Vec<_>>(),
    })
}

fn types_json(entries: &[SpanTypeEntry]) -> serde_json::Value {
    serde_json::Value::Array(
        entries
            .iter()
            .map(|e| serde_json::json!({"type_id": e.type_id, "name": e.name, "span_ids": e.span_ids}))
            .collect(),
    )
}

fn parse(json: &str) -> serde_json::Value {
    serde_json::from_str(json).unwrap_or_else(|e| panic!("not JSON ({e}): {json}"))
}

/// What a crossing writes: an open record when it begins, the settled one
/// when it ends.
fn crossing(span_id: u64, start_step: u64, end_step: Option<u64>) -> SpanRecord {
    SpanRecord {
        span_id,
        is_open: end_step.is_none(),
        status: if end_step.is_some() { SPAN_STATUS_OK } else { 0 },
        start_step,
        end_step: end_step.unwrap_or(0),
        span_type: "vm".into(),
        contiguous_on_one_thread: true,
        shares_timeline: true,
        ..SpanRecord::default()
    }
}

/// The records the recording appends, in append order.
fn expected_records() -> Vec<SpanRecord> {
    let mut v = vec![
        crossing(1, 1, None),
        web_request_open(),
        external_process(),
        crossing(2, 2, None),
        crossing(2, 2, Some(3)),
        web_request_settled(),
        crossing(1, 1, Some(4)),
        failed_test(),
    ];
    v.extend((0..BULK).map(bulk));
    v
}

#[test]
fn both_writers_write_the_same_container() {
    let _g = nim_lock();
    let p = write_both();
    assert_eq!(p.nim_ids, (1, 2), "the Nim writer's crossing ids");
    assert_eq!(p.rust_ids, p.nim_ids, "the two writers mint different crossing ids");

    let (n, r) = (members(&p.nim), members(&p.rust));
    let names = |m: &[(String, Vec<u8>)]| m.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    assert_eq!(names(&n), names(&r), "the two containers hold different members, or in another order");
    for ((name, a), (_, b)) in n.iter().zip(&r) {
        assert_eq!(a, b, "{name} differs between the writers ({} against {} bytes)", a.len(), b.len());
    }
    assert!(names(&n).iter().any(|k| k == "spantype.ns"), "the recording has a span-type index");
    assert_eq!(
        std::fs::metadata(&p.nim).unwrap().len(),
        std::fs::metadata(&p.rust).unwrap().len(),
        "the two containers allocate a different number of blocks"
    );
}

#[test]
fn a_compact_container_carries_the_same_spans_from_both_writers() {
    let _g = nim_lock();
    let p = write_both_with(1 << 30);
    for ct in [&p.nim, &p.rust] {
        let c = CtfsReader::open(ct).unwrap();
        assert_eq!(
            c.profile(),
            codetracer_ctfs::compact::Profile::Compact,
            "{}: the compact profile",
            ct.display()
        );
    }
    let (n, r) = (members(&p.nim), members(&p.rust));
    assert_eq!(n, r, "the two compact containers differ");
    assert_eq!(std::fs::metadata(&p.nim).unwrap().len(), std::fs::metadata(&p.rust).unwrap().len());
    let mut reader = open_spans(&p.nim);
    assert_eq!(
        reader.read_all_span_records().unwrap(),
        expected_records(),
        "the Rust reader, compact chunks"
    );
    assert_eq!(
        reader.read_span(70).unwrap(),
        expected_records()[70],
        "a record past the first full chunk"
    );
    let raw: Vec<_> = reader.read_all_span_records().unwrap().iter().map(span_json).collect();
    assert_eq!(
        parse(&read_span_stream_json(&p.rust, false).unwrap()),
        serde_json::Value::Array(raw),
        "the Nim reader, compact chunks"
    );
}

#[test]
fn the_rust_reader_reads_the_nim_written_spans() {
    let _g = nim_lock();
    let p = write_both();
    let mut r = open_spans(&p.nim);
    let expected = expected_records();
    assert_eq!(r.count(), expected.len() as u64, "record count from the index");
    // Opening a crossing publishes its open record at once, in a chunk of
    // its own; the explicit flush then seals the next two records.
    assert_eq!(r.records_in_chunk(0), 1, "the chunk the crossing sealed");
    assert_eq!(r.records_in_chunk(1), 2, "the flushed chunk");
    assert_eq!(r.first_record_of_chunk(2), 3, "the cumulative column");
    assert_eq!(r.chunk_size_records(), 64);
    assert!(r.chunk_count() >= 3, "the bulk spans cross a chunk boundary");
    assert_eq!(r.read_all_span_records().unwrap(), expected, "the raw records in append order");
    for (i, want) in expected.iter().enumerate().rev() {
        assert_eq!(&r.read_span(i as u64).unwrap(), want, "record {i}, addressed through the index");
    }
    assert!(r.read_span(expected.len() as u64).is_err(), "a record past the end is refused");
    let since = r.read_spans_since(2).unwrap();
    assert_eq!(since, expected[3..], "the records sealed after the first two chunks");
    assert!(r.read_spans_since(r.chunk_count() + 1).is_err(), "a cursor past the index is refused");

    let settled = r.settled_spans().unwrap();
    let ids: Vec<u64> = settled.iter().map(|s| s.span_id).collect();
    let mut want_ids = vec![1, 2, 10, 11, 12];
    want_ids.extend((0..BULK).map(|i| 100 + i));
    assert_eq!(ids, want_ids, "settled spans ascend by id");
    assert_eq!(settled[0], crossing(1, 1, Some(4)), "the outer crossing, settled");
    assert_eq!(settled[1], crossing(2, 2, Some(3)), "the inner crossing, settled");
    assert_eq!(settled[2], web_request_settled(), "last record wins");
    let page = r.page_spans(11, 2).unwrap();
    assert_eq!(page, vec![external_process(), failed_test()], "a page from span 11");

    let mut c = CtfsReader::open(&p.nim).unwrap();
    let types = read_span_type_namespace(&mut c).unwrap().expect("the container has a span-type index");
    let names: Vec<&str> = types.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        ["vm", "web-request", "process", "test", "bulk-a", "bulk-b"],
        "types in first-appearance order"
    );
    assert_eq!(types[0].span_ids, [1, 2], "both crossings, once each");
    assert_eq!(types[1].span_ids, [10], "the open and settled record name one span");
}

#[test]
fn the_nim_reader_reads_the_rust_written_spans_as_the_rust_reader_does() {
    let _g = nim_lock();
    let p = write_both();
    let r = open_spans(&p.rust);
    let raw: Vec<_> = r.read_all_span_records().unwrap().iter().map(span_json).collect();
    let settled: Vec<_> = r.settled_spans().unwrap().iter().map(span_json).collect();
    assert_eq!(
        parse(&read_span_stream_json(&p.rust, false).unwrap()),
        serde_json::Value::Array(raw),
        "raw records"
    );
    assert_eq!(
        parse(&read_span_stream_json(&p.rust, true).unwrap()),
        serde_json::Value::Array(settled),
        "settled spans"
    );
    let mut c = CtfsReader::open(&p.rust).unwrap();
    let types = read_span_type_namespace(&mut c).unwrap().unwrap();
    assert_eq!(parse(&read_span_types_json(&p.rust).unwrap()), types_json(&types), "the span-type index");
}

#[test]
fn a_call_that_exits_by_an_exception_reads_alike_from_both_writers() {
    let _g = nim_lock();
    let p = write_both();
    for ct in [&p.nim, &p.rust] {
        let mut c = CtfsReader::open(ct).unwrap();
        let t = read_trace_with_details(&mut c).unwrap();
        let calls = &t.details.calls;
        assert_eq!(calls.len(), 2, "{}: two calls", ct.display());
        assert_eq!(calls[0].raised_exception, None, "{}: main returned", ct.display());
        assert_eq!(
            calls[1].raised_exception,
            Some(exception_value()),
            "{}: g exits by the exception",
            ct.display()
        );
    }
    let nim_reader = NimTraceReaderHandle::open(p.rust.to_str().unwrap()).expect("the Nim reader opens the Rust container");
    let own = NimTraceReaderHandle::open(p.nim.to_str().unwrap()).expect("the Nim reader opens the Nim container");
    for key in 0..2 {
        assert_eq!(
            nim_reader.call_json(key).unwrap(),
            own.call_json(key).unwrap(),
            "call {key} through the Nim reader"
        );
    }
    let g = parse(&nim_reader.call_json(1).unwrap());
    assert_ne!(
        g["exception"],
        serde_json::json!([]),
        "the Nim reader finds the exception in the Rust container"
    );
    assert_eq!(
        g["return_value"].as_array().map(Vec::len),
        Some(1),
        "an exception exit has no return value, only the void marker"
    );
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

/// A container with the members of `base`, `replace`d by name.
fn rebuilt(dir: &Path, name: &str, base: &[(String, Vec<u8>)], replace: &[(&str, Vec<u8>)]) -> PathBuf {
    let path = dir.join(format!("{name}.ct"));
    let mut w = CtfsWriter::create(&path, 4096, 31).unwrap();
    for (member, bytes) in base {
        let bytes = replace
            .iter()
            .find(|(n, _)| n == member)
            .map(|(_, b)| b.clone())
            .unwrap_or_else(|| bytes.clone());
        let h = w.add_file(member).unwrap();
        w.write(h, &bytes).unwrap();
    }
    w.close().unwrap();
    path
}

fn chunk_of(records: &[Vec<u8>]) -> Vec<u8> {
    let mut raw = Vec::new();
    for r in records {
        codetracer_trace_writer::column_aware::encode_varint(r.len() as u64, &mut raw);
        raw.extend_from_slice(r);
    }
    raw
}

fn frame(raw: &[u8]) -> Vec<u8> {
    codetracer_ctfs::compress_pledged(raw, 3, "spans.dat").unwrap()
}

/// `spans.dat` and `spans.idx`.
type Stream = (Vec<u8>, Vec<u8>);

/// `spans.dat` and `spans.idx` for chunks given as their frames and the
/// number of records each holds.
fn stream(chunks: &[(Vec<u8>, u64)]) -> Stream {
    let mut dat = Vec::new();
    let mut idx = vec![64, 0, 0, 0, 2, 0, 0, 0];
    let mut total = 0;
    for (f, n) in chunks {
        total += n;
        idx.extend_from_slice(&(dat.len() as u64).to_le_bytes());
        idx.extend_from_slice(&total.to_le_bytes());
        dat.extend_from_slice(f);
    }
    (dat, idx)
}

fn plain() -> SpanRecord {
    SpanRecord {
        span_id: 1,
        status: SPAN_STATUS_OK,
        end_step: 4,
        span_type: "t".into(),
        label: "l".into(),
        ..SpanRecord::default()
    }
}

/// Both readers' verdicts on a container: `Ok` with the raw records as JSON,
/// or the refusal.
fn verdicts(ct: &Path) -> (Result<serde_json::Value, String>, Result<serde_json::Value, String>) {
    let rust = (|| {
        let mut c = CtfsReader::open(ct).map_err(|e| format!("{e:?}"))?;
        let r = SpanStreamReader::open(&mut c)?.ok_or("no span stream")?;
        let all = r.read_all_span_records()?;
        Ok(serde_json::Value::Array(all.iter().map(span_json).collect()))
    })();
    let nim = read_span_stream_json(ct, false).map(|j| parse(&j)).map_err(|e| e.to_string());
    (rust, nim)
}

fn type_verdicts(ct: &Path) -> (Result<serde_json::Value, String>, Result<serde_json::Value, String>) {
    let rust = (|| {
        let mut c = CtfsReader::open(ct).map_err(|e| format!("{e:?}"))?;
        let t = read_span_type_namespace(&mut c)?.ok_or("no span-type index")?;
        Ok(types_json(&t))
    })();
    let nim = read_span_types_json(ct).map(|j| parse(&j)).map_err(|e| e.to_string());
    (rust, nim)
}

/// The words each reader's refusal of the case `what` must contain.
fn refusal_of(what: &str) -> &'static str {
    match what {
        "unknown flags bit" => "unknown flags bits",
        "status out of range" => "invalid status value",
        "unknown structural bit" => "unknown structural bits",
        "truncated record" => "varint: unexpected end of input",
        "open record with an end" => "must have end_wall_ns and end_step == 0",
        "trailing byte" => "trailing bytes after record",
        "span id 0" => "span_id must be 1-based",
        "label not UTF-8" | "name not UTF-8" => "not UTF-8",
        "record length past its chunk" => "span record length extends past chunk",
        "chunk frame without a content size" => "cannot determine decompressed size",
        "offsets not monotonic" => "chunk offsets are not monotonic",
        "cumulative counts not monotonic" => "cumulative record counts are not monotonic",
        "index version 1" => "unsupported index version 1",
        "reserved header field" => "reserved header field is not 0",
        "chunk size 0" => "chunkSize in spans.idx is 0",
        "entry region not a multiple of 16" => "trailing bytes in the entry region",
        "offset past the data" => "offset is past the end of spans.dat",
        "short header" => "spantype.ns too short",
        "bad magic" => "spantype.ns: bad magic",
        "version 2" => "spantype.ns: unsupported version 2",
        "type table before the header" | "type table past the end" => "type table out of bounds",
        "name past the end" => "name out of bounds",
        "span list past the end" => "span id list out of bounds",
        other => panic!("no refusal is named for {other}"),
    }
}

fn assert_refused(what: &str, reason: &str, rust: Result<serde_json::Value, String>, nim: Result<serde_json::Value, String>) {
    match rust {
        Err(e) => assert!(e.contains(reason), "{what}: the Rust reader refused it for another reason: {e}"),
        Ok(v) => panic!("{what}: the Rust reader accepted it: {v}"),
    }
    match nim {
        Err(e) => assert!(e.contains(reason), "{what}: the Nim reader refused it for another reason: {e}"),
        Ok(v) => panic!("{what}: the Nim reader accepted it: {v}"),
    }
}

#[test]
fn both_readers_refuse_the_same_malformed_span_streams() {
    let _g = nim_lock();
    let p = write_both();
    let base = members(&p.rust);
    let dir = tempfile::tempdir().unwrap();
    let good = encode_span_record(&plain()).unwrap();
    let edit = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut r = good.clone();
        f(&mut r);
        r
    };
    let one = |rec: Vec<u8>| stream(&[(frame(&chunk_of(&[rec])), 1)]);
    let two = stream(&[
        (frame(&chunk_of(std::slice::from_ref(&good))), 1),
        (frame(&chunk_of(std::slice::from_ref(&good))), 1),
    ]);
    let swap_offsets = {
        let (dat, mut idx) = two.clone();
        let (a, b) = (idx[8..16].to_vec(), idx[24..32].to_vec());
        idx[8..16].copy_from_slice(&b);
        idx[24..32].copy_from_slice(&a);
        (dat, idx)
    };
    let shrink_cumulative = {
        let (dat, mut idx) = two.clone();
        idx[32..40].copy_from_slice(&0u64.to_le_bytes());
        (dat, idx)
    };
    let header = |at: usize, v: u8| {
        let (dat, mut idx) = one(good.clone());
        idx[at] = v;
        (dat, idx)
    };
    let mut unpledged_frame = vec![0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x50];
    let raw = chunk_of(std::slice::from_ref(&good));
    let bh = 1u32 | ((raw.len() as u32) << 3);
    unpledged_frame.extend_from_slice(&bh.to_le_bytes()[..3]);
    unpledged_frame.extend_from_slice(&raw);
    let mut over_length = Vec::new();
    codetracer_trace_writer::column_aware::encode_varint(good.len() as u64 + 5, &mut over_length);
    over_length.extend_from_slice(&good);

    let cases: Vec<(&str, Stream)> = vec![
        ("unknown flags bit", one(edit(&|r| r[2] = 0x04))),
        ("status out of range", one(edit(&|r| r[3] = 3))),
        (
            "unknown structural bit",
            one(edit(&|r| {
                let at = r.len() - 2;
                r[at] = 0x08;
            })),
        ),
        ("truncated record", one(edit(&|r| r.truncate(r.len() - 1)))),
        ("open record with an end", one(edit(&|r| r[2] = 0x01))),
        ("trailing byte", one(edit(&|r| r.push(0)))),
        ("span id 0", one(edit(&|r| r[0] = 0))),
        (
            "label not UTF-8",
            one(edit(&|r| {
                let at = r.len() - 3;
                r[at] = 0xFF;
            })),
        ),
        ("record length past its chunk", stream(&[(frame(&over_length), 1)])),
        ("chunk frame without a content size", stream(&[(unpledged_frame, 1)])),
        ("offsets not monotonic", swap_offsets),
        ("cumulative counts not monotonic", shrink_cumulative),
        ("index version 1", header(4, 1)),
        ("reserved header field", header(6, 1)),
        ("chunk size 0", {
            let (dat, mut idx) = one(good.clone());
            idx[..4].copy_from_slice(&[0; 4]);
            (dat, idx)
        }),
        ("entry region not a multiple of 16", {
            let (dat, mut idx) = one(good.clone());
            idx.push(0);
            (dat, idx)
        }),
        ("offset past the data", {
            let (dat, mut idx) = one(good.clone());
            idx[8..16].copy_from_slice(&1_000_000u64.to_le_bytes());
            (dat, idx)
        }),
    ];

    let (control_dat, control_idx) = one(good.clone());
    let control = rebuilt(dir.path(), "control", &base, &[("spans.dat", control_dat), ("spans.idx", control_idx)]);
    let (rust, nim) = verdicts(&control);
    let want = serde_json::Value::Array(vec![span_json(&plain())]);
    assert_eq!(rust.as_ref(), Ok(&want), "the Rust reader accepts the control");
    assert_eq!(nim.as_ref(), Ok(&want), "the Nim reader accepts the control");

    for (i, (what, (dat, idx))) in cases.into_iter().enumerate() {
        let ct = rebuilt(dir.path(), &format!("case{i}"), &base, &[("spans.dat", dat), ("spans.idx", idx)]);
        let (rust, nim) = verdicts(&ct);
        assert_refused(what, refusal_of(what), rust, nim);
    }
}

#[test]
fn both_readers_refuse_the_same_malformed_span_type_indexes() {
    let _g = nim_lock();
    let p = write_both();
    let base = members(&p.rust);
    let dir = tempfile::tempdir().unwrap();
    let good = encode_span_type_namespace(&["a".to_string(), "bé".to_string()], &[vec![1, 3], vec![2]]);
    let edit = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut b = good.clone();
        f(&mut b);
        b
    };
    let put = |b: &mut Vec<u8>, at: usize, v: u64, width: usize| b[at..at + width].copy_from_slice(&v.to_le_bytes()[..width]);
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("short header", good[..17].to_vec()),
        ("bad magic", edit(&|b| b[0] = b'X')),
        ("version 2", edit(&|b| put(b, 4, 2, 2))),
        ("type table before the header", edit(&|b| put(b, 10, 4, 8))),
        ("type table past the end", edit(&|b| put(b, 6, 1000, 4))),
        ("name past the end", edit(&|b| put(b, 18 + 8, 10_000, 8))),
        ("span list past the end", edit(&|b| put(b, 18 + 16, 10_000, 4))),
        ("name not UTF-8", {
            let name_at = 18 + 2 * 28;
            edit(&|b| b[name_at] = 0xFF)
        }),
    ];
    let control = rebuilt(dir.path(), "control", &base, &[("spantype.ns", good.clone())]);
    let (rust, nim) = type_verdicts(&control);
    let want = serde_json::json!([
        {"type_id": 0, "name": "a", "span_ids": [1, 3]},
        {"type_id": 1, "name": "bé", "span_ids": [2]},
    ]);
    assert_eq!(rust.as_ref(), Ok(&want), "the Rust reader accepts the control");
    assert_eq!(nim.as_ref(), Ok(&want), "the Nim reader accepts the control");
    for (i, (what, bytes)) in cases.into_iter().enumerate() {
        let ct = rebuilt(dir.path(), &format!("types{i}"), &base, &[("spantype.ns", bytes)]);
        let (rust, nim) = type_verdicts(&ct);
        assert_refused(what, refusal_of(what), rust, nim);
    }
}

#[test]
fn both_readers_follow_the_span_stream_of_a_container_being_written() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let src = PathBuf::from(SRC);
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    let out = dir.path().join(PROGRAM);
    let ct = out.with_extension("ct");
    TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
    AbstractTraceWriter::register_step(&mut w, &src, Line(1));
    w.register_span(&web_request_open()).unwrap();
    w.register_span(&external_process()).unwrap();
    w.flush_spans().unwrap();

    let first = open_spans(&ct);
    assert_eq!(first.count(), 2, "the flushed spans are readable while the recording runs");
    let seen = first.chunk_count();
    assert_eq!(
        parse(&read_span_stream_json(&ct, false).unwrap()),
        serde_json::Value::Array(first.read_all_span_records().unwrap().iter().map(span_json).collect()),
        "the Nim reader reads the growing container alike"
    );

    w.register_span(&web_request_settled()).unwrap();
    assert_eq!(open_spans(&ct).count(), 2, "a span not yet flushed is not published");
    w.flush_spans().unwrap();
    let grown = open_spans(&ct);
    assert_eq!(grown.read_spans_since(seen).unwrap(), vec![web_request_settled()], "only the new chunk");
    assert_eq!(grown.settled_spans().unwrap(), vec![web_request_settled(), external_process()]);
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
}

#[test]
fn both_readers_follow_the_span_stream_the_nim_writer_is_writing() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let src = PathBuf::from(SRC);
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.path().join("e.json")).unwrap();
    let ct = dir.path().join(format!("{PROGRAM}.ct"));
    w.register_step(&src, Line(1));
    w.register_span(&to_nim(&web_request_open())).unwrap();
    w.register_span(&to_nim(&external_process())).unwrap();
    w.flush_spans().unwrap();

    let first = open_spans(&ct);
    assert_eq!(first.count(), 2, "the flushed spans are readable while the recording runs");
    let seen = first.chunk_count();
    assert_eq!(
        parse(&read_span_stream_json(&ct, false).unwrap()),
        serde_json::Value::Array(first.read_all_span_records().unwrap().iter().map(span_json).collect()),
        "the Nim reader reads the growing container alike"
    );
    w.register_span(&to_nim(&web_request_settled())).unwrap();
    w.flush_spans().unwrap();
    assert_eq!(
        open_spans(&ct).read_spans_since(seen).unwrap(),
        vec![web_request_settled()],
        "only the new chunk"
    );
    w.finish_writing_trace_events().unwrap();
    w.close().unwrap();
}

#[test]
fn a_crossing_closed_out_of_order_is_refused_by_both_writers() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let src = PathBuf::from(SRC);

    let mut n = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    n.begin_writing_trace_events(&dir.path().join("e.json")).unwrap();
    n.register_path(&src);
    n.register_step(&src, Line(1));
    let a = n.begin_crossing("vm").unwrap();
    let _b = n.begin_crossing("vm").unwrap();
    assert!(n.end_crossing(a).is_err(), "Nim: the outer crossing closed before the inner one");
    assert!(n.end_crossing(99).is_err(), "Nim: a crossing never opened");

    let mut r = CtfsTraceWriter::new(PROGRAM, &[]);
    TraceWriter::begin_writing_trace_events(&mut r, &dir.path().join("r")).unwrap();
    AbstractTraceWriter::register_step(&mut r, &src, Line(1));
    let a = r.begin_crossing("vm").unwrap();
    let _b = r.begin_crossing("vm").unwrap();
    assert!(r.end_crossing(a).is_err(), "Rust: the outer crossing closed before the inner one");
    assert!(r.end_crossing(99).is_err(), "Rust: a crossing never opened");
    drop(n);
}

#[test]
fn a_return_by_exception_without_a_call_is_refused() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut r = CtfsTraceWriter::new(PROGRAM, &[]);
    TraceWriter::begin_writing_trace_events(&mut r, &dir.path().join("r")).unwrap();
    assert!(r.register_return_exception(&exception_value()).is_err(), "no call is open");
}
