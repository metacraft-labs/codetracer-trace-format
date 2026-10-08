//! `linehits.tc`, `corrmark.ns` and the marker label table, written by both
//! writers and read by both readers.
//!
//! What is asserted, and why each can fail:
//!
//! 1. For the same recording the two writers produce the same container:
//!    the same members, each byte for byte, and the same total size. That
//!    covers a line-only recording with revisited lines, a column-aware one
//!    with column steps, one with enough distinct positions for a three-level
//!    tree, one with markers of both kinds (labels interned before the first
//!    record and later, repeated keys sharing a bucket, a payload needing
//!    JSON escapes), one with a label but no marker, and one with neither.
//!    A writer that keys a hit by a different position, numbers steps
//!    differently, orders a bucket differently, writes the reserved bytes,
//!    escapes a payload differently or creates a member in a different place
//!    differs.
//! 2. Each reader reads each container to the same answer: the Rust reader
//!    on the Nim-written container and the Nim reader (its C ABI) on the
//!    Rust-written one, and each on its own, rendered in one document form.
//!    That includes span and boundary lookups that hit, and near misses that
//!    must answer nothing.
//! 3. A recording without line hits or markers has none of the members, and
//!    both readers answer "absent" rather than an empty index.
//! 4. Both readers refuse the same damaged members: each damage is applied to
//!    the image inside a real container, checked to have landed, and must
//!    make both readers fail while the undamaged container reads.
//!
//! No mocks: both writers are the shipped ones and both readers the shipped
//! ones.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::correlation_reader::{marker_labels, open_correlation_index, open_line_hits};
use codetracer_trace_types::{Line, TypeKind, ValueRecord};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{
    lookup_correlation_boundary_json, lookup_correlation_span_json, read_correlation_index_json, read_line_hits_json, read_marker_labels_json,
    MemberAnswer, NimTraceWriter, TraceEventsFileFormat,
};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "hits";
const RECORDING_ID: &str = "01949fcc-7d92-7e9c-aaaa-cccccccccccc";
const WORKDIR: &str = "/work/hits";
const A: &str = "/src/a.py";
const B: &str = "/src/b.py";
const TRACE_ID: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
const SPAN_ID: [u8; 8] = [0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7];
const ODD_KEY: &str = "q\"uo\\te\n\t\r\u{1}\u{1b}é";

/// The calls both writers receive.
#[allow(clippy::too_many_arguments)]
trait Recorder {
    fn line_hits(&mut self);
    fn table(&mut self, path: &str, line_lengths: &[u32]);
    fn counted(&mut self, path: &str, lines: u64);
    fn step(&mut self, path: &str, line: i64, column: Option<i64>);
    fn column_move(&mut self, delta: i64);
    fn call(&mut self, name: &str, path: &str, line: i64);
    fn ret(&mut self);
    fn label(&mut self, label: &str) -> u64;
    fn mark_by_id(&mut self, id: u64, label: &str, direction: &str, key: &str, show: &str, description: &str, key_text: &str, show_text: &str);
    fn mark(&mut self, direction: &str, label: &str, key: &str);
    fn cover(&mut self, trace: &[u8; 16], span: &[u8; 8], wall: u64, mono: u64);
    fn cover_hex(&mut self, trace: &str, span: &str, wall: u64, mono: u64);
}

impl Recorder for NimTraceWriter {
    fn line_hits(&mut self) {
        self.enable_line_hits().expect("nim enable_line_hits");
    }
    fn table(&mut self, path: &str, line_lengths: &[u32]) {
        self.register_path_with_line_lengths(Path::new(path), line_lengths).expect("nim table");
    }
    fn counted(&mut self, path: &str, lines: u64) {
        self.register_path_with_line_count(Path::new(path), lines).expect("nim counted path");
    }
    fn step(&mut self, path: &str, line: i64, column: Option<i64>) {
        self.register_step_with_column(Path::new(path), Line(line), column.map(Line));
    }
    fn column_move(&mut self, delta: i64) {
        self.write_delta_column(delta);
    }
    fn call(&mut self, name: &str, path: &str, line: i64) {
        let f = self.ensure_function_id(name, Path::new(path), Line(line));
        self.register_call(f, vec![]);
    }
    fn ret(&mut self) {
        let t = self.ensure_type_id(TypeKind::None, "None");
        self.register_return(ValueRecord::None { type_id: t });
    }
    fn label(&mut self, label: &str) -> u64 {
        self.ensure_marker_id(label).expect("nim ensure_marker_id")
    }
    fn mark_by_id(&mut self, id: u64, label: &str, direction: &str, key: &str, show: &str, description: &str, key_text: &str, show_text: &str) {
        self.mark_correlation_by_id(id, label, direction, key, show, description, key_text, show_text)
            .expect("nim mark_correlation_by_id");
    }
    fn mark(&mut self, direction: &str, label: &str, key: &str) {
        self.mark_correlation(direction, label, key, "", "", "", "")
            .expect("nim mark_correlation");
    }
    fn cover(&mut self, trace: &[u8; 16], span: &[u8; 8], wall: u64, mono: u64) {
        self.mark_span_coverage(trace, span, wall, mono).expect("nim mark_span_coverage");
    }
    fn cover_hex(&mut self, trace: &str, span: &str, wall: u64, mono: u64) {
        self.mark_span_coverage_hex(trace, span, wall, mono).expect("nim mark_span_coverage_hex");
    }
}

impl Recorder for CtfsTraceWriter {
    fn line_hits(&mut self) {
        self.enable_line_hits();
    }
    fn table(&mut self, path: &str, line_lengths: &[u32]) {
        TraceWriter::register_path_with_line_lengths(self, Path::new(path), line_lengths).expect("rust table");
    }
    fn counted(&mut self, path: &str, lines: u64) {
        self.register_path_with_line_count(Path::new(path), lines).expect("rust counted path");
    }
    fn step(&mut self, path: &str, line: i64, column: Option<i64>) {
        AbstractTraceWriter::register_step_with_column(self, Path::new(path), Line(line), column.map(Line));
    }
    fn column_move(&mut self, delta: i64) {
        TraceWriter::write_delta_column(self, delta);
    }
    fn call(&mut self, name: &str, path: &str, line: i64) {
        let f = AbstractTraceWriter::ensure_function_id(self, name, Path::new(path), Line(line));
        AbstractTraceWriter::register_call(self, f, vec![]);
    }
    fn ret(&mut self) {
        let t = AbstractTraceWriter::ensure_type_id(self, TypeKind::None, "None");
        AbstractTraceWriter::register_return(self, ValueRecord::None { type_id: t });
    }
    fn label(&mut self, label: &str) -> u64 {
        self.ensure_marker_id(label).expect("rust ensure_marker_id")
    }
    fn mark_by_id(&mut self, id: u64, label: &str, direction: &str, key: &str, show: &str, description: &str, key_text: &str, show_text: &str) {
        self.register_correlation_marker_by_id(direction, id, label, key, show, description, key_text, show_text, None)
            .expect("rust register_correlation_marker_by_id");
    }
    fn mark(&mut self, direction: &str, label: &str, key: &str) {
        self.register_correlation_marker(direction, label, key, "", "", "", "", None)
            .expect("rust register_correlation_marker");
    }
    fn cover(&mut self, trace: &[u8; 16], span: &[u8; 8], wall: u64, mono: u64) {
        self.register_span_coverage(trace, span, wall, mono, 0, false, None)
            .expect("rust register_span_coverage");
    }
    fn cover_hex(&mut self, trace: &str, span: &str, wall: u64, mono: u64) {
        self.register_span_coverage_hex(trace, span, wall, mono, 0, false, None)
            .expect("rust register_span_coverage_hex");
    }
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    /// Line-only, two files sized by the line-count table, revisited lines.
    LineOnly,
    /// Column-aware: steps with columns and column moves.
    ColumnAware,
    /// 30,000 distinct positions: a tree three levels deep.
    ManyPositions,
    /// Markers of both kinds, labels interned before the first record and
    /// later, keys repeated, a payload needing escapes.
    Markers,
    /// A label interned and no marker declared.
    LabelOnly,
    /// No line hits and no markers.
    Neither,
}

fn drive<R: Recorder>(w: &mut R, s: Scenario) {
    match s {
        Scenario::LineOnly => {
            w.line_hits();
            w.counted(A, 20);
            w.counted(B, 30);
            for (path, line) in [(A, 1), (A, 2), (B, 7), (A, 2), (B, 30), (A, 2), (A, 20)] {
                w.step(path, line, None);
            }
        }
        Scenario::ColumnAware => {
            w.line_hits();
            w.table(A, &[40, 40, 40, 40]);
            w.table(B, &[10, 10]);
            // A column move right after a step is folded into that step by
            // the Nim C ABI, which buffers the step; a call in between
            // makes it a column step record in both writers.
            w.step(A, 1, None);
            w.step(A, 2, Some(5));
            w.call("f", A, 3);
            w.column_move(3);
            w.step(B, 2, Some(1));
            w.ret();
            w.step(A, 2, Some(5));
            w.call("g", A, 3);
            w.column_move(-2);
            w.step(A, 4, None);
            w.ret();
        }
        Scenario::ManyPositions => {
            w.line_hits();
            w.counted(A, 40_000);
            for line in 1..=30_000 {
                w.step(A, line, None);
            }
            w.step(A, 7, None);
        }
        Scenario::Markers => {
            w.counted(A, 50);
            let api = w.label("api-call");
            w.step(A, 1, None);
            w.mark_by_id(api, "api-call", "send", "order-42", "", "", "", "");
            w.step(A, 2, None);
            w.mark_by_id(api, "api-call", "recv", "order-42", "shown", "a description", "req.id", "");
            w.mark_by_id(api, "api-call", "receive", ODD_KEY, "", "", "", "resp");
            w.cover(&TRACE_ID, &SPAN_ID, 1_788_878_366_340_223_810, 2_032_454_727_205_762);
            w.step(A, 3, None);
            w.mark("sideways", "queue", "job-7");
            w.mark("send", "queue", "job-7");
            w.cover(&TRACE_ID, &SPAN_ID, 5, 6);
            w.cover_hex("00112233445566778899AABBCCDDEEFF", "0102030405060708", 7, 8);
            w.step(A, 4, None);
        }
        Scenario::LabelOnly => {
            w.counted(A, 5);
            w.label("unused");
            w.step(A, 1, None);
        }
        Scenario::Neither => {
            w.counted(A, 5);
            w.step(A, 1, None);
            w.step(A, 2, None);
        }
    }
}

fn nim_writer(dir: &Path, column_aware: bool) -> NimTraceWriter {
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.set_recording_id(RECORDING_ID).expect("nim set_recording_id");
    w.set_workdir(Path::new(WORKDIR));
    w.begin_writing_trace_events(&dir.join("trace.json")).expect("nim begin_events");
    w.begin_writing_trace_metadata(&dir.join("trace_metadata.json"))
        .expect("nim begin_metadata");
    w.begin_writing_trace_paths(&dir.join("trace_paths.json")).expect("nim begin_paths");
    if column_aware {
        w.enable_column_aware_steps();
    } else {
        w.enable_line_count_table().expect("nim enable_line_count_table");
    }
    w
}

fn rust_writer(dir: &Path, column_aware: bool) -> CtfsTraceWriter {
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    w.set_recording_id(RECORDING_ID);
    AbstractTraceWriter::set_workdir(&mut w, Path::new(WORKDIR));
    if column_aware {
        w.enable_column_aware_steps();
    }
    TraceWriter::begin_writing_trace_events(&mut w, &dir.join(PROGRAM)).expect("rust begin_events");
    if !column_aware {
        w.enable_line_count_table().expect("rust enable_line_count_table");
    }
    w
}

/// Write `s` with both writers; the Nim container's path, then the Rust one's.
fn write_both(root: &Path, s: Scenario) -> (PathBuf, PathBuf) {
    let column_aware = matches!(s, Scenario::ColumnAware);
    let (nd, rd) = (root.join(format!("{s:?}-nim")), root.join(format!("{s:?}-rust")));
    std::fs::create_dir_all(&nd).unwrap();
    std::fs::create_dir_all(&rd).unwrap();

    let mut n = nim_writer(&nd, column_aware);
    drive(&mut n, s);
    n.finish_writing_trace_events().expect("nim finish_events");
    n.finish_writing_trace_metadata().expect("nim finish_metadata");
    n.finish_writing_trace_paths().expect("nim finish_paths");
    n.close().expect("nim close");
    drop(n);

    let mut r = rust_writer(&rd, column_aware);
    drive(&mut r, s);
    TraceWriter::finish_writing_trace_events(&mut r).expect("rust finish");
    assert!(r.refusals().is_empty(), "{s:?}: the Rust writer refused {:?}", r.refusals());
    (nd.join(format!("{PROGRAM}.ct")), rd.join(format!("{PROGRAM}.ct")))
}

fn files(ct: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut r = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()));
    r.list_files()
        .into_iter()
        .map(|name| {
            let bytes = r.read_file(&name).unwrap_or_else(|e| panic!("{name}: {e:?}"));
            (name, bytes)
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn entry_json(key: u64, m: &codetracer_trace_reader::correlation_reader::CorrelationMarker) -> String {
    format!(
        "{{\"key\":{key},\"kind\":{},\"flags\":{},\"identity\":\"{}\",\"wall_time_unix_ns\":{},\"monotonic_time_ns\":{},\"geid\":{},\"thread_id\":{}}}",
        m.kind,
        m.flags,
        hex(&m.identity),
        m.wall_time_unix_ns,
        m.monotonic_time_ns,
        m.geid,
        m.thread_id
    )
}

fn answer<T>(r: Result<Option<T>, String>, render: impl FnOnce(T) -> Result<String, String>) -> MemberAnswer {
    match r {
        Ok(None) => MemberAnswer::Absent,
        Ok(Some(v)) => match render(v) {
            Ok(doc) => MemberAnswer::Document(doc),
            Err(e) => MemberAnswer::Refused(e),
        },
        Err(e) => MemberAnswer::Refused(e),
    }
}

/// The Rust reader's answers, in the Nim C ABI's document forms.
fn rust_line_hits(ct: &Path) -> MemberAnswer {
    let mut r = CtfsReader::open(ct).expect("open");
    answer(open_line_hits(&mut r), |lh| {
        let mut parts = Vec::new();
        for p in lh.positions() {
            let steps = lh.hits(p)?.expect("a listed position has hits");
            let steps: Vec<String> = steps.iter().map(u64::to_string).collect();
            parts.push(format!("{{\"position\":{p},\"steps\":[{}]}}", steps.join(",")));
        }
        Ok(format!("[{}]", parts.join(",")))
    })
}

fn rust_index(ct: &Path) -> MemberAnswer {
    let mut r = CtfsReader::open(ct).expect("open");
    answer(open_correlation_index(&mut r), |idx| {
        let all: Vec<String> = idx.entries()?.iter().map(|(k, m)| entry_json(*k, m)).collect();
        Ok(format!("[{}]", all.join(",")))
    })
}

fn rust_span_lookup(ct: &Path, trace: &[u8; 16], span: &[u8; 8]) -> MemberAnswer {
    let mut r = CtfsReader::open(ct).expect("open");
    let key = codetracer_trace_writer::corrmark::span_key(trace, span);
    answer(open_correlation_index(&mut r), |idx| {
        let hits: Vec<String> = idx.lookup_span(trace, span)?.iter().map(|m| entry_json(key, m)).collect();
        Ok(format!("[{}]", hits.join(",")))
    })
}

fn rust_boundary_lookup(ct: &Path, id: u64, key_value: &[u8]) -> MemberAnswer {
    let mut r = CtfsReader::open(ct).expect("open");
    let key = codetracer_trace_writer::corrmark::boundary_key(id, key_value);
    answer(open_correlation_index(&mut r), |idx| {
        let hits: Vec<String> = idx.lookup_boundary(id, key_value)?.iter().map(|m| entry_json(key, m)).collect();
        Ok(format!("[{}]", hits.join(",")))
    })
}

fn rust_labels(ct: &Path) -> MemberAnswer {
    let mut r = CtfsReader::open(ct).expect("open");
    answer(marker_labels(&mut r), |labels| {
        let all: Vec<String> = labels.iter().map(|l| format!("\"{}\"", hex(l))).collect();
        Ok(format!("[{}]", all.join(",")))
    })
}

/// Every read, by both readers, of `ct`.
fn reads(ct: &Path) -> Vec<(&'static str, MemberAnswer, MemberAnswer)> {
    let mut other_span = SPAN_ID;
    other_span[7] ^= 1;
    vec![
        ("line hits", rust_line_hits(ct), read_line_hits_json(ct)),
        ("correlation index", rust_index(ct), read_correlation_index_json(ct)),
        ("labels", rust_labels(ct), read_marker_labels_json(ct)),
        (
            "span lookup",
            rust_span_lookup(ct, &TRACE_ID, &SPAN_ID),
            lookup_correlation_span_json(ct, &TRACE_ID, &SPAN_ID),
        ),
        (
            "span near miss",
            rust_span_lookup(ct, &TRACE_ID, &other_span),
            lookup_correlation_span_json(ct, &TRACE_ID, &other_span),
        ),
        (
            "boundary lookup",
            rust_boundary_lookup(ct, 0, b"order-42"),
            lookup_correlation_boundary_json(ct, 0, b"order-42"),
        ),
        (
            "boundary lookup, escaped key",
            rust_boundary_lookup(ct, 0, ODD_KEY.as_bytes()),
            lookup_correlation_boundary_json(ct, 0, ODD_KEY.as_bytes()),
        ),
        (
            "boundary near miss on the label",
            rust_boundary_lookup(ct, 1, b"order-42"),
            lookup_correlation_boundary_json(ct, 1, b"order-42"),
        ),
        (
            "boundary near miss on the key",
            rust_boundary_lookup(ct, 0, b"order-43"),
            lookup_correlation_boundary_json(ct, 0, b"order-43"),
        ),
    ]
}

fn assert_same_container(nim: &Path, rust: &Path, s: Scenario) {
    let (a, b) = (files(nim), files(rust));
    let names = |m: &BTreeMap<String, Vec<u8>>| m.keys().cloned().collect::<Vec<_>>();
    assert_eq!(names(&a), names(&b), "{s:?}: the containers hold different members");
    for (name, bytes) in &a {
        assert!(
            bytes == &b[name],
            "{s:?}: {name} differs ({} Nim bytes, {} Rust bytes)",
            bytes.len(),
            b[name].len()
        );
    }
    let size = |p: &Path| std::fs::metadata(p).unwrap().len();
    assert_eq!(size(nim), size(rust), "{s:?}: the containers differ in size");
    let order = |p: &Path| CtfsReader::open(p).unwrap().list_files();
    assert_eq!(order(nim), order(rust), "{s:?}: the members are in a different order");
}

fn assert_read_alike(nim: &Path, rust: &Path, s: Scenario) {
    let (from_nim, from_rust) = (reads(nim), reads(rust));
    for ((what, rust_on_nim, nim_on_nim), (_, rust_on_rust, nim_on_rust)) in from_nim.iter().zip(&from_rust) {
        assert_eq!(
            rust_on_nim, nim_on_rust,
            "{s:?}, {what}: the Rust reader on the Nim container and the Nim reader on the Rust one"
        );
        assert_eq!(rust_on_nim, nim_on_nim, "{s:?}, {what}: the two readers on the Nim container");
        assert_eq!(rust_on_rust, nim_on_rust, "{s:?}, {what}: the two readers on the Rust container");
        if let MemberAnswer::Refused(e) = rust_on_nim {
            panic!("{s:?}, {what}: a well-formed container was refused: {e}");
        }
    }
}

fn check(s: Scenario) -> (PathBuf, PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let (nim, rust) = write_both(dir.path(), s);
    assert_same_container(&nim, &rust, s);
    assert_read_alike(&nim, &rust, s);
    (nim, rust, dir)
}

fn doc(a: MemberAnswer) -> String {
    match a {
        MemberAnswer::Document(d) => d,
        other => panic!("expected a document, got {other:?}"),
    }
}

#[test]
fn line_only_hits_are_written_and_read_alike() {
    let _g = nim_lock();
    let (nim, _, _d) = check(Scenario::LineOnly);
    // Files of 20 and 30 lines: a.py lines 1, 2, 20 are positions 0, 1, 19;
    // b.py lines 7, 30 are 26, 49. Exec records 0..6 in step order.
    assert_eq!(
        doc(rust_line_hits(&nim)),
        r#"[{"position":0,"steps":[0]},{"position":1,"steps":[1,3,5]},{"position":19,"steps":[6]},{"position":26,"steps":[2]},{"position":49,"steps":[4]}]"#
    );
}

#[test]
fn column_aware_hits_are_written_and_read_alike() {
    let _g = nim_lock();
    let (nim, _, _d) = check(Scenario::ColumnAware);
    let hits = doc(rust_line_hits(&nim));
    // Line 2 column 5 of a.py is position 40 + 4; the column move by 3 is
    // 47, and back by 2 from a revisit of 44 is 42.
    assert!(hits.contains(r#"{"position":44,"steps":[1,4]}"#), "{hits}");
    assert!(hits.contains(r#"{"position":47,"steps":[2]}"#), "{hits}");
    assert!(hits.contains(r#"{"position":42,"steps":[5]}"#), "{hits}");
}

#[test]
fn a_three_level_tree_is_written_and_read_alike() {
    let _g = nim_lock();
    let (nim, _, _d) = check(Scenario::ManyPositions);
    let mut r = CtfsReader::open(&nim).unwrap();
    let lh = open_line_hits(&mut r).unwrap().unwrap();
    assert_eq!(lh.position_count(), 30_000);
    assert_eq!(lh.hits(6).unwrap(), Some(vec![6, 30_000]));
    assert_eq!(lh.hits(30_000).unwrap(), None);
    let image = r.read_file("linehits.tc").unwrap();
    let root = u64::from_le_bytes(image[4..12].try_into().unwrap()) as usize;
    assert_eq!(image[root * 4096], 0, "the root is an internal node");
    let child = u64::from_le_bytes(image[root * 4096 + 16..root * 4096 + 24].try_into().unwrap()) as usize;
    assert_eq!(image[child * 4096], 0, "the root's children are internal nodes: three levels");
}

#[test]
fn markers_are_written_and_read_alike() {
    let _g = nim_lock();
    let (nim, _, _d) = check(Scenario::Markers);
    let all = doc(rust_index(&nim));
    assert_eq!(all.matches("\"kind\":0").count(), 3, "{all}");
    assert_eq!(all.matches("\"kind\":1").count(), 5, "{all}");
    assert_eq!(
        doc(rust_labels(&nim)),
        format!("[\"{}\",\"{}\"]", hex(b"api-call"), hex(b"queue")),
        "labels in id order"
    );
    // Two coverages of one span share a bucket, in identity-then-geid order.
    let spans = doc(rust_span_lookup(&nim, &TRACE_ID, &SPAN_ID));
    assert_eq!(spans.matches("\"kind\":0").count(), 2, "{spans}");
    assert!(spans.find("\"geid\":1").unwrap() < spans.find("\"geid\":2").unwrap(), "{spans}");
    // The send and the receive of order-42 share a bucket.
    assert_eq!(doc(rust_boundary_lookup(&nim, 0, b"order-42")).matches("\"kind\":1").count(), 2);
    // The MarkerPayload documents in events.dat are compared byte for byte by
    // the container comparison; here the escaped one is checked to be there.
    let events = files(&nim)["events.dat"].clone();
    assert!(!events.is_empty());
}

#[test]
fn a_label_with_no_marker_writes_labels_and_no_index() {
    let _g = nim_lock();
    let (nim, rust, _d) = check(Scenario::LabelOnly);
    for ct in [&nim, &rust] {
        assert_eq!(rust_index(ct), MemberAnswer::Absent);
        assert_eq!(read_correlation_index_json(ct), MemberAnswer::Absent);
        assert_eq!(doc(rust_labels(ct)), format!("[\"{}\"]", hex(b"unused")));
    }
}

#[test]
fn a_recording_with_neither_has_no_members_and_both_readers_say_so() {
    let _g = nim_lock();
    let (nim, rust, _d) = check(Scenario::Neither);
    for ct in [&nim, &rust] {
        let names = CtfsReader::open(ct).unwrap().list_files();
        for m in ["linehits.tc", "corrmark.ns", "markers.dat", "markers.off"] {
            assert!(!names.iter().any(|n| n == m), "{m} in {names:?}");
        }
        for (what, rust_answer, nim_answer) in reads(ct) {
            assert_eq!(rust_answer, MemberAnswer::Absent, "{what}");
            assert_eq!(nim_answer, MemberAnswer::Absent, "{what}");
        }
    }
}

/// Where the namespace image of `member` starts in the container file.
fn image_offset(ct: &Path, member: &str) -> usize {
    let image = CtfsReader::open(ct).unwrap().read_file(member).unwrap();
    let file = std::fs::read(ct).unwrap();
    let probe = &image[..4096.min(image.len())];
    file.windows(probe.len())
        .position(|w| w == probe)
        .expect("the image's first page is in the file")
}

/// Apply `damage` to `member`'s image inside a copy of `ct`, check it landed,
/// and return the copy.
fn damaged(ct: &Path, member: &str, name: &str, damage: &dyn Fn(&mut Vec<u8>)) -> PathBuf {
    let original = CtfsReader::open(ct).unwrap().read_file(member).unwrap();
    let mut want = original.clone();
    damage(&mut want);
    assert_eq!(want.len(), original.len(), "{name}: a damage keeps the member's size");
    let at = image_offset(ct, member);
    let mut file = std::fs::read(ct).unwrap();
    // The image's pages are contiguous in these small containers; the
    // read-back below proves it for each damage.
    file[at..at + want.len()].copy_from_slice(&want);
    let out = ct.with_file_name(format!("{name}.ct"));
    std::fs::write(&out, &file).unwrap();
    let landed = CtfsReader::open(&out).unwrap().read_file(member).unwrap();
    assert_eq!(landed, want, "{name}: the damage did not land where intended");
    out
}

fn put_u64(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn get_u64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// The offset of the first descriptor of the one-leaf tree's leaf (page 1).
fn first_descriptor(image: &[u8]) -> usize {
    let count = u16::from_le_bytes([image[4096 + 2], image[4096 + 3]]) as usize;
    4096 + 8 + count * 8
}

type Damage = (&'static str, Box<dyn Fn(&mut Vec<u8>)>);

fn namespace_damages() -> Vec<Damage> {
    vec![
        ("bad-magic", Box::new(|i: &mut Vec<u8>| i[3] = b'X')),
        ("leaf-type-a", Box::new(|i: &mut Vec<u8>| i[36] = 0b10)),
        ("unknown-flag", Box::new(|i: &mut Vec<u8>| i[36] |= 0b100)),
        ("page-count-past-image", Box::new(|i: &mut Vec<u8>| put_u64(i, 53, 1000))),
        ("root-past-pages", Box::new(|i: &mut Vec<u8>| put_u64(i, 4, 900))),
        ("node-kind", Box::new(|i: &mut Vec<u8>| i[4096] = 9)),
        ("node-reserved", Box::new(|i: &mut Vec<u8>| i[4096 + 6] = 1)),
        (
            "node-empty",
            Box::new(|i: &mut Vec<u8>| i[4096 + 2..4096 + 4].copy_from_slice(&0u16.to_le_bytes())),
        ),
        (
            "node-overfull",
            Box::new(|i: &mut Vec<u8>| i[4096 + 2..4096 + 4].copy_from_slice(&500u16.to_le_bytes())),
        ),
        ("commit-to-empty-slot", Box::new(|i: &mut Vec<u8>| put_u64(i, 28, 2))),
        (
            "descriptor-past-image",
            Box::new(|i: &mut Vec<u8>| {
                let d = first_descriptor(i);
                put_u64(i, d, u64::MAX - 4)
            }),
        ),
        (
            "descriptor-too-long",
            Box::new(|i: &mut Vec<u8>| {
                let d = first_descriptor(i);
                let n = i.len() as u64;
                put_u64(i, d + 8, n)
            }),
        ),
    ]
}

fn assert_both_refuse(ct: &Path, s: Scenario, name: &str) {
    for (what, rust_answer, nim_answer) in reads(ct) {
        if what != "line hits" && what != "correlation index" {
            continue;
        }
        let present = !matches!(nim_answer, MemberAnswer::Absent);
        if !present {
            continue;
        }
        assert!(
            matches!(rust_answer, MemberAnswer::Refused(_)),
            "{s:?}/{name}, {what}: the Rust reader accepted it: {rust_answer:?}"
        );
        assert!(
            matches!(nim_answer, MemberAnswer::Refused(_)),
            "{s:?}/{name}, {what}: the Nim reader accepted it: {nim_answer:?}"
        );
    }
}

#[test]
fn both_readers_refuse_the_same_damaged_line_hits() {
    let _g = nim_lock();
    let (nim, rust, _d) = check(Scenario::LineOnly);
    let mut damages = namespace_damages();
    // a.py line 2's list is three one-byte varints; a continuation bit on
    // the last runs it past the list.
    damages.push((
        "varint-past-list",
        Box::new(|i: &mut Vec<u8>| {
            let d = first_descriptor(i) + 16;
            let (off, len) = (get_u64(i, d) as usize, get_u64(i, d + 8) as usize);
            i[off + len - 1] |= 0x80;
        }),
    ));
    for ct in [&nim, &rust] {
        for (name, damage) in &damages {
            let bad = damaged(ct, "linehits.tc", name, damage.as_ref());
            assert_both_refuse(&bad, Scenario::LineOnly, name);
        }
    }
}

#[test]
fn both_readers_refuse_the_same_damaged_correlation_index() {
    let _g = nim_lock();
    let (nim, rust, _d) = check(Scenario::Markers);
    let mut damages = namespace_damages();
    damages.push((
        "bucket-truncated",
        Box::new(|i: &mut Vec<u8>| {
            let d = first_descriptor(i);
            let len = get_u64(i, d + 8);
            put_u64(i, d + 8, len - 1)
        }),
    ));
    damages.push((
        "bucket-shorter-than-descriptor",
        Box::new(|i: &mut Vec<u8>| {
            let d = first_descriptor(i);
            let len = get_u64(i, d + 8);
            put_u64(i, d + 8, len + 1)
        }),
    ));
    damages.push((
        "bucket-count-past-descriptor",
        Box::new(|i: &mut Vec<u8>| {
            let d = first_descriptor(i);
            let off = get_u64(i, d) as usize;
            i[off] += 1;
        }),
    ));
    for ct in [&nim, &rust] {
        for (name, damage) in &damages {
            let bad = damaged(ct, "corrmark.ns", name, damage.as_ref());
            assert_both_refuse(&bad, Scenario::Markers, name);
        }
    }
}
