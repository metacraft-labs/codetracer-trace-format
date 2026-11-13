//! The split-stream reader reports what the streams carry and
//! `TraceLowLevelEvent` cannot spell: a column-aware step's column, a call's
//! children and raised exception, `Raise`/`Catch` exec records, and source
//! reload markers with the path versions they mint.
//!
//! Every container is written by the real Rust writer and read back by the
//! real reader. No writer records a call's raised exception yet, so the one
//! fixture that needs it takes a Rust-written container and re-encodes its
//! `calls.dat` with the exception set, through the writer's own call-stream
//! encoder, into a real CTFS file. No mocks.

use std::path::{Path, PathBuf};

use codetracer_ctfs::{CtfsReader, CtfsWriter};
use codetracer_trace_reader::interning_tables_reader::PathVersion;
use codetracer_trace_reader::split_stream_reader::{
    CallDetail, ExceptionEventKind, SplitStreamTrace, read_trace_from_split_streams, read_trace_with_details, read_window_with_details,
};
use codetracer_trace_types::*;
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::call_stream::{CallStreamRecord, encode_call_stream};
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::step_stream::SourceReloadChange;
use codetracer_trace_writer::trace_writer::TraceWriter;

fn read(ct: &Path) -> SplitStreamTrace {
    let mut r = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()));
    let t = read_trace_with_details(&mut r).expect("split-stream read with details");
    // The details are beside the events, never instead of them: the event
    // sequence is the one the plain read returns.
    let mut r = CtfsReader::open(ct).unwrap();
    let plain = read_trace_from_split_streams(&mut r).expect("plain split-stream read");
    assert_eq!(format!("{:?}", t.events), format!("{plain:?}"), "the details read changed the events");
    t
}

fn step_event_indices(events: &[TraceLowLevelEvent]) -> Vec<usize> {
    events
        .iter()
        .enumerate()
        .filter_map(|(i, e)| matches!(e, TraceLowLevelEvent::Step(_)).then_some(i))
        .collect()
}

#[test]
fn a_column_aware_step_reports_its_column() {
    let dir = tempfile::tempdir().unwrap();
    let src = PathBuf::from("/src/a.js");
    let mut w = CtfsTraceWriter::new("cols", &[]);
    w.enable_column_aware_steps();
    let out = dir.path().join("cols");
    TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
    w.register_path_with_line_lengths(&src, &[12, 6, 40]);
    TraceWriter::start(&mut w, &src, Line(1));
    AbstractTraceWriter::register_step_with_column(&mut w, &src, Line(1), Some(Line(9)));
    AbstractTraceWriter::register_step_with_column(&mut w, &src, Line(3), Some(Line(17)));
    // A column-only move: three columns right, on line 3.
    w.register_column_step(3).expect("column step");
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();

    let t = read(&out.with_extension("ct"));
    let got: Vec<(usize, u64, u64)> = t.details.step_columns.iter().map(|c| (c.event_index, c.step_index, c.column)).collect();
    let steps = step_event_indices(&t.events);
    assert_eq!(steps.len(), 4, "the entry step and three recorded ones: {:?}", t.events);
    assert_eq!(
        got,
        vec![(steps[0], 0, 1), (steps[1], 1, 9), (steps[2], 2, 17), (steps[3], 3, 20)],
        "every step's column, tied to its Step event and exec record"
    );
}

#[test]
fn a_line_only_step_reports_no_column() {
    let dir = tempfile::tempdir().unwrap();
    let src = PathBuf::from("/src/a.py");
    let mut w = CtfsTraceWriter::new("lines", &[]);
    let out = dir.path().join("lines");
    TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
    TraceWriter::start(&mut w, &src, Line(1));
    TraceWriter::register_step(&mut w, &src, Line(2));
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
    let t = read(&out.with_extension("ct"));
    assert_eq!(step_event_indices(&t.events).len(), 2);
    assert!(t.details.step_columns.is_empty(), "a line-only position has no column");
}

/// `Raise` and `Catch` are exec records with no event; both writer modes write
/// them, so both are read.
#[test]
fn raise_and_catch_are_reported_where_they_occurred() {
    for column_aware in [false, true] {
        let label = if column_aware { "column-aware" } else { "line-only" };
        let dir = tempfile::tempdir().unwrap();
        let src = PathBuf::from("/src/e.py");
        let mut w = CtfsTraceWriter::new("exc", &[]);
        if column_aware {
            w.enable_column_aware_steps();
        }
        let out = dir.path().join("exc");
        TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
        if column_aware {
            w.register_path_with_line_lengths(&src, &[]);
        }
        TraceWriter::start(&mut w, &src, Line(1));
        TraceWriter::register_step(&mut w, &src, Line(2));
        w.register_raise(5, b"boom").expect("raise");
        w.register_catch(5).expect("catch");
        TraceWriter::register_step(&mut w, &src, Line(4));
        TraceWriter::finish_writing_trace_events(&mut w).unwrap();

        let t = read(&out.with_extension("ct"));
        let steps = step_event_indices(&t.events);
        assert_eq!(steps.len(), 3, "{label}: the two exception records are not steps");
        let got: Vec<(usize, u64, ExceptionEventKind)> = t
            .details
            .exception_events
            .iter()
            .map(|e| (e.event_index, e.step_index, e.kind.clone()))
            .collect();
        // Both sit after the step at line 2 (exec record 1) and before the
        // step at line 4, which is exec record 4.
        assert_eq!(
            got,
            vec![
                (
                    steps[1] + 1,
                    2,
                    ExceptionEventKind::Raise {
                        exception_type_id: 5,
                        message: b"boom".to_vec()
                    }
                ),
                (steps[1] + 1, 3, ExceptionEventKind::Catch { exception_type_id: 5 }),
            ],
            "{label}: Raise/Catch, in stream order, at their exec index"
        );
        assert_eq!(steps[2], steps[1] + 1, "{label}: no event stands in for them");
    }
}

/// `<toplevel>` (key 0) calls `main` (key 1), which calls `ok` (key 2) and
/// `bad` (key 3); `bad` ends by raising.
fn write_calls(dir: &Path) -> PathBuf {
    let src = Path::new("/src/c.py");
    let mut w = CtfsTraceWriter::new("calls", &[]);
    let out = dir.join("calls");
    TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
    TraceWriter::start(&mut w, src, Line(1));
    let int = TraceWriter::ensure_type_id(&mut w, TypeKind::Int, "Int");
    let main_fn = TraceWriter::ensure_function_id(&mut w, "main", src, Line(1));
    let ok_fn = TraceWriter::ensure_function_id(&mut w, "ok", src, Line(10));
    let bad_fn = TraceWriter::ensure_function_id(&mut w, "bad", src, Line(20));
    TraceWriter::register_call(&mut w, main_fn, vec![]);
    TraceWriter::register_step(&mut w, src, Line(2));
    TraceWriter::register_call(&mut w, ok_fn, vec![]);
    TraceWriter::register_step(&mut w, src, Line(11));
    TraceWriter::register_return(&mut w, ValueRecord::Int { i: 1, type_id: int });
    TraceWriter::register_call(&mut w, bad_fn, vec![]);
    TraceWriter::register_step(&mut w, src, Line(21));
    TraceWriter::register_return(&mut w, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::register_step(&mut w, src, Line(3));
    TraceWriter::register_return(&mut w, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
    out.with_extension("ct")
}

fn exception_value() -> ValueRecord {
    ValueRecord::Error {
        msg: "division by zero".to_string(),
        type_id: TypeId(0),
    }
}

/// Copy `ct` to a new container whose `calls.dat` gives `call_key` a raised
/// exception — every other file byte for byte.
fn with_raised_exception(ct: &Path, call_key: u64) -> PathBuf {
    let mut r = CtfsReader::open(ct).unwrap();
    let mut calls = codetracer_trace_reader::call_stream_reader::CallStreamReader::open(&mut r)
        .unwrap()
        .expect("calls.dat present");
    let mut records: Vec<CallStreamRecord> = calls.read_all().expect("calls");
    records[call_key as usize].raised_exception = cbor4ii::serde::to_vec(Vec::new(), &exception_value()).unwrap();
    let encoded = encode_call_stream(&records, 256, 3).unwrap();

    let out = ct.with_file_name("raised.ct");
    let mut c = CtfsWriter::create(&out, r.block_size(), 63).unwrap();
    for name in r.list_files() {
        let bytes = match name.as_str() {
            "calls.dat" => encoded.dat.clone(),
            "calls.idx" => encoded.idx.clone(),
            _ => r.read_file(&name).unwrap(),
        };
        let h = c.add_file(&name).unwrap();
        c.write(h, &bytes).unwrap();
    }
    c.close().unwrap();
    out
}

#[test]
fn a_call_reports_its_children_and_parent() {
    let dir = tempfile::tempdir().unwrap();
    let t = read(&write_calls(dir.path()));
    let tree: Vec<(u64, Option<u64>, u64, Vec<u64>)> = t
        .details
        .calls
        .iter()
        .map(|c| (c.call_key, c.parent_key, c.depth, c.children.clone()))
        .collect();
    assert_eq!(
        tree,
        vec![
            (0, None, 0, vec![1]),
            (1, Some(0), 1, vec![2, 3]),
            (2, Some(1), 2, vec![]),
            (3, Some(1), 2, vec![])
        ],
        "the call tree, in entry order"
    );
    for c in &t.details.calls {
        assert!(
            matches!(t.events[c.call_event_index], TraceLowLevelEvent::Call(_)),
            "call {} points at its Call event",
            c.call_key
        );
        let ret = c
            .return_event_index
            .unwrap_or_else(|| panic!("call {} returned within the read", c.call_key));
        assert!(
            matches!(t.events[ret], TraceLowLevelEvent::Return(_)),
            "call {} points at a Return",
            c.call_key
        );
        assert!(ret > c.call_event_index);
        assert_eq!(c.raised_exception, None, "call {} returned normally", c.call_key);
    }
}

#[test]
fn a_call_that_raised_reports_its_exception() {
    let dir = tempfile::tempdir().unwrap();
    let raised = with_raised_exception(&write_calls(dir.path()), 3);
    let t = read(&raised);
    let exceptions: Vec<(u64, Option<ValueRecord>)> = t.details.calls.iter().map(|c| (c.call_key, c.raised_exception.clone())).collect();
    assert_eq!(
        exceptions,
        vec![(0, None), (1, None), (2, None), (3, Some(exception_value()))],
        "only `bad` raised"
    );
}

#[test]
fn a_window_that_ends_inside_a_call_reports_no_return_for_it() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_calls(dir.path());
    let mut r = CtfsReader::open(&ct).unwrap();
    // Exec records: 0 entry, 1 line 2, 2 line 11, 3 line 21, 4 line 3. Read
    // records 1..3: `main` and `ok` enter, `ok` returns, `main` does not.
    let t = read_window_with_details(&mut r, 1, 2).expect("window");
    let got: Vec<(u64, bool)> = t.details.calls.iter().map(|c| (c.call_key, c.return_event_index.is_some())).collect();
    assert_eq!(got, vec![(1, false), (2, true)]);
    let CallDetail { children, .. } = &t.details.calls[0];
    assert_eq!(children, &vec![2, 3], "children are the record's, not only the window's");
}

#[test]
fn a_source_reload_is_reported_with_the_path_versions_it_minted() {
    let dir = tempfile::tempdir().unwrap();
    let game = PathBuf::from("/src/game.gd");
    let util = PathBuf::from("/src/util.gd");
    let mut w = CtfsTraceWriter::new("reload", &[]);
    let out = dir.path().join("reload");
    w.declare_source_reload().unwrap();
    TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
    w.enable_line_count_table().unwrap();
    w.register_path_with_line_count(&game, 12).unwrap();
    w.register_path_with_line_count(&util, 6).unwrap();
    AbstractTraceWriter::register_step(&mut w, &game, Line(1));
    AbstractTraceWriter::register_step(&mut w, &util, Line(4));
    let v2 = w.register_path_version(&game, 15).unwrap();
    let change = SourceReloadChange {
        old_path_id: 0,
        new_path_id: v2.0 as u64,
        generation: 2,
    };
    assert_eq!(w.register_source_reload(&[change], 1).unwrap(), 1);
    AbstractTraceWriter::register_step(&mut w, &game, Line(14));
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
    assert!(w.refusals().is_empty(), "{:?}", w.refusals());
    let ct = out.with_extension("ct");

    let t = read(&ct);
    let steps = step_event_indices(&t.events);
    assert_eq!(steps.len(), 3);
    let got: Vec<(usize, u64, u64, Vec<SourceReloadChange>, u64)> = t
        .details
        .source_reloads
        .iter()
        .map(|m| (m.event_index, m.step_index, m.reload_ordinal, m.changed.clone(), m.in_flight_frames))
        .collect();
    assert_eq!(
        got,
        vec![(steps[1] + 1, 2, 1, vec![change], 1)],
        "the marker, between the last step of the old version and the first of the new"
    );
    assert_eq!(
        t.details.path_versions,
        vec![
            PathVersion { ordinal: 0, count: 2 },
            PathVersion { ordinal: 0, count: 1 },
            PathVersion { ordinal: 1, count: 2 },
        ],
        "game.gd v1, util.gd, game.gd v2"
    );

    let mut r = CtfsReader::open(&ct).unwrap();
    let tables = codetracer_trace_reader::interning_tables_reader::InterningTablesReader::open(&mut r)
        .unwrap()
        .unwrap();
    assert_eq!(tables.path_ids_for(b"/src/game.gd").unwrap(), vec![0, 2], "oldest version first");
    assert_eq!(tables.path_ids_for(b"/src/util.gd").unwrap(), vec![1]);
    assert!(tables.path_ids_for(b"/src/none.gd").unwrap().is_empty());
}

#[test]
fn a_trace_without_reloads_has_one_version_per_path() {
    let dir = tempfile::tempdir().unwrap();
    let t = read(&write_calls(dir.path()));
    assert!(t.details.source_reloads.is_empty());
    assert!(!t.details.path_versions.is_empty());
    assert!(t.details.path_versions.iter().all(|v| *v == PathVersion { ordinal: 0, count: 1 }));
}
