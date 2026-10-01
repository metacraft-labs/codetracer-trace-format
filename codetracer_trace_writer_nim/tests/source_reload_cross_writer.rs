//! Both writers record a source reload the same way, and each reader reads the
//! other writer's container.
//!
//! A reload is the one fixture that exercises every piece of `meta.dat` schema
//! version 5 at once (`codetracer-trace-format-spec`):
//!
//! * the per-file line-count table (`internal-files.md` §"`paths.dat`
//!   line-count table"), which path versions require;
//! * a second `paths.dat` record for the same path — a version — with its own
//!   size (§"`paths.dat` path versions");
//! * a `SourceReload` exec record, tag 0x08 (`trace-events.md` §"Source Reload
//!   Marker (Tag 0x08)"), which must advance the exec-record index every other
//!   stream is expressed in;
//! * `flags_ext` bit 0 and schema version 5 (§"Extended flags").
//!
//! The same operations are driven through the Nim writer (via its C ABI) and
//! the pure-Rust `CtfsTraceWriter`, and the containers are compared:
//!
//! * byte for byte where the two writers share an encoder — `paths.dat`,
//!   `paths.off`, `funcs.dat`, `values.dat`, `values.idx`, and `meta.dat` apart
//!   from the minted `recording_id`;
//! * record for record elsewhere — `steps.dat`, `calls.dat`, `events.dat` —
//!   because the two writers choose between `AbsoluteStep` and `DeltaStep`
//!   differently (a documented divergence, see `writer_differential.rs`), which
//!   changes bytes and not what a record decodes to;
//! * through each reader: the Rust split-stream reader reads both containers
//!   to the same event sequence, and the production Nim reader (`ct-print
//!   --full`) reads both to the same document.
//!
//! No mocks: every container is produced by a real writer and read by a real
//! reader.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::call_stream_reader::CallStreamReader;
use codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader;
use codetracer_trace_reader::step_stream_reader::StepStreamReader;
use codetracer_trace_types::{EventLogKind, FunctionId, Line, TraceLowLevelEvent, TypeKind, ValueRecord};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::meta_dat::{decode_meta_dat, FLAG_EXT_HAS_SOURCE_RELOAD, FLAG_HAS_LINE_COUNT_TABLE, META_DAT_VERSION};
use codetracer_trace_writer::step_stream::{SourceReloadChange, StepStreamRecord};
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "reload_parity";
const GAME: &str = "/src/game.gd";
const UTIL: &str = "/src/util.gd";

/// Which writer to drive.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Writer {
    Nim,
    Rust,
}

/// The operations, identical for both writers:
///
/// 1. table on; `game.gd` (12 lines) and `util.gd` (6 lines); a function in
///    `util.gd`; a call;
/// 2. steps in both files with a value each and an I/O event;
/// 3. `game.gd` reloaded as a 15-line version; the marker; one frame in flight;
/// 4. a step at line 14 — legal only in the new version — and a value staged
///    after a thread switch, which belongs to the next step;
/// 5. a return, closing the call across the reload.
fn record(writer: Writer, dir: &Path) -> PathBuf {
    let game = PathBuf::from(GAME);
    let util = PathBuf::from(UTIL);
    let int = |tid, i| ValueRecord::Int { i, type_id: tid };
    match writer {
        Writer::Nim => {
            let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
            w.begin_writing_trace_events(&dir.join("e.json")).expect("nim begin_events");
            w.begin_writing_trace_metadata(&dir.join("m.json")).expect("nim begin_metadata");
            w.begin_writing_trace_paths(&dir.join("p.json")).expect("nim begin_paths");
            w.enable_line_count_table().expect("nim enable_line_count_table");
            w.register_path_with_line_count(&game, 12).expect("nim game");
            w.register_path_with_line_count(&util, 6).expect("nim util");
            w.register_function("helper", &util, Line(3));
            let tid = w.ensure_type_id(TypeKind::Int, "Int");
            w.register_call(FunctionId(0), vec![]);
            w.register_step(&game, Line(1));
            w.register_variable_with_full_value("v", int(tid, 1));
            w.register_step(&util, Line(4));
            w.register_variable_with_full_value("v", int(tid, 2));
            w.register_special_event(EventLogKind::Write, "", "before");

            let v2 = w.register_path_version(&game, 15).expect("nim register_path_version");
            let ordinal = w
                .register_source_reload(
                    &[codetracer_trace_writer_nim::SourceReloadChange {
                        old_path_id: 0,
                        new_path_id: v2.0 as u64,
                        generation: 2,
                    }],
                    1,
                )
                .expect("nim register_source_reload");
            assert_eq!(ordinal, 1);

            w.register_step(&game, Line(14));
            w.register_special_event(EventLogKind::Write, "", "after");
            w.register_thread_switch(7);
            w.register_variable_with_full_value("v", int(tid, 3));
            w.register_step(&game, Line(2));
            w.register_return(ValueRecord::None { type_id: tid });

            w.finish_writing_trace_events().expect("nim finish_events");
            w.finish_writing_trace_metadata().expect("nim finish_metadata");
            w.finish_writing_trace_paths().expect("nim finish_paths");
            w.close().expect("nim close");
            drop(w);
            dir.join(format!("{PROGRAM}.ct"))
        }
        Writer::Rust => {
            let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
            let out = dir.join(PROGRAM);
            w.declare_source_reloads().expect("rust declare_source_reloads");
            TraceWriter::begin_writing_trace_events(&mut w, &out).expect("rust begin_events");
            w.enable_line_count_table().expect("rust enable_line_count_table");
            w.register_path_with_line_count(&game, 12).expect("rust game");
            w.register_path_with_line_count(&util, 6).expect("rust util");
            AbstractTraceWriter::register_function(&mut w, "helper", &util, Line(3));
            let tid = AbstractTraceWriter::ensure_type_id(&mut w, TypeKind::Int, "Int");
            AbstractTraceWriter::register_call(&mut w, FunctionId(0), vec![]);
            AbstractTraceWriter::register_step(&mut w, &game, Line(1));
            AbstractTraceWriter::register_variable_with_full_value(&mut w, "v", int(tid, 1));
            AbstractTraceWriter::register_step(&mut w, &util, Line(4));
            AbstractTraceWriter::register_variable_with_full_value(&mut w, "v", int(tid, 2));
            AbstractTraceWriter::register_special_event(&mut w, EventLogKind::Write, "", "before");

            let v2 = w.register_path_version(&game, 15).expect("rust register_path_version");
            let ordinal = w
                .register_source_reload(
                    &[SourceReloadChange {
                        old_path_id: 0,
                        new_path_id: v2.0 as u64,
                        generation: 2,
                    }],
                    1,
                )
                .expect("rust register_source_reload");
            assert_eq!(ordinal, 1);

            AbstractTraceWriter::register_step(&mut w, &game, Line(14));
            AbstractTraceWriter::register_special_event(&mut w, EventLogKind::Write, "", "after");
            AbstractTraceWriter::add_event(&mut w, TraceLowLevelEvent::ThreadSwitch(codetracer_trace_types::ThreadId(7)));
            AbstractTraceWriter::register_variable_with_full_value(&mut w, "v", int(tid, 3));
            AbstractTraceWriter::register_step(&mut w, &game, Line(2));
            AbstractTraceWriter::register_return(&mut w, ValueRecord::None { type_id: tid });

            TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish_events");
            assert!(w.refusals().is_empty(), "the fixture is legal; refused: {:?}", w.refusals());
            out.with_extension("ct")
        }
    }
}

/// The source paths of a container, from `paths.dat` — the only list of them
/// (`internal-files.md` §"`meta.dat` carries no path list").
fn paths_of(ct: &Path) -> Vec<String> {
    let mut r = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()));
    let t = codetracer_trace_reader::interning_tables_reader::InterningTablesReader::open(&mut r)
        .expect("interning tables decode")
        .expect("interning tables present");
    (0..t.path_count() as u64).map(|i| t.path_str(i).expect("path")).collect()
}

fn read_internal(ct: &Path, name: &str) -> Vec<u8> {
    CtfsReader::open(ct)
        .unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()))
        .read_file(name)
        .unwrap_or_else(|e| panic!("{name} in {}: {e:?}", ct.display()))
}

fn steps(ct: &Path) -> Vec<StepStreamRecord> {
    let mut r = CtfsReader::open(ct).expect("open");
    StepStreamReader::open(&mut r)
        .expect("steps.dat decodes")
        .expect("steps.dat present")
        .read_all()
        .expect("read_all")
}

fn calls(ct: &Path) -> Vec<(u64, u64, u64)> {
    let mut r = CtfsReader::open(ct).expect("open");
    let mut c = CallStreamReader::open(&mut r).expect("calls.dat decodes").expect("calls.dat present");
    (0..c.count())
        .map(|k| {
            let rec = c.read(k).expect("call");
            (rec.function_id, rec.first_step_id, rec.last_step_id)
        })
        .collect()
}

fn io_events(ct: &Path) -> Vec<(u64, Vec<u8>)> {
    let mut r = CtfsReader::open(ct).expect("open");
    let mut e = IoEventStreamReader::open(&mut r)
        .expect("events.dat decodes")
        .expect("events.dat present");
    (0..e.count())
        .map(|i| {
            let rec = e.read(i).expect("event");
            (rec.step_id, rec.content)
        })
        .collect()
}

fn both(dir: &Path) -> (PathBuf, PathBuf) {
    let nd = dir.join("nim");
    let rd = dir.join("rust");
    std::fs::create_dir_all(&nd).unwrap();
    std::fs::create_dir_all(&rd).unwrap();
    (record(Writer::Nim, &nd), record(Writer::Rust, &rd))
}

/// The exec records the fixture must produce, from either writer.
fn expected_steps() -> Vec<StepStreamRecord> {
    // game.gd v1 is file 0 (12 lines), util.gd file 1 (6), game.gd v2 file 2
    // (15): bases 0, 12, 18.
    vec![
        StepStreamRecord::Step { global_line_index: 0 },
        StepStreamRecord::Step { global_line_index: 12 + 3 },
        StepStreamRecord::SourceReload {
            reload_ordinal: 1,
            changed: vec![SourceReloadChange {
                old_path_id: 0,
                new_path_id: 2,
                generation: 2,
            }],
            in_flight_frames: 1,
        },
        StepStreamRecord::Step { global_line_index: 18 + 13 },
        StepStreamRecord::ThreadSwitch { thread_id: 7 },
        StepStreamRecord::Step { global_line_index: 18 + 1 },
    ]
}

#[test]
fn both_writers_record_the_reload_as_the_spec_states() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let (nim, rust) = both(dir.path());

    for (label, ct) in [("nim", &nim), ("rust", &rust)] {
        let meta = decode_meta_dat(&read_internal(ct, "meta.dat")).unwrap_or_else(|e| panic!("{label}: meta.dat: {e}"));
        assert_eq!(meta.version, META_DAT_VERSION, "{label}: meta.dat version");
        assert_eq!(meta.ext_flags, FLAG_EXT_HAS_SOURCE_RELOAD, "{label}: flags_ext");
        assert!(meta.flags & FLAG_HAS_LINE_COUNT_TABLE != 0, "{label}: bit 14");
        assert_eq!(
            paths_of(ct),
            vec![GAME, UTIL, GAME],
            "{label}: the version is a second record for the same path"
        );
        assert_eq!(steps(ct), expected_steps(), "{label}: exec records");
    }
}

#[test]
fn both_writers_produce_the_same_container() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let (nim, rust) = both(dir.path());

    for name in ["paths.dat", "paths.off", "funcs.dat", "funcs.off", "values.dat", "values.idx"] {
        let (a, b) = (read_internal(&nim, name), read_internal(&rust, name));
        assert!(!a.is_empty(), "{name} is empty — nothing compared");
        assert_eq!(a, b, "{name} differs between the writers");
    }

    let mut ma = decode_meta_dat(&read_internal(&nim, "meta.dat")).expect("nim meta");
    let mut mb = decode_meta_dat(&read_internal(&rust, "meta.dat")).expect("rust meta");
    ma.recording_id.clear();
    mb.recording_id.clear();
    ma.workdir.clear();
    mb.workdir.clear();
    assert_eq!(ma, mb, "meta.dat differs beyond the minted recording_id and the workdir");

    assert_eq!(steps(&nim), steps(&rust), "steps.dat decodes differently");
    assert_eq!(calls(&nim), calls(&rust), "calls.dat step ids differ");
    assert_eq!(
        calls(&nim),
        vec![(0, 0, 5)],
        "the call spans every exec record, the marker and thread switch included"
    );
    assert_eq!(io_events(&nim), io_events(&rust), "events.dat step ids differ");
    assert_eq!(
        io_events(&nim),
        vec![(1, b"before".to_vec()), (3, b"after".to_vec())],
        "I/O is attributed by exec-record index, and the marker is one"
    );
}

#[test]
fn the_rust_reader_reads_both_containers_alike() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let (nim, rust) = both(dir.path());
    let read = |ct: &Path| {
        let mut r = CtfsReader::open(ct).expect("open");
        codetracer_trace_reader::split_stream_reader::read_trace_from_split_streams(&mut r).expect("split-stream read")
    };
    let (a, b) = (read(&nim), read(&rust));
    // `TraceLowLevelEvent` has no `PartialEq`; its `Debug` rendering is a
    // complete, field-by-field projection.
    assert_eq!(
        format!("{a:#?}"),
        format!("{b:#?}"),
        "the two containers read back to different event sequences"
    );
    let steps: Vec<_> = a
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some((s.path_id.0, s.line.0)),
            _ => None,
        })
        .collect();
    assert_eq!(
        steps,
        vec![(0, 1), (1, 4), (2, 14), (2, 2)],
        "steps resolve to the version they were written against"
    );
}

/// The production Nim reader reads the Rust container to the same document it
/// reads the Nim container to.
#[test]
fn the_nim_reader_reads_both_containers_alike() {
    let _g = nim_lock();
    let ct_print = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../codetracer-trace-format-nim/ct-print");
    assert!(
        ct_print.exists(),
        "{} is not built; this is the only check that the Nim reader accepts a Rust-written reload",
        ct_print.display()
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let (nim, rust) = both(dir.path());
    let doc = |ct: &Path| -> serde_json::Value {
        let out = Command::new(&ct_print).arg("--full").arg(ct).output().expect("run ct-print");
        assert!(
            out.status.success(),
            "ct-print --full {} failed: {}",
            ct.display(),
            String::from_utf8_lossy(&out.stderr)
        );
        let mut v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("ct-print emits JSON");
        blank_volatile(&mut v);
        v
    };
    let (a, b) = (doc(&nim), doc(&rust));
    let reloads = a.pointer("/counts/source_reloads").and_then(|v| v.as_i64());
    assert_eq!(reloads, Some(1), "ct-print must report the marker; document: {a}");
    let mut diffs = Vec::new();
    json_diff(&a, &b, String::new(), &mut diffs);
    assert!(
        diffs.is_empty(),
        "the Nim reader reads the two containers differently (nim vs rust): {diffs:#?}"
    );
}

/// Every JSON pointer at which `a` and `b` differ, with both values.
fn json_diff(a: &serde_json::Value, b: &serde_json::Value, at: String, out: &mut Vec<String>) {
    use serde_json::Value;
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let keys: std::collections::BTreeSet<&String> = x.keys().chain(y.keys()).collect();
            for k in keys {
                match (x.get(k), y.get(k)) {
                    (Some(p), Some(q)) => json_diff(p, q, format!("{at}/{k}"), out),
                    (p, q) => out.push(format!("{at}/{k}: {p:?} vs {q:?}")),
                }
            }
        }
        (Value::Array(x), Value::Array(y)) if x.len() == y.len() => {
            for (i, (p, q)) in x.iter().zip(y).enumerate() {
                json_diff(p, q, format!("{at}/{i}"), out);
            }
        }
        _ if a != b => out.push(format!("{at}: {a} vs {b}")),
        _ => {}
    }
}

/// Remove the fields that legitimately differ between two recordings of the
/// same operations: the minted recording id and the workdir.
///
/// `step_kind` — whether a step was encoded `AbsoluteStep` or `DeltaStep` — is
/// removed too. The two writers choose between the two differently, both
/// differently from the spec's encoding rules (`trace-events.md` §"Encoding
/// Rules"; see `writer_differential.rs`), and the choice changes no decoded
/// position. It is an open divergence, not an equivalence this test asserts.
fn blank_volatile(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(m) => {
            for key in ["recording_id", "workdir", "step_kind"] {
                m.remove(key);
            }
            for (_, child) in m.iter_mut() {
                blank_volatile(child);
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(blank_volatile),
        _ => {}
    }
}

/// Thread records are exec records in both writers: each owns a value record
/// and advances the index `calls.dat` and `events.dat` use, and a value staged
/// after one belongs to the next step. Uses only the line-only API every
/// writer has had, so it measures the writers rather than the reload feature.
#[test]
fn thread_records_occupy_an_exec_slot_in_both_writers() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let src = PathBuf::from("/src/threads.rb");
    let int = |tid, i| ValueRecord::Int { i, type_id: tid };

    let nim = {
        let d = dir.path().join("nim");
        std::fs::create_dir_all(&d).unwrap();
        let mut w = NimTraceWriter::new("threads", &[], TraceEventsFileFormat::Ctfs);
        w.begin_writing_trace_events(&d.join("e.json")).unwrap();
        w.begin_writing_trace_metadata(&d.join("m.json")).unwrap();
        w.begin_writing_trace_paths(&d.join("p.json")).unwrap();
        w.register_function("f", &src, Line(1));
        let tid = w.ensure_type_id(TypeKind::Int, "Int");
        w.register_call(FunctionId(0), vec![]);
        w.register_step(&src, Line(1));
        w.register_variable_with_full_value("v", int(tid, 1));
        w.register_thread_switch(2);
        w.register_variable_with_full_value("v", int(tid, 2));
        w.register_step(&src, Line(2));
        w.register_special_event(EventLogKind::Write, "", "io");
        w.register_step(&src, Line(3));
        w.register_return(ValueRecord::None { type_id: tid });
        w.finish_writing_trace_events().unwrap();
        w.finish_writing_trace_metadata().unwrap();
        w.finish_writing_trace_paths().unwrap();
        w.close().unwrap();
        drop(w);
        d.join("threads.ct")
    };
    let rust = {
        let d = dir.path().join("rust");
        std::fs::create_dir_all(&d).unwrap();
        let mut w = CtfsTraceWriter::new("threads", &[]);
        let out = d.join("threads");
        TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
        AbstractTraceWriter::register_function(&mut w, "f", &src, Line(1));
        let tid = AbstractTraceWriter::ensure_type_id(&mut w, TypeKind::Int, "Int");
        AbstractTraceWriter::register_call(&mut w, FunctionId(0), vec![]);
        AbstractTraceWriter::register_step(&mut w, &src, Line(1));
        AbstractTraceWriter::register_variable_with_full_value(&mut w, "v", int(tid, 1));
        AbstractTraceWriter::add_event(&mut w, TraceLowLevelEvent::ThreadSwitch(codetracer_trace_types::ThreadId(2)));
        AbstractTraceWriter::register_variable_with_full_value(&mut w, "v", int(tid, 2));
        AbstractTraceWriter::register_step(&mut w, &src, Line(2));
        AbstractTraceWriter::register_special_event(&mut w, EventLogKind::Write, "", "io");
        AbstractTraceWriter::register_step(&mut w, &src, Line(3));
        AbstractTraceWriter::register_return(&mut w, ValueRecord::None { type_id: tid });
        TraceWriter::finish_writing_trace_events(&mut w).unwrap();
        out.with_extension("ct")
    };

    for name in ["values.dat", "values.idx"] {
        assert_eq!(
            read_internal(&nim, name),
            read_internal(&rust, name),
            "{name} differs between the writers"
        );
    }
    assert_eq!(calls(&nim), vec![(0, 0, 3)], "nim: the call spans the thread switch's exec record");
    assert_eq!(calls(&nim), calls(&rust), "calls.dat step ids differ");
    assert_eq!(
        io_events(&nim),
        vec![(2, b"io".to_vec())],
        "nim: I/O after the second step is at exec record 2"
    );
    assert_eq!(io_events(&nim), io_events(&rust), "events.dat step ids differ");
}

/// A reload marker in a container that does not declare
/// `FLAG_EXT_HAS_SOURCE_RELOAD` is refused by name — not skipped — by the
/// Rust reader, and the same stream is accepted under its real header.
#[test]
fn an_undeclared_reload_marker_is_refused_by_name() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let (nim, _rust) = both(dir.path());
    let dat = read_internal(&nim, "steps.dat");
    let idx = read_internal(&nim, "steps.idx");

    let declared = read_internal(&nim, "meta.dat");
    let mut r = StepStreamReader::from_files(&declared, dat.clone(), idx.clone())
        .expect("the declared stream opens")
        .expect("present");
    assert_eq!(
        r.read_all().expect("reads").len(),
        expected_steps().len(),
        "control: the declared stream reads whole"
    );

    let undeclared = codetracer_trace_writer::meta_dat::encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", PROGRAM, &[], "", "", 0);
    let err = match StepStreamReader::from_files(&undeclared, dat, idx) {
        Err(e) => e,
        Ok(Some(mut r)) => r.read_all().expect_err("an undeclared tag 8 must be refused, not skipped"),
        Ok(None) => panic!("steps.dat vanished"),
    };
    assert!(
        err.contains("SourceReload") && err.contains("FLAG_EXT_HAS_SOURCE_RELOAD"),
        "the refusal must name the tag and the missing flag; got: {err}"
    );
}

/// Layout for [`function_before_its_file`].
#[derive(Clone, Copy, Debug, PartialEq)]
enum FnLayout {
    ColumnAware,
    LineCountTable,
}

/// A function declared in a file that is registered only LATER (with its
/// per-line table, or its line count), and a function in a file never
/// registered at all (line-count mode excepted, where that is refused), with a
/// call into each. Both writers write the declaration records at close, so
/// the path ids follow the recorder's registrations and the later
/// registration keeps its table; and neither adds a step for a call.
fn function_before_its_file(writer: Writer, layout: FnLayout, dir: &Path) -> PathBuf {
    let main = PathBuf::from("/src/main.ex");
    let nested = PathBuf::from("/src/nested.ex");
    let orphan = PathBuf::from("/src/orphan.ex");
    let program = format!("fn_before_file_{layout:?}");
    let with_orphan = layout == FnLayout::ColumnAware;
    match writer {
        Writer::Nim => {
            let mut w = NimTraceWriter::new(&program, &[], TraceEventsFileFormat::Ctfs);
            w.begin_writing_trace_events(&dir.join("e.json")).unwrap();
            w.begin_writing_trace_metadata(&dir.join("m.json")).unwrap();
            w.begin_writing_trace_paths(&dir.join("p.json")).unwrap();
            match layout {
                FnLayout::ColumnAware => {
                    w.enable_column_aware_steps();
                    w.register_path_with_line_lengths(&main, &[5, 5, 5]).unwrap();
                }
                FnLayout::LineCountTable => {
                    w.enable_line_count_table().unwrap();
                    w.register_path_with_line_count(&main, 3).unwrap();
                }
            }
            w.start(&main, Line(1));
            let f = w.ensure_function_id("nested_fn", &nested, Line(2));
            if with_orphan {
                w.ensure_function_id("orphan_fn", &orphan, Line(4));
            }
            w.register_step(&main, Line(1));
            w.register_call(f, vec![]);
            match layout {
                FnLayout::ColumnAware => {
                    w.register_path_with_line_lengths(&nested, &[7, 7, 7, 7]).unwrap();
                }
                FnLayout::LineCountTable => {
                    w.register_path_with_line_count(&nested, 4).unwrap();
                }
            }
            w.register_step(&nested, Line(2));
            w.register_step(&nested, Line(3));
            w.register_return(ValueRecord::None {
                type_id: codetracer_trace_types::NONE_TYPE_ID,
            });
            w.register_step(&main, Line(2));
            w.finish_writing_trace_events().unwrap();
            w.finish_writing_trace_metadata().unwrap();
            w.finish_writing_trace_paths().unwrap();
            w.close().expect("nim close");
            drop(w);
            dir.join(format!("{program}.ct"))
        }
        Writer::Rust => {
            let mut w = CtfsTraceWriter::new(&program, &[]);
            if layout == FnLayout::ColumnAware {
                w.enable_column_aware_steps();
            }
            let out = dir.join(&program);
            TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
            match layout {
                FnLayout::ColumnAware => {
                    w.register_path_with_line_lengths(&main, &[5, 5, 5]);
                }
                FnLayout::LineCountTable => {
                    w.enable_line_count_table().unwrap();
                    w.register_path_with_line_count(&main, 3).unwrap();
                }
            }
            TraceWriter::start(&mut w, &main, Line(1));
            let f = AbstractTraceWriter::ensure_function_id(&mut w, "nested_fn", &nested, Line(2));
            if with_orphan {
                AbstractTraceWriter::ensure_function_id(&mut w, "orphan_fn", &orphan, Line(4));
            }
            AbstractTraceWriter::register_step(&mut w, &main, Line(1));
            AbstractTraceWriter::register_call(&mut w, f, vec![]);
            match layout {
                FnLayout::ColumnAware => {
                    w.register_path_with_line_lengths(&nested, &[7, 7, 7, 7]);
                }
                FnLayout::LineCountTable => {
                    w.register_path_with_line_count(&nested, 4).unwrap();
                }
            }
            AbstractTraceWriter::register_step(&mut w, &nested, Line(2));
            AbstractTraceWriter::register_step(&mut w, &nested, Line(3));
            AbstractTraceWriter::register_return(
                &mut w,
                ValueRecord::None {
                    type_id: codetracer_trace_types::NONE_TYPE_ID,
                },
            );
            AbstractTraceWriter::register_step(&mut w, &main, Line(2));
            TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish");
            out.with_extension("ct")
        }
    }
}

#[test]
fn a_function_before_its_file_is_written_alike_by_both_writers() {
    let _g = nim_lock();
    for layout in [FnLayout::ColumnAware, FnLayout::LineCountTable] {
        let dir = tempfile::tempdir().expect("tempdir");
        let nd = dir.path().join("nim");
        let rd = dir.path().join("rust");
        std::fs::create_dir_all(&nd).unwrap();
        std::fs::create_dir_all(&rd).unwrap();
        let nim = function_before_its_file(Writer::Nim, layout, &nd);
        let rust = function_before_its_file(Writer::Rust, layout, &rd);

        let (mn, mr) = (paths_of(&nim), paths_of(&rust));
        let mut want = vec!["/src/main.ex", "/src/nested.ex"];
        if layout == FnLayout::ColumnAware {
            want.push("/src/orphan.ex");
        }
        assert_eq!(mn, want, "{layout:?}: nim path ids follow the recorder's registrations");
        assert_eq!(mr, mn, "{layout:?}: path ids differ between the writers");
        for name in ["paths.dat", "paths.off", "funcs.dat", "funcs.off"] {
            assert_eq!(
                read_internal(&nim, name),
                read_internal(&rust, name),
                "{layout:?}: {name} differs — the later registration's table or count was lost, or ids moved"
            );
        }
        let (sn, sr) = (steps(&nim), steps(&rust));
        assert_eq!(
            sn.len(),
            5,
            "{layout:?}: the entry step and four recorded steps, and no step for the call; nim wrote {sn:?}"
        );
        assert_eq!(sr.len(), sn.len(), "{layout:?}: step counts differ: rust {sr:?}");
        assert_eq!(calls(&nim), calls(&rust), "{layout:?}: calls.dat differs");
    }
}

/// A call registered while the previous step is still pending — right after
/// `start`, and in a recording that never called `start` — begins at the
/// callee's first step in both writers (`calls.dat` `first_step_id`, "First
/// step in this call").
#[test]
fn a_call_after_a_pending_step_begins_at_the_callee_s_first_step_in_both_writers() {
    let _g = nim_lock();
    let main = PathBuf::from("/src/main.ex");
    let f_src = PathBuf::from("/src/f.ex");
    for with_start in [true, false] {
        let dir = tempfile::tempdir().expect("tempdir");
        let program = format!("pending_call_{with_start}");
        let nim = {
            let mut w = NimTraceWriter::new(&program, &[], TraceEventsFileFormat::Ctfs);
            w.begin_writing_trace_events(&dir.path().join("e.json")).unwrap();
            w.begin_writing_trace_metadata(&dir.path().join("m.json")).unwrap();
            w.begin_writing_trace_paths(&dir.path().join("p.json")).unwrap();
            if with_start {
                w.start(&main, Line(1));
            } else {
                w.register_step(&main, Line(1));
            }
            let f = w.ensure_function_id("f", &f_src, Line(3));
            w.register_call(f, vec![]);
            w.register_step(&f_src, Line(3));
            w.register_step(&f_src, Line(4));
            w.register_return(ValueRecord::None {
                type_id: codetracer_trace_types::NONE_TYPE_ID,
            });
            w.finish_writing_trace_events().unwrap();
            w.finish_writing_trace_metadata().unwrap();
            w.finish_writing_trace_paths().unwrap();
            w.close().expect("nim close");
            drop(w);
            dir.path().join(format!("{program}.ct"))
        };
        let rust = {
            let rd = dir.path().join("rust");
            std::fs::create_dir_all(&rd).unwrap();
            let mut w = CtfsTraceWriter::new(&program, &[]);
            let out = rd.join(&program);
            TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
            if with_start {
                TraceWriter::start(&mut w, &main, Line(1));
            } else {
                AbstractTraceWriter::register_step(&mut w, &main, Line(1));
            }
            let f = AbstractTraceWriter::ensure_function_id(&mut w, "f", &f_src, Line(3));
            AbstractTraceWriter::register_call(&mut w, f, vec![]);
            AbstractTraceWriter::register_step(&mut w, &f_src, Line(3));
            AbstractTraceWriter::register_step(&mut w, &f_src, Line(4));
            AbstractTraceWriter::register_return(
                &mut w,
                ValueRecord::None {
                    type_id: codetracer_trace_types::NONE_TYPE_ID,
                },
            );
            TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish");
            out.with_extension("ct")
        };
        let (cn, cr) = (calls(&nim), calls(&rust));
        let f_call = cn.last().copied().expect("nim wrote f's call");
        assert_eq!(
            (f_call.1, f_call.2),
            (1, 2),
            "with_start={with_start}: f begins at its own first step (1) and ends at 2; nim wrote {cn:?}"
        );
        assert_eq!(cn, cr, "with_start={with_start}: calls.dat differs between the writers");
        assert_eq!(steps(&nim), steps(&rust), "with_start={with_start}: steps differ");
    }
}
