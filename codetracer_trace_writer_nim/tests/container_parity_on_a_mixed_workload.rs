//! The two writers produce the same container for the same recording, file for
//! file and byte for byte, on a workload large enough to cross every chunk
//! boundary.
//!
//! # Why this test exists
//!
//! The other cross-writer tests in this crate (`writer_differential.rs`,
//! `source_reload_cross_writer.rs`, `paths_dat_framing_cross_reader.rs`, ...)
//! drive a handful of events and compare the streams they target. That is
//! what let two writers that each passed all of them still produce containers
//! of materially different size on the benchmark mix: a whole internal file
//! written by one writer and not the other, an interning table that one
//! writer filled eagerly and the other lazily, and values attributed to
//! different steps all decode cleanly, so a test that only decodes cannot see
//! them. This test compares the whole file set and every byte of it.
//!
//! # The workload
//!
//! `WASM-WRITER-MIX-1`, the eight-phase cycle of
//! `tracing-formats-benchmarks/wasm_writer` (step, call, return, value, path,
//! function, a no-op slot, thread switch), preceded by a prologue that declares
//! the types, paths and functions the loop refers to. It is driven twice:
//!
//! * through the recorder-facing API (`register_step(path, line)`,
//!   `ensure_function_id`, `register_variable_with_full_value`, ...), which is
//!   what recorders call;
//! * through `add_event` with `TraceLowLevelEvent`s, which is what the
//!   benchmark calls.
//!
//! [`ITERATIONS`] is chosen so that every chunked stream spans several chunks
//! (values and calls chunk at 256 records, steps at 4096) and several interning
//! tables outgrow one CTFS block.
//!
//! The loop is preceded by one thread switch, so execution-stream records
//! alternate thread switch / step with the switches on even indices, and
//! `steps.dat`'s second chunk (record 4096) opens with a thread record: its
//! first position must still be an `AbsoluteStep` (`trace-events.md`
//! §"Encoding Rules"). It is followed by an epilogue that writes one I/O event
//! of every `EventLogKind` (0-13) and one value event of every value-stream
//! tag (0-9), so neither table can be narrowed by one writer unnoticed.
//!
//! # What is compared
//!
//! Every internal file of either container must be present in both and
//! byte-identical — `steps.dat`, `steps.idx` and `meta.dat` included. Both
//! writers are given the same `recording_id`, so `meta.dat` has no field that
//! differs by construction (`internal-files.md` §"Metadata": every field is
//! fixed when the trace opens). The containers' total sizes must agree too,
//! so a divergence in CTFS block allocation is caught even where every file
//! matches.
//!
//! No mocks: both containers are produced by the real writers and read back
//! through the real CTFS and stream readers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::step_stream_reader::StepStreamReader;
use codetracer_trace_types::{
    AssignCellRecord, AssignCompoundItemRecord, AssignmentRecord, BindVariableRecord, CallRecord, CellValueRecord, CompoundValueRecord, EventLogKind,
    FullValueRecord, FunctionId, FunctionRecord, Line, PassBy, PathId, Place, RValue, RecordEvent, ReturnRecord, StepRecord, ThreadId,
    TraceLowLevelEvent, TypeId, TypeKind, TypeRecord, TypeSpecificInfo, ValueRecord, VariableCellRecord, VariableId,
};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::step_stream::StepStreamRecord;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Loop iterations of the mix. 20,000 gives 2,500 steps and 2,500 thread
/// switches (with the leading switch, 5,001 exec records: two `steps.dat`
/// chunks), twenty `values.dat` chunks, ten `calls.dat` chunks, and
/// `paths.dat` / `funcs.dat` of several blocks each.
const ITERATIONS: usize = 20_000;
const PATH_COUNT: usize = 50;
const FUNCTION_COUNT: usize = 100;
const TYPE_COUNT: usize = 8;
const VARIABLE_COUNT: usize = 200;
/// Every integer value in the mix carries this type id; [`TYPE_COUNT`] types
/// are declared first so that it resolves.
const INT_TYPE_ID: usize = 7;
const PROGRAM: &str = "mix";
const WORKDIR: &str = "/work/mix";
/// Given to both writers, so `meta.dat` is byte-comparable.
const RECORDING_ID: &str = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb";
/// The thread the leading switch selects.
const FIRST_THREAD: u64 = 9;
/// Exec-record index of `steps.dat`'s second chunk.
const SECOND_CHUNK: u64 = 4096;

/// Every `EventLogKind`, in ordinal order.
const ALL_KINDS: [EventLogKind; 14] = [
    EventLogKind::Write,
    EventLogKind::WriteFile,
    EventLogKind::WriteOther,
    EventLogKind::Read,
    EventLogKind::ReadFile,
    EventLogKind::ReadOther,
    EventLogKind::ReadDir,
    EventLogKind::OpenDir,
    EventLogKind::CloseDir,
    EventLogKind::Socket,
    EventLogKind::Open,
    EventLogKind::Error,
    EventLogKind::TraceLogEvent,
    EventLogKind::EvmEvent,
];

#[derive(Clone, Copy, Debug, PartialEq)]
enum Drive {
    /// The recorder-facing API.
    RecorderApi,
    /// `add_event` with low-level events.
    Events,
}

fn src(i: usize) -> PathBuf {
    PathBuf::from(format!("/src/file_{i}.nr"))
}

fn int(i: i64) -> ValueRecord {
    ValueRecord::Int {
        i,
        type_id: TypeId(INT_TYPE_ID),
    }
}

/// A path the loop's path phase registers. Distinct from every prologue path:
/// the low-level `Path` event IS the interning (its id is its position among
/// the `Path` events), so a repeated path there would be a second record, not a
/// lookup, and the interning tables are deduplicated.
fn late(i: usize) -> PathBuf {
    PathBuf::from(format!("/src/late_{i}.nr"))
}

/// The mix as low-level events. Function names are unique for the same reason
/// paths are (see [`late`]).
fn events() -> Vec<TraceLowLevelEvent> {
    let mut ev = Vec::new();
    for k in 0..TYPE_COUNT {
        ev.push(TraceLowLevelEvent::Type(TypeRecord {
            kind: TypeKind::Int,
            lang_type: format!("i64_{k}"),
            specific_info: TypeSpecificInfo::None,
        }));
    }
    for k in 0..VARIABLE_COUNT {
        ev.push(TraceLowLevelEvent::VariableName(format!("var_{k}")));
    }
    for i in 0..PATH_COUNT {
        ev.push(TraceLowLevelEvent::Path(src(i)));
    }
    for i in 0..FUNCTION_COUNT {
        ev.push(TraceLowLevelEvent::Function(FunctionRecord {
            path_id: PathId(i % PATH_COUNT),
            line: Line((i % 500) as i64),
            name: format!("fn_{i}"),
        }));
    }
    ev.push(TraceLowLevelEvent::ThreadSwitch(ThreadId(FIRST_THREAD)));
    for i in 0..ITERATIONS {
        match i % 8 {
            0 => ev.push(TraceLowLevelEvent::Step(StepRecord {
                path_id: PathId(i % PATH_COUNT),
                line: Line((i % 1000) as i64),
            })),
            1 => ev.push(TraceLowLevelEvent::Call(CallRecord {
                function_id: FunctionId(i % FUNCTION_COUNT),
                args: vec![],
            })),
            2 => ev.push(TraceLowLevelEvent::Return(ReturnRecord { return_value: int(i as i64) })),
            3 => ev.push(TraceLowLevelEvent::Value(FullValueRecord {
                variable_id: VariableId(i % VARIABLE_COUNT),
                value: int((i as i64) * 3),
            })),
            4 => ev.push(TraceLowLevelEvent::Path(late(i))),
            5 => ev.push(TraceLowLevelEvent::Function(FunctionRecord {
                path_id: PathId(i % PATH_COUNT),
                line: Line((i % 500) as i64),
                name: format!("func_{i}"),
            })),
            6 => {}
            _ => ev.push(TraceLowLevelEvent::ThreadSwitch(ThreadId((i % 4) as u64))),
        }
    }
    ev.extend(epilogue_events());
    ev
}

/// The same mix through the recorder-facing API. One macro body for both
/// writers, so the two call sequences cannot drift apart.
macro_rules! drive_recorder_api {
    ($w:expr, $ensure_type:path, $ensure_path:path, $ensure_fn:path, $step:path, $call:path, $ret:path, $var:path, $switch:expr) => {{
        for k in 0..TYPE_COUNT {
            let _ = $ensure_type($w, TypeKind::Int, &format!("i64_{k}"));
        }
        for i in 0..PATH_COUNT {
            let _ = $ensure_path($w, &src(i));
        }
        for i in 0..FUNCTION_COUNT {
            let _ = $ensure_fn($w, &format!("fn_{i}"), &src(i % PATH_COUNT), Line((i % 500) as i64));
        }
        $switch($w, FIRST_THREAD);
        for i in 0..ITERATIONS {
            match i % 8 {
                0 => $step($w, &src(i % PATH_COUNT), Line((i % 1000) as i64)),
                1 => $call($w, FunctionId(i % FUNCTION_COUNT), vec![]),
                2 => $ret($w, int(i as i64)),
                3 => $var($w, &format!("var_{}", i % VARIABLE_COUNT), int((i as i64) * 3)),
                4 => {
                    let _ = $ensure_path($w, &late(i));
                }
                5 => {
                    let _ = $ensure_fn($w, &format!("func_{i}"), &src(i % PATH_COUNT), Line((i % 500) as i64));
                }
                6 => {}
                _ => $switch($w, (i % 4) as u64),
            }
        }
    }};
}

fn var(k: usize) -> String {
    format!("var_{k}")
}

/// The epilogue as low-level events: a step, one I/O event of every
/// `EventLogKind`, one value event of every value-stream tag (0-9), a step.
/// Variable ids are the prologue's `VariableName` interning order.
fn epilogue_events() -> Vec<TraceLowLevelEvent> {
    let mut ev = vec![TraceLowLevelEvent::Step(StepRecord {
        path_id: PathId(0),
        line: Line(2),
    })];
    for (k, kind) in ALL_KINDS.iter().enumerate() {
        ev.push(TraceLowLevelEvent::Event(RecordEvent {
            kind: *kind,
            metadata: format!("m{k}"),
            content: format!("c{k}"),
        }));
    }
    ev.extend([
        TraceLowLevelEvent::Value(FullValueRecord {
            variable_id: VariableId(0),
            value: int(1),
        }),
        TraceLowLevelEvent::BindVariable(BindVariableRecord {
            variable_id: VariableId(1),
            place: Place(5),
        }),
        TraceLowLevelEvent::DropVariable(VariableId(2)),
        TraceLowLevelEvent::DropVariables(vec![VariableId(3), VariableId(4)]),
        TraceLowLevelEvent::CellValue(CellValueRecord {
            place: Place(6),
            value: int(2),
        }),
        TraceLowLevelEvent::CompoundValue(CompoundValueRecord {
            place: Place(-7),
            value: int(3),
        }),
        TraceLowLevelEvent::AssignCell(AssignCellRecord {
            place: Place(6),
            new_value: int(4),
        }),
        TraceLowLevelEvent::AssignCompoundItem(AssignCompoundItemRecord {
            place: Place(-7),
            index: 1,
            item_place: Place(6),
        }),
        TraceLowLevelEvent::VariableCell(VariableCellRecord {
            variable_id: VariableId(5),
            place: Place(8),
        }),
        TraceLowLevelEvent::Assignment(AssignmentRecord {
            to: VariableId(6),
            pass_by: PassBy::Value,
            from: RValue::Simple(VariableId(0)),
        }),
        TraceLowLevelEvent::Step(StepRecord {
            path_id: PathId(0),
            line: Line(3),
        }),
    ]);
    ev
}

/// The recorder-facing operations of the epilogue, implemented once per
/// writer so the two call sequences cannot drift apart.
trait Epilogue {
    fn step(&mut self, path: &Path, line: i64);
    fn io(&mut self, kind: EventLogKind, metadata: &str, content: &str);
    fn value(&mut self, name: &str, value: ValueRecord);
    fn bind(&mut self, name: &str, place: Place);
    fn drop_one(&mut self, name: &str);
    fn drop_many(&mut self, names: &[String]);
    fn cell(&mut self, place: Place, value: ValueRecord);
    fn compound(&mut self, place: Place, value: ValueRecord);
    fn set_cell(&mut self, place: Place, value: ValueRecord);
    fn set_item(&mut self, place: Place, index: usize, item_place: Place);
    fn var_cell(&mut self, name: &str, place: Place);
    fn assign(&mut self, name: &str, rvalue: RValue);
}

impl Epilogue for NimTraceWriter {
    fn step(&mut self, path: &Path, line: i64) {
        self.register_step(path, Line(line));
    }
    fn io(&mut self, kind: EventLogKind, metadata: &str, content: &str) {
        self.register_special_event(kind, metadata, content);
    }
    fn value(&mut self, name: &str, value: ValueRecord) {
        self.register_variable_with_full_value(name, value);
    }
    fn bind(&mut self, name: &str, place: Place) {
        self.bind_variable(name, place);
    }
    fn drop_one(&mut self, name: &str) {
        self.drop_variable(name);
    }
    fn drop_many(&mut self, names: &[String]) {
        self.drop_variables(names);
    }
    fn cell(&mut self, place: Place, value: ValueRecord) {
        self.register_cell_value(place, value);
    }
    fn compound(&mut self, place: Place, value: ValueRecord) {
        self.register_compound_value(place, value);
    }
    fn set_cell(&mut self, place: Place, value: ValueRecord) {
        NimTraceWriter::assign_cell(self, place, value);
    }
    fn set_item(&mut self, place: Place, index: usize, item_place: Place) {
        self.assign_compound_item(place, index, item_place);
    }
    fn var_cell(&mut self, name: &str, place: Place) {
        self.register_variable(name, place);
    }
    fn assign(&mut self, name: &str, rvalue: RValue) {
        NimTraceWriter::assign(self, name, rvalue, PassBy::Value);
    }
}

impl Epilogue for CtfsTraceWriter {
    fn step(&mut self, path: &Path, line: i64) {
        AbstractTraceWriter::register_step(self, path, Line(line));
    }
    fn io(&mut self, kind: EventLogKind, metadata: &str, content: &str) {
        AbstractTraceWriter::register_special_event(self, kind, metadata, content);
    }
    fn value(&mut self, name: &str, value: ValueRecord) {
        AbstractTraceWriter::register_variable_with_full_value(self, name, value);
    }
    fn bind(&mut self, name: &str, place: Place) {
        AbstractTraceWriter::bind_variable(self, name, place);
    }
    fn drop_one(&mut self, name: &str) {
        AbstractTraceWriter::drop_variable(self, name);
    }
    fn drop_many(&mut self, names: &[String]) {
        AbstractTraceWriter::drop_variables(self, names);
    }
    fn cell(&mut self, place: Place, value: ValueRecord) {
        AbstractTraceWriter::register_cell_value(self, place, value);
    }
    fn compound(&mut self, place: Place, value: ValueRecord) {
        AbstractTraceWriter::register_compound_value(self, place, value);
    }
    fn set_cell(&mut self, place: Place, value: ValueRecord) {
        AbstractTraceWriter::assign_cell(self, place, value);
    }
    fn set_item(&mut self, place: Place, index: usize, item_place: Place) {
        AbstractTraceWriter::assign_compound_item(self, place, index, item_place);
    }
    fn var_cell(&mut self, name: &str, place: Place) {
        AbstractTraceWriter::register_variable(self, name, place);
    }
    fn assign(&mut self, name: &str, rvalue: RValue) {
        AbstractTraceWriter::assign(self, name, rvalue, PassBy::Value);
    }
}

/// The epilogue through the recorder-facing API: the same records as
/// [`epilogue_events`].
fn epilogue_api(w: &mut impl Epilogue) {
    w.step(&src(0), 2);
    for (k, kind) in ALL_KINDS.iter().enumerate() {
        w.io(*kind, &format!("m{k}"), &format!("c{k}"));
    }
    w.value(&var(0), int(1));
    w.bind(&var(1), Place(5));
    w.drop_one(&var(2));
    w.drop_many(&[var(3), var(4)]);
    w.cell(Place(6), int(2));
    w.compound(Place(-7), int(3));
    w.set_cell(Place(6), int(4));
    w.set_item(Place(-7), 1, Place(6));
    w.var_cell(&var(5), Place(8));
    // The variable an `RValue::Simple` names is the recorder's id for it,
    // which in this mix is its varnames interning order.
    w.assign(&var(6), RValue::Simple(VariableId(0)));
    w.step(&src(0), 3);
}

fn write_nim(drive: Drive, dir: &Path) -> PathBuf {
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.set_recording_id(RECORDING_ID).expect("nim set_recording_id");
    w.set_workdir(Path::new(WORKDIR));
    w.begin_writing_trace_events(&dir.join("trace.json")).expect("nim begin_events");
    w.begin_writing_trace_metadata(&dir.join("trace_metadata.json"))
        .expect("nim begin_metadata");
    w.begin_writing_trace_paths(&dir.join("trace_paths.json")).expect("nim begin_paths");
    match drive {
        Drive::Events => {
            for e in events() {
                w.add_event(e);
            }
        }
        Drive::RecorderApi => drive_recorder_api!(
            &mut w,
            NimTraceWriter::ensure_type_id,
            NimTraceWriter::ensure_path_id,
            NimTraceWriter::ensure_function_id,
            NimTraceWriter::register_step,
            NimTraceWriter::register_call,
            NimTraceWriter::register_return,
            NimTraceWriter::register_variable_with_full_value,
            |w: &mut NimTraceWriter, t| w.register_thread_switch(t)
        ),
    }
    if drive == Drive::RecorderApi {
        epilogue_api(&mut w);
    }
    w.finish_writing_trace_events().expect("nim finish_events");
    w.finish_writing_trace_metadata().expect("nim finish_metadata");
    w.finish_writing_trace_paths().expect("nim finish_paths");
    w.close().expect("nim close");
    assert!(
        w.discarded_record_counts().is_empty(),
        "{drive:?}: the Nim writer discarded records: {:?}",
        w.discarded_record_counts()
    );
    drop(w);
    dir.join(format!("{PROGRAM}.ct"))
}

fn write_rust(drive: Drive, dir: &Path) -> PathBuf {
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    w.set_recording_id(RECORDING_ID);
    AbstractTraceWriter::set_workdir(&mut w, Path::new(WORKDIR));
    let out = dir.join(PROGRAM);
    TraceWriter::begin_writing_trace_events(&mut w, &out).expect("rust begin_events");
    match drive {
        Drive::Events => {
            for e in events() {
                AbstractTraceWriter::add_event(&mut w, e);
            }
        }
        Drive::RecorderApi => drive_recorder_api!(
            &mut w,
            AbstractTraceWriter::ensure_type_id,
            AbstractTraceWriter::ensure_path_id,
            AbstractTraceWriter::ensure_function_id,
            AbstractTraceWriter::register_step,
            AbstractTraceWriter::register_call,
            AbstractTraceWriter::register_return,
            AbstractTraceWriter::register_variable_with_full_value,
            |w: &mut CtfsTraceWriter, t| AbstractTraceWriter::thread_switch(w, ThreadId(t))
        ),
    }
    if drive == Drive::RecorderApi {
        epilogue_api(&mut w);
    }
    TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish_events");
    assert!(w.refusals().is_empty(), "{drive:?}: the Rust writer refused: {:?}", w.refusals());
    out.with_extension("ct")
}

fn files(ct: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut r = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()));
    r.list_files()
        .into_iter()
        .map(|name| {
            let bytes = r.read_file(&name).unwrap_or_else(|e| panic!("{name} in {}: {e:?}", ct.display()));
            (name, bytes)
        })
        .collect()
}

fn steps(ct: &Path) -> Vec<StepStreamRecord> {
    let mut r = CtfsReader::open(ct).expect("open");
    StepStreamReader::open(&mut r)
        .expect("steps.dat decodes")
        .expect("steps.dat present")
        .read_all()
        .expect("read_all")
}

/// A per-file size table, printed on failure so a divergence names its stream.
fn table(nim: &BTreeMap<String, Vec<u8>>, rust: &BTreeMap<String, Vec<u8>>) -> String {
    let mut names: Vec<&String> = nim.keys().chain(rust.keys()).collect();
    names.sort();
    names.dedup();
    let mut out = format!("{:<14}{:>10}{:>10}  same\n", "file", "nim", "rust");
    for n in names {
        let a = nim.get(n);
        let b = rust.get(n);
        let fmt = |v: Option<&Vec<u8>>| v.map(|x| x.len().to_string()).unwrap_or_else(|| "-".into());
        out += &format!("{:<14}{:>10}{:>10}  {}\n", n, fmt(a), fmt(b), a.is_some() && a == b);
    }
    out
}

fn assert_containers_match(drive: Drive) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (nd, rd) = (dir.path().join("nim"), dir.path().join("rust"));
    std::fs::create_dir_all(&nd).unwrap();
    std::fs::create_dir_all(&rd).unwrap();
    let nim_ct = write_nim(drive, &nd);
    let rust_ct = write_rust(drive, &rd);
    let (nim, rust) = (files(&nim_ct), files(&rust_ct));
    let sizes = table(&nim, &rust);

    let nim_names: Vec<&String> = nim.keys().collect();
    let rust_names: Vec<&String> = rust.keys().collect();
    assert_eq!(nim_names, rust_names, "{drive:?}: the two containers hold different files\n{sizes}");

    let differing: Vec<&String> = nim.keys().filter(|name| rust[*name] != nim[*name]).collect();
    assert!(
        differing.is_empty(),
        "{drive:?}: files differ between the writers: {differing:?}\n{sizes}"
    );

    // The fixture exercises what it claims to, in the Rust container (which
    // the Nim one equals byte for byte by now): chunk 1 of steps.dat opens
    // with a thread record, and every kind and value tag is present.
    let records = steps(&rust_ct);
    assert!(records.len() as u64 > SECOND_CHUNK, "{drive:?}: steps.dat holds one chunk only");
    assert!(
        matches!(records[SECOND_CHUNK as usize], StepStreamRecord::ThreadSwitch { .. }),
        "{drive:?}: record {SECOND_CHUNK} is {:?}, not a thread record",
        records[SECOND_CHUNK as usize]
    );
    assert_eq!(steps(&nim_ct), records, "{drive:?}: the execution streams decode to different records");
    assert_eq!(
        io_kinds(&rust_ct),
        (0..14u8).collect::<Vec<_>>(),
        "{drive:?}: every EventLogKind, in order"
    );
    assert_eq!(value_tags(&rust_ct), (0..10u8).collect::<Vec<_>>(), "{drive:?}: every value-stream tag");

    let len = |p: &Path| std::fs::metadata(p).expect("container").len();
    assert_eq!(
        len(&nim_ct),
        len(&rust_ct),
        "{drive:?}: the containers differ in size (block allocation)\n{sizes}"
    );
}

/// The kinds of `events.dat`'s records, in order.
fn io_kinds(ct: &Path) -> Vec<u8> {
    let mut r = CtfsReader::open(ct).expect("open");
    let mut io = codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader::open(&mut r)
        .expect("events.dat decodes")
        .expect("events.dat present");
    (0..io.count()).map(|i| io.read(i).expect("event").kind).collect()
}

/// The distinct value-stream tags `values.dat` carries, ascending.
fn value_tags(ct: &Path) -> Vec<u8> {
    use codetracer_trace_writer::value_stream::ValueStreamEvent as E;
    let mut r = CtfsReader::open(ct).expect("open");
    let all = codetracer_trace_reader::value_stream_reader::ValueStreamReader::open(&mut r)
        .expect("values.dat decodes")
        .expect("values.dat present")
        .read_all()
        .expect("read_all");
    let mut tags: Vec<u8> = all
        .iter()
        .flat_map(|rec| rec.events.iter())
        .map(|e| match e {
            E::StepValues { values } if values.is_empty() => u8::MAX,
            E::StepValues { .. } => 0,
            E::BindVariable { .. } => 1,
            E::DropVariable { .. } => 2,
            E::DropVariables { .. } => 3,
            E::CellValue { .. } => 4,
            E::CompoundValue { .. } => 5,
            E::AssignCell { .. } => 6,
            E::AssignCompoundItem { .. } => 7,
            E::VariableCell { .. } => 8,
            E::Assignment { .. } => 9,
            _ => u8::MAX - 1,
        })
        .filter(|t| *t != u8::MAX)
        .collect();
    tags.sort_unstable();
    tags.dedup();
    tags
}

#[test]
fn the_recorder_api_mix_yields_the_same_container_from_both_writers() {
    let _g = nim_lock();
    assert_containers_match(Drive::RecorderApi);
}

#[test]
fn the_event_mix_yields_the_same_container_from_both_writers() {
    let _g = nim_lock();
    assert_containers_match(Drive::Events);
}
