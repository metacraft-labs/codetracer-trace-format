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
//! # What is compared, and the only exclusions
//!
//! Every internal file of either container must be present in both and
//! byte-identical, with exactly these exceptions:
//!
//! * **`steps.dat` / `steps.idx`**: the two writers choose between
//!   `AbsoluteStep` and `DeltaStep` differently (the Nim writer uses a ±63/64
//!   delta window without forcing an absolute step after a call or return; the
//!   Rust writer forces one after a call, a return and a thread switch). That
//!   difference is documented and awaiting a decision, so it is excluded here.
//!   The two streams must still decode to the same records.
//! * **`meta.dat`**: compared field by field with `recording_id` cleared,
//!   because each writer mints a fresh UUIDv7. `workdir` is set to the same
//!   value on both writers, so it is compared like every other field.
//!
//! On top of the per-file comparison the containers' total sizes must agree
//! once the step-stream files' blocks are taken out, so a divergence in CTFS
//! block allocation is caught even where every file matches.
//!
//! No mocks: both containers are produced by the real writers and read back
//! through the real CTFS and stream readers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::step_stream_reader::StepStreamReader;
use codetracer_trace_types::{
    CallRecord, FullValueRecord, FunctionId, FunctionRecord, Line, PathId, ReturnRecord, StepRecord, ThreadId, TraceLowLevelEvent, TypeId, TypeKind,
    TypeRecord, TypeSpecificInfo, ValueRecord, VariableId,
};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::meta_dat::decode_meta_dat;
use codetracer_trace_writer::step_stream::StepStreamRecord;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Loop iterations of the mix. 20,000 gives 2,500 steps and 2,500 thread
/// switches (5,000 exec records: two `steps.dat` chunks), twenty `values.dat`
/// chunks, ten `calls.dat` chunks, and `paths.dat` / `funcs.dat` of several
/// blocks each.
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

/// The files whose bytes legitimately differ because of the step-encoding
/// choice (see the module header).
const STEP_ENCODING_FILES: [&str; 2] = ["steps.dat", "steps.idx"];

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

fn write_nim(drive: Drive, dir: &Path) -> PathBuf {
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.join("trace.json")).expect("nim begin_events");
    w.begin_writing_trace_metadata(&dir.join("trace_metadata.json"))
        .expect("nim begin_metadata");
    w.begin_writing_trace_paths(&dir.join("trace_paths.json")).expect("nim begin_paths");
    w.set_workdir(Path::new(WORKDIR));
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

/// Blocks an internal file of `len` bytes occupies: its mapping block plus its
/// data blocks. Both writers give every file a mapping block, including a
/// file of at most one block.
fn blocks_of(len: usize) -> usize {
    1 + len.div_ceil(4096)
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

    let mut differing = Vec::new();
    for (name, bytes) in &nim {
        if STEP_ENCODING_FILES.contains(&name.as_str()) || name == "meta.dat" {
            continue;
        }
        if rust[name] != *bytes {
            differing.push(name.clone());
        }
    }
    assert!(
        differing.is_empty(),
        "{drive:?}: files differ between the writers: {differing:?}\n{sizes}"
    );

    let mut mn = decode_meta_dat(&nim["meta.dat"]).expect("nim meta.dat");
    let mut mr = decode_meta_dat(&rust["meta.dat"]).expect("rust meta.dat");
    mn.recording_id.clear();
    mr.recording_id.clear();
    assert_eq!(mn, mr, "{drive:?}: meta.dat differs beyond the minted recording_id\n{sizes}");

    assert_eq!(
        steps(&nim_ct),
        steps(&rust_ct),
        "{drive:?}: the execution streams decode to different records"
    );

    let step_blocks = |f: &BTreeMap<String, Vec<u8>>| STEP_ENCODING_FILES.iter().map(|n| blocks_of(f[*n].len())).sum::<usize>();
    let len = |p: &Path| std::fs::metadata(p).expect("container").len() as usize;
    assert_eq!(
        len(&nim_ct) - 4096 * step_blocks(&nim),
        len(&rust_ct) - 4096 * step_blocks(&rust),
        "{drive:?}: the containers differ in size beyond the step-stream files (nim {} bytes, rust {} bytes)\n{sizes}",
        len(&nim_ct),
        len(&rust_ct)
    );
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
