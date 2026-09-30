//! Three rules the pure-Rust `CtfsTraceWriter` shares with the canonical Nim
//! writer, each asserted on a container read back from disk.
//!
//! 1. **Values staged after a call or a return belong to the next step.**
//!    `trace-events.md` §"Recorder Integration — Staging Values": the next
//!    step, call, or return closes a step, and values registered while no step
//!    is open attach to the next step the writer emits. `let x = f()` reaches
//!    the writer as `step / call / … / return / variable x`, and `x` is visible
//!    at the step after the return, not at the step before the call. A writer
//!    that attached it to the step before the call would show `x` holding
//!    `f()`'s result before `f` ran.
//! 2. **A line-only trace carries `step-map.ns`**, the `(path_id, line)` →
//!    step-id index a reader answers a breakpoint from without scanning the
//!    execution stream (`internal-files.md` §"`step-map.ns`"). The ids it lists
//!    must be the exec-record indices of the steps at that line, in order.
//! 3. **`meta.dat`'s path list is `paths.dat`'s**, in id order, however the
//!    paths were interned — including through `add_event(Path)`, which is how a
//!    recorder that interns its own ids reaches the writer.
//!
//! No mocks: containers are produced by the real writer and read through the
//! real CTFS and stream readers.

use std::path::{Path, PathBuf};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::value_stream_reader::ValueStreamReader;
use codetracer_trace_types::{Line, PathId, StepRecord, ThreadId, TraceLowLevelEvent, TypeId, TypeKind, ValueRecord};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::meta_dat::decode_meta_dat;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer::value_stream::ValueStreamEvent;

fn record(dir: &Path, name: &str, drive: impl FnOnce(&mut CtfsTraceWriter)) -> PathBuf {
    let mut w = CtfsTraceWriter::new(name, &[]);
    let out = dir.join(name);
    TraceWriter::begin_writing_trace_events(&mut w, &out).expect("begin");
    drive(&mut w);
    TraceWriter::finish_writing_trace_events(&mut w).expect("finish");
    out.with_extension("ct")
}

fn read(ct: &Path, name: &str) -> Vec<u8> {
    CtfsReader::open(ct)
        .expect("open")
        .read_file(name)
        .unwrap_or_else(|e| panic!("{name}: {e:?}"))
}

/// The variable-name ids carried by each value record, in record order.
fn value_names_per_record(ct: &Path) -> Vec<Vec<u64>> {
    let mut r = CtfsReader::open(ct).expect("open");
    ValueStreamReader::open(&mut r)
        .expect("values.dat decodes")
        .expect("values.dat present")
        .read_all()
        .expect("read_all")
        .into_iter()
        .map(|rec| {
            rec.events
                .iter()
                .flat_map(|e| match e {
                    ValueStreamEvent::StepValues { values } => values.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                    _ => vec![],
                })
                .collect()
        })
        .collect()
}

#[test]
fn a_value_registered_after_a_call_or_a_return_belongs_to_the_next_step() {
    let dir = tempfile::tempdir().unwrap();
    let main = PathBuf::from("/src/main.rs");
    let ct = record(dir.path(), "carry", |w| {
        let int = AbstractTraceWriter::ensure_type_id(w, TypeKind::Int, "i64");
        let v = |i| ValueRecord::Int { i, type_id: int };
        let f = AbstractTraceWriter::ensure_function_id(w, "f", &main, Line(10));
        let before = AbstractTraceWriter::ensure_variable_id(w, "before");
        let arg = AbstractTraceWriter::ensure_variable_id(w, "after_call");
        let x = AbstractTraceWriter::ensure_variable_id(w, "after_return");
        let tail = AbstractTraceWriter::ensure_variable_id(w, "tail");
        AbstractTraceWriter::register_step(w, &main, Line(1)); // step 0
        AbstractTraceWriter::register_full_value(w, before, v(1)); //   open: step 0
        AbstractTraceWriter::register_call(w, f, vec![]); //            closes step 0
        AbstractTraceWriter::register_full_value(w, arg, v(2)); //      staged -> step 1
        AbstractTraceWriter::register_step(w, &main, Line(10)); // step 1
        AbstractTraceWriter::register_return(w, v(3)); //               closes step 1
        AbstractTraceWriter::register_full_value(w, x, v(4)); //        staged -> step 2
        AbstractTraceWriter::register_step(w, &main, Line(2)); // step 2
        AbstractTraceWriter::register_call(w, f, vec![]);
        AbstractTraceWriter::register_full_value(w, tail, v(5)); //     staged, no next step -> step 2
        let _ = (before, arg, x, tail);
    });
    let names = value_names_per_record(&ct);
    assert_eq!(
        names,
        vec![vec![0], vec![1], vec![2, 3]],
        "value record N lists the variable ids visible at step N: a value staged after a call or a return \
         belongs to the NEXT step, and one staged with no step to follow belongs to the last step"
    );
}

/// Decode `step-map.ns` (`internal-files.md` §"`step-map.ns`") into
/// `(path_id, line, step ids)` triples.
fn decode_step_map(b: &[u8]) -> Vec<(u64, u32, Vec<i64>)> {
    let u16_at = |o: usize| u16::from_le_bytes(b[o..o + 2].try_into().unwrap());
    let u32_at = |o: usize| u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
    let u64_at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
    assert_eq!(u32_at(0), 0x5354_4D50, "magic");
    assert_eq!(u16_at(4), 1, "version");
    let path_count = u32_at(6) as usize;
    let table = u64_at(10) as usize;
    let mut out = vec![];
    for p in 0..path_count {
        let pe = table + p * 20;
        let (path_id, line_count, lines) = (u64_at(pe), u32_at(pe + 8) as usize, u64_at(pe + 12) as usize);
        for l in 0..line_count {
            let le = lines + l * 32;
            let (line, n, first, last, ids_at) = (
                u32_at(le),
                u32_at(le + 4) as usize,
                u64_at(le + 8) as i64,
                u64_at(le + 16) as i64,
                u64_at(le + 24) as usize,
            );
            let ids: Vec<i64> = (0..n).map(|k| u64_at(ids_at + 8 * k) as i64).collect();
            assert_eq!((ids[0], ids[n - 1]), (first, last), "first/last bound the id list");
            out.push((path_id, line, ids));
        }
    }
    out
}

#[test]
fn a_line_only_trace_carries_the_step_map() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (PathBuf::from("/src/a.rs"), PathBuf::from("/src/b.rs"));
    let ct = record(dir.path(), "stepmap", |w| {
        AbstractTraceWriter::ensure_path_id(w, &a);
        AbstractTraceWriter::ensure_path_id(w, &b);
        AbstractTraceWriter::register_step(w, &b, Line(7)); // exec 0
        AbstractTraceWriter::register_step(w, &a, Line(3)); // exec 1
        AbstractTraceWriter::add_event(w, TraceLowLevelEvent::ThreadSwitch(ThreadId(2))); // exec 2, not a step
        AbstractTraceWriter::register_step(w, &b, Line(7)); // exec 3
        AbstractTraceWriter::register_step(w, &a, Line(1)); // exec 4
    });
    assert_eq!(
        decode_step_map(&read(&ct, "step-map.ns")),
        vec![(0, 1, vec![4]), (0, 3, vec![1]), (1, 7, vec![0, 3])],
        "step-map.ns lists, per path id and then per line, the exec-record index of every step there"
    );
}

#[test]
fn meta_dat_lists_the_paths_of_paths_dat_whichever_way_they_were_interned() {
    let dir = tempfile::tempdir().unwrap();
    let ct = record(dir.path(), "metapaths", |w| {
        for p in ["/src/one.rs", "/src/two.rs", "/src/three.rs"] {
            AbstractTraceWriter::add_event(w, TraceLowLevelEvent::Path(PathBuf::from(p)));
        }
        AbstractTraceWriter::add_event(
            w,
            TraceLowLevelEvent::Step(StepRecord {
                path_id: PathId(1),
                line: Line(1),
            }),
        );
        let _ = TypeId(0);
    });
    let meta = decode_meta_dat(&read(&ct, "meta.dat")).expect("meta.dat");
    assert_eq!(
        meta.paths,
        vec!["/src/one.rs", "/src/two.rs", "/src/three.rs"],
        "meta.dat must list paths.dat's paths in id order"
    );
}
