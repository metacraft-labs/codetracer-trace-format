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
//! `mixed_workload`'s `WASM-WRITER-MIX-1` mix, through each writer's
//! recorder-facing API and through `add_event`.
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
use std::path::Path;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::step_stream_reader::StepStreamReader;
use codetracer_trace_writer::step_stream::StepStreamRecord;

mod mixed_workload;

use mixed_workload::{nim_lock, write_nim, write_rust, Drive};

mod whole_file;

/// Exec-record index of `steps.dat`'s second chunk.
const SECOND_CHUNK: u64 = 4096;

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
    let nim_ct = write_nim(drive, &nd, 0);
    let rust_ct = write_rust(drive, &rd, 0);
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
    whole_file::assert_same_file(&nim_ct, &rust_ct, &format!("{drive:?}"));
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
