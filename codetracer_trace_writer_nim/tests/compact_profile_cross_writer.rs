//! The two writers choose the container profile at close alike, and the
//! compact container they choose is byte-identical and reads as the
//! recording (`ctfs-container.md` §1e, "A writer may instead write the full
//! profile throughout and convert it at close"; §1f).
//!
//! # What is checked
//!
//! The recording is `mixed_workload`'s mix, whose framed members cross chunk
//! boundaries. Its raw bytes `R` -- the members of its compact container,
//! every zstd frame inflated -- are measured on the Rust writer's full
//! container.
//!
//! * At a threshold of `R + 1` the Rust writer (`with_compact_threshold`) and
//!   the Nim writer (`trace_writer_set_compact_threshold`, through the C ABI)
//!   both emit the compact container, byte for byte the same, and its members
//!   total `R`.
//! * At a threshold of `R` both emit the full container: the Rust writer's is
//!   byte for byte what it emits with no threshold, and the Nim writer's holds
//!   the same members, in the same order and byte for byte, in a container of
//!   the same size (the parity `container_parity_on_a_mixed_workload.rs`
//!   checks; the two writers allocate blocks in different orders).
//! * The compact container answers every step, value, call, I/O event and
//!   step-map query, and reassembles to the same event sequence, as the full
//!   container of the same recording does.
//! * Control: a compact container laid out from the full container's members
//!   copied verbatim, frames and all, does not read as the recording, so the
//!   comparison above is able to fail.
//!
//! No mocks: every container comes from a real writer and is read back
//! through the real CTFS and stream readers.

mod mixed_workload;

use std::collections::BTreeMap;
use std::path::Path;

use codetracer_ctfs::compact::Profile;
use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::call_stream_reader::CallStreamReader;
use codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader;
use codetracer_trace_reader::split_stream_reader::read_trace_from_split_streams;
use codetracer_trace_reader::step_map_reader::StepMapReader;
use codetracer_trace_reader::step_stream_reader::StepStreamReader;
use codetracer_trace_reader::value_stream_reader::ValueStreamReader;
use codetracer_trace_writer::call_stream::CallStreamRecord;
use codetracer_trace_writer::compact_profile::{compact_members_of, encode_compact, raw_member_bytes};
use codetracer_trace_writer::event_stream::IoEventRecord;
use codetracer_trace_writer::step_stream::StepStreamRecord;
use codetracer_trace_writer::value_stream::ValueRecordEntry;
use mixed_workload::{nim_lock, write_nim, write_rust, Drive};

/// Every answer a reader gives about a recording.
#[derive(Debug, PartialEq)]
struct Answers {
    steps: Vec<StepStreamRecord>,
    values: Vec<ValueRecordEntry>,
    calls: Vec<CallStreamRecord>,
    io_events: Vec<IoEventRecord>,
    /// `load_all`, and every key looked up one at a time.
    step_map: BTreeMap<(u64, u32), Vec<u64>>,
    step_map_lookups: Vec<Option<Vec<u64>>>,
    /// The reassembled event sequence, `Debug`-printed: the events are not
    /// `PartialEq`.
    events: Vec<String>,
}

fn answers(container: Vec<u8>) -> Result<Answers, String> {
    let open = || CtfsReader::from_bytes(container.clone()).map_err(|e| format!("open: {e:?}"));
    let mut r = open()?;
    let present = |what: &str| format!("{what} absent");
    let steps = StepStreamReader::open(&mut r)?.ok_or_else(|| present("steps.dat"))?.read_all()?;
    let values = ValueStreamReader::open(&mut r)?.ok_or_else(|| present("values.dat"))?.read_all()?;
    let calls = CallStreamReader::open(&mut r)?.ok_or_else(|| present("calls.dat"))?.read_all()?;
    let io_events = IoEventStreamReader::open(&mut r)?.ok_or_else(|| present("events.dat"))?.read_all()?;
    let mut map = StepMapReader::open(&mut r)?.ok_or_else(|| present("step-map.ns"))?;
    let step_map = map.load_all()?;
    let step_map_lookups = step_map.keys().map(|(path, line)| map.lookup(*path, *line)).collect::<Result<_, _>>()?;
    let events = read_trace_from_split_streams(&mut open()?)?.iter().map(|e| format!("{e:?}")).collect();
    Ok(Answers {
        steps,
        values,
        calls,
        io_events,
        step_map,
        step_map_lookups,
        events,
    })
}

fn profile_of(container: &[u8]) -> Profile {
    CtfsReader::from_bytes(container.to_vec()).expect("a container").profile()
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The recording's full container from the Rust writer, and its raw bytes.
fn full_and_raw(drive: Drive, dir: &Path) -> (Vec<u8>, u64) {
    let full = read(&write_rust(drive, &dir.join("full"), 0));
    let raw = raw_member_bytes(&compact_members_of(&full).expect("convert"));
    (full, raw)
}

/// The containers both writers emit for the recording at `threshold`.
fn both(drive: Drive, dir: &Path, threshold: u64) -> (Vec<u8>, Vec<u8>) {
    let (nd, rd) = (dir.join(format!("nim-{threshold}")), dir.join(format!("rust-{threshold}")));
    std::fs::create_dir_all(&nd).expect("dir");
    std::fs::create_dir_all(&rd).expect("dir");
    (read(&write_nim(drive, &nd, threshold)), read(&write_rust(drive, &rd, threshold)))
}

/// Where two containers part: their member orders, the members whose bytes
/// differ, and the first differing byte. Printed when a comparison fails.
fn difference(a: &[u8], b: &[u8]) -> String {
    let members = |c: &[u8]| CtfsReader::from_bytes(c.to_vec()).and_then(|mut r| r.members()).unwrap_or_default();
    let (ma, mb) = (members(a), members(b));
    let order = |m: &[(String, Vec<u8>)]| m.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();
    let differing: Vec<&String> = ma
        .iter()
        .filter(|(n, p)| mb.iter().all(|(m, q)| m != n || q != p))
        .map(|(n, _)| n)
        .collect();
    let first = a.iter().zip(b).position(|(x, y)| x != y);
    format!(
        "{} and {} bytes, first difference at {first:?}\nmember order: {:?}\n          vs: {:?}\nmembers differing: {differing:?}",
        a.len(),
        b.len(),
        order(&ma),
        order(&mb)
    )
}

fn tempdir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("full")).expect("dir");
    dir
}

#[test]
fn above_the_raw_bytes_both_writers_emit_the_same_compact_container() {
    let _g = nim_lock();
    for drive in [Drive::RecorderApi, Drive::Events] {
        let dir = tempdir();
        let (full, raw) = full_and_raw(drive, dir.path());
        let (nim, rust) = both(drive, dir.path(), raw + 1);

        assert_eq!(
            profile_of(&rust),
            Profile::Compact,
            "{drive:?}: the Rust writer kept the full profile at R + 1"
        );
        assert_eq!(
            profile_of(&nim),
            Profile::Compact,
            "{drive:?}: the Nim writer kept the full profile at R + 1"
        );
        assert!(
            nim == rust,
            "{drive:?}: the two writers' compact containers differ: {}",
            difference(&nim, &rust)
        );

        let members = compact_members_of(&full).expect("convert");
        assert!(
            rust == encode_compact(&members).expect("lay out"),
            "{drive:?}: the writer's container is not the conversion's"
        );
        let count = members.len() as u64;
        assert_eq!(
            rust.len() as u64 - 28 - 24 * count,
            raw,
            "{drive:?}: the members of the compact container total R"
        );
    }
}

#[test]
fn at_the_raw_bytes_both_writers_emit_the_full_container() {
    let _g = nim_lock();
    let dir = tempdir();
    let (full, raw) = full_and_raw(Drive::RecorderApi, dir.path());
    let (nim, rust) = both(Drive::RecorderApi, dir.path(), raw);

    assert_eq!(profile_of(&rust), Profile::Full, "the Rust writer chose compact at R");
    assert_eq!(profile_of(&nim), Profile::Full, "the Nim writer chose compact at R");
    assert!(
        rust == full,
        "the Rust writer's full container changed under a threshold it does not meet"
    );
    let members = |c: &[u8]| CtfsReader::from_bytes(c.to_vec()).and_then(|mut r| r.members()).expect("members");
    assert!(
        members(&nim) == members(&rust) && nim.len() == rust.len(),
        "the two writers' full containers differ: {}",
        difference(&nim, &rust)
    );
}

#[test]
fn the_compact_container_answers_every_query_as_the_full_one() {
    let _g = nim_lock();
    let dir = tempdir();
    let (full, raw) = full_and_raw(Drive::RecorderApi, dir.path());
    let (nim, rust) = both(Drive::RecorderApi, dir.path(), raw + 1);

    let expected = answers(full).expect("the full container reads");
    assert!(expected.steps.len() > 4096, "the recording must span more than one steps.dat chunk");
    assert!(!expected.step_map.is_empty() && !expected.io_events.is_empty() && !expected.calls.is_empty());
    for (writer, compact) in [("Rust", rust), ("Nim", nim)] {
        let got = answers(compact).unwrap_or_else(|e| panic!("the {writer} writer's compact container does not read: {e}"));
        assert!(
            got == expected,
            "the {writer} writer's compact container answers differently from the full one"
        );
    }
}

#[test]
fn control_frames_copied_verbatim_do_not_read_as_the_recording() {
    let _g = nim_lock();
    let dir = tempdir();
    let (full, _) = full_and_raw(Drive::RecorderApi, dir.path());
    let verbatim = CtfsReader::from_bytes(full.clone()).expect("open").members().expect("members");
    let verbatim = encode_compact(&verbatim).expect("lay out");
    assert_eq!(profile_of(&verbatim), Profile::Compact);

    let expected = answers(full).expect("the full container reads");
    match answers(verbatim) {
        Err(_) => {}
        Ok(got) => assert!(got != expected, "a compact container of frames read as the recording"),
    }
}
