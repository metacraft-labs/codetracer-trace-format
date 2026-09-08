//! A line-only `steps.dat` must decode back to the `(path_id, line)` the trace
//! recorded — on a trace with MORE THAN ONE source file.
//!
//! `step_stream_tests` already round-trips the execution stream, but it compares
//! the raw `global_line_index` integers the writer produced against the same
//! integers re-derived from `events.log` with the writer's own arithmetic. Two
//! sides computing one function agree whatever that function is, so that test
//! passes under any addressing scheme, correct or not. The same blind spot is in
//! `nim_step_stream_crossread`, which compares the Rust reader's integers against
//! the Nim reader's integers and never asks either what location they name.
//!
//! Every assertion here is about the LOCATION, and every fixture registers three
//! files. Path 0 is where every apportionment of the address space agrees —
//! whatever the rule, file 0's base is 0 — so a single-path fixture cannot see
//! how the addresses were apportioned at all.

use std::path::Path;

use codetracer_trace_reader::step_stream_reader::open_step_stream;
use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::line_position::{DEFAULT_LINES_PER_FILE, LinePositionError, LinePositionSpace};
use codetracer_trace_writer::step_stream::StepStreamRecord;
use codetracer_trace_writer::trace_writer::TraceWriter;

const MAIN_SRC: &str = "/test/main.rs";
const LIB_SRC: &str = "/test/lib.rs";
const UTIL_SRC: &str = "/test/util.rs";

/// Write a line-only split-stream trace that steps through three files, and
/// return the `.ct` path plus the `(path_id, line)` sequence it recorded.
fn write_three_path_trace(dir: &tempfile::TempDir) -> (std::path::PathBuf, Vec<(usize, i64)>) {
    let path_buf = dir.path().join("trace");
    let mut writer = CtfsTraceWriter::new("three_paths", &[]).with_step_stream(true);
    writer = writer.with_steps_chunk_size(4);
    TraceWriter::begin_writing_trace_events(&mut writer, &path_buf).unwrap();

    let main_src = Path::new(MAIN_SRC);
    let lib_src = Path::new(LIB_SRC);
    let util_src = Path::new(UTIL_SRC);

    TraceWriter::start(&mut writer, main_src, Line(1));
    let main_fn = TraceWriter::ensure_function_id(&mut writer, "main", main_src, Line(1));
    let lib_fn = TraceWriter::ensure_function_id(&mut writer, "lib_call", lib_src, Line(20));
    let util_fn = TraceWriter::ensure_function_id(&mut writer, "util_call", util_src, Line(7));

    // `register_call` emits an implicit step at the callee's own declaration
    // site before the Call event, so each call below contributes one step too.
    // Path ids follow interning order, which is the order the three files were
    // first named.
    let mut expected: Vec<(usize, i64)> = Vec::new();

    TraceWriter::register_call(&mut writer, main_fn, vec![]);
    expected.push((0, 1));
    for round in 0..6i64 {
        for (path_id, src, line) in [
            (0usize, main_src, 2 + round),
            (1usize, lib_src, 20 + round),
            (2usize, util_src, 7 + round),
        ] {
            TraceWriter::register_step(&mut writer, src, Line(line));
            expected.push((path_id, line));
        }
    }

    // A cross-file jump wide enough to exceed the DeltaStep range, so the
    // AbsoluteStep path is exercised on a multi-file address too.
    TraceWriter::register_call(&mut writer, lib_fn, vec![]);
    expected.push((1, 20));
    TraceWriter::register_step(&mut writer, lib_src, Line(99_000));
    expected.push((1, 99_000));
    TraceWriter::register_call(&mut writer, util_fn, vec![]);
    expected.push((2, 7));
    TraceWriter::register_step(&mut writer, util_src, Line(3));
    expected.push((2, 3));

    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();

    (path_buf.with_extension("ct"), expected)
}

/// Read every `Step` record's absolute address out of `steps.dat`.
fn read_addresses(ct: &Path) -> Vec<u64> {
    let mut stream = open_step_stream(ct).expect("open_step_stream").expect("the trace declares a step stream");
    stream
        .read_all()
        .expect("read_all")
        .iter()
        .map(|r| match r {
            StepStreamRecord::Step { global_line_index } => *global_line_index,
            other => panic!("unexpected non-Step record: {other:?}"),
        })
        .collect()
}

/// THE REPRODUCER. Every step's address resolves to the file and line the step
/// was recorded at, in all three files.
#[test]
fn every_step_address_resolves_to_the_location_it_was_recorded_at() {
    let dir = tempfile::tempdir().unwrap();
    let (ct, expected) = write_three_path_trace(&dir);
    let addresses = read_addresses(&ct);
    assert_eq!(addresses.len(), expected.len(), "one address per recorded step");

    let space = LinePositionSpace::uniform(3);
    for (i, (address, (path_id, line))) in addresses.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            space.resolve(*address),
            Ok((*path_id, *line)),
            "step {i} was recorded at (path {path_id}, line {line}); \
             its address {address} resolves elsewhere"
        );
    }
}

/// The addresses are the spec's prefix sum, stated as concrete integers so the
/// test above cannot pass for an unrelated reason.
#[test]
fn the_addresses_are_the_prefix_sum_the_spec_defines() {
    let dir = tempfile::tempdir().unwrap();
    let (ct, expected) = write_three_path_trace(&dir);
    let addresses = read_addresses(&ct);

    let mut space = LinePositionSpace::uniform(3);
    for (i, (path_id, line)) in expected.iter().enumerate() {
        assert_eq!(
            addresses[i],
            space.global_index(*path_id, *line),
            "step {i} (path {path_id}, line {line})"
        );
    }

    // (path 1, line 20) is the first address outside file 0, and it is the
    // file's base plus the 0-based in-file offset.
    assert_eq!(addresses[2], DEFAULT_LINES_PER_FILE + 19);
    // (path 2, line 7) likewise.
    assert_eq!(addresses[3], 2 * DEFAULT_LINES_PER_FILE + 6);
}

/// Every address a three-file trace produces lies inside a three-file space.
/// Under a bit-field packing the addresses for files 1 and 2 sit at least 2^32
/// up, which no prefix-sum space of any plausible size contains.
#[test]
fn no_address_escapes_the_traces_own_space() {
    let dir = tempfile::tempdir().unwrap();
    let (ct, _) = write_three_path_trace(&dir);
    let addresses = read_addresses(&ct);
    let space = LinePositionSpace::uniform(3);

    for (i, address) in addresses.iter().enumerate() {
        assert!(
            *address < space.total_lines(),
            "step {i}'s address {address} is at or above the top of the trace's \
             address space ({}); resolving it would require an assumption the \
             container does not carry",
            space.total_lines()
        );
    }
}

/// The refusal is reachable and names what it refused. A reader handed an
/// address from a different scheme must fail rather than answer.
#[test]
fn an_address_from_another_scheme_is_refused_not_answered() {
    let space = LinePositionSpace::uniform(3);
    let foreign = (1u64 << 32) | 20;
    match space.resolve(foreign) {
        Err(LinePositionError::OutOfSpace {
            position,
            total_lines,
            file_count,
        }) => {
            assert_eq!(position, foreign);
            assert_eq!(total_lines, 3 * DEFAULT_LINES_PER_FILE);
            assert_eq!(file_count, 3);
        }
        other => panic!("an address 2^32 up must be refused, got {other:?}"),
    }
}
