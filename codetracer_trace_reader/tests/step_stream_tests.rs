//! M23a round-trip tests for the dedicated `steps.dat` execution stream.
//!
//! A trace is written with the `has_step_stream` flag on; the step records are
//! then read back two ways and compared:
//!   1. directly from `steps.dat` via the seekable `StepStreamReader`
//!      (AbsoluteStep/DeltaStep decoded back to absolute `global_line_index`),
//!      and
//!   2. re-derived from the unchanged `events.log` (read with the normal
//!      reader): every `Step{path_id, line}` event's expected
//!      `global_line_index` is computed with the SAME packing the writer uses.
//! The two MUST agree — proving `steps.dat` is consistent with the unified
//! stream it was split from, and that AbsoluteStep/DeltaStep decode (incl.
//! across a chunk boundary) recovers the exact step sequence. A flag-off
//! (legacy) trace is also exercised to confirm the step stream is absent and old
//! readers are unaffected.

use std::path::Path;

use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::step_stream::{global_line_index, StepStreamRecord};
use codetracer_trace_writer::trace_writer::TraceWriter;

/// Write a trace whose steps exercise: a forced-absolute first step, sequential
/// small deltas, a post-call absolute, a large jump (forces AbsoluteStep even
/// mid-function), and enough steps to cross several small chunks. Returns the
/// `.ct` path plus the ordered list of `Step` line numbers emitted (for an
/// independent expected-sequence cross-check).
fn write_trace(dir: &tempfile::TempDir, with_step_stream: bool, steps_chunk_size: usize) -> (std::path::PathBuf, Vec<i64>) {
    let path_buf = dir.path().join("trace");
    let mut writer = CtfsTraceWriter::new("test_program", &[]).with_step_stream(with_step_stream);
    writer = writer.with_steps_chunk_size(steps_chunk_size);
    TraceWriter::begin_writing_trace_events(&mut writer, &path_buf).unwrap();

    let src = Path::new("/test/prog.rs");
    TraceWriter::start(&mut writer, src, Line(1));

    let main_fn = TraceWriter::ensure_function_id(&mut writer, "main", src, Line(1));
    let helper = TraceWriter::ensure_function_id(&mut writer, "helper", src, Line(50));

    let mut lines: Vec<i64> = Vec::new();

    // start() emitted an implicit toplevel Step at line 1.
    lines.push(1);

    // main(): a run of sequential lines (small +1 deltas).
    TraceWriter::register_call(&mut writer, main_fn, vec![]);
    for ln in 2..=12 {
        TraceWriter::register_step(&mut writer, src, Line(ln));
        lines.push(ln);
    }

    // A large jump that exceeds the DeltaStep range → forces AbsoluteStep.
    // global_line_index packs path_id<<32 | line, so even a modest line jump
    // here stays in range; instead jump to a far line to exceed MAX_DELTA.
    TraceWriter::register_step(&mut writer, src, Line(2_000_000));
    lines.push(2_000_000);
    TraceWriter::register_step(&mut writer, src, Line(2_000_001));
    lines.push(2_000_001);

    // helper(): post-call → AbsoluteStep, then small deltas.
    TraceWriter::register_call(&mut writer, helper, vec![]);
    for ln in 50..=60 {
        TraceWriter::register_step(&mut writer, src, Line(ln));
        lines.push(ln);
    }
    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    // post-return → AbsoluteStep
    TraceWriter::register_step(&mut writer, src, Line(13));
    lines.push(13);

    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });

    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
    (path_buf.with_extension("ct"), lines)
}

/// Re-derive the expected execution-stream `Step` records straight from
/// `events.log` (read with the unchanged unified-stream reader): each `Step`
/// event maps to its packed `global_line_index`.
fn expected_step_glis_from_events(ct_path: &Path) -> Vec<u64> {
    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(ct_path).unwrap();
    let mut out = Vec::new();
    for ev in &events {
        if let TraceLowLevelEvent::Step(s) = ev {
            out.push(global_line_index(s));
        }
    }
    out
}

#[test]
fn steps_dat_matches_events_log() {
    let dir = tempfile::tempdir().unwrap();
    // A small chunk size so the step sequence spans multiple chunks and the
    // round-trip exercises per-chunk independent decode.
    let (ct_path, _lines) = write_trace(&dir, true, 4);

    let mut ss = codetracer_trace_reader::step_stream_reader::open_step_stream(&ct_path)
        .expect("open_step_stream ok")
        .expect("step stream present when has_step_stream flag is set");
    let from_dat = ss.read_all().unwrap();

    // Keep only Step records (this trace emits no Raise/Catch/ThreadSwitch).
    let dat_glis: Vec<u64> = from_dat
        .iter()
        .map(|r| match r {
            StepStreamRecord::Step { global_line_index } => *global_line_index,
            other => panic!("unexpected non-step record in trace: {other:?}"),
        })
        .collect();

    let expected = expected_step_glis_from_events(&ct_path);
    assert_eq!(
        dat_glis, expected,
        "steps.dat decoded global_line_indices must equal the events.log-derived step sequence"
    );
    assert!(!expected.is_empty(), "the trace must have produced steps");
}

#[test]
fn seek_to_step_across_chunk_boundary() {
    let dir = tempfile::tempdir().unwrap();
    // chunk_size 4 → many chunks; we will seek into a non-first chunk and back.
    let (ct_path, _lines) = write_trace(&dir, true, 4);

    let mut ss = codetracer_trace_reader::step_stream_reader::open_step_stream(&ct_path).unwrap().unwrap();
    let expected = expected_step_glis_from_events(&ct_path);
    assert_eq!(ss.count() as usize, expected.len());

    // Seek to a step in a later chunk (index well past chunk 0): it must decode
    // correctly even though that chunk's first record is AbsoluteStep and the
    // target may be a DeltaStep resolved within that chunk.
    let target = expected.len() - 2;
    let rec = ss.read(target as u64).unwrap();
    match rec {
        StepStreamRecord::Step { global_line_index } => assert_eq!(global_line_index, expected[target]),
        other => panic!("expected Step, got {other:?}"),
    }

    // Seek back into chunk 0.
    let rec0 = ss.read(0).unwrap();
    match rec0 {
        StepStreamRecord::Step { global_line_index } => assert_eq!(global_line_index, expected[0]),
        other => panic!("expected Step, got {other:?}"),
    }

    // Out-of-range index errors, never panics.
    assert!(ss.read(ss.count() + 5).is_err());
}

#[test]
fn fetching_one_step_decompresses_only_its_chunk() {
    let dir = tempfile::tempdir().unwrap();
    // chunk_size 4 guarantees several chunks for the ~36-step trace.
    let (ct_path, _lines) = write_trace(&dir, true, 4);

    let mut ss = codetracer_trace_reader::step_stream_reader::open_step_stream(&ct_path).unwrap().unwrap();
    let chunk_size = ss.chunk_size();
    assert_eq!(chunk_size, 4);
    let count = ss.count();
    assert!(count > chunk_size as u64 * 2, "need at least 3 chunks to make the bound meaningful");

    // A fresh read of a step in chunk K must leave exactly chunk K cached — no
    // other chunk is inflated. Counter-proven via cached_chunk() observation.
    let target_index = count - 1; // last step → last chunk
    let expected_chunk = (target_index as usize) / chunk_size;
    let _ = ss.read(target_index).unwrap();
    assert_eq!(
        ss.cached_chunk(),
        Some(expected_chunk),
        "fetching one step must decompress only its own chunk"
    );

    // Reading another step in the SAME chunk reuses the cache (still that chunk).
    if target_index % chunk_size as u64 != 0 {
        let sibling = target_index - 1;
        if (sibling as usize) / chunk_size == expected_chunk {
            let _ = ss.read(sibling).unwrap();
            assert_eq!(ss.cached_chunk(), Some(expected_chunk));
        }
    }

    // Reading a step in chunk 0 switches the single-chunk cache to chunk 0.
    let _ = ss.read(0).unwrap();
    assert_eq!(ss.cached_chunk(), Some(0));
}

#[test]
fn events_log_byte_identical_with_and_without_step_stream() {
    // The step-stream split is ADDITIVE: enabling it must not perturb the
    // unified events.log a single byte. Write the same trace twice (flag off,
    // flag on) and compare the raw events.log internal file byte-for-byte.
    let dir_off = tempfile::tempdir().unwrap();
    let dir_on = tempfile::tempdir().unwrap();
    let (ct_off, _) = write_trace(&dir_off, false, 4);
    let (ct_on, _) = write_trace(&dir_on, true, 4);

    let mut r_off = codetracer_ctfs::CtfsReader::open(&ct_off).unwrap();
    let mut r_on = codetracer_ctfs::CtfsReader::open(&ct_on).unwrap();
    let events_off = r_off.read_file("events.log").unwrap();
    let events_on = r_on.read_file("events.log").unwrap();
    assert_eq!(events_off, events_on, "events.log must be byte-identical regardless of the step-stream flag");

    // The flag-on container additionally carries steps.dat + steps.idx; the
    // flag-off one must not.
    assert!(r_on.read_file("steps.dat").is_ok());
    assert!(r_on.read_file("steps.idx").is_ok());
    assert!(r_off.read_file("steps.dat").is_err());
}

#[test]
fn legacy_trace_has_no_step_stream() {
    let dir = tempfile::tempdir().unwrap();
    let (ct_path, _lines) = write_trace(&dir, false, 4);

    // No dedicated step stream when the flag is off.
    let ss = codetracer_trace_reader::step_stream_reader::open_step_stream(&ct_path).unwrap();
    assert!(ss.is_none(), "a flag-off trace must not expose a step stream");

    // ...and the unified events.log still reads exactly as before, with the
    // same step sequence derivable from it.
    let expected = expected_step_glis_from_events(&ct_path);
    assert!(!expected.is_empty());
}
