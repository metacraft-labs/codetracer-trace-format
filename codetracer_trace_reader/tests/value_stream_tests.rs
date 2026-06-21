//! M23b round-trip tests for the dedicated `values.dat` parallel value stream.
//!
//! A trace is written with the `has_value_stream` flag on; the per-step value
//! records are then read back two ways and compared:
//!   1. directly from `values.dat` via the seekable `ValueStreamReader`, and
//!   2. re-derived from the unchanged `events.log` (read with the normal
//!      reader) by feeding its events through the SAME `ValueStreamBuilder` the
//!      writer used.
//! The two MUST agree — proving `values.dat` is consistent with the unified
//! stream it was split from, that the parallel-index invariant holds (value
//! record N ↔ step N, empty record for value-less steps), and that seeking
//! (incl. across a chunk boundary) recovers the exact per-step records. A
//! flag-off (legacy) trace is also exercised to confirm the value stream is
//! absent and old readers are unaffected, and an `events.log` byte-identity
//! check proves the split is additive.

use std::path::Path;

use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer::value_stream::{ValueRecordEntry, ValueStreamBuilder, ValueStreamEvent};

/// Write a trace whose steps exercise a variety of value-stream events:
///   * steps WITH variable values (StepValues),
///   * steps with NO values (the empty-record case in the parallel index),
///   * cell values / assign-cell / bind / drop events,
/// across enough steps to cross several small `values.dat` chunks. Returns the
/// `.ct` path.
fn write_trace(dir: &tempfile::TempDir, with_value_stream: bool, values_chunk_size: usize) -> std::path::PathBuf {
    let path_buf = dir.path().join("trace");
    let mut writer = CtfsTraceWriter::new("test_program", &[])
        .with_value_stream(with_value_stream)
        .with_values_chunk_size(values_chunk_size);
    TraceWriter::begin_writing_trace_events(&mut writer, &path_buf).unwrap();

    let src = Path::new("/test/prog.rs");
    TraceWriter::start(&mut writer, src, Line(1));

    let main_fn = TraceWriter::ensure_function_id(&mut writer, "main", src, Line(1));
    let int_type = TraceWriter::ensure_type_id(&mut writer, TypeKind::Int, "Int");
    let int_value = |i: i64| ValueRecord::Int { i, type_id: int_type };

    TraceWriter::register_call(&mut writer, main_fn, vec![]);

    // A run of steps, each with one or two variable values, plus some value-less
    // steps and some cell/bind/assign activity, so the per-step records vary.
    for ln in 2..=20i64 {
        TraceWriter::register_step(&mut writer, src, Line(ln));
        if ln % 3 == 0 {
            // value-less step (exercises the empty-record path)
            continue;
        }
        TraceWriter::register_variable_with_full_value(&mut writer, "x", int_value(ln));
        if ln % 2 == 0 {
            TraceWriter::register_variable_with_full_value(&mut writer, "y", int_value(ln * 10));
        }
        if ln % 5 == 0 {
            TraceWriter::register_cell_value(&mut writer, Place(ln), int_value(ln + 1));
            TraceWriter::assign_cell(&mut writer, Place(ln), int_value(ln + 2));
        }
        if ln % 7 == 0 {
            TraceWriter::bind_variable(&mut writer, "z", Place(ln));
        }
    }

    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
    path_buf.with_extension("ct")
}

/// Re-derive the expected per-step value records straight from `events.log`
/// (read with the unchanged unified-stream reader), by replaying its events
/// through the SAME `ValueStreamBuilder` the writer uses. This is the ground
/// truth the `values.dat`-decoded records must equal.
fn expected_records_from_events(ct_path: &Path) -> Vec<ValueRecordEntry> {
    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(ct_path).unwrap();
    let mut builder = ValueStreamBuilder::new();
    for ev in &events {
        builder.observe(ev);
    }
    builder.finish()
}

#[test]
fn values_dat_matches_events_log() {
    let dir = tempfile::tempdir().unwrap();
    // A small chunk size so the records span multiple chunks and the round-trip
    // exercises per-chunk independent decode.
    let ct_path = write_trace(&dir, true, 4);

    let mut vs = codetracer_trace_reader::value_stream_reader::open_value_stream(&ct_path)
        .expect("open_value_stream ok")
        .expect("value stream present when has_value_stream flag is set");
    let from_dat = vs.read_all().unwrap();

    let expected = expected_records_from_events(&ct_path);
    assert_eq!(
        from_dat, expected,
        "values.dat decoded per-step records must equal the events.log-derived records"
    );
    assert!(!expected.is_empty(), "the trace must have produced value records");

    // Parallel-index invariant: the value-record count equals the step count.
    let step_count = {
        let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
        let events = reader.load_trace_events(&ct_path).unwrap();
        events.iter().filter(|e| matches!(e, TraceLowLevelEvent::Step(_))).count()
    };
    assert_eq!(from_dat.len(), step_count, "value record count must equal step count (record N ↔ step N)");

    // At least one step must be the empty-record case, and at least one must
    // carry StepValues — otherwise the test is not exercising the variety.
    assert!(from_dat.iter().any(|r| r.events.is_empty()), "expected at least one value-less (empty) step record");
    assert!(
        from_dat.iter().any(|r| r.events.iter().any(|e| matches!(e, ValueStreamEvent::StepValues { .. }))),
        "expected at least one step with StepValues"
    );
}

#[test]
fn seek_to_step_values_across_chunk_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true, 4);

    let mut vs = codetracer_trace_reader::value_stream_reader::open_value_stream(&ct_path).unwrap().unwrap();
    let expected = expected_records_from_events(&ct_path);
    assert_eq!(vs.count() as usize, expected.len());
    assert!(vs.count() > vs.chunk_size() as u64 * 2, "need ≥3 chunks for a meaningful cross-boundary seek");

    // Seek to a record in a later chunk, then back into chunk 0.
    let target = expected.len() - 2;
    assert_eq!(vs.read(target as u64).unwrap(), expected[target]);
    assert_eq!(vs.read(0).unwrap(), expected[0]);

    // A value-less step in a non-first chunk decodes as an empty record.
    if let Some((i, _)) = expected.iter().enumerate().skip(vs.chunk_size()).find(|(_, r)| r.events.is_empty()) {
        assert_eq!(vs.read(i as u64).unwrap(), ValueRecordEntry::default());
    }

    // Out-of-range index errors, never panics.
    assert!(vs.read(vs.count() + 5).is_err());
}

#[test]
fn fetching_one_step_decompresses_only_its_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true, 4);

    let mut vs = codetracer_trace_reader::value_stream_reader::open_value_stream(&ct_path).unwrap().unwrap();
    let chunk_size = vs.chunk_size();
    assert_eq!(chunk_size, 4);
    let count = vs.count();
    assert!(count > chunk_size as u64 * 2, "need at least 3 chunks to make the bound meaningful");

    // A fresh read of step N's values must leave exactly chunk N's chunk cached
    // — no other chunk inflated. Counter-proven via cached_chunk() observation.
    let target_index = count - 1; // last step → last chunk
    let expected_chunk = (target_index as usize) / chunk_size;
    let _ = vs.read(target_index).unwrap();
    assert_eq!(
        vs.cached_chunk(),
        Some(expected_chunk),
        "fetching one step's values must decompress only its own chunk"
    );

    // Reading another step in the SAME chunk reuses the cache.
    if target_index % chunk_size as u64 != 0 {
        let sibling = target_index - 1;
        if (sibling as usize) / chunk_size == expected_chunk {
            let _ = vs.read(sibling).unwrap();
            assert_eq!(vs.cached_chunk(), Some(expected_chunk));
        }
    }

    // Reading a step in chunk 0 switches the single-chunk cache to chunk 0.
    let _ = vs.read(0).unwrap();
    assert_eq!(vs.cached_chunk(), Some(0));
}

#[test]
fn events_log_byte_identical_with_and_without_value_stream() {
    // The value-stream split is ADDITIVE: enabling it must not perturb the
    // unified events.log a single byte.
    let dir_off = tempfile::tempdir().unwrap();
    let dir_on = tempfile::tempdir().unwrap();
    let ct_off = write_trace(&dir_off, false, 4);
    let ct_on = write_trace(&dir_on, true, 4);

    let mut r_off = codetracer_ctfs::CtfsReader::open(&ct_off).unwrap();
    let mut r_on = codetracer_ctfs::CtfsReader::open(&ct_on).unwrap();
    let events_off = r_off.read_file("events.log").unwrap();
    let events_on = r_on.read_file("events.log").unwrap();
    assert_eq!(events_off, events_on, "events.log must be byte-identical regardless of the value-stream flag");

    // The flag-on container additionally carries values.dat + values.idx; the
    // flag-off one must not.
    assert!(r_on.read_file("values.dat").is_ok());
    assert!(r_on.read_file("values.idx").is_ok());
    assert!(r_off.read_file("values.dat").is_err());
}

#[test]
fn legacy_trace_has_no_value_stream() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, false, 4);

    // No dedicated value stream when the flag is off.
    let vs = codetracer_trace_reader::value_stream_reader::open_value_stream(&ct_path).unwrap();
    assert!(vs.is_none(), "a flag-off trace must not expose a value stream");

    // ...and the unified events.log still reads exactly as before, with the
    // same per-step value records derivable from it.
    let expected = expected_records_from_events(&ct_path);
    assert!(!expected.is_empty());
}
