//! M23c round-trip tests for the dedicated `events.dat` I/O event stream.
//!
//! A trace is written with the `has_io_event_stream` flag on, interleaving
//! stdout/stderr/file I/O events with steps. The I/O event records are then read
//! back two ways and compared:
//!   1. directly from `events.dat` via the seekable, paginated
//!      `IoEventStreamReader`, and
//!   2. re-derived from the unchanged `events.log` (read with the normal
//!      reader) by feeding its events through the SAME `IoEventStreamBuilder` the
//!      writer used.
//! The two MUST agree — proving `events.dat` is consistent with the unified
//! stream it was split from, that each record's kind / step_id / metadata /
//! content match the `Event` records derived from `events.log`, and that
//! paginated reads (incl. across a chunk boundary) recover the exact records. A
//! flag-off (legacy) trace is also exercised to confirm the I/O event stream is
//! absent and old readers are unaffected, and an `events.log` byte-identity
//! check proves the split is additive (and that the distinct `events.dat`/
//! `events.log` files never collide).

use std::path::Path;

use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::event_stream::{IoEventRecord, IoEventStreamBuilder};
use codetracer_trace_writer::trace_writer::TraceWriter;

/// Write a trace whose steps interleave a variety of I/O events:
///   * stdout/stderr writes,
///   * file writes,
///   * an error event,
/// across enough steps to cross several small `events.dat` chunks. Returns the
/// `.ct` path.
fn write_trace(dir: &tempfile::TempDir, with_io_event_stream: bool, events_chunk_size: usize) -> std::path::PathBuf {
    let path_buf = dir.path().join("trace");
    let mut writer = CtfsTraceWriter::new("test_program", &[])
        .with_io_event_stream(with_io_event_stream)
        .with_events_chunk_size(events_chunk_size);
    TraceWriter::begin_writing_trace_events(&mut writer, &path_buf).unwrap();

    let src = Path::new("/test/prog.rs");
    TraceWriter::start(&mut writer, src, Line(1));

    let main_fn = TraceWriter::ensure_function_id(&mut writer, "main", src, Line(1));
    TraceWriter::register_call(&mut writer, main_fn, vec![]);

    // An I/O event BEFORE the first register_step (attributes to step 0).
    TraceWriter::register_special_event(&mut writer, EventLogKind::Write, "stdout", "startup banner\n");

    for ln in 2..=30i64 {
        TraceWriter::register_step(&mut writer, src, Line(ln));
        // Every step writes to stdout.
        TraceWriter::register_special_event(&mut writer, EventLogKind::Write, "stdout", &format!("line {ln}\n"));
        if ln % 4 == 0 {
            TraceWriter::register_special_event(&mut writer, EventLogKind::Error, "stderr", &format!("warn {ln}\n"));
        }
        if ln % 5 == 0 {
            TraceWriter::register_special_event(&mut writer, EventLogKind::WriteFile, "/tmp/out.log", &format!("file write {ln}\n"));
        }
    }

    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
    path_buf.with_extension("ct")
}

/// Re-derive the expected I/O event records straight from `events.log` (read
/// with the unchanged unified-stream reader), by replaying its events through
/// the SAME `IoEventStreamBuilder` the writer uses. This is the ground truth the
/// `events.dat`-decoded records must equal.
fn expected_records_from_events(ct_path: &Path) -> Vec<IoEventRecord> {
    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(ct_path).unwrap();
    let mut builder = IoEventStreamBuilder::new();
    for ev in &events {
        builder.observe(ev);
    }
    builder.finish()
}

#[test]
fn events_dat_matches_events_log() {
    let dir = tempfile::tempdir().unwrap();
    // A small chunk size so the records span multiple chunks and the round-trip
    // exercises per-chunk independent decode.
    let ct_path = write_trace(&dir, true, 8);

    let mut es = codetracer_trace_reader::io_event_stream_reader::open_io_event_stream(&ct_path)
        .expect("open_io_event_stream ok")
        .expect("io event stream present when has_io_event_stream flag is set");
    let from_dat = es.read_all().unwrap();

    let expected = expected_records_from_events(&ct_path);
    assert_eq!(
        from_dat, expected,
        "events.dat decoded I/O event records must equal the events.log-derived records (kind/step_id/metadata/content)"
    );
    assert!(!expected.is_empty(), "the trace must have produced I/O event records");

    // The record count equals the number of Event records in events.log.
    let event_count = {
        let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
        let events = reader.load_trace_events(&ct_path).unwrap();
        events.iter().filter(|e| matches!(e, TraceLowLevelEvent::Event(_))).count()
    };
    assert_eq!(from_dat.len(), event_count, "io event record count must equal the Event count in events.log");

    // The pre-first-step event attributes to step 0; later events to their step.
    assert_eq!(from_dat[0].step_id, 0, "the startup banner (pre-first-step) attributes to step 0");
    assert!(from_dat.iter().any(|r| r.step_id > 0), "expected events attributed to later steps");
    // Variety: at least one stderr/error and one file-write record.
    assert!(from_dat.iter().any(|r| r.kind == EventLogKind::Error as u8), "expected an error/stderr record");
    assert!(from_dat.iter().any(|r| r.kind == EventLogKind::WriteFile as u8), "expected a file-write record");
}

#[test]
fn paginated_read_across_chunk_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true, 8);

    let mut es = codetracer_trace_reader::io_event_stream_reader::open_io_event_stream(&ct_path).unwrap().unwrap();
    let expected = expected_records_from_events(&ct_path);
    assert_eq!(es.count() as usize, expected.len());
    assert!(es.count() > es.chunk_size() as u64 * 2, "need ≥3 chunks for a meaningful cross-boundary page");

    // A page that straddles the chunk-0 / chunk-1 boundary.
    let cs = es.chunk_size() as u64;
    let page_start = cs - 2;
    let page_len = 5; // spans into the next chunk
    let page = es.read_page(page_start, page_len).unwrap();
    assert_eq!(page.as_slice(), &expected[page_start as usize..(page_start + page_len) as usize]);

    // A page that straddles three chunks.
    let big_start = cs - 1;
    let big_len = cs * 2 + 2;
    let big_page = es.read_page(big_start, big_len).unwrap();
    let big_end = (big_start + big_len).min(es.count()) as usize;
    assert_eq!(big_page.as_slice(), &expected[big_start as usize..big_end]);

    // A page clamped past the end returns only the in-range records.
    let tail = es.read_page(es.count() - 2, 100).unwrap();
    assert_eq!(tail.as_slice(), &expected[expected.len() - 2..]);

    // An empty page (len 0, or start past end) returns nothing, never errors.
    assert!(es.read_page(0, 0).unwrap().is_empty());
    assert!(es.read_page(es.count() + 10, 5).unwrap().is_empty());

    // Single-record read out of range errors, never panics.
    assert!(es.read(es.count() + 5).is_err());
}

#[test]
fn fetching_one_event_decompresses_only_its_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true, 8);

    let mut es = codetracer_trace_reader::io_event_stream_reader::open_io_event_stream(&ct_path).unwrap().unwrap();
    let chunk_size = es.chunk_size();
    assert_eq!(chunk_size, 8);
    let count = es.count();
    assert!(count > chunk_size as u64 * 2, "need at least 3 chunks to make the bound meaningful");

    // A fresh read of record N must leave exactly chunk N's chunk cached — no
    // other chunk inflated. Counter-proven via cached_chunk() observation.
    let target_index = count - 1; // last record → last chunk
    let expected_chunk = (target_index as usize) / chunk_size;
    let _ = es.read(target_index).unwrap();
    assert_eq!(
        es.cached_chunk(),
        Some(expected_chunk),
        "fetching one I/O event must decompress only its own chunk"
    );

    // Reading another record in the SAME chunk reuses the cache.
    if target_index % chunk_size as u64 != 0 {
        let sibling = target_index - 1;
        if (sibling as usize) / chunk_size == expected_chunk {
            let _ = es.read(sibling).unwrap();
            assert_eq!(es.cached_chunk(), Some(expected_chunk));
        }
    }

    // Reading a record in chunk 0 switches the single-chunk cache to chunk 0.
    let _ = es.read(0).unwrap();
    assert_eq!(es.cached_chunk(), Some(0));

    // A single-chunk page decompresses only that one chunk.
    let _ = es.read_page(0, chunk_size as u64).unwrap();
    assert_eq!(es.cached_chunk(), Some(0));
}

#[test]
fn events_log_byte_identical_with_and_without_io_event_stream() {
    // The I/O event-stream split is ADDITIVE: enabling it must not perturb the
    // unified events.log a single byte. (And the distinct events.dat must never
    // be mistaken for events.log.)
    let dir_off = tempfile::tempdir().unwrap();
    let dir_on = tempfile::tempdir().unwrap();
    let ct_off = write_trace(&dir_off, false, 8);
    let ct_on = write_trace(&dir_on, true, 8);

    let mut r_off = codetracer_ctfs::CtfsReader::open(&ct_off).unwrap();
    let mut r_on = codetracer_ctfs::CtfsReader::open(&ct_on).unwrap();
    let events_off = r_off.read_file("events.log").unwrap();
    let events_on = r_on.read_file("events.log").unwrap();
    assert_eq!(events_off, events_on, "events.log must be byte-identical regardless of the io-event-stream flag");

    // The flag-on container additionally carries events.dat + events.idx; the
    // flag-off one must not. events.dat is DISTINCT from events.log.
    assert!(r_on.read_file("events.dat").is_ok());
    assert!(r_on.read_file("events.idx").is_ok());
    assert!(r_on.read_file("events.log").is_ok(), "events.log still present alongside events.dat");
    assert!(r_off.read_file("events.dat").is_err());
}

#[test]
fn legacy_trace_has_no_io_event_stream() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, false, 8);

    // No dedicated I/O event stream when the flag is off.
    let es = codetracer_trace_reader::io_event_stream_reader::open_io_event_stream(&ct_path).unwrap();
    assert!(es.is_none(), "a flag-off trace must not expose an I/O event stream");

    // ...and the unified events.log still reads exactly as before, with the
    // same I/O event records derivable from it.
    let expected = expected_records_from_events(&ct_path);
    assert!(!expected.is_empty());
}
