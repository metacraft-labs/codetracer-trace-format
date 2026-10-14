//! An `events.dat` record's kind byte is the recorder's exact `EventLogKind`
//! ordinal, 0-13 (`trace-events.md` §"EventLogKind (u8 enum)"):
//!
//! * every kind round-trips exactly, through the writer, the seekable
//!   `IoEventStreamReader` and the split-stream event assembly;
//! * a writer refuses an unassigned value, naming it;
//! * a reader refuses a record carrying one, naming it, rather than
//!   substituting a kind.
//!
//! No mocks. The refused container is written with the real CTFS writer from
//! a hand-encoded chunk, because no conforming writer produces one.

use std::path::Path;

use codetracer_ctfs::CtfsWriter;
use codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader;
use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::event_stream::{IoEventRecord, encode_io_event_stream};
use codetracer_trace_writer::trace_writer::TraceWriter;

const ALL: [EventLogKind; 14] = [
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

#[test]
fn every_kind_round_trips_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("kinds");
    let mut w = CtfsTraceWriter::new("kinds", &[]).with_events_chunk_size(4);
    TraceWriter::begin_writing_trace_events(&mut w, &base).unwrap();
    let src = Path::new("/k.rs");
    TraceWriter::start(&mut w, src, Line(1));
    for (i, kind) in ALL.iter().enumerate() {
        TraceWriter::register_step(&mut w, src, Line(i as i64 + 2));
        TraceWriter::register_special_event(&mut w, *kind, &format!("m{i}"), &format!("c{i}"));
    }
    TraceWriter::finish_writing_trace_events(&mut w).unwrap();
    let ct = base.with_extension("ct");

    let mut r = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let mut io = IoEventStreamReader::open(&mut r).unwrap().expect("events.dat");
    let kinds: Vec<u8> = (0..io.count()).map(|i| io.read(i).unwrap().kind).collect();
    assert_eq!(kinds, (0..14u8).collect::<Vec<_>>(), "the kind byte is the EventLogKind ordinal");

    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct).unwrap();
    let read: Vec<EventLogKind> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Event(RecordEvent { kind, .. }) => Some(*kind),
            _ => None,
        })
        .collect();
    assert_eq!(read, ALL.to_vec(), "the assembled events report each kind as written");
}

#[test]
fn a_writer_refuses_an_unassigned_kind() {
    let rec = |kind| IoEventRecord {
        kind,
        step_id: 0,
        metadata: vec![],
        content: vec![],
    };
    assert!(encode_io_event_stream(&[rec(13)], 64, 3).is_ok(), "control: 13 is EvmEvent");
    let err = encode_io_event_stream(&[rec(13), rec(14)], 64, 3).err().expect("14 is unassigned");
    assert!(err.contains("14"), "{err}");
}

#[test]
fn a_reader_refuses_an_unassigned_kind_by_name() {
    let err = IoEventRecord::decode(&[14, 0, 0, 0]).expect_err("14 is unassigned");
    assert!(err.contains("14") && err.contains("EventLogKind"), "{err}");
    assert_eq!(IoEventRecord::decode(&[13, 0, 0, 0]).unwrap().kind, 13, "control");

    // The same record inside a container, through the seekable reader.
    let dir = tempfile::tempdir().unwrap();
    let ct = dir.path().join("bad.ct");
    let raw = [4u8, 200, 5, 0, 0]; // record_len 4, then kind 200, step 5, two empty blobs
    let dat = codetracer_ctfs::compress_pledged(&raw, 3, "events.dat").unwrap();
    let mut idx = 64u32.to_le_bytes().to_vec();
    idx.extend_from_slice(&0u64.to_le_bytes());
    let mut c = CtfsWriter::create(&ct, 4096, 31).unwrap();
    let h = c.add_file("events.dat").unwrap();
    c.write(h, &dat).unwrap();
    let h = c.add_file("events.idx").unwrap();
    c.write(h, &idx).unwrap();
    c.close().unwrap();
    let mut r = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let err = match IoEventStreamReader::open(&mut r) {
        Err(e) => e,
        Ok(Some(mut io)) => io.read(0).expect_err("refused"),
        Ok(None) => panic!("events.dat vanished"),
    };
    assert!(err.contains("200"), "{err}");
}
