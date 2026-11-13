//! Every `EventLogKind` of `trace-events.md` §"EventLogKind (u8 enum)" --
//! the twenty-four of `meta.dat` version 7 -- round-trips through either
//! writer and either reader, and the Rust and the Nim reader refuse the same
//! unassigned kind bytes, each naming the value.
//!
//! The refused containers are real recordings whose `events.dat` is replaced
//! by a hand-encoded chunk carrying the bad kind byte, because no conforming
//! writer produces one; the control (the same chunk with an assigned kind) is
//! read by both. No mocks.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::{compress_pledged, CompressionMethod, CtfsReader, CtfsWriter};
use codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader;
use codetracer_trace_types::{EventLogKind, Line};
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::event_stream::IoEventRecord;
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "kinds";

fn nim_recording(dir: &Path) -> PathBuf {
    let src = PathBuf::from("/src/k.py");
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.join("e.json")).expect("begin");
    w.start(&src, Line(1));
    for (i, kind) in EventLogKind::ALL.iter().enumerate() {
        w.register_step(&src, Line(i as i64 + 2));
        w.register_special_event(*kind, &format!("m{i}"), &format!("c{i}"));
    }
    w.finish_writing_trace_events().expect("finish");
    w.close().expect("close");
    dir.join(format!("{PROGRAM}.ct"))
}

fn rust_recording(dir: &Path) -> PathBuf {
    use codetracer_trace_writer::trace_writer::TraceWriter as _;
    let src = PathBuf::from("/src/k.py");
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    w.begin_writing_trace_events(&dir.join(PROGRAM)).expect("begin");
    w.start(&src, Line(1));
    for (i, kind) in EventLogKind::ALL.iter().enumerate() {
        w.register_step(&src, Line(i as i64 + 2));
        w.register_special_event(*kind, &format!("m{i}"), &format!("c{i}"));
    }
    w.finish_writing_trace_events().expect("finish");
    dir.join(format!("{PROGRAM}.ct"))
}

fn rust_kinds(ct: &Path) -> Result<Vec<u8>, String> {
    let mut r = CtfsReader::open(ct).map_err(|e| e.to_string())?;
    let mut io = IoEventStreamReader::open(&mut r)?.ok_or("no events.dat")?;
    (0..io.count()).map(|i| io.read(i).map(|e| e.kind)).collect()
}

fn nim_kinds(ct: &Path) -> Result<Vec<u8>, String> {
    let r = NimTraceReaderHandle::open(ct.to_str().unwrap()).map_err(|e| e.to_string())?;
    (0..r.event_count())
        .map(|i| r.event_fields(i).map(|(k, _, _)| k).map_err(|e| e.to_string()))
        .collect()
}

/// The container at `ct` with `events.dat` replaced by one chunk holding one
/// record of kind byte `kind` at step 1.
fn with_one_event_of_kind(ct: &Path, kind: u8, out: &Path) {
    let mut rec = Vec::new();
    IoEventRecord {
        kind: 0,
        step_id: 1,
        metadata: b"m".to_vec(),
        content: b"c".to_vec(),
    }
    .encode(&mut rec);
    rec[0] = kind;
    assert!(rec.len() < 0x80, "the length prefix below is a one-byte varint");
    let mut raw = vec![rec.len() as u8];
    raw.extend_from_slice(&rec);
    let dat = compress_pledged(&raw, 3, "events.dat").unwrap();
    let mut idx = 64u32.to_le_bytes().to_vec();
    idx.extend_from_slice(&0u64.to_le_bytes());

    let mut reader = CtfsReader::open(ct).expect("open");
    let mut w = CtfsWriter::create_in_memory(4096, 31, CompressionMethod::None).expect("create");
    for (member, data) in reader.members().expect("members") {
        let data = match member.as_str() {
            "events.dat" => dat.clone(),
            "events.idx" => idx.clone(),
            _ => data,
        };
        let h = w.add_file(&member).expect("add");
        w.write(h, &data).expect("write");
    }
    std::fs::write(out, w.finish_to_bytes().expect("finish")).expect("write file");
}

fn sub(dir: &Path, name: &str) -> PathBuf {
    let d = dir.join(name);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn every_kind_round_trips_through_either_writer_and_either_reader() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let all: Vec<u8> = (0..24).collect();
    for (writer, ct) in [
        ("nim", nim_recording(&sub(dir.path(), "nim"))),
        ("rust", rust_recording(&sub(dir.path(), "rust"))),
    ] {
        assert_eq!(rust_kinds(&ct).unwrap(), all, "{writer} writer, Rust reader");
        assert_eq!(nim_kinds(&ct).unwrap(), all, "{writer} writer, Nim reader");
    }
}

#[test]
fn both_readers_refuse_the_same_unassigned_kinds_by_value() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let ct = rust_recording(&sub(dir.path(), "rust"));
    // The control: the same hand-encoded chunk with the last assigned kind.
    let control = dir.path().join("control.ct");
    with_one_event_of_kind(&ct, 23, &control);
    assert_eq!(rust_kinds(&control).unwrap(), vec![23], "Rust reader, control");
    assert_eq!(nim_kinds(&control).unwrap(), vec![23], "Nim reader, control");
    for bad in [24u8, 25, 128, 255] {
        let damaged = dir.path().join(format!("kind{bad}.ct"));
        with_one_event_of_kind(&ct, bad, &damaged);
        let rust = rust_kinds(&damaged).expect_err("the Rust reader read an unassigned kind");
        assert!(rust.contains(&bad.to_string()), "the Rust refusal does not name {bad}: {rust}");
        let nim = nim_kinds(&damaged).expect_err("the Nim reader read an unassigned kind");
        assert!(nim.contains(&bad.to_string()), "the Nim refusal does not name {bad}: {nim}");
    }
}
