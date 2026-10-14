//! Each record of a `calls.dat`, `values.dat` or `events.dat` chunk is framed
//! by its length, and a reader refuses a record whose fields do not consume
//! exactly `record_len` bytes, naming the stream and the record
//! (`trace-events.md` §"Call Stream (`calls.dat`)", "Each record is framed by
//! its length").
//!
//! The chunks are encoded by hand from real records (the writer never frames
//! a record wrongly), compressed as a writer's are, and read through the real
//! seekable readers.

use codetracer_trace_reader::call_stream_reader::CallStreamReader;
use codetracer_trace_reader::value_stream_reader::ValueStreamReader;
use codetracer_trace_writer::call_stream::{CallStreamRecord, VOID_RETURN_MARKER};
use codetracer_trace_writer::value_stream::{ValueRecordEntry, ValueStreamEvent};

fn put(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// A `.dat` + `.idx` pair: one chunk holding `records`, each framed by
/// `record_len` — with `pad` extra bytes inside the frame of record 1.
fn stream(stream: &str, records: &[Vec<u8>], pad: usize, shrink: usize) -> (Vec<u8>, Vec<u8>) {
    let mut raw = Vec::new();
    for (i, r) in records.iter().enumerate() {
        let mut r = r.clone();
        if i == 1 {
            r.extend(std::iter::repeat_n(0u8, pad));
            r.truncate(r.len() - shrink);
        }
        put(r.len() as u64, &mut raw);
        raw.extend_from_slice(&r);
    }
    let dat = codetracer_ctfs::compress_pledged(&raw, 3, stream).unwrap();
    let mut idx = 64u32.to_le_bytes().to_vec();
    idx.extend_from_slice(&0u64.to_le_bytes());
    (dat, idx)
}

fn call(key: u64) -> Vec<u8> {
    let mut out = Vec::new();
    CallStreamRecord {
        call_key: key,
        function_id: 3,
        parent_key: -1,
        first_step_id: 0,
        last_step_id: 4,
        depth: 0,
        args: vec![],
        return_value: vec![VOID_RETURN_MARKER],
        raised_exception: vec![],
        children: vec![],
    }
    .encode(&mut out);
    out
}

fn value() -> Vec<u8> {
    let mut out = Vec::new();
    ValueRecordEntry {
        events: vec![ValueStreamEvent::BindVariable { variable_id: 2, place: -5 }],
    }
    .encode(&mut out);
    out
}

fn read_calls(pad: usize, shrink: usize) -> Result<usize, String> {
    let (dat, idx) = stream("calls.dat", &[call(0), call(1), call(2)], pad, shrink);
    CallStreamReader::from_files(&[], dat, idx)?.expect("present").read_all().map(|v| v.len())
}

fn read_values(pad: usize, shrink: usize) -> Result<usize, String> {
    let (dat, idx) = stream("values.dat", &[value(), value(), value()], pad, shrink);
    ValueStreamReader::from_files(&[], dat, idx)?
        .expect("present")
        .read_all()
        .map(|v| v.len())
}

#[test]
fn well_framed_records_read() {
    assert_eq!(read_calls(0, 0), Ok(3));
    assert_eq!(read_values(0, 0), Ok(3));
}

#[test]
fn a_call_record_with_bytes_left_in_its_frame_is_refused() {
    let err = read_calls(1, 0).expect_err("refused");
    assert!(err.contains("calls.dat") && err.contains("record 1"), "{err}");
}

#[test]
fn a_call_record_whose_fields_overrun_its_frame_is_refused() {
    let err = read_calls(0, 1).expect_err("refused");
    assert!(err.contains("calls.dat") && err.contains("record 1"), "{err}");
}

#[test]
fn a_value_record_whose_fields_overrun_its_frame_is_refused() {
    // One byte short: the last event's place varint is cut, so the event
    // continues past the frame.
    let err = read_values(0, 1).expect_err("refused");
    assert!(err.contains("values.dat") && err.contains("record 1"), "{err}");
}

#[test]
fn an_event_record_with_bytes_left_in_its_frame_is_refused() {
    let err = codetracer_trace_writer::event_stream::IoEventRecord::decode(&[0, 0, 0, 0, 0]).expect_err("refused");
    assert!(err.contains("events.dat"), "{err}");
    // Through the seekable reader, the record is named.
    let dir = tempfile::tempdir().unwrap();
    let ct = dir.path().join("e.ct");
    let rec = vec![0u8, 1, 0, 0];
    let (dat, idx) = stream("events.dat", &[rec.clone(), rec.clone(), rec], 1, 0);
    let mut c = codetracer_ctfs::CtfsWriter::create(&ct, 4096, 31).unwrap();
    let h = c.add_file("events.dat").unwrap();
    c.write(h, &dat).unwrap();
    let h = c.add_file("events.idx").unwrap();
    c.write(h, &idx).unwrap();
    c.close().unwrap();
    let mut r = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let err = match codetracer_trace_reader::io_event_stream_reader::IoEventStreamReader::open(&mut r) {
        Err(e) => e,
        Ok(Some(mut io)) => io.read(1).expect_err("refused"),
        Ok(None) => panic!("events.dat vanished"),
    };
    assert!(err.contains("events.dat") && err.contains("record 1"), "{err}");
}
