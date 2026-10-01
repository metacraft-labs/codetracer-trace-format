//! A reader refuses a `DeltaStep` or `DeltaColumn` that comes before its
//! chunk's first `AbsoluteStep`, naming the chunk (`trace-events.md`
//! §"Encoding Rules", "Reading"). Resolving it against `0`, or against the
//! previous chunk's cursor, would turn a non-conforming chunk into positions
//! that were never recorded.
//!
//! The chunks are built by hand, because no conforming writer produces one;
//! they are framed and compressed exactly as `steps.dat` chunks are, and read
//! through the real `StepStreamReader`.

use codetracer_trace_reader::step_stream_reader::{StepStreamReader, decode_chunk_records, decode_chunk_records_at};
use codetracer_trace_writer::meta_dat::encode_meta_dat;

const CHUNK_SIZE: u32 = 2;

/// `steps.dat` + `steps.idx` holding `chunks` (raw, uncompressed).
fn stream(chunks: &[&[u8]]) -> (Vec<u8>, Vec<u8>) {
    let mut dat = Vec::new();
    let mut idx = CHUNK_SIZE.to_le_bytes().to_vec();
    for c in chunks {
        idx.extend_from_slice(&(dat.len() as u64).to_le_bytes());
        dat.extend_from_slice(&codetracer_ctfs::compress_pledged(c, 3, "steps.dat").unwrap());
    }
    (dat, idx)
}

fn read_all(chunks: &[&[u8]]) -> Result<usize, String> {
    let (dat, idx) = stream(chunks);
    let meta = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "p", &[], "", "", 0);
    let mut r = StepStreamReader::from_files(&meta, dat, idx)?.expect("present");
    r.read_all().map(|v| v.len())
}

#[test]
fn an_anchored_stream_reads() {
    // Chunk 0: absolute 200, delta +1. Chunk 1: thread switch, absolute 300.
    assert_eq!(read_all(&[&[0, 0xc8, 0x01, 1, 2], &[4, 1, 0, 0xac, 0x02]]), Ok(4));
}

#[test]
fn a_delta_step_after_a_thread_record_and_before_any_absolute_is_refused() {
    let err = read_all(&[&[0, 0xc8, 0x01, 1, 2], &[4, 1, 1, 2]]).expect_err("an unanchored delta must be refused");
    assert!(err.contains("chunk 1") && err.contains("DeltaStep"), "{err}");
}

#[test]
fn a_delta_column_opening_a_chunk_is_refused() {
    let err = read_all(&[&[7, 2, 0, 5]]).expect_err("an unanchored column delta must be refused");
    assert!(err.contains("chunk 0") && err.contains("DeltaColumn"), "{err}");
}

#[test]
fn the_chunk_decoders_refuse_it_too() {
    let (dat, _) = stream(&[&[4, 1, 1, 2]]);
    let err = decode_chunk_records_at(&dat, 7, false).expect_err("refused");
    assert!(err.contains("chunk 7"), "{err}");
    let err = decode_chunk_records(&dat).expect_err("refused");
    assert!(err.contains("DeltaStep"), "{err}");
}
