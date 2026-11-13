//! `StepStreamReader::read` answers every step the same whatever order the
//! steps are read in.
//!
//! The reader decodes a chunk only as far as reads reach and restarts a read
//! behind its cursor from the nearest checkpoint, because a step's position
//! can depend on every record before it in its chunk. The oracle is
//! `decode_chunk_records`, which decodes a whole chunk from its first byte.
//! The trace is written by the production `CtfsTraceWriter` with chunks of
//! several hundred records, so a chunk spans many checkpoints and holds both
//! absolute and delta records. No mocks.

use std::path::Path;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::step_stream_reader::{StepStreamReader, decode_chunk_records};
use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::step_stream::StepStreamRecord;
use codetracer_trace_writer::trace_writer::TraceWriter;

const CHUNK: usize = 300;

/// A trace of `n` steps that walk a loop body (small deltas) and jump between
/// two files (absolute records) at irregular intervals.
fn write_trace(dir: &tempfile::TempDir, n: usize) -> std::path::PathBuf {
    let path_buf = dir.path().join("trace");
    let mut writer = CtfsTraceWriter::new("steps_any_order", &[]).with_steps_chunk_size(CHUNK);
    TraceWriter::begin_writing_trace_events(&mut writer, &path_buf).unwrap();
    let a = Path::new("/t/a.rs");
    let b = Path::new("/t/b.rs");
    TraceWriter::start(&mut writer, a, Line(1));
    for i in 0..n {
        let (file, line) = if i % 37 < 5 {
            (b, 200 + (i % 11) as i64)
        } else {
            (a, 1 + (i % 9) as i64)
        };
        TraceWriter::register_step(&mut writer, file, Line(line));
    }
    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
    path_buf.with_extension("ct")
}

/// Every record, chunk by chunk, through the whole-chunk decoder.
fn oracle(ct: &Path) -> Vec<StepStreamRecord> {
    let mut r = CtfsReader::open(ct).unwrap();
    let dat = r.read_file("steps.dat").unwrap();
    let idx = r.read_file("steps.idx").unwrap();
    let offsets: Vec<usize> = idx[4..]
        .chunks_exact(8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()) as usize)
        .collect();
    let mut out = Vec::new();
    for (k, start) in offsets.iter().enumerate() {
        let end = offsets.get(k + 1).copied().unwrap_or(dat.len());
        out.extend(decode_chunk_records(&dat[*start..end]).unwrap());
    }
    out
}

#[test]
fn reads_in_any_order_agree_with_the_whole_chunk_decode() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_trace(&dir, 2000);
    let expected = oracle(&ct);
    assert!(expected.len() > 3 * CHUNK, "several chunks, or nothing was tested");

    // Backwards through the whole stream: every read but a chunk's last is
    // behind the cursor.
    let mut r = StepStreamReader::open(&mut CtfsReader::open(&ct).unwrap()).unwrap().unwrap();
    assert_eq!(r.count() as usize, expected.len());
    for i in (0..expected.len()).rev() {
        assert_eq!(r.read(i as u64).unwrap(), expected[i], "step {i}, read backwards");
    }

    // A pseudo-random walk that stays in one chunk for a while, jumping
    // forwards and back across checkpoints, then moves on.
    let mut s: u64 = 0x9e37_79b9_7f4a_7c15;
    for _ in 0..5000 {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let chunk = (s >> 40) as usize % expected.len().div_ceil(CHUNK);
        let i = (chunk * CHUNK + (s >> 8) as usize % CHUNK).min(expected.len() - 1);
        assert_eq!(r.read(i as u64).unwrap(), expected[i], "step {i}");
    }

    // And forwards, the way a full decode walks it.
    assert_eq!(r.read_all().unwrap(), expected);
}

/// A read past the last record of the last chunk is refused, and does not
/// disturb the reads after it.
#[test]
fn a_read_past_the_end_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_trace(&dir, 700);
    let expected = oracle(&ct);
    let mut r = StepStreamReader::open(&mut CtfsReader::open(&ct).unwrap()).unwrap().unwrap();
    let last = expected.len() - 1;
    assert_eq!(r.read(last as u64).unwrap(), expected[last]);
    assert!(r.read(expected.len() as u64).is_err());
    assert_eq!(r.read(3).unwrap(), expected[3]);
}
