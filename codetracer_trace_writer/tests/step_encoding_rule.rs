//! `steps.dat` follows the one encoding rule of `trace-events.md`
//! §"Encoding Rules", on both of this writer's step paths (line-only and
//! column-aware):
//!
//! 1. the first position record of every chunk is an `AbsoluteStep`, whatever
//!    records precede it in the chunk;
//! 2. otherwise a record is a delta (`DeltaColumn` for a registered column
//!    step, `DeltaStep` otherwise) exactly when the varint of `zigzag(d)` is
//!    strictly shorter than the varint of the position;
//! 3. otherwise an `AbsoluteStep` — a tie goes to the absolute.
//!
//! Calls, returns and thread switches force nothing.
//!
//! The expected chunks are written out byte for byte, so the test pins the
//! rule rather than any decoder's opinion of it. No mocks: the containers are
//! produced by the real writer and the chunks read out of them with the real
//! CTFS reader.

use std::path::Path;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::{Line, ThreadId, TraceLowLevelEvent};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::{CtfsOutput, CtfsTraceWriter};
use codetracer_trace_writer::trace_writer::TraceWriter;

const ABS: u8 = 0;
const DELTA: u8 = 1;
const THREAD_SWITCH: u8 = 4;
const COLUMN: u8 = 7;

/// The decompressed chunks of `steps.dat`.
fn chunks(container: &[u8]) -> Vec<Vec<u8>> {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("c.ct");
    std::fs::write(&p, container).unwrap();
    let mut r = CtfsReader::open(&p).unwrap();
    let dat = r.read_file("steps.dat").unwrap();
    let idx = r.read_file("steps.idx").unwrap();
    let offsets: Vec<usize> = idx[4..].chunks(8).map(|c| u64::from_le_bytes(c.try_into().unwrap()) as usize).collect();
    (0..offsets.len())
        .map(|i| {
            let end = offsets.get(i + 1).copied().unwrap_or(dat.len());
            codetracer_ctfs::zstd_compat::decode_all(&dat[offsets[i]..end]).unwrap()
        })
        .collect()
}

fn finish(mut w: CtfsTraceWriter) -> Vec<u8> {
    TraceWriter::finish_writing_trace_events(&mut w).expect("finish");
    w.take_container_bytes().expect("in-memory container")
}

fn step(w: &mut CtfsTraceWriter, path: &str, line: i64) {
    AbstractTraceWriter::register_step(w, Path::new(path), Line(line));
}

#[test]
fn line_only_steps_take_the_shorter_encoding_and_a_tie_goes_to_the_absolute() {
    let mut w = CtfsTraceWriter::new("rule", &[]).with_output(CtfsOutput::Memory);
    TraceWriter::begin_writing_trace_events(&mut w, Path::new("rule")).unwrap();
    AbstractTraceWriter::ensure_path_id(&mut w, Path::new("/a"));
    AbstractTraceWriter::ensure_path_id(&mut w, Path::new("/b"));
    let f = AbstractTraceWriter::ensure_function_id(&mut w, "f", Path::new("/a"), Line(1));
    step(&mut w, "/a", 1); // p 0: the chunk's first position
    step(&mut w, "/a", 2); // p 1: delta 1 is one byte, so is 1: tie
    step(&mut w, "/a", 201); // p 200: delta 199 is two bytes, so is 200: tie
    step(&mut w, "/a", 202); // p 201: delta 1 (one byte) beats 201 (two)
    AbstractTraceWriter::register_call(&mut w, f, vec![]);
    step(&mut w, "/a", 203); // a call forces nothing
    AbstractTraceWriter::register_return(
        &mut w,
        codetracer_trace_types::ValueRecord::None {
            type_id: codetracer_trace_types::TypeId(0),
        },
    );
    step(&mut w, "/a", 204); // nor does a return
    AbstractTraceWriter::add_event(&mut w, TraceLowLevelEvent::ThreadSwitch(ThreadId(3)));
    step(&mut w, "/a", 205); // nor a thread switch
    step(&mut w, "/b", 1); // p 100000: delta 99796 is three bytes, so is 100000: tie
    step(&mut w, "/a", 300); // p 299: delta -99701 is three bytes, 299 two
    let got = chunks(&finish(w));
    let want: Vec<u8> = [
        vec![ABS, 0],
        vec![ABS, 1],
        vec![ABS, 0xc8, 0x01],
        vec![DELTA, 2],
        vec![DELTA, 2],
        vec![DELTA, 2],
        vec![THREAD_SWITCH, 3],
        vec![DELTA, 2],
        vec![ABS, 0xa0, 0x8d, 0x06],
        vec![ABS, 0xab, 0x02],
    ]
    .concat();
    assert_eq!(got, vec![want]);
}

#[test]
fn every_chunk_opens_with_an_absolute_even_after_a_thread_record() {
    let mut w = CtfsTraceWriter::new("chunks", &[])
        .with_output(CtfsOutput::Memory)
        .with_steps_chunk_size(3);
    TraceWriter::begin_writing_trace_events(&mut w, Path::new("chunks")).unwrap();
    AbstractTraceWriter::ensure_path_id(&mut w, Path::new("/a"));
    step(&mut w, "/a", 201);
    step(&mut w, "/a", 202);
    step(&mut w, "/a", 203);
    AbstractTraceWriter::add_event(&mut w, TraceLowLevelEvent::ThreadSwitch(ThreadId(1)));
    step(&mut w, "/a", 204);
    step(&mut w, "/a", 205);
    let got = chunks(&finish(w));
    assert_eq!(
        got,
        vec![
            vec![ABS, 0xc8, 0x01, DELTA, 2, DELTA, 2],
            // The chunk opens with a thread record; its first POSITION is still absolute.
            vec![THREAD_SWITCH, 1, ABS, 0xcb, 0x01, DELTA, 2],
        ]
    );
}

#[test]
fn column_aware_steps_follow_the_same_rule() {
    let mut w = CtfsTraceWriter::new("cols", &[]).with_output(CtfsOutput::Memory);
    w.enable_column_aware_steps();
    TraceWriter::begin_writing_trace_events(&mut w, Path::new("cols")).unwrap();
    w.register_path_with_line_lengths(Path::new("/a"), &[200, 200]);
    AbstractTraceWriter::add_event(&mut w, TraceLowLevelEvent::ThreadSwitch(ThreadId(2)));
    step(&mut w, "/a", 1); // p 0, the chunk's first position, after a thread record
    w.register_column_step(150).unwrap(); // p 150: delta 150 and 150 are both two bytes: tie
    w.register_column_step(1).unwrap(); // p 151: a one-byte column delta
    step(&mut w, "/a", 2); // p 200: delta 49 (one byte) beats 200 (two): a line delta
    let got = chunks(&finish(w));
    assert_eq!(got, vec![vec![THREAD_SWITCH, 2, ABS, 0, ABS, 0x96, 0x01, COLUMN, 2, DELTA, 0x62]]);
}
