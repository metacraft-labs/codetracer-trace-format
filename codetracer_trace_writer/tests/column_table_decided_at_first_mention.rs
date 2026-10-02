//! A column-aware file's table is decided by the writer when the file is first
//! mentioned.
//!
//! Spec: `codetracer-trace-format-spec/internal-files.md` §"`paths.dat` Layout
//! A", "Every Layout A record carries a table of non-zero size, and a file's
//! table is fixed when the file is first interned":
//!
//! * a table whose lines hold nothing gives its first line one position (`[0]`
//!   is recorded as `[1]`, `[0, 0]` as `[1, 0]`); any other table as given;
//! * an empty table, or none — the path first mentioned by a step, a function
//!   or an id request — records the conventional table, 100000 lines of 1024
//!   positions;
//! * on a file with the conventional table a column above 1024 is recorded at
//!   column 1024 of its line, and a line above 100000 is refused, naming the
//!   path; the refusal fails the recording (`trace-events.md` §"Recorder
//!   Integration — A Failed Call Fails the Recording").
//!
//! The Nim writer applies the same rules to the byte; that is asserted in
//! `codetracer_trace_writer_nim/tests/column_table_cross_writer.rs`.
//!
//! No mocks: every container is written to memory by the real writer.

use std::path::Path;

use codetracer_trace_types::Line;
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::{CtfsOutput, CtfsTraceWriter};
use codetracer_trace_writer::trace_writer::TraceWriter;

const P: &str = "/src/app.py";
const Q: &str = "/src/other.py";

fn column_aware(program: &str) -> CtfsTraceWriter {
    let mut w = CtfsTraceWriter::new(program, &[]).with_output(CtfsOutput::Memory);
    w.enable_column_aware_steps();
    TraceWriter::begin_writing_trace_events(&mut w, Path::new(program)).expect("begin");
    w
}

fn is_conventional(t: &[u32]) -> bool {
    t.len() == 100_000 && t.iter().all(|l| *l == 1024)
}

/// `(path id, line, column)` of every step, resolved through the writer's
/// own tables.
fn steps_of(w: &CtfsTraceWriter, bytes: &[u8]) -> Vec<(u64, u64, Option<u64>)> {
    use codetracer_trace_writer::column_aware::{PositionSpace, StepEvent, decode_step_event};
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("c.ct");
    std::fs::write(&p, bytes).unwrap();
    let mut r = codetracer_ctfs::CtfsReader::open(&p).unwrap();
    let dat = r.read_file("steps.dat").unwrap();
    let idx = r.read_file("steps.idx").unwrap();
    let offsets: Vec<usize> = idx[4..].chunks(8).map(|c| u64::from_le_bytes(c.try_into().unwrap()) as usize).collect();
    let mut space = PositionSpace::new(true);
    for t in w.line_lengths() {
        space.push_path(t);
    }
    let mut out = Vec::new();
    let mut position: u64 = 0;
    for i in 0..offsets.len() {
        let end = offsets.get(i + 1).copied().unwrap_or(dat.len());
        let chunk = codetracer_ctfs::zstd_compat::decode_all(&dat[offsets[i]..end]).unwrap();
        let mut pos = 0;
        while pos < chunk.len() {
            match decode_step_event(&chunk, &mut pos).expect("step event") {
                StepEvent::AbsoluteStep { global_position_index } => position = global_position_index,
                StepEvent::DeltaStep { delta } => position = (position as i64 + delta) as u64,
                StepEvent::DeltaColumn { column_delta } => position = (position as i64 + column_delta) as u64,
                _ => continue,
            }
            out.push(space.resolve(position).expect("a position inside the space"));
        }
    }
    out
}

fn finish(mut w: CtfsTraceWriter) -> (CtfsTraceWriter, Vec<u8>) {
    TraceWriter::finish_writing_trace_events(&mut w).expect("finish");
    let bytes = w.take_container_bytes().expect("in-memory container");
    (w, bytes)
}

#[test]
fn an_empty_table_records_the_conventional_table() {
    let mut w = column_aware("empty");
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new(P), &[]).expect("registered");
    assert!(
        is_conventional(&w.line_lengths()[0]),
        "an empty table must be recorded as 100000 lines of 1024 positions; it has {} lines",
        w.line_lengths()[0].len()
    );
}

#[test]
fn a_path_first_mentioned_without_a_table_gets_the_conventional_one() {
    let mut w = column_aware("implicit");
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new(Q), &[5, 5]).expect("Q");
    AbstractTraceWriter::ensure_path_id(&mut w, Path::new(P));
    AbstractTraceWriter::register_step(&mut w, Path::new("/src/stepped.py"), Line(3));
    AbstractTraceWriter::register_function(&mut w, "f", Path::new("/src/declared.py"), Line(7));
    let (w, bytes) = finish(w);
    let tables = w.line_lengths();
    assert_eq!(tables.len(), 4);
    assert_eq!(tables[0], vec![5, 5], "a given table is recorded as given");
    for (f, t) in tables.iter().enumerate().skip(1) {
        assert!(is_conventional(t), "file {f} has {} lines, not the conventional table", t.len());
    }
    assert_eq!(steps_of(&w, &bytes), vec![(2, 3, Some(1))]);
}

#[test]
fn a_table_whose_lines_hold_nothing_gives_its_first_line_a_position() {
    let mut w = column_aware("all_zero");
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new("/src/empty.py"), &[0]).unwrap();
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new("/src/blank.py"), &[0, 0]).unwrap();
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new("/src/mixed.py"), &[0, 3, 0]).unwrap();
    assert_eq!(w.line_lengths(), &[vec![1], vec![1, 0], vec![0, 3, 0]]);
}

#[test]
fn the_same_table_again_and_a_bare_lookup_return_the_id() {
    let mut w = column_aware("identical");
    let first = TraceWriter::register_path_with_line_lengths(&mut w, Path::new(P), &[3, 4]).unwrap();
    assert_eq!(
        TraceWriter::register_path_with_line_lengths(&mut w, Path::new(P), &[3, 4]).unwrap(),
        first
    );
    assert_eq!(AbstractTraceWriter::ensure_path_id(&mut w, Path::new(P)), first);
    assert_eq!(TraceWriter::register_path_with_line_lengths(&mut w, Path::new(P), &[]).unwrap(), first);
    assert_eq!(w.line_lengths(), &[vec![3, 4]]);
}

#[test]
fn columns_and_lines_on_a_conventional_file() {
    let mut w = column_aware("conventional");
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new(Q), &[2000]).unwrap();
    let col = |c: i64| Some(Line(c));
    AbstractTraceWriter::register_step_with_column(&mut w, Path::new(P), Line(5), col(2000));
    AbstractTraceWriter::register_step_with_column(&mut w, Path::new(P), Line(6), col(1001));
    w.register_column_step(100).expect("column move");
    AbstractTraceWriter::register_step(&mut w, Path::new(P), Line(100_000));
    AbstractTraceWriter::register_step_with_column(&mut w, Path::new(Q), Line(1), col(1500));
    AbstractTraceWriter::register_step(&mut w, Path::new(P), Line(0));
    assert!(w.refusals().is_empty(), "{:?}", w.refusals());
    let (w, bytes) = finish(w);
    assert_eq!(
        steps_of(&w, &bytes),
        vec![
            (1, 5, Some(1024)),
            (1, 6, Some(1001)),
            (1, 6, Some(1024)),
            (1, 100_000, Some(1)),
            (0, 1, Some(1500)),
            (1, 1, Some(1)),
        ],
        "columns past 1024 on the conventional file stop at 1024; the file with its own table keeps its column"
    );
}

#[test]
fn a_line_past_the_conventional_table_is_refused_naming_the_path() {
    let mut w = column_aware("past");
    AbstractTraceWriter::register_step(&mut w, Path::new(P), Line(100_001));
    assert!(w.refusals().iter().any(|r| r.contains(P) && r.contains("100001")), "{:?}", w.refusals());
    let err = TraceWriter::finish_writing_trace_events(&mut w)
        .expect_err("the refusal must fail the recording")
        .to_string();
    assert!(err.contains(P), "{err}");
}

#[test]
fn a_recorder_built_conventional_table_is_the_conventional_table() {
    let mut w = column_aware("recorder_built");
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new(P), &vec![1024; 100_000]).unwrap();
    AbstractTraceWriter::register_step_with_column(&mut w, Path::new(P), Line(2), Some(Line(5000)));
    let (w, bytes) = finish(w);
    assert_eq!(steps_of(&w, &bytes), vec![(0, 2, Some(1024))]);
}

#[test]
fn a_line_only_writer_ignores_tables() {
    let mut w = CtfsTraceWriter::new("line_only", &[]).with_output(CtfsOutput::Memory);
    TraceWriter::begin_writing_trace_events(&mut w, Path::new("line_only")).expect("begin");
    AbstractTraceWriter::ensure_path_id(&mut w, Path::new(P));
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new(P), &[3, 4]).unwrap();
    TraceWriter::register_path_with_line_lengths(&mut w, Path::new(Q), &[]).unwrap();
    AbstractTraceWriter::register_step(&mut w, Path::new(P), Line(200_000));
    TraceWriter::finish_writing_trace_events(&mut w).expect("finish");
}
