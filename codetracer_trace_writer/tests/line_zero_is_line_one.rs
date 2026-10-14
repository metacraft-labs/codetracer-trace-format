//! A location registered at line 0 is recorded as line 1 of the same file,
//! in every member that carries it: `steps.dat`'s position, `funcs.dat`'s
//! `global_line_index`, and the `step-map.ns` key (`internal-files.md`
//! §"Global Line Index", "Line 0 is line 1, everywhere"). A writer does not
//! refuse line 0.
//!
//! No mocks: the container is the real writer's, read back with the real
//! stream, interning and step-map readers.

use std::path::Path;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;
use codetracer_trace_reader::step_map_reader::StepMapReader;
use codetracer_trace_reader::step_stream_reader::StepStreamReader;
use codetracer_trace_types::Line;
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::step_stream::StepStreamRecord;
use codetracer_trace_writer::trace_writer::TraceWriter;

#[test]
fn a_step_and_a_function_at_line_0_are_recorded_at_line_1() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("zero");
    let mut w = CtfsTraceWriter::new("zero", &[]);
    TraceWriter::begin_writing_trace_events(&mut w, &base).unwrap();
    let (a, b) = (Path::new("/a.py"), Path::new("/b.py"));
    AbstractTraceWriter::ensure_path_id(&mut w, a);
    AbstractTraceWriter::ensure_path_id(&mut w, b);
    let f0 = AbstractTraceWriter::ensure_function_id(&mut w, "at_zero", b, Line(0));
    let f1 = AbstractTraceWriter::ensure_function_id(&mut w, "at_one", b, Line(1));
    AbstractTraceWriter::register_step(&mut w, b, Line(0)); // exec 0
    AbstractTraceWriter::register_step(&mut w, b, Line(1)); // exec 1
    AbstractTraceWriter::register_step(&mut w, a, Line(3)); // exec 2
    AbstractTraceWriter::register_step(&mut w, b, Line(0)); // exec 3
    TraceWriter::finish_writing_trace_events(&mut w).expect("line 0 is not refused");
    let ct = base.with_extension("ct");
    let mut r = CtfsReader::open(&ct).unwrap();

    let steps = StepStreamReader::open(&mut r).unwrap().unwrap().read_all().unwrap();
    let pos = |i: usize| match &steps[i] {
        StepStreamRecord::Step { global_line_index } => *global_line_index,
        other => panic!("exec {i} is {other:?}"),
    };
    assert_eq!(pos(0), pos(1), "steps.dat: line 0 is line 1's address");

    let t = InterningTablesReader::open(&mut r).unwrap().unwrap();
    assert_eq!(
        t.func(f0.0 as u64).unwrap().global_line_index,
        t.func(f1.0 as u64).unwrap().global_line_index,
        "funcs.dat: a function at line 0 is at line 1"
    );

    let map = StepMapReader::open(&mut r)
        .unwrap()
        .expect("a line-only trace carries step-map.ns")
        .load_all()
        .unwrap();
    assert_eq!(map.get(&(1, 1)), Some(&vec![0, 1, 3]), "step-map.ns keys line 0 under line 1");
    assert_eq!(map.get(&(1, 0)), None, "no key for line 0");
    assert_eq!(map.get(&(0, 3)), Some(&vec![2]));
}
