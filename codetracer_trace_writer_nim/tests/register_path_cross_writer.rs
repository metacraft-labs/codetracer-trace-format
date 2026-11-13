//! A path registered on its own, before anything uses it, through both
//! writers' `register_path`, with no line-count table.
//!
//! The path is then named by a function and by steps, and registered again.
//! Registration interns: the path gets one `paths.dat` record and one id, and
//! every later mention resolves to it. A writer that appends a second record
//! for the later mentions shifts every position address in `steps.dat` and
//! the declaration address in `funcs.dat`, so the four members below differ
//! between the writers.
//!
//! No mocks: both writers are the shipped ones, and the containers are read
//! by the shipped CTFS reader.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::Line;
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

mod whole_file;

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "paths";
const A: &str = "/src/a.py";
const B: &str = "/src/b.py";

fn write_nim(dir: &Path) -> PathBuf {
    let (a, b) = (PathBuf::from(A), PathBuf::from(B));
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.set_workdir(Path::new("/work"));
    w.set_recording_id("01900000-0000-7000-8000-0000000000cc").expect("nim recording id");
    w.begin_writing_trace_events(&dir.join("e.json")).expect("nim begin_events");
    w.begin_writing_trace_metadata(&dir.join("m.json")).expect("nim begin_metadata");
    w.begin_writing_trace_paths(&dir.join("p.json")).expect("nim begin_paths");
    w.register_path(&a);
    w.register_path(&b);
    w.register_path(&a);
    w.register_function("f", &b, Line(3));
    w.register_step(&a, Line(2));
    w.register_step(&b, Line(4));
    w.register_step(&a, Line(5));
    w.finish_writing_trace_events().expect("nim finish_events");
    w.finish_writing_trace_metadata().expect("nim finish_metadata");
    w.finish_writing_trace_paths().expect("nim finish_paths");
    w.close().expect("nim close");
    drop(w);
    dir.join(format!("{PROGRAM}.ct"))
}

fn write_rust(dir: &Path) -> PathBuf {
    let (a, b) = (PathBuf::from(A), PathBuf::from(B));
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    AbstractTraceWriter::set_workdir(&mut w, Path::new("/work"));
    w.set_recording_id("01900000-0000-7000-8000-0000000000cc");
    let out = dir.join(PROGRAM);
    TraceWriter::begin_writing_trace_events(&mut w, &out).expect("rust begin_events");
    AbstractTraceWriter::register_path(&mut w, &a);
    AbstractTraceWriter::register_path(&mut w, &b);
    AbstractTraceWriter::register_path(&mut w, &a);
    AbstractTraceWriter::register_function(&mut w, "f", &b, Line(3));
    AbstractTraceWriter::register_step(&mut w, &a, Line(2));
    AbstractTraceWriter::register_step(&mut w, &b, Line(4));
    AbstractTraceWriter::register_step(&mut w, &a, Line(5));
    TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish_events");
    assert!(w.refusals().is_empty(), "the recording is legal; refused: {:?}", w.refusals());
    out.with_extension("ct")
}

fn member(ct: &Path, name: &str) -> Vec<u8> {
    let mut r = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()));
    r.read_file(name).unwrap_or_else(|e| panic!("{name} in {}: {e:?}", ct.display()))
}

#[test]
fn a_path_registered_before_use_is_interned_once_by_both_writers() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let (nd, rd) = (dir.path().join("nim"), dir.path().join("rust"));
    std::fs::create_dir_all(&nd).unwrap();
    std::fs::create_dir_all(&rd).unwrap();
    let (nim, rust) = (write_nim(&nd), write_rust(&rd));

    let paths = String::from_utf8_lossy(&member(&nim, "paths.dat")).into_owned();
    assert_eq!(paths.matches(A).count(), 1, "the Nim writer records a.py once: {paths:?}");
    assert_eq!(paths.matches(B).count(), 1, "the Nim writer records b.py once: {paths:?}");
    for name in ["paths.dat", "paths.off", "funcs.dat", "funcs.off", "steps.dat", "step-map.ns", "meta.dat"] {
        assert_eq!(member(&nim, name), member(&rust, name), "{name} differs between the writers");
    }
    whole_file::assert_same_file(&nim, &rust, "paths registered before use");
}
