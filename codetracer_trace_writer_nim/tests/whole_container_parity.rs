//! The two writers write the same container FILE for the same recording:
//! every byte, block placement included, not only the same members.
//!
//! A member-by-member comparison cannot see where a member's blocks are: two
//! containers whose members are equal may still claim blocks in another order
//! and so differ in their mapping slots and root entries. Here the files are
//! compared whole, for recordings written to a path through the recorder API
//! (a small one and the mixed workload, through both of its drives) and for
//! a recording written in memory. On a difference the failure names each
//! differing block with the member that owns it on each side, and both sides'
//! block allocation order.
//!
//! No mocks: both writers are the shipped ones.

use std::path::{Path, PathBuf};

use codetracer_trace_types::{EventLogKind, FunctionId, Line, TypeKind, ValueRecord};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

mod mixed_workload;
mod whole_file;

use mixed_workload::{nim_lock, write_nim, write_rust, Drive};

const PROGRAM: &str = "small";
const RECORDING_ID: &str = "01949fcc-7d92-7e9c-aaaa-cccccccccccc";

fn assert_same_file(nim: &[u8], rust: &[u8], what: &str) {
    if let Err(report) = whole_file::compare("nim", nim, "rust", rust) {
        panic!("{what}: the two containers differ\n{report}");
    }
}

/// A small recording: two files, two calls, values, an I/O event.
macro_rules! small {
    ($w:expr, $path:expr, $func:expr, $ty:expr, $step:expr, $call:expr, $ret:expr, $var:expr, $event:expr) => {{
        let a = PathBuf::from("/src/a.py");
        let b = PathBuf::from("/src/b.py");
        let int = $ty($w, TypeKind::Int, "int");
        $path($w, &a);
        let main = $func($w, "main", &a, Line(1));
        let helper = $func($w, "helper", &b, Line(1));
        $call($w, main, vec![]);
        for i in 0..40 {
            $step($w, &a, Line(2 + i % 5));
            $var($w, "i", ValueRecord::Int { i, type_id: int });
            if i % 10 == 0 {
                $call($w, helper, vec![]);
                $step($w, &b, Line(3));
                $event($w, EventLogKind::Write, "", "hello\n");
                $ret($w, ValueRecord::Int { i, type_id: int });
            }
        }
        $ret($w, ValueRecord::None { type_id: int });
    }};
}

fn small_nim(w: &mut NimTraceWriter) {
    small!(
        w,
        |w: &mut NimTraceWriter, p: &Path| w.register_path(p),
        |w: &mut NimTraceWriter, n: &str, p: &Path, l| w.ensure_function_id(n, p, l),
        |w: &mut NimTraceWriter, k, t: &str| w.ensure_type_id(k, t),
        |w: &mut NimTraceWriter, p: &Path, l| w.register_step(p, l),
        |w: &mut NimTraceWriter, f: FunctionId, a| w.register_call(f, a),
        |w: &mut NimTraceWriter, v| w.register_return(v),
        |w: &mut NimTraceWriter, n: &str, v| w.register_variable_with_full_value(n, v),
        |w: &mut NimTraceWriter, k, m: &str, c: &str| w.register_special_event(k, m, c)
    );
}

fn small_rust(w: &mut CtfsTraceWriter) {
    small!(
        w,
        |w: &mut CtfsTraceWriter, p: &Path| AbstractTraceWriter::register_path(w, p),
        |w: &mut CtfsTraceWriter, n: &str, p: &Path, l| AbstractTraceWriter::ensure_function_id(w, n, p, l),
        |w: &mut CtfsTraceWriter, k, t: &str| AbstractTraceWriter::ensure_type_id(w, k, t),
        |w: &mut CtfsTraceWriter, p: &Path, l| AbstractTraceWriter::register_step(w, p, l),
        |w: &mut CtfsTraceWriter, f: FunctionId, a| AbstractTraceWriter::register_call(w, f, a),
        |w: &mut CtfsTraceWriter, v| AbstractTraceWriter::register_return(w, v),
        |w: &mut CtfsTraceWriter, n: &str, v| AbstractTraceWriter::register_variable_with_full_value(w, n, v),
        |w: &mut CtfsTraceWriter, k, m: &str, c: &str| AbstractTraceWriter::register_special_event(w, k, m, c)
    );
}

fn nim_writer() -> NimTraceWriter {
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.set_recording_id(RECORDING_ID).expect("nim recording id");
    w.set_workdir(Path::new("/work"));
    w
}

fn rust_writer(w: CtfsTraceWriter) -> CtfsTraceWriter {
    let mut w = w;
    w.set_recording_id(RECORDING_ID);
    AbstractTraceWriter::set_workdir(&mut w, Path::new("/work"));
    w
}

#[test]
fn a_small_recording_written_to_a_path_is_the_same_file() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut n = nim_writer();
    n.begin_writing_trace_events(&dir.path().join("e.json")).unwrap();
    small_nim(&mut n);
    n.finish_writing_trace_events().unwrap();
    n.close().unwrap();
    drop(n);
    let mut r = rust_writer(CtfsTraceWriter::new(PROGRAM, &[]));
    let out = dir.path().join("rust").join(PROGRAM);
    std::fs::create_dir_all(out.parent().unwrap()).unwrap();
    TraceWriter::begin_writing_trace_events(&mut r, &out).unwrap();
    small_rust(&mut r);
    TraceWriter::finish_writing_trace_events(&mut r).unwrap();
    let nim = std::fs::read(dir.path().join(format!("{PROGRAM}.ct"))).unwrap();
    let rust = std::fs::read(out.with_extension("ct")).unwrap();
    assert_same_file(&nim, &rust, "small, to a path");
}

#[test]
fn a_small_recording_written_in_memory_is_the_same_bytes() {
    let _g = nim_lock();
    let mut n = nim_writer();
    n.begin_in_memory().unwrap();
    small_nim(&mut n);
    n.finish_writing_trace_events().unwrap();
    n.close().unwrap();
    let nim = n.container_bytes().expect("the Nim container");
    drop(n);
    let mut r = rust_writer(CtfsTraceWriter::new_in_memory(PROGRAM, &[]));
    TraceWriter::begin_writing_trace_events(&mut r, Path::new(PROGRAM)).unwrap();
    small_rust(&mut r);
    TraceWriter::finish_writing_trace_events(&mut r).unwrap();
    let rust = r.take_container_bytes().expect("the Rust container");
    assert_same_file(&nim, &rust, "small, in memory");
}

fn mixed(drive: Drive) {
    let dir = tempfile::tempdir().unwrap();
    let (nd, rd) = (dir.path().join("nim"), dir.path().join("rust"));
    std::fs::create_dir_all(&nd).unwrap();
    std::fs::create_dir_all(&rd).unwrap();
    let nim = std::fs::read(write_nim(drive, &nd, 0)).unwrap();
    let rust = std::fs::read(write_rust(drive, &rd, 0)).unwrap();
    assert_same_file(&nim, &rust, &format!("the mixed workload, {drive:?}, to a path"));
}

#[test]
fn the_mixed_workload_through_the_recorder_api_is_the_same_file() {
    let _g = nim_lock();
    mixed(Drive::RecorderApi);
}

#[test]
fn the_mixed_workload_through_events_is_the_same_file() {
    let _g = nim_lock();
    mixed(Drive::Events);
}
