//! Raise and Catch, and a call that exits by an exception, written by the Nim
//! writer through its C ABI and read by this repository's split-stream reader.
//!
//! What is asserted, and why each can fail:
//!
//! 1. The same recording with a `Raise` and a `Catch` between two steps,
//!    written by both writers, gives the same `steps.dat` byte for byte and
//!    the same split-reader details: the two exception records where they
//!    occurred, with the type id and message given. A Nim C ABI that drops an
//!    event, writes it as a step, or loses the message differs.
//! 2. A Nim-written call that exits by an exception reports it as its
//!    `raised_exception`, decoded to the value given; the enclosing call,
//!    which returned, reports none. (The Rust writer has no way to record a
//!    call's exception, so this half is Nim-written only.)
//!
//! No mocks: both writers are the shipped ones, and the containers are read
//! by the shipped reader.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::split_stream_reader::{read_trace_with_details, ExceptionEventKind, SplitStreamTrace};
use codetracer_trace_types::{FunctionId, Line, TypeId, TypeKind, ValueRecord};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is not re-entrant across threads; every test in this
/// binary takes this lock.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "exceptions";
const SRC: &str = "/src/e.py";

#[derive(Clone, Copy, Debug)]
enum Writer {
    Nim,
    Rust,
}

/// A ten-line file sized by the line-count table (so both writers address
/// it alike), a call, a step, a Raise and a Catch of type 5, a step, a
/// return.
fn raise_and_catch(writer: Writer, dir: &Path) -> PathBuf {
    let src = PathBuf::from(SRC);
    match writer {
        Writer::Nim => {
            let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
            w.begin_writing_trace_events(&dir.join("e.json")).expect("nim begin_events");
            w.begin_writing_trace_metadata(&dir.join("m.json")).expect("nim begin_metadata");
            w.begin_writing_trace_paths(&dir.join("p.json")).expect("nim begin_paths");
            w.enable_line_count_table().expect("nim enable_line_count_table");
            w.register_path_with_line_count(&src, 10).expect("nim path");
            w.register_function("f", &src, Line(1));
            let tid = w.ensure_type_id(TypeKind::Int, "Int");
            w.register_call(FunctionId(0), vec![]);
            w.register_step(&src, Line(2));
            w.register_raise(5, b"boom");
            w.register_catch(5);
            w.register_step(&src, Line(4));
            w.register_return(ValueRecord::None { type_id: tid });
            w.finish_writing_trace_events().expect("nim finish_events");
            w.finish_writing_trace_metadata().expect("nim finish_metadata");
            w.finish_writing_trace_paths().expect("nim finish_paths");
            w.close().expect("nim close");
            drop(w);
            dir.join(format!("{PROGRAM}.ct"))
        }
        Writer::Rust => {
            let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
            let out = dir.join(PROGRAM);
            TraceWriter::begin_writing_trace_events(&mut w, &out).expect("rust begin_events");
            w.enable_line_count_table().expect("rust enable_line_count_table");
            w.register_path_with_line_count(&src, 10).expect("rust path");
            AbstractTraceWriter::register_function(&mut w, "f", &src, Line(1));
            let tid = AbstractTraceWriter::ensure_type_id(&mut w, TypeKind::Int, "Int");
            AbstractTraceWriter::register_call(&mut w, FunctionId(0), vec![]);
            AbstractTraceWriter::register_step(&mut w, &src, Line(2));
            w.register_raise(5, b"boom").expect("rust raise");
            w.register_catch(5).expect("rust catch");
            AbstractTraceWriter::register_step(&mut w, &src, Line(4));
            AbstractTraceWriter::register_return(&mut w, ValueRecord::None { type_id: tid });
            TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish_events");
            assert!(w.refusals().is_empty(), "the fixture is legal; refused: {:?}", w.refusals());
            out.with_extension("ct")
        }
    }
}

fn read(ct: &Path) -> SplitStreamTrace {
    let mut r = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()));
    read_trace_with_details(&mut r).unwrap_or_else(|e| panic!("split-stream read of {}: {e}", ct.display()))
}

fn member(ct: &Path, name: &str) -> Vec<u8> {
    let mut r = CtfsReader::open(ct).expect("open");
    r.read_file(name).unwrap_or_else(|e| panic!("{name}: {e:?}"))
}

#[test]
fn raise_and_catch_read_alike_from_both_writers() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let nim_dir = dir.path().join("nim");
    let rust_dir = dir.path().join("rust");
    std::fs::create_dir_all(&nim_dir).unwrap();
    std::fs::create_dir_all(&rust_dir).unwrap();
    let nim = raise_and_catch(Writer::Nim, &nim_dir);
    let rust = raise_and_catch(Writer::Rust, &rust_dir);

    assert_eq!(member(&nim, "steps.dat"), member(&rust, "steps.dat"), "the two step streams differ");
    let (a, b) = (read(&nim), read(&rust));
    let kinds: Vec<ExceptionEventKind> = a.details.exception_events.iter().map(|e| e.kind.clone()).collect();
    assert_eq!(
        kinds,
        vec![
            ExceptionEventKind::Raise {
                exception_type_id: 5,
                message: b"boom".to_vec()
            },
            ExceptionEventKind::Catch { exception_type_id: 5 },
        ],
        "the Nim-written container's exception records"
    );
    assert_eq!(
        a.details.exception_events, b.details.exception_events,
        "the two writers' exception records differ"
    );
}

#[test]
fn a_call_that_exits_by_an_exception_carries_it() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    let src = PathBuf::from(SRC);
    let exception = ValueRecord::Error {
        msg: "boom".to_string(),
        type_id: TypeId(0),
    };
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.path().join("e.json")).expect("begin_events");
    w.begin_writing_trace_metadata(&dir.path().join("m.json")).expect("begin_metadata");
    w.begin_writing_trace_paths(&dir.path().join("p.json")).expect("begin_paths");
    w.register_path(&src);
    w.register_function("outer", &src, Line(1));
    w.register_function("inner", &src, Line(5));
    let tid = w.ensure_type_id(TypeKind::Error, "ValueError");
    assert_eq!(tid, TypeId(0));
    w.register_call(FunctionId(0), vec![]);
    w.register_step(&src, Line(2));
    w.register_call(FunctionId(1), vec![]);
    w.register_step(&src, Line(6));
    w.register_raise(tid.0 as u64, b"boom");
    w.register_return_exception(&exception);
    w.register_catch(tid.0 as u64);
    w.register_step(&src, Line(3));
    w.register_return(ValueRecord::None { type_id: tid });
    w.finish_writing_trace_events().expect("finish_events");
    w.finish_writing_trace_metadata().expect("finish_metadata");
    w.finish_writing_trace_paths().expect("finish_paths");
    w.close().expect("close");
    drop(w);

    let t = read(&dir.path().join(format!("{PROGRAM}.ct")));
    let calls = &t.details.calls;
    assert_eq!(calls.len(), 2, "two calls: {calls:?}");
    assert_eq!(calls[0].raised_exception, None, "the outer call returned");
    assert_eq!(calls[1].raised_exception, Some(exception), "the inner call exits by the exception");
}
