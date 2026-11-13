//! A trace's step count is the number of step records (`AbsoluteStep`,
//! `DeltaStep`), and the Rust and the Nim reader report the same one
//! (`trace-events.md` §"Multi-Stream Architecture", §"Which exec records are
//! steps"). `Raise`, `Catch` and the thread records occupy exec records but
//! are not steps; the exec-record count, which the other streams are indexed
//! by, stays the total.
//!
//! Each writer records three steps around a raise and a catch (the Nim writer
//! also a thread start, switch and exit); each reader must report three
//! steps, and the same exec-record count as the other. No mocks.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_trace_reader::step_stream_reader::open_step_stream;
use codetracer_trace_types::Line;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "raise_catch";

fn nim_recording(dir: &Path) -> PathBuf {
    let src = PathBuf::from("/src/rc.php");
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.join("e.json")).expect("begin");
    w.start(&src, Line(1));
    w.register_thread_start(2);
    w.register_thread_switch(2);
    w.register_step(&src, Line(2));
    w.register_raise(0, b"boom");
    w.register_catch(0);
    w.register_thread_exit(2);
    w.register_step(&src, Line(3));
    w.finish_writing_trace_events().expect("finish");
    w.close().expect("close");
    dir.join(format!("{PROGRAM}.ct"))
}

fn rust_recording(dir: &Path) -> PathBuf {
    use codetracer_trace_writer::trace_writer::TraceWriter as _;
    let src = PathBuf::from("/src/rc.php");
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    w.begin_writing_trace_events(&dir.join(PROGRAM)).expect("begin");
    w.start(&src, Line(1));
    w.register_step(&src, Line(2));
    w.register_raise(0, b"boom").expect("raise");
    w.register_catch(0).expect("catch");
    w.register_step(&src, Line(3));
    w.finish_writing_trace_events().expect("finish");
    dir.join(format!("{PROGRAM}.ct"))
}

fn sub(dir: &Path, name: &str) -> PathBuf {
    let d = dir.join(name);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn both_readers_count_the_step_records_only() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    for (writer, ct, exec_records) in [
        ("nim", nim_recording(&sub(dir.path(), "nim")), 8u64),
        ("rust", rust_recording(&sub(dir.path(), "rust")), 5u64),
    ] {
        let mut rust = open_step_stream(&ct).unwrap().expect("steps.dat");
        let nim = NimTraceReaderHandle::open(ct.to_str().unwrap()).unwrap();
        assert_eq!(rust.count(), exec_records, "{writer}: Rust exec-record count");
        assert_eq!(nim.step_count(), exec_records, "{writer}: Nim exec-record count");
        assert_eq!(rust.logical_step_count().unwrap(), 3, "{writer}: Rust step count");
        assert_eq!(nim.logical_step_count(), 3, "{writer}: Nim step count");
    }
}
