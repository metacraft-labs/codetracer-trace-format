//! `events.log` and `events.fmt` are not part of the trace format: the Rust
//! and the Nim reader refuse the same container carrying either, each naming
//! the member.
//!
//! The containers are real recordings, written by each writer, with the
//! retired member added beside their streams; without it both readers read
//! them, which the control asserts. No mocks.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter};
use codetracer_trace_reader::ctfs_reader::read_trace_from_ctfs;
use codetracer_trace_types::Line;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "retired";

fn nim_recording(dir: &Path) -> PathBuf {
    let src = PathBuf::from("/src/r.py");
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.join("e.json")).expect("begin");
    w.start(&src, Line(1));
    w.register_step(&src, Line(2));
    w.finish_writing_trace_events().expect("finish");
    w.close().expect("close");
    dir.join(format!("{PROGRAM}.ct"))
}

fn rust_recording(dir: &Path) -> PathBuf {
    use codetracer_trace_writer::trace_writer::TraceWriter as _;
    let src = PathBuf::from("/src/r.py");
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    w.begin_writing_trace_events(&dir.join(PROGRAM)).expect("begin");
    w.start(&src, Line(1));
    w.register_step(&src, Line(2));
    w.finish_writing_trace_events().expect("finish");
    dir.join(format!("{PROGRAM}.ct"))
}

/// The container at `ct` rewritten with one more member, `name`.
fn with_member(ct: &Path, name: &str, content: &[u8], out: &Path) {
    let mut reader = CtfsReader::open(ct).expect("open");
    let mut w = CtfsWriter::create_in_memory(4096, 31, CompressionMethod::None).expect("create");
    for (member, data) in reader.members().expect("members") {
        let h = w.add_file(&member).expect("add");
        w.write(h, &data).expect("write");
    }
    let h = w.add_file(name).expect("add retired");
    w.write(h, content).expect("write retired");
    std::fs::write(out, w.finish_to_bytes().expect("finish")).expect("write file");
}

fn both_refuse(ct: &Path, member: &str) {
    let rust = read_trace_from_ctfs(ct).expect_err("the Rust reader read it");
    assert!(rust.to_string().contains(member), "the Rust refusal does not name {member}: {rust}");
    let nim = NimTraceReaderHandle::open(ct.to_str().unwrap()).err().expect("the Nim reader read it");
    assert!(nim.to_string().contains(member), "the Nim refusal does not name {member}: {nim}");
}

#[test]
fn both_readers_refuse_a_container_carrying_a_retired_member() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().unwrap();
    for (writer, ct) in [
        (
            "nim",
            nim_recording(&std::fs::create_dir_all(dir.path().join("nim")).map(|_| dir.path().join("nim")).unwrap()),
        ),
        (
            "rust",
            rust_recording(&std::fs::create_dir_all(dir.path().join("rust")).map(|_| dir.path().join("rust")).unwrap()),
        ),
    ] {
        // The control: as written, both read it.
        read_trace_from_ctfs(&ct).unwrap_or_else(|e| panic!("{writer}: the Rust reader refused the recording: {e}"));
        NimTraceReaderHandle::open(ct.to_str().unwrap()).unwrap_or_else(|e| panic!("{writer}: the Nim reader refused the recording: {e}"));
        for (member, content) in [("events.log", &b"\0"[..]), ("events.fmt", &b"split-binary"[..]), ("events.log", &b""[..])] {
            let damaged = dir.path().join(format!("{writer}-{member}-{}.ct", content.len()));
            with_member(&ct, member, content, &damaged);
            both_refuse(&damaged, member);
        }
    }
}
