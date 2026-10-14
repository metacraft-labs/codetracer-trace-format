//! `NimTraceWriter::set_recording_id` pins the recording's identity before the
//! trace opens, and is refused once it has — the C ABI's
//! `trace_writer_set_recording_id` contract. Pinning lets two writers given
//! the same recording write the same `meta.dat`.
//!
//! No mocks: the real Nim writer through its C ABI.

use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

#[test]
fn an_id_is_accepted_before_begin_and_refused_after() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut w = NimTraceWriter::new("pinned", &[], TraceEventsFileFormat::Ctfs);
    w.set_recording_id("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb")
        .expect("an id set before the trace opens is accepted");
    w.begin_writing_trace_events(&dir.path().join("e.json")).expect("begin_events");
    let err = w
        .set_recording_id("01949fcc-7d92-7e9c-aaaa-cccccccccccc")
        .expect_err("an id set after the trace opened is refused");
    assert!(!err.to_string().is_empty(), "the refusal is named");
    drop(w);
}
