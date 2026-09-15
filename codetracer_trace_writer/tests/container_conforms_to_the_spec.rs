//! The container this writer produces must be the one the spec defines.
//!
//! `codetracer-trace-format-spec` is the single description both writers are
//! held to — the Rust one here and the Nim one behind the same C ABI. Where
//! they disagreed, the disagreement used to be catalogued as a difference to
//! explain. It is not: one of them is wrong, and these tests say which by
//! quoting the spec rather than the other implementation.
//!
//! What is pinned here is the CONTAINER HEADER, read straight out of
//! `ctfs-container.md` section 1:
//!
//!   * **Byte 5 is `4`.** v4 also RE-DEFINES bytes 6 and 7 — under v2/v3 they
//!     were compression and encryption, under v4 they are encryption and
//!     max_shards — so the version is asserted together with what those bytes
//!     now mean, rather than alone. A bump that left a compression tag sitting
//!     in the encryption byte would satisfy a version assertion and produce a
//!     container declaring itself AES-256-GCM encrypted.
//!
//!   * **Byte 7 is `0`.** The spec reads `0 = no sharding`, and this writer
//!     produces a single unsharded container.
//!
//! No mocks. The writer is the production writer, the container is a real file,
//! and the entry list comes from the production CTFS reader.

use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use std::path::{Path, PathBuf};

const RECORDING_ID: &str = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb";

/// Drive enough of the writer that every split stream has real content — a
/// container with nothing in it would satisfy "no `events.log`" for the wrong
/// reason.
fn write_sample_trace(writer: &mut dyn TraceWriter) {
    let path = PathBuf::from("/src/example.rs");
    TraceWriter::set_workdir(writer, Path::new("/src"));
    TraceWriter::start(writer, &path, Line(1));

    let function_id = TraceWriter::ensure_function_id(writer, "main", &path, Line(1));
    let int_type = TraceWriter::ensure_type_id(writer, TypeKind::Int, "Int");

    let args = vec![TraceWriter::arg(writer, "n", ValueRecord::Int { i: 7, type_id: int_type })];
    TraceWriter::register_call(writer, function_id, args);

    for line in 1..=40 {
        TraceWriter::register_step(writer, &path, Line(line));
        TraceWriter::register_variable_with_full_value(
            writer,
            "acc",
            ValueRecord::Int {
                i: line * 2,
                type_id: int_type,
            },
        );
    }

    TraceWriter::register_special_event(writer, EventLogKind::Write, "", "hello from the sample trace\n");
    TraceWriter::register_return(writer, ValueRecord::Int { i: 80, type_id: int_type });
}

fn write_container(dir: &Path) -> PathBuf {
    let base = dir.join("trace");
    let mut writer = CtfsTraceWriter::new("example", &["--flag".to_string()]);
    writer.set_recording_id(RECORDING_ID);
    writer.begin_writing_trace_events(&base).unwrap();
    write_sample_trace(&mut writer);
    writer.finish_writing_trace_events().unwrap();
    base.with_extension("ct")
}

#[test]
fn the_container_header_declares_version_4() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_container(dir.path());
    let bytes = std::fs::read(&ct).unwrap();

    assert!(bytes.len() >= 16, "container is only {} bytes", bytes.len());
    assert_eq!(
        &bytes[0..5],
        &[0xC0, 0xDE, 0x72, 0xAC, 0xE2],
        "magic is not the one ctfs-container.md states"
    );
    assert_eq!(
        bytes[5], 4,
        "ctfs-container.md section 1 states header byte 5 (Version) is 4; this writer wrote {}",
        bytes[5]
    );
}

#[test]
fn version_4_reinterprets_bytes_6_and_7_and_this_writer_respects_that() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_container(dir.path());
    let bytes = std::fs::read(&ct).unwrap();

    // Under v4 byte 6 is Encryption, not the compression tag it was under
    // v2/v3. This writer does not encrypt, so it must be 0 — and it is only
    // 0 by luck if the compression tag it used to hold was also 0, which is
    // exactly why this is asserted beside the version rather than trusted.
    assert_eq!(
        bytes[6], 0,
        "ctfs-container.md: byte 6 under version 4 is Encryption (0 = none); this writer wrote {}",
        bytes[6]
    );
    // Under v4 byte 7 is MaxShards, where `0` means no sharding. This writer
    // produces a single unsharded container.
    assert_eq!(
        bytes[7], 0,
        "ctfs-container.md: byte 7 under version 4 is MaxShards (0 = no sharding); this writer wrote {}",
        bytes[7]
    );
}

// THE `events.log` TEST IS NOT HERE YET, AND ITS ABSENCE IS DELIBERATE.
//
// The spec defines no `events.log` and this writer still emits one, so the
// assertion would be red. It is not written as a pending red because removing
// the stream is not a writer-side change: `read_trace_from_ctfs` reads
// `events.log` and NOTHING ELSE, and the Rust reader has no combiner that
// reconstructs `TraceLowLevelEvent`s from the split streams — `step_stream_reader`
// and `value_stream_reader` yield stream RECORDS, and nothing assembles them.
// Measured: with the stream suppressed, the writer's own round-trip test fails
// `FileNotFound("events.log")`, and 36 workspace tests go red.
//
// So the writer cannot stop emitting it until the Rust reader gains the
// equivalent of the Nim `NewTraceReader`. That is a new component, not a
// deletion, and it is tracked rather than half-done here.

/// THE CONTROL, and it is what stops the test above passing for the wrong
/// reason. "No `events.log`" is satisfied by a writer that produced nothing at
/// all, by one whose entry list failed to read, and by one that silently
/// stopped writing every stream. So the streams the spec DOES define are
/// asserted present in the same breath, with content.
#[test]
fn and_the_streams_the_spec_does_define_are_there_with_content() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_container(dir.path());
    let reader = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let entries = reader.list_files();

    for required in ["meta.dat", "steps.dat", "steps.idx", "values.dat", "events.dat"] {
        assert!(
            entries.iter().any(|f| f == required),
            "the spec defines `{required}` and this container does not carry it; entries: {entries:?}"
        );
        let size = reader.file_size(required).unwrap_or(0);
        assert!(size > 0, "`{required}` is present but empty, so its presence says nothing");
    }
}
