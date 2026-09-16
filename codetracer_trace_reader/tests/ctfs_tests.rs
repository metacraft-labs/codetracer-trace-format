//! Integration tests for CTFS trace writer and reader roundtrip.

use std::path::Path;

use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::EventSerializationFormat;
use codetracer_trace_writer::trace_writer::TraceWriter;

/// Helper: create a CtfsTraceWriter with default (SplitBinary) format,
/// write some events, and return the .ct path.
fn write_ctfs_trace(dir: &tempfile::TempDir, events_fn: impl FnOnce(&mut dyn TraceWriter)) -> std::path::PathBuf {
    write_ctfs_trace_with_format(dir, EventSerializationFormat::SplitBinary, events_fn)
}

/// Helper: create a CtfsTraceWriter with a specific format.
fn write_ctfs_trace_with_format(
    dir: &tempfile::TempDir,
    format: EventSerializationFormat,
    events_fn: impl FnOnce(&mut dyn TraceWriter),
) -> std::path::PathBuf {
    let path = dir.path().join("trace");
    let mut writer: Box<dyn TraceWriter + Send> = match format {
        EventSerializationFormat::Cbor => Box::new(codetracer_trace_writer::ctfs_writer::CtfsTraceWriter::new_cbor("test_program", &[])),
        EventSerializationFormat::SplitBinary => Box::new(codetracer_trace_writer::ctfs_writer::CtfsTraceWriter::new("test_program", &[])),
    };
    TraceWriter::begin_writing_trace_events(writer.as_mut(), &path).unwrap();
    events_fn(writer.as_mut());
    TraceWriter::finish_writing_trace_events(writer.as_mut()).unwrap();
    path.with_extension("ct")
}

#[test]
fn test_ctfs_writer_creates_ct_file() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace(&dir, |writer| {
        let path = Path::new("/test/hello.rs");
        TraceWriter::start(writer, path, Line(1));
        TraceWriter::register_step(writer, path, Line(2));
    });
    assert!(ct_path.exists(), ".ct file should exist at {:?}", ct_path);
}

#[test]
fn test_ctfs_roundtrip_step_events() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace(&dir, |writer| {
        let path = Path::new("/test/hello.rs");
        TraceWriter::start(writer, path, Line(1));
        for i in 2..=10 {
            TraceWriter::register_step(writer, path, Line(i));
        }
    });

    // Read back via CtfsTraceReader
    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct_path).unwrap();

    // The first events should be Path and Function registrations from start(),
    // followed by Step events. Count the Step events.
    let step_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some(s),
            _ => None,
        })
        .collect();

    // start() registers the toplevel function and its call AND emits the entry step at the
    // position it was given — here line 1 — and then we register 9 more, lines 2..=10.
    // CONFORMED TO THE SPEC, NOT LOOSENED. `codetracer-trace-format-spec`'s `trace-events.md`,
    // "Recorder Integration — Starting a Recording": *a recording contains one more step than
    // the recorder emitted — the entry step, which `start` emits.* The constant below counted
    // only the steps this test registers, which silently encoded the old behaviour in which
    // `start` emitted none.
    assert_eq!(step_events.len(), 10, "Expected 10 step events, got {}", step_events.len());
    for (i, step) in step_events.iter().enumerate() {
        // The entry step is at line 1, so the i-th step is at line i + 1.
        assert_eq!(step.line, Line(i as i64 + 1));
    }
}

#[test]
fn test_ctfs_roundtrip_special_events() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace(&dir, |writer| {
        let path = Path::new("/test/hello.rs");
        TraceWriter::start(writer, path, Line(1));
        TraceWriter::register_special_event(writer, EventLogKind::Write, "", "hello world");
        TraceWriter::register_special_event(writer, EventLogKind::Error, "meta", "something broke");
    });

    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct_path).unwrap();

    let special_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Event(re) => Some(re),
            _ => None,
        })
        .collect();

    assert_eq!(special_events.len(), 2);
    assert_eq!(special_events[0].kind, EventLogKind::Write);
    assert_eq!(special_events[0].content, "hello world");
    assert_eq!(special_events[1].kind, EventLogKind::Error);
    assert_eq!(special_events[1].content, "something broke");
}

#[test]
fn test_ctfs_roundtrip_variables() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace(&dir, |writer| {
        let path = Path::new("/test/vars.rs");
        TraceWriter::start(writer, path, Line(1));
        TraceWriter::register_step(writer, path, Line(2));

        let type_id = TraceWriter::ensure_type_id(writer, TypeKind::Int, "Int");
        let value = ValueRecord::Int { i: 42, type_id };
        TraceWriter::register_variable_with_full_value(writer, "x", value);
    });

    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct_path).unwrap();

    // Find the Value event for variable "x"
    let value_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Value(fvr) => Some(fvr),
            _ => None,
        })
        .collect();

    assert!(!value_events.is_empty(), "Expected at least one Value event");
    // The last value event should be our Int(42)
    let last_val = value_events.last().unwrap();
    match &last_val.value {
        ValueRecord::Int { i, .. } => assert_eq!(*i, 42),
        other => panic!("Expected Int value, got {:?}", other),
    }
}

#[test]
fn test_ctfs_roundtrip_many_events() {
    let dir = tempfile::tempdir().unwrap();
    let n = 1000;
    let ct_path = write_ctfs_trace(&dir, |writer| {
        let path = Path::new("/test/many.rs");
        TraceWriter::start(writer, path, Line(1));
        for i in 1..n {
            TraceWriter::register_step(writer, path, Line(i as i64 + 1));
        }
    });

    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct_path).unwrap();

    let step_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some(s),
            _ => None,
        })
        .collect();

    // CONFORMED TO THE SPEC, NOT LOOSENED. `codetracer-trace-format-spec`'s `trace-events.md`,
    // "Recorder Integration — Starting a Recording": *a recording contains one more step than
    // the recorder emitted — the entry step, which `start` emits.* The constant below counted
    // only the steps this test registers, which silently encoded the old behaviour in which
    // `start` emitted none.
    // The loop registers `n - 1` steps and `start` adds the entry step, so `n` in total.
    assert_eq!(step_events.len(), n, "Expected {} step events, got {}", n, step_events.len());
}

#[test]
fn test_ctfs_container_has_expected_files() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace(&dir, |writer| {
        let path = Path::new("/test/hello.rs");
        TraceWriter::start(writer, path, Line(1));
    });

    // Open the CTFS container directly and verify the embedded files
    let mut r = codetracer_ctfs::CtfsReader::open(&ct_path).unwrap();
    let files = r.list_files();
    // `events.log` is NOT expected: the spec defines no such stream, and the
    // recording lives in the split streams asserted below.
    assert!(
        !files.contains(&"events.log".to_string()),
        "events.log was written; the spec defines no such stream"
    );
    assert!(
        !files.contains(&"events.fmt".to_string()),
        "events.fmt was written; it described events.log"
    );
    for required in ["steps.dat", "steps.idx", "paths.dat", "funcs.dat"] {
        assert!(files.contains(&required.to_string()), "Missing {required}, got: {files:?}");
    }
    assert!(files.contains(&"meta.dat".to_string()), "Missing meta.dat");
    // The legacy JSON sidecars are retired.
    assert!(!files.contains(&"meta.json".to_string()), "meta.json was written");
    assert!(!files.contains(&"paths.json".to_string()), "paths.json was written");

    // Verify meta.dat content
    let meta_data = r.read_file("meta.dat").unwrap();
    let meta = codetracer_trace_writer::meta_dat::decode_meta_dat(&meta_data).expect("meta.dat must decode");
    assert_eq!(meta.program, "test_program");
    // M-REC-1: the recorder must have stamped a canonical UUIDv7
    // recording_id.  Parse it back to verify the version and variant
    // nibbles round-trip correctly.
    let parsed_id = uuid::Uuid::parse_str(&meta.recording_id).expect("recording_id must parse as a UUID");
    assert_eq!(
        parsed_id.get_version_num(),
        7,
        "recording_id must be a UUIDv7; got version {}",
        parsed_id.get_version_num()
    );

    // meta.dat carries the registered paths.
    assert!(!meta.paths.is_empty(), "Expected at least one path registered");
}

// ---- Split Binary format tests ----

#[test]
fn test_ctfs_split_binary_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace_with_format(&dir, EventSerializationFormat::SplitBinary, |writer| {
        let path = Path::new("/test/split.rs");
        TraceWriter::start(writer, path, Line(1));
        for i in 2..=20 {
            TraceWriter::register_step(writer, path, Line(i));
        }
        TraceWriter::register_special_event(writer, EventLogKind::Write, "", "hello");
        TraceWriter::register_asm(writer, &["nop".to_string(), "ret".to_string()]);
    });

    // Read back via the standard reader.
    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct_path).unwrap();

    let step_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some(s),
            _ => None,
        })
        .collect();
    // CONFORMED TO THE SPEC, NOT LOOSENED. `codetracer-trace-format-spec`'s `trace-events.md`,
    // "Recorder Integration — Starting a Recording": *a recording contains one more step than
    // the recorder emitted — the entry step, which `start` emits.* The constant below counted
    // only the steps this test registers, which silently encoded the old behaviour in which
    // `start` emitted none.
    assert_eq!(step_events.len(), 20, "Expected 20 step events");

    let special_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Event(re) => Some(re),
            _ => None,
        })
        .collect();
    assert_eq!(special_events.len(), 1);
    assert_eq!(special_events[0].content, "hello");

    // `Asm` DOES NOT SURVIVE, AND THAT IS THE SPEC'S DECISION RATHER THAN THIS
    // READER'S OMISSION. `trace-events.md`'s event disposition table reads
    // `| 10 | Asm | Removed (unused by current recorders) |`, and no split
    // stream carries it — it existed only in the combined `events.log`.
    //
    // It is asserted ABSENT rather than left untested, because the writer still
    // exposes `register_asm`: a caller can still hand it instructions, and they
    // now go nowhere. Pinning that here makes it a known, stated property
    // instead of something a recorder discovers when its disassembly vanishes.
    let asm_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Asm(lines) => Some(lines),
            _ => None,
        })
        .collect();
    assert!(
        asm_events.is_empty(),
        "the spec retired Asm and no split stream carries it, but {} survived the round trip",
        asm_events.len()
    );
}

#[test]
fn test_ctfs_split_binary_seek() {
    let dir = tempfile::tempdir().unwrap();
    let n = 10000;
    let ct_path = write_ctfs_trace_with_format(&dir, EventSerializationFormat::SplitBinary, |writer| {
        let path = Path::new("/test/seek.rs");
        TraceWriter::start(writer, path, Line(1));
        for i in 1..n {
            TraceWriter::register_step(writer, path, Line(i as i64 + 1));
        }
    });

    // Seek to the middle of the trace and read 100 events.
    let target = 5000;
    let count = 100;
    let events = codetracer_trace_reader::ctfs_reader::seek_events_in_ctfs(&ct_path, target, count).unwrap();

    // THE BOUND IS ON STEPS, NOT ON EVENTS, because the unit changed with the
    // format. `count` was an event count while the combined `events.log` had a
    // global event ordinal; a split-stream container has no such number, so
    // `seek_events_in_ctfs` takes a STEP index and a step count there. A window
    // of 100 steps necessarily yields MORE than 100 events — the interning
    // tables precede it, and a step can carry values, an I/O event, a call or a
    // return — so bounding the events at 100 would be asserting the old unit
    // against the new answer.
    assert!(!events.is_empty(), "Expected events from seek at {}", target);
    let stepped = events.iter().filter(|e| matches!(e, TraceLowLevelEvent::Step(_))).count();
    assert!(stepped > 0, "the window contains no steps at all");
    assert!(stepped <= count, "Expected at most {} steps in the window, got {}", count, stepped);
}

#[test]
fn test_ctfs_backward_compat_cbor() {
    // Write a trace using CBOR format and verify it can still be read.
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace_with_format(&dir, EventSerializationFormat::Cbor, |writer| {
        let path = Path::new("/test/cbor.rs");
        TraceWriter::start(writer, path, Line(1));
        for i in 2..=10 {
            TraceWriter::register_step(writer, path, Line(i));
        }
    });

    // There is no format marker to check any more, and no format to choose
    // between: `EventSerializationFormat` only ever selected how the combined
    // `events.log` was encoded. With that stream gone both settings produce the
    // same split streams, so this test now asserts that a CBOR-configured
    // writer still yields a readable recording rather than that it yields a
    // different encoding.
    // Read back via the standard reader.
    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct_path).unwrap();

    let step_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some(s),
            _ => None,
        })
        .collect();
    // CONFORMED TO THE SPEC, NOT LOOSENED. `codetracer-trace-format-spec`'s `trace-events.md`,
    // "Recorder Integration — Starting a Recording": *a recording contains one more step than
    // the recorder emitted — the entry step, which `start` emits.* The constant below counted
    // only the steps this test registers, which silently encoded the old behaviour in which
    // `start` emitted none.
    assert_eq!(step_events.len(), 10, "Expected 10 step events from CBOR trace");
    for (i, step) in step_events.iter().enumerate() {
        // The entry step is at line 1, so the i-th step is at line i + 1.
        assert_eq!(step.line, Line(i as i64 + 1));
    }
}

#[test]
fn test_ctfs_split_binary_variables_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace_with_format(&dir, EventSerializationFormat::SplitBinary, |writer| {
        let path = Path::new("/test/vars.rs");
        TraceWriter::start(writer, path, Line(1));
        TraceWriter::register_step(writer, path, Line(2));

        let type_id = TraceWriter::ensure_type_id(writer, TypeKind::Int, "Int");
        let value = ValueRecord::Int { i: 42, type_id };
        TraceWriter::register_variable_with_full_value(writer, "x", value);
    });

    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct_path).unwrap();

    let value_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Value(fvr) => Some(fvr),
            _ => None,
        })
        .collect();

    assert!(!value_events.is_empty(), "Expected at least one Value event");
    let last_val = value_events.last().unwrap();
    match &last_val.value {
        ValueRecord::Int { i, .. } => assert_eq!(*i, 42),
        other => panic!("Expected Int value, got {:?}", other),
    }
}

#[test]
fn test_ctfs_container_has_format_file() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_ctfs_trace(&dir, |writer| {
        let path = Path::new("/test/hello.rs");
        TraceWriter::start(writer, path, Line(1));
    });

    let r = codetracer_ctfs::CtfsReader::open(&ct_path).unwrap();
    let files = r.list_files();
    // `events.fmt` described how to decode `events.log`; with the stream gone
    // the marker describes nothing and is not written either.
    assert!(!files.contains(&"events.fmt".to_string()), "events.fmt was written, got: {:?}", files);
    assert!(!files.contains(&"events.log".to_string()), "events.log was written");
    assert!(files.contains(&"meta.dat".to_string()), "Missing meta.dat");
    assert!(!files.contains(&"meta.json".to_string()), "meta.json was written");
    assert!(!files.contains(&"paths.json".to_string()), "paths.json was written");
}
