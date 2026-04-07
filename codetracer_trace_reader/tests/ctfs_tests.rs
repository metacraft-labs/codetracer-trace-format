//! Integration tests for CTFS trace writer and reader roundtrip.

use std::path::Path;

use codetracer_trace_types::*;
use codetracer_trace_writer::trace_writer::TraceWriter;

/// Helper: create a CtfsTraceWriter, write some events, and return the .ct path.
fn write_ctfs_trace(
    dir: &tempfile::TempDir,
    events_fn: impl FnOnce(&mut dyn TraceWriter),
) -> std::path::PathBuf {
    let path = dir.path().join("trace");
    let mut writer = codetracer_trace_writer::create_trace_writer(
        "test_program",
        &[],
        codetracer_trace_writer::TraceEventsFileFormat::Ctfs,
    );
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
    let mut reader = codetracer_trace_reader::create_trace_reader(
        codetracer_trace_reader::TraceEventsFileFormat::Ctfs,
    );
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

    // start() registers the toplevel call which produces a Call event (no auto-step
    // for toplevel), then we register 9 more steps (lines 2..=10).
    assert_eq!(step_events.len(), 9, "Expected 9 step events, got {}", step_events.len());
    for (i, step) in step_events.iter().enumerate() {
        assert_eq!(step.line, Line(i as i64 + 2));
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

    let mut reader = codetracer_trace_reader::create_trace_reader(
        codetracer_trace_reader::TraceEventsFileFormat::Ctfs,
    );
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

    let mut reader = codetracer_trace_reader::create_trace_reader(
        codetracer_trace_reader::TraceEventsFileFormat::Ctfs,
    );
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

    let mut reader = codetracer_trace_reader::create_trace_reader(
        codetracer_trace_reader::TraceEventsFileFormat::Ctfs,
    );
    let events = reader.load_trace_events(&ct_path).unwrap();

    let step_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some(s),
            _ => None,
        })
        .collect();

    assert_eq!(
        step_events.len(),
        n - 1,
        "Expected {} step events, got {}",
        n - 1,
        step_events.len()
    );
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
    assert!(files.contains(&"events.log".to_string()), "Missing events.log");
    assert!(files.contains(&"meta.json".to_string()), "Missing meta.json");
    assert!(files.contains(&"paths.json".to_string()), "Missing paths.json");

    // Verify meta.json content
    let meta_data = r.read_file("meta.json").unwrap();
    let meta: codetracer_trace_types::TraceMetadata =
        serde_json::from_slice(&meta_data).unwrap();
    assert_eq!(meta.program, "test_program");

    // Verify paths.json content
    let paths_data = r.read_file("paths.json").unwrap();
    let paths: Vec<std::path::PathBuf> = serde_json::from_slice(&paths_data).unwrap();
    assert!(!paths.is_empty(), "Expected at least one path registered");
}
