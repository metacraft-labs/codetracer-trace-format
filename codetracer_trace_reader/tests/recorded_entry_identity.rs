//! Real native writer/container/owned-reader controls. No mocks or path-reopening getter.
#[path = "common/entry_fixture_capture.rs"]
mod entry_fixture_capture;
use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::recorded_entry_reader::RecordedEntryReader;
use codetracer_trace_types::*;
use codetracer_trace_writer::{ctfs_writer::CtfsTraceWriter, trace_writer::TraceWriter};
use std::path::Path;

#[test]
fn marked_active_child_and_partial_root_finalize_from_same_image() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("trace");
    let mut writer = CtfsTraceWriter::new("entry", &[]);
    TraceWriter::begin_writing_trace_events(&mut writer, &path).unwrap();
    let source = Path::new("/real-entry.rs");
    TraceWriter::start(&mut writer, source, Line(1));
    let function = TraceWriter::ensure_function_id(&mut writer, "main", source, Line(1));
    TraceWriter::register_call(&mut writer, function, vec![]);
    TraceWriter::mark_current_call_as_entry(&mut writer).unwrap();
    TraceWriter::mark_current_call_as_entry(&mut writer).unwrap();
    TraceWriter::register_step(&mut writer, source, Line(2));
    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
    let mut reader = CtfsReader::open(&path.with_extension("ct")).unwrap();
    let identity = reader.recorded_entry().unwrap().unwrap();
    assert_eq!(identity.function_id, function.0 as u64);
    assert_eq!(identity.call_key, 1);
    assert_eq!(reader.recorded_entry().unwrap(), Some(identity));
    assert!(reader.is_owned_image());
    let owned = &mut reader;
    let before_steps = codetracer_trace_reader::step_stream_reader::StepStreamReader::open(owned)
        .unwrap()
        .unwrap()
        .read_all()
        .unwrap();
    std::fs::write(path.with_extension("ct"), b"replaced after held-image capture").unwrap();
    assert_eq!(owned.recorded_entry().unwrap(), Some(identity));
    let after_steps = codetracer_trace_reader::step_stream_reader::StepStreamReader::open(owned)
        .unwrap()
        .unwrap()
        .read_all()
        .unwrap();
    assert_eq!(before_steps, after_steps);
}

#[test]
fn no_active_call_refuses_without_fabricating_identity() {
    let mut writer = CtfsTraceWriter::new("entry", &[]);
    assert!(TraceWriter::mark_current_call_as_entry(&mut writer).is_err());
}

#[test]
fn conflicting_mark_refuses_actual_different_active_call() {
    let directory = tempfile::tempdir().unwrap();
    let mut writer = CtfsTraceWriter::new("entry", &[]);
    TraceWriter::begin_writing_trace_events(&mut writer, &directory.path().join("trace")).unwrap();
    let source = Path::new("/real-entry.rs");
    TraceWriter::start(&mut writer, source, Line(1));
    TraceWriter::mark_current_call_as_entry(&mut writer).unwrap();
    let child = TraceWriter::ensure_function_id(&mut writer, "child", source, Line(1));
    TraceWriter::register_call(&mut writer, child, vec![]);
    assert!(TraceWriter::mark_current_call_as_entry(&mut writer).is_err());
}

#[test]
fn later_empty_root_cannot_take_marked_steps() {
    let directory = tempfile::tempdir().unwrap();
    let mut writer = CtfsTraceWriter::new("entry", &[]);
    TraceWriter::begin_writing_trace_events(&mut writer, &directory.path().join("trace")).unwrap();
    let source = Path::new("/real-entry.rs");
    TraceWriter::start(&mut writer, source, Line(1));
    TraceWriter::mark_current_call_as_entry(&mut writer).unwrap();
    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    TraceWriter::register_call(&mut writer, TOP_LEVEL_FUNCTION_ID, vec![]);
    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    let error = TraceWriter::finish_writing_trace_events(&mut writer).unwrap_err();
    assert!(error.to_string().contains("entry.dat: inconsistent"), "{error}");
}

#[test]
fn genuine_nim_images_crossread() {
    let captured = entry_fixture_capture::capture();
    let base = captured.path();
    let mut marked = CtfsReader::open(&Path::new(&base).join("marked.ct")).unwrap();
    let entry = marked.recorded_entry().unwrap().unwrap();
    assert_eq!((entry.call_key, entry.function_id, entry.entry_step), (1, 1, 1));
    assert!(marked.is_owned_image());
    let mut unmarked = CtfsReader::open(&Path::new(&base).join("unmarked.ct")).unwrap();
    assert_eq!(unmarked.recorded_entry().unwrap(), None);
    assert!(!unmarked.is_owned_image());
}
