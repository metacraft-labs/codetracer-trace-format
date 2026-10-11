//! Exercise the actual Nim writer through its public Rust trait and checked C ABI.
//! Containers and corruptions use genuine finalized recordings; no mock backend.
use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter};
use codetracer_trace_types::{FunctionId, Line, TypeKind, ValueRecord};
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat, TraceWriter};
use std::path::Path;

fn record(root: &Path, program: &str, marked: bool) -> std::path::PathBuf {
    let mut writer = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    writer.begin_writing_trace_events(&root.join("events.json")).unwrap();
    writer.begin_writing_trace_metadata(&root.join("metadata.json")).unwrap();
    writer.begin_writing_trace_paths(&root.join("paths.json")).unwrap();
    let path = root.join("program.src");
    writer.register_path_with_line_count(&path, 2).unwrap();
    writer.register_function("program", &path, Line(1));
    writer.register_call(FunctionId(0), vec![]);
    if marked {
        TraceWriter::mark_current_call_as_entry(&mut writer).unwrap();
        TraceWriter::mark_current_call_as_entry(&mut writer).unwrap();
    }
    writer.register_step(&path, Line(1));
    let type_id = writer.ensure_type_id(TypeKind::Int, "Int");
    writer.register_return(ValueRecord::None { type_id });
    writer.finish_writing_trace_events().unwrap();
    writer.finish_writing_trace_metadata().unwrap();
    writer.finish_writing_trace_paths().unwrap();
    writer.close().unwrap();
    root.join(format!("{program}.ct"))
}

#[test]
fn checked_trait_marker_and_reader_preserve_absence_and_refuse_malformed_presence() {
    let root = tempfile::tempdir().unwrap();
    let marked = record(root.path(), "marked", true);
    let unmarked = record(root.path(), "unmarked", false);
    let reader = NimTraceReaderHandle::open(marked.to_str().unwrap()).unwrap();
    assert_eq!(reader.recorded_entry().unwrap(), Some((0, 0, 0)));
    assert_eq!(
        NimTraceReaderHandle::open(unmarked.to_str().unwrap()).unwrap().recorded_entry().unwrap(),
        None
    );

    let mut original = CtfsReader::open(&marked).unwrap();
    let mut malformed = CtfsWriter::create_in_memory(original.block_size(), original.max_entries(), CompressionMethod::None).unwrap();
    for name in original.list_files() {
        let bytes = if name == "entry.dat" {
            b"bad entry".to_vec()
        } else {
            original.read_file(&name).unwrap()
        };
        let file = malformed.add_file(&name).unwrap();
        malformed.write(file, &bytes).unwrap();
    }
    let corrupt = root.path().join("malformed.ct");
    std::fs::write(&corrupt, malformed.finish_to_bytes().unwrap()).unwrap();
    let error = match NimTraceReaderHandle::open(corrupt.to_str().unwrap()) {
        Ok(reader) => reader.recorded_entry().unwrap_err().to_string(),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("entry.dat"), "actual checked reader error: {error}");

    let mut no_active = NimTraceWriter::new("no-active", &[], TraceEventsFileFormat::Ctfs);
    let error = TraceWriter::mark_current_call_as_entry(&mut no_active).unwrap_err().to_string();
    assert!(error.contains("entry.dat"), "actual checked writer error: {error}");
}
