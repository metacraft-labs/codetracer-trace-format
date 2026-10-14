//! No mocks: the real Nim streaming encoder and writer produce a CTFS container
//! read by the Rust reader. An empty Struct must preserve its type and both
//! nonempty siblings instead of resetting or prematurely ending its parent.
use codetracer_trace_types::{Line, TraceLowLevelEvent, TypeKind, ValueRecord};
use codetracer_trace_writer_nim::{NimTraceWriter, StreamingValueEncoder, TraceEventsFileFormat};

#[test]
fn an_empty_struct_preserves_its_outer_tuple_and_trailing_sibling() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut writer = NimTraceWriter::new("streaming_struct", &[], TraceEventsFileFormat::Ctfs);
    writer.begin_writing_trace_events(&dir.path().join("e.json")).unwrap();
    writer.begin_writing_trace_metadata(&dir.path().join("m.json")).unwrap();
    writer.begin_writing_trace_paths(&dir.path().join("p.json")).unwrap();
    let path = dir.path().join("example.rb");
    writer.register_path_with_line_count(&path, 2).unwrap();
    writer.register_step(&path, Line(1));
    let tuple_type = writer.ensure_type_id(TypeKind::Tuple, "Tuple");
    let proc_type = writer.ensure_type_id(TypeKind::Struct, "Proc");
    let int_type = writer.ensure_type_id(TypeKind::Int, "Integer");
    let string_type = writer.ensure_type_id(TypeKind::String, "String");
    let mut encoder = StreamingValueEncoder::new();
    encoder.begin_tuple(tuple_type, 3);
    encoder.write_int(17, int_type);
    encoder.begin_struct(proc_type, 0);
    encoder.end_compound();
    encoder.write_string("after", string_type);
    encoder.end_compound();
    assert!(encoder.take_failure().is_none());
    let bytes = encoder.get_bytes_copy();
    let expected = ValueRecord::Tuple {
        elements: vec![
            ValueRecord::Int { i: 17, type_id: int_type },
            ValueRecord::Struct {
                field_values: vec![],
                type_id: proc_type,
            },
            ValueRecord::String {
                text: "after".into(),
                type_id: string_type,
            },
        ],
        type_id: tuple_type,
    };
    let mut recursive = StreamingValueEncoder::new();
    assert_eq!(
        bytes,
        recursive.encode(&expected),
        "streaming bytes preserve the complete canonical value"
    );
    assert!(recursive.take_failure().is_none());
    writer.register_variable_cbor("nested", &bytes);
    writer.register_step(&path, Line(2));
    writer.finish_writing_trace_events().unwrap();
    writer.finish_writing_trace_metadata().unwrap();
    writer.finish_writing_trace_paths().unwrap();
    writer.close().unwrap();
    drop(writer);
    let container = dir.path().join("streaming_struct.ct");
    let events = codetracer_trace_reader::ctfs_reader::read_trace_from_ctfs(&container).unwrap();
    let values: Vec<_> = events
        .into_iter()
        .filter_map(|event| match event {
            TraceLowLevelEvent::Value(value) => Some(value.value),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec![expected]);
    let tables = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&container)
        .unwrap()
        .unwrap();
    let record = tables.type_record(proc_type.0 as u64).unwrap();
    assert_eq!(record.kind, TypeKind::Struct as u8);
    assert_eq!(record.lang_type, b"Proc");
}
