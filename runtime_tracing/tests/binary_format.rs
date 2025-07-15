use runtime_tracing::{create_trace_reader, create_trace_writer, TraceEventsFileFormat};
use std::fs;
use std::path::Path;

#[test]
fn test_binary_roundtrip() {
    let json_path = Path::new("tests/data/trace.json");

    let mut json_reader = create_trace_reader(TraceEventsFileFormat::Json);
    let original = json_reader.load_trace_events(json_path).unwrap();

    let bin_path = Path::new("tests/data/trace.bin");

    let mut bin_writer = create_trace_writer("", &[], TraceEventsFileFormat::Binary);
    bin_writer.begin_writing_trace_events(bin_path).unwrap();
    bin_writer.append_events(&mut original.clone());
    bin_writer.finish_writing_trace_events().unwrap();

    let mut bin_reader = create_trace_reader(TraceEventsFileFormat::Binary);
    let tracer2_events = bin_reader.load_trace_events(bin_path).unwrap();

    fs::remove_file(bin_path).unwrap();

    let orig_json = serde_json::to_string(&original).unwrap();
    let new_json = serde_json::to_string(&tracer2_events).unwrap();

    assert_eq!(orig_json, new_json);
}
