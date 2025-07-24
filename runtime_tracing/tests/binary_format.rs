use runtime_tracing::{TraceEventsFileFormat, TraceWriter, create_trace_reader, create_trace_writer};
use std::fs;
use std::path::Path;

fn test_binary_roundtrip(ver: TraceEventsFileFormat, binfile: &str) {
    let json_path = Path::new("tests/data/trace.json");

    let mut json_reader = create_trace_reader(TraceEventsFileFormat::Json);
    let original = json_reader.load_trace_events(json_path).unwrap();

    let bin_path_str = format!("tests/data/{}", binfile);
    let bin_path = Path::new(&bin_path_str);

    let mut bin_writer = create_trace_writer("", &[], ver);
    bin_writer.begin_writing_trace_events(bin_path).unwrap();
    TraceWriter::append_events(bin_writer.as_mut(), &mut original.clone());
    bin_writer.finish_writing_trace_events().unwrap();

    let mut bin_reader = create_trace_reader(TraceEventsFileFormat::Binary);
    let tracer2_events = bin_reader.load_trace_events(bin_path).unwrap();

    fs::remove_file(bin_path).unwrap();

    let orig_json = serde_json::to_string(&original).unwrap();
    let new_json = serde_json::to_string(&tracer2_events).unwrap();

    assert_eq!(orig_json, new_json);
}

#[test]
fn test_binary_roundtrip_v0() {
    test_binary_roundtrip(TraceEventsFileFormat::BinaryV0, "trace.v0.bin");
}

#[test]
fn test_binary_roundtrip_v1() {
    test_binary_roundtrip(TraceEventsFileFormat::Binary, "trace.v1.bin");
}
