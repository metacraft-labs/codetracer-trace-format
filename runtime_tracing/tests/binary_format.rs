use runtime_tracing::{TraceEventsFileFormat, Tracer};
use std::path::Path;
use std::fs;

#[test]
fn test_binary_roundtrip() {
    let json_path = Path::new("tests/data/trace.json");

    let mut tracer = Tracer::new("", &[]);
    tracer
        .load_trace_events(json_path, TraceEventsFileFormat::Json)
        .unwrap();
    let original = tracer.events.clone();

    let bin_path = Path::new("tests/data/trace.bin");

    tracer
        .store_trace_events(bin_path, TraceEventsFileFormat::Binary)
        .unwrap();

    let mut tracer2 = Tracer::new("", &[]);
    tracer2
        .load_trace_events(bin_path, TraceEventsFileFormat::Binary)
        .unwrap();

    fs::remove_file(bin_path).unwrap();

    let orig_json = serde_json::to_string(&original).unwrap();
    let new_json = serde_json::to_string(&tracer2.events).unwrap();

    assert_eq!(orig_json, new_json);
}
