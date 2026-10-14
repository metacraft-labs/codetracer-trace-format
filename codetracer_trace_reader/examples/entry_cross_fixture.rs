//! Genuine native CTFS producer for reciprocal Nim cross-reader qualification; no mocks.
use codetracer_trace_types::*;
use codetracer_trace_writer::{ctfs_writer::CtfsTraceWriter, trace_writer::TraceWriter};
use std::path::Path;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = std::env::args().collect();
    let destination = Path::new(&arguments[1]);
    for (name, marked) in [("rust-marked", true), ("rust-unmarked", false)] {
        let mut writer = CtfsTraceWriter::new("entry-cross-control", &[]);
        writer.set_recording_id("01900000-0000-7000-8000-000000000001");
        TraceWriter::set_workdir(&mut writer, Path::new("/entry-cross-control"));
        TraceWriter::begin_writing_trace_events(&mut writer, &destination.join(name))?;
        let source = Path::new("/entry-cross-control.rs");
        TraceWriter::start(&mut writer, source, Line(1));
        let function = TraceWriter::ensure_function_id(&mut writer, "main", source, Line(1));
        TraceWriter::register_call(&mut writer, function, vec![]);
        if marked {
            TraceWriter::mark_current_call_as_entry(&mut writer)?;
        }
        TraceWriter::register_step(&mut writer, source, Line(2));
        TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
        TraceWriter::finish_writing_trace_events(&mut writer)?;
        println!("{name}: native production complete");
    }
    Ok(())
}
