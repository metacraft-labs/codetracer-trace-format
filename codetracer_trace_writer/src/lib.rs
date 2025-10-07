mod abstract_trace_writer;
mod non_streaming_trace_writer;
mod trace_writer;

#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
    Json,
    BinaryV0,
    Binary,
}
