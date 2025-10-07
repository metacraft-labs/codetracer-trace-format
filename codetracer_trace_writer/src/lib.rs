mod abstract_trace_writer;
mod non_streaming_trace_writer;
pub mod trace_writer;

#[cfg(target_arch = "wasm32")]
#[path = "./cbor_zstd_writer_wasm.rs"]
mod cbor_zstd_writer;

#[cfg(not(target_arch = "wasm32"))]
mod cbor_zstd_writer;

#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
    Json,
    BinaryV0,
    Binary,
}

pub fn create_trace_writer(program: &str, args: &[String], format: TraceEventsFileFormat) -> Box<dyn trace_writer::TraceWriter> {
    match format {
        TraceEventsFileFormat::Json | TraceEventsFileFormat::BinaryV0 => {
            let mut result = Box::new(non_streaming_trace_writer::NonStreamingTraceWriter::new(program, args));
            result.set_format(format);
            result
        }
        TraceEventsFileFormat::Binary => Box::new(crate::cbor_zstd_writer::CborZstdTraceWriter::new(program, args)),
    }
}
