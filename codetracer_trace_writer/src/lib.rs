mod abstract_trace_writer;
mod non_streaming_trace_writer;
mod trace_writer;

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
