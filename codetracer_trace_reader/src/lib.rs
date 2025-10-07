mod trace_readers;

#[cfg(target_arch = "wasm32")]
#[path = "./cbor_zstd_reader_wasm.rs"]
mod cbor_zstd_reader;

#[cfg(not(target_arch = "wasm32"))]
mod cbor_zstd_reader;

#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
    Json,
    BinaryV0,
    Binary,
}
