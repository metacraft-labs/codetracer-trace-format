pub mod trace_readers;

/// M4 — pure-Rust port of the Nim `decodeGlobalPositionIndex`
/// algorithm.  Lives outside the `cfg(not(target_arch = "wasm32"))`
/// gating because the decoder is pure-arithmetic (no I/O, no CTFS
/// container access) and is the natural building block for the
/// browser-replay path's column-aware step rendering.
pub mod global_position_decoder;

#[cfg(target_arch = "wasm32")]
#[path = "./cbor_zstd_reader_wasm.rs"]
pub mod cbor_zstd_reader;

#[cfg(not(target_arch = "wasm32"))]
pub mod cbor_zstd_reader;

#[cfg(not(target_arch = "wasm32"))]
pub mod seekable_reader;

#[cfg(not(target_arch = "wasm32"))]
pub mod ctfs_reader;

#[cfg(not(target_arch = "wasm32"))]
pub mod call_stream_reader;

#[cfg(not(target_arch = "wasm32"))]
pub mod step_stream_reader;

#[cfg(not(target_arch = "wasm32"))]
pub mod value_stream_reader;

#[cfg(not(target_arch = "wasm32"))]
pub mod streaming_ctfs_reader;

#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
    Json,
    BinaryV0,
    Binary,
    Ctfs,
}

pub fn create_trace_reader(format: TraceEventsFileFormat) -> Box<dyn trace_readers::TraceReader> {
    match format {
        TraceEventsFileFormat::Json => Box::new(trace_readers::JsonTraceReader {}),
        TraceEventsFileFormat::BinaryV0 | TraceEventsFileFormat::Binary => Box::new(trace_readers::BinaryTraceReader {}),
        #[cfg(not(target_arch = "wasm32"))]
        TraceEventsFileFormat::Ctfs => Box::new(trace_readers::CtfsTraceReader {}),
        #[cfg(target_arch = "wasm32")]
        TraceEventsFileFormat::Ctfs => panic!("CTFS format is not supported on wasm32"),
    }
}
