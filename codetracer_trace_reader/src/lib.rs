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
pub mod io_event_stream_reader;

#[cfg(not(target_arch = "wasm32"))]
pub mod interning_tables_reader;

// `streaming_ctfs_reader` (the legacy `events.log`-tailing `StreamingCtfsReader`)
// was retired in M1 of the CTFS Lazy/Seekable Coverage initiative. Live/streaming
// replay now runs the REAL-PRODUCT db-backend split-stream reader over a
// `FollowFileSource` (see `Seek-Based-CTFS-Reader.md` §5.6 and the db-backend
// `ctfs_trace_reader::follow_stream_source` module + its `follow_stream_flow_test`).
// The parallel `events.log` streaming reader no longer validated anything the
// product ships, and had no remaining caller, so it was removed rather than kept
// as a dead shim.

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
