use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use codetracer_ctfs::{ChunkedWriter, CompressionMethod, CtfsWriter};
use codetracer_trace_format_cbor_zstd::HEADERV1;

// The legacy `Cbor` serialization mode streams through zeekstd, which is
// libzstd-backed (C) and needs a libc.  `wasm32-wasip1` has one (wasi-libc)
// and links it; `wasm32-unknown-unknown` does not, and is the only target
// where the encoder is replaced by a stub with the same shape whose only job
// is to keep the `Cbor` code paths compiling.  The DEFAULT `SplitBinary` mode
// does not use zeekstd at all -- it compresses whole chunks through
// `codetracer_ctfs::zstd_compat` -- and `begin_writing_trace_events` refuses
// `Cbor` on that one target before any stub method can be reached.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use zeekstd::{EncodeOptions, Encoder, FrameSizePolicy};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_cbor_mode_stub::{EncodeOptions, Encoder, FrameSizePolicy};

/// Stand-in for the zeekstd streaming encoder on `wasm32-unknown-unknown`.
///
/// Mirrors only the surface [`CtfsTraceWriter`]'s `Cbor` mode uses. Every
/// method fails; nothing constructs one, because `begin_writing_trace_events`
/// rejects `EventSerializationFormat::Cbor` on that target up front. Keeping
/// the shape means the `Cbor` arms need no `cfg` of their own.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod wasm_cbor_mode_stub {
    use std::io::{Error, Result, Write};
    use std::marker::PhantomData;

    fn unsupported() -> Error {
        Error::other(
            "the CTFS `Cbor` serialization mode is not available on wasm32-unknown-unknown, which has no libc for zeekstd \
             to link against; use `SplitBinary` (the default), or build for wasm32-wasip1, where zeekstd does link",
        )
    }

    pub enum FrameSizePolicy {
        Uncompressed(#[allow(dead_code)] u32),
    }

    pub struct EncodeOptions;

    impl EncodeOptions {
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            EncodeOptions
        }
        pub fn frame_size_policy(self, _policy: FrameSizePolicy) -> Self {
            self
        }
        pub fn compression_level(self, _level: i32) -> Self {
            self
        }
        pub fn into_encoder<W: Write>(self, _sink: W) -> Result<Encoder<'static, W>> {
            Err(unsupported())
        }
    }

    pub struct Encoder<'a, W> {
        _marker: PhantomData<(&'a (), W)>,
    }

    impl<W: Write> Encoder<'_, W> {
        pub fn end_frame(&mut self) -> Result<u64> {
            Err(unsupported())
        }
        pub fn finish(self) -> Result<u64> {
            Err(unsupported())
        }
    }

    impl<W: Write> Write for Encoder<'_, W> {
        fn write(&mut self, _buf: &[u8]) -> Result<usize> {
            Err(unsupported())
        }
        fn flush(&mut self) -> Result<()> {
            Err(unsupported())
        }
    }
}

use crate::{
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
    call_stream::{CallStreamBuilder, DEFAULT_CALLS_CHUNK_SIZE, encode_call_stream},
    column_aware::{EXEC_COMPRESSION_LEVEL, ExecStreamEncoder, PositionSpace, StepEncoder},
    event_stream::{DEFAULT_EVENTS_CHUNK_SIZE, IoEventStreamBuilder, encode_io_event_stream},
    interning_tables::InterningTablesBuilder,
    meta_dat::{
        FLAG_HAS_CALL_STREAM, FLAG_HAS_COLUMN_AWARE_STEPS, FLAG_HAS_INTERNING_TABLES, FLAG_HAS_IO_EVENT_STREAM, FLAG_HAS_STEP_STREAM,
        FLAG_HAS_VALUE_STREAM, FLAG_SUPPORTS_COLUMN_BREAKPOINTS, FLAG_SUPPORTS_COLUMN_MOTIONS, encode_meta_dat,
    },
    step_stream::{DEFAULT_STEPS_CHUNK_SIZE, StepStreamBuilder, encode_step_stream},
    trace_writer::TraceWriter,
    value_stream::{DEFAULT_VALUES_CHUNK_SIZE, ValueStreamBuilder, encode_value_stream},
};
use codetracer_trace_types::TraceLowLevelEvent;

/// Default Zstd level for the dedicated call stream, matching the unified
/// stream and seekable-zstd.md §Configuration.
const DEFAULT_CALLS_ZSTD_LEVEL: i32 = 3;

/// Default flush threshold: 64 KiB of uncompressed data triggers a flush.
const DEFAULT_FLUSH_THRESHOLD: usize = 64 * 1024;

/// Default number of events per chunk in SplitBinary mode.
const DEFAULT_CHUNK_SIZE: usize = 4096;

/// Serialization format for events within the CTFS container.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EventSerializationFormat {
    /// Legacy CBOR format with zeekstd streaming compression.
    Cbor,
    /// Split binary format with chunked Zstd compression.
    SplitBinary,
}

/// A shared byte buffer that implements `Write`, allowing us to drain accumulated
/// compressed data from outside the encoder.
#[derive(Clone)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        SharedBuffer(Arc::new(Mutex::new(Vec::new())))
    }

    /// Drain all accumulated bytes, returning them and clearing the buffer.
    fn drain(&self) -> Vec<u8> {
        let mut buf = self.0.lock().unwrap();
        std::mem::take(&mut *buf)
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Where a [`CtfsTraceWriter`] lays its container out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CtfsOutput {
    /// A `.ct` file on disk, at the path handed to
    /// `begin_writing_trace_events` (with the extension replaced). The
    /// default, and the only behaviour that existed before in-memory output.
    File,
    /// A `Vec<u8>` held by the writer, collected after
    /// `finish_writing_trace_events` with
    /// [`take_container_bytes`](CtfsTraceWriter::take_container_bytes).
    /// The only mode available on `wasm32-unknown-unknown`, which has no
    /// filesystem.
    Memory,
}

/// A trace writer that outputs a single `.ct` CTFS container file.
///
/// The container holds:
/// - `events.log` — encoded events (CBOR+Zstd or split-binary+chunked-Zstd)
/// - `events.fmt` — format marker ("cbor" or "split-binary")
/// - `meta.json`  — trace metadata (program, args, workdir)
/// - `paths.json` — registered source paths
///
/// As of M23e-4 it ALSO emits, BY DEFAULT, the spec multi-stream split files
/// (the same layout the production Nim writer produces) — `calls.dat`/`.idx`,
/// `steps.dat`/`.idx`, `values.dat`/`.idx`, `events.dat`/`.idx`, the
/// `paths`/`funcs`/`types`/`varnames` `.dat`+`.off` interning tables, and a
/// `meta.dat` carrying the capability flags. This is additive: `events.log`
/// is still written (M23e-5 will remove it), so old readers keep working while
/// new readers consume the split streams. Each split can be turned off with the
/// corresponding `with_*_stream(false)` lever (used by tests of the legacy
/// `events.log` postprocessing path).
///
/// In `SplitBinary` mode (the default), events are serialized using the compact
/// split binary encoding and accumulated into chunks of `chunk_size` events.
/// Each chunk is independently Zstd-compressed with an inline header for
/// GEID-based seeking.
///
/// In `Cbor` mode (legacy), events are CBOR-serialized and streamed through
/// zeekstd, flushing to the CTFS file when `flush_threshold` bytes have
/// accumulated.
pub struct CtfsTraceWriter {
    base: AbstractTraceWriterData,
    ctfs_writer: Option<CtfsWriter>,
    events_handle: Option<codetracer_ctfs::FileHandle>,

    /// File or memory. See [`CtfsOutput`].
    output: CtfsOutput,
    /// The finished container, when `output` is [`CtfsOutput::Memory`].
    container_bytes: Option<Vec<u8>>,
    /// Overrides the `recording_id` that would otherwise be minted at
    /// `finish_writing_trace_events`. See
    /// [`set_recording_id`](CtfsTraceWriter::set_recording_id).
    recording_id: Option<String>,
    /// The serialization format to use.
    serialization_format: EventSerializationFormat,

    // --- CBOR mode fields ---
    /// Zstd encoder that compresses CBOR data into `compressed_sink`.
    encoder: Option<Encoder<'static, SharedBuffer>>,
    /// Shared buffer that the encoder writes compressed data into.
    compressed_sink: Option<SharedBuffer>,

    // --- SplitBinary mode fields ---
    /// Buffered serialized event bytes awaiting chunk flush.
    event_buffer: Vec<u8>,
    /// Per-event byte sizes within `event_buffer`.
    event_sizes: Vec<usize>,
    /// GEIDs for buffered events.
    event_geids: Vec<u64>,
    /// Total events written so far (used as GEID counter).
    total_events: u64,
    /// Number of events buffered since the last chunk flush.
    unflushed_events: usize,
    /// Number of events per chunk.
    chunk_size: usize,

    // --- Common fields ---
    /// Tracks uncompressed bytes written since the last flush (CBOR mode).
    unflushed_bytes: usize,
    /// Flush when uncompressed bytes exceed this threshold (CBOR mode, default 64 KiB).
    flush_threshold: usize,
    /// Number of flushes performed so far (visible for testing).
    flush_count: usize,
    /// Whether HEADERV1 has been written to the CTFS file.
    header_written: bool,

    // --- M17a: dedicated call stream ---
    /// When set, the writer ALSO emits a dedicated `calls.dat` call stream
    /// (plus its companion `calls.idx`), derived from the same Call/Return/Step
    /// events that feed `events.log`, and sets the `has_call_stream` meta.dat
    /// flag. Off by default so existing recorders are byte-for-byte unchanged.
    emit_call_stream: bool,
    /// Builds the call records from the observed event sequence (present only
    /// while `emit_call_stream` is on and a trace is being written).
    call_stream_builder: Option<CallStreamBuilder>,
    /// Records-per-chunk for `calls.dat`.
    calls_chunk_size: usize,

    // --- M23a / M23e-4: dedicated execution (step) stream (default-on) ---
    /// When set, the writer ALSO emits a dedicated `steps.dat` compact
    /// execution stream (plus its companion `steps.idx`), derived from the same
    /// Step/Call/Return/ThreadSwitch events that feed `events.log`, and sets the
    /// `has_step_stream` meta.dat flag. ON by default (M23e-4) — the secondary
    /// Rust writer emits the spec split format; `events.log` is still written
    /// alongside (additive). Disable with `with_step_stream(false)`.
    emit_step_stream: bool,
    /// Builds the compact step records from the observed event sequence (present
    /// only while `emit_step_stream` is on and a trace is being written).
    step_stream_builder: Option<StepStreamBuilder>,
    /// Records-per-chunk for `steps.dat`.
    steps_chunk_size: usize,

    // --- M23b / M23e-4: dedicated parallel value stream (default-on) ---
    /// When set, the writer ALSO emits a dedicated `values.dat` parallel value
    /// stream (plus its companion `values.idx`), derived from the same
    /// Value/BindVariable/Cell/Assign… events that feed `events.log`, and sets
    /// the `has_value_stream` meta.dat flag. The value stream is parallel-indexed
    /// to the execution stream — value record N ↔ step N — with an empty record
    /// for steps that have no variable activity. ON by default (M23e-4);
    /// `events.log` is still written alongside (additive). Disable with
    /// `with_value_stream(false)`.
    emit_value_stream: bool,
    /// Builds the per-step value records from the observed event sequence
    /// (present only while `emit_value_stream` is on and a trace is being
    /// written).
    value_stream_builder: Option<ValueStreamBuilder>,
    /// Records-per-chunk for `values.dat`.
    values_chunk_size: usize,

    // --- M23c / M23e-4: dedicated I/O event stream (default-on) ---
    /// When set, the writer ALSO emits a dedicated `events.dat` I/O event stream
    /// (plus its companion `events.idx`), derived from the same `Event` records
    /// (the `EventLogKind`-tagged I/O / log events) that feed `events.log`, and
    /// sets the `has_io_event_stream` meta.dat flag. Each record carries kind /
    /// step_id (cross-ref to the execution stream) / metadata / content. ON by
    /// default (M23e-4); `events.log` is still written alongside (additive).
    /// NOTE: this `events.dat` is DISTINCT from the legacy `events.log`. Disable
    /// with `with_io_event_stream(false)`.
    emit_io_event_stream: bool,
    /// Builds the I/O event records from the observed event sequence (present
    /// only while `emit_io_event_stream` is on and a trace is being written).
    io_event_stream_builder: Option<IoEventStreamBuilder>,
    /// Records-per-chunk for `events.dat`.
    events_chunk_size: usize,

    // --- M23d / M23e-4: binary varint interning tables (default-on) ---
    /// When set, the writer ALSO emits the binary varint interning tables
    /// (`paths.dat`+`paths.off`, `funcs.dat`+`funcs.off`, `types.dat`+`types.off`,
    /// `varnames.dat`+`varnames.off`), derived from the SAME
    /// Path/Function/Type/VariableName interning that feeds `events.log` /
    /// `paths.json`, and sets the `has_interning_tables` meta.dat flag. These use
    /// the Variable-Size Record Table (`.dat` + `.off`) pattern. ON by default
    /// (M23e-4); `events.log` / `paths.json` are still written alongside
    /// (additive). Disable with `with_interning_tables(false)`.
    emit_interning_tables: bool,
    /// Builds the interning-table records from the observed event sequence
    /// (present only while `emit_interning_tables` is on and a trace is being
    /// written).
    interning_tables_builder: Option<InterningTablesBuilder>,

    // --- Column-aware step mode (parity with the Nim writer) ---
    //
    // When on, the writer produces a `steps.dat` whose `global_position_index`
    // addresses `(line, column)` pairs, a `paths.dat` in spec Layout A, and
    // sets `meta.dat` bit 4. All three change together — see
    // `crate::meta_dat::FLAG_HAS_COLUMN_AWARE_STEPS` — because the flag is what
    // tells a reader which parse to use.
    //
    // The mode is TRACE-GLOBAL and must be selected before
    // `begin_writing_trace_events`. A request that arrives after the trace has
    // started is refused rather than half-applied, and
    // `dropped_column_awareness()` reports it.
    /// True once a caller asked for column-aware output.
    column_aware_requested: bool,
    /// True once column-aware output is actually in effect for the trace being
    /// written. Diverges from `column_aware_requested` exactly when the request
    /// arrived too late.
    column_aware_active: bool,
    /// Set when a column-aware request could not be honoured. Read through
    /// [`CtfsTraceWriter::dropped_column_awareness`].
    column_awareness_dropped: bool,
    /// Capability bit 6 — the recorder's columns are breakpoint-sharp.
    column_breakpoints_requested: bool,
    /// Capability bit 7 — the recorder supports per-column motions.
    column_motions_requested: bool,
    /// The `(line, column)` address space, built from the per-path
    /// `line_lengths` tables. Only consulted in column-aware mode.
    position_space: PositionSpace,
    /// Nim's delta-vs-absolute policy and running cursor.
    step_encoder: StepEncoder,
    /// The column-aware `steps.dat` encoder. `Some` only while a column-aware
    /// trace is being written; the line-only path keeps using
    /// `step_stream_builder` so its bytes do not move.
    exec_encoder: Option<ExecStreamEncoder>,
    /// Per-line table waiting to be attached to the next `Path` event. Set by
    /// [`CtfsTraceWriter::register_path_with_line_lengths`] immediately before
    /// the path is registered, and consumed when that `Path` event arrives, so
    /// the table lands on the right interning id without a second lookup.
    pending_line_lengths: Option<Vec<u32>>,
    /// Column offset to fold into the next `Step` event, in the same
    /// consume-once way as `pending_line_lengths`. Set by
    /// [`AbstractTraceWriter::register_step_with_column`].
    pending_column_delta: i64,
}

impl CtfsTraceWriter {
    /// Create a new CTFS trace writer using the default SplitBinary format.
    pub fn new(program: &str, args: &[String]) -> Self {
        Self::with_options(
            program,
            args,
            EventSerializationFormat::SplitBinary,
            DEFAULT_FLUSH_THRESHOLD,
            DEFAULT_CHUNK_SIZE,
        )
    }

    /// Create a new CTFS trace writer with a custom flush threshold.
    ///
    /// Uses the default SplitBinary format. The `flush_threshold` controls
    /// CBOR mode flushing; in SplitBinary mode, flushing is chunk-based.
    pub fn with_flush_threshold(program: &str, args: &[String], flush_threshold: usize) -> Self {
        Self::with_options(program, args, EventSerializationFormat::SplitBinary, flush_threshold, DEFAULT_CHUNK_SIZE)
    }

    /// Create a new CTFS trace writer with explicit format and tuning options.
    pub fn with_options(program: &str, args: &[String], format: EventSerializationFormat, flush_threshold: usize, chunk_size: usize) -> Self {
        CtfsTraceWriter {
            base: AbstractTraceWriterData::new(program, args),
            ctfs_writer: None,
            events_handle: None,
            output: CtfsOutput::File,
            container_bytes: None,
            recording_id: None,
            serialization_format: format,
            encoder: None,
            compressed_sink: None,
            event_buffer: Vec::new(),
            event_sizes: Vec::new(),
            event_geids: Vec::new(),
            total_events: 0,
            unflushed_events: 0,
            chunk_size,
            unflushed_bytes: 0,
            flush_threshold,
            flush_count: 0,
            header_written: false,
            // M20: the dedicated `calls.dat` call stream is emitted BY DEFAULT so
            // every recorder driving `CtfsTraceWriter` (Ruby, Python, JS, shell,
            // Wasm, …) materializes the calls/steps split without an explicit
            // opt-in. This is additive and backward-compatible: old readers ignore
            // the extra `calls.dat`/`calls.idx` files and the unset-aware `meta.dat`
            // flag; new readers (ct-print, the engine, the db-backend seekable
            // reader) use the `has_call_stream` flag to read the call tree on
            // demand. Disable explicitly with `with_call_stream(false)` if a caller
            // must reproduce the pre-M20 flag-off output (e.g. a legacy golden).
            emit_call_stream: true,
            call_stream_builder: None,
            calls_chunk_size: DEFAULT_CALLS_CHUNK_SIZE,
            // M23e-4: the dedicated `steps.dat` execution stream is now emitted
            // BY DEFAULT, joining the M20 `calls.dat` default. The secondary Rust
            // `CtfsTraceWriter` thus produces the spec multi-stream format (the
            // same split layout the production Nim writer emits) even for its
            // non-production (tests/legacy) bundles. This is ADDITIVE: `events.log`
            // is still written alongside (M23e-5 removes it), so old readers keep
            // working. Disable explicitly with `with_step_stream(false)` to
            // reproduce the legacy `events.log`-only bundle (tests of the legacy
            // postprocessing path use this lever).
            emit_step_stream: true,
            step_stream_builder: None,
            steps_chunk_size: DEFAULT_STEPS_CHUNK_SIZE,
            // M23e-4: the dedicated `values.dat` parallel value stream is now
            // emitted BY DEFAULT, parallel-indexed to the default `steps.dat`.
            // Additive (events.log retained). Disable explicitly with
            // `with_value_stream(false)` for the legacy-path bundle.
            emit_value_stream: true,
            value_stream_builder: None,
            values_chunk_size: DEFAULT_VALUES_CHUNK_SIZE,
            // M23e-4: the dedicated `events.dat` I/O event stream is now emitted
            // BY DEFAULT. Additive (events.log retained). Disable explicitly with
            // `with_io_event_stream(false)` for the legacy-path bundle.
            emit_io_event_stream: true,
            io_event_stream_builder: None,
            events_chunk_size: DEFAULT_EVENTS_CHUNK_SIZE,
            // M23e-4: the binary varint interning tables are now emitted BY
            // DEFAULT, so the split bundle is self-describing (the new-format
            // reader resolves path/func/type/varname ids from the binary tables
            // rather than `paths.json`). Additive — the existing `paths.json`
            // interning is untouched. Disable explicitly with
            // `with_interning_tables(false)` for the legacy-path bundle.
            emit_interning_tables: true,
            interning_tables_builder: None,
            // Column-aware mode is OFF by default. Turning it on changes
            // `steps.dat` addressing, `paths.dat` record shape and a meta.dat
            // bit that column-unaware readers are required to reject, so it is
            // a deliberate opt-in per trace and never a default.
            column_aware_requested: false,
            column_aware_active: false,
            column_awareness_dropped: false,
            column_breakpoints_requested: false,
            column_motions_requested: false,
            position_space: PositionSpace::new(false),
            step_encoder: StepEncoder::new(),
            exec_encoder: None,
            pending_line_lengths: None,
            pending_column_delta: 0,
        }
    }

    // --- Column-aware step mode ---------------------------------------------

    /// Opt this trace into column-aware step encoding.
    ///
    /// Must be called **before** `begin_writing_trace_events`: the mode decides
    /// `paths.dat`'s record shape and `steps.dat`'s addressing, and the spec
    /// forbids mixing column-aware and line-only records inside one trace. A
    /// call after the trace has begun is refused and recorded — see
    /// [`Self::dropped_column_awareness`] — rather than applied to the tail of
    /// the stream, which would produce a container no reader can decode.
    ///
    /// Mirrors the Nim writer's `enableColumnAwareSteps`.
    pub fn enable_column_aware_steps(&mut self) {
        self.column_aware_requested = true;
        if self.ctfs_writer.is_some() {
            // Too late: the trace is already open.
            self.column_awareness_dropped = true;
            return;
        }
        self.column_aware_active = true;
    }

    /// Declare that this recorder's columns are sharp enough for the GUI to
    /// place per-column breakpoints (`meta.dat` bit 6).
    ///
    /// Implies [`Self::enable_column_aware_steps`], because a capability bit
    /// without column data on the wire is undefined per spec — the Nim writer's
    /// `enableColumnBreakpointsSupport` auto-enables it for the same reason.
    pub fn enable_column_breakpoints_support(&mut self) {
        self.column_breakpoints_requested = true;
        self.enable_column_aware_steps();
    }

    /// Declare that this recorder supports per-column step over / in / out
    /// (`meta.dat` bit 7). Implies [`Self::enable_column_aware_steps`].
    pub fn enable_column_motions_support(&mut self) {
        self.column_motions_requested = true;
        self.enable_column_aware_steps();
    }

    /// Whether this writer is producing a column-aware trace.
    pub fn column_aware_steps_enabled(&self) -> bool {
        self.column_aware_active
    }

    /// Whether a caller asked for column-aware output that this writer could
    /// not produce.
    ///
    /// A caller whose correctness depends on columns should assert this is
    /// `false` at close. It answers `true` in exactly one reachable situation:
    /// [`Self::enable_column_aware_steps`] was called after
    /// `begin_writing_trace_events`, when the mode can no longer be made
    /// trace-global. It answers `false` both when nobody asked and when the
    /// request was honoured, so the signal is only meaningful where columns
    /// were requested — asserting it unconditionally would pass on every
    /// ordinary recording for the wrong reason.
    pub fn dropped_column_awareness(&self) -> bool {
        self.column_awareness_dropped
    }

    /// Register a source path together with its per-line addressable column
    /// counts (spec `paths.dat` Layout A), returning its interning id.
    ///
    /// `line_lengths[i]` is the number of addressable columns on line `i + 1`.
    /// Implementations are free to use `actual_columns + 1` so the trailing
    /// "one past end of line" position gets its own address.
    ///
    /// Outside column-aware mode the table is accepted and ignored, exactly as
    /// the Nim writer ignores it, so a recorder can call this unconditionally
    /// without changing a line-only trace's bytes.
    ///
    /// Mirrors the Nim writer's `registerPath(path, lineLengths)`.
    pub fn register_path_with_line_lengths(&mut self, path: &Path, line_lengths: &[u32]) -> codetracer_trace_types::PathId {
        if self.base.paths.contains_key(path) {
            // Already interned; the table was attached when it was first seen.
            return *self.base.paths.get(path).unwrap();
        }
        self.pending_line_lengths = Some(line_lengths.to_vec());
        let id = AbstractTraceWriter::ensure_path_id(self, path);
        // `ensure_path_id` emits the `Path` event, which consumes the pending
        // table. Clear it defensively so a path that somehow did not emit one
        // cannot leak its table onto the next path registered.
        self.pending_line_lengths = None;
        id
    }

    /// Emit a column-only step: a `DeltaColumn` (tag 0x07) record that advances
    /// the cursor's column inside the current line.
    ///
    /// `column_delta` is signed and zigzag-encoded; magnitudes up to ±63 cost
    /// two bytes. A value record is opened alongside it so `values.dat` stays
    /// parallel-indexed to `steps.dat` — without that the two streams drift by
    /// one record per column move.
    ///
    /// Refused when the trace is not column-aware, when no trace is open, or
    /// when it would be the first step (the running cursor must be defined
    /// first). Mirrors the Nim writer's `registerColumnStep`.
    pub fn register_column_step(&mut self, column_delta: i64) -> Result<(), String> {
        if !self.column_aware_active {
            return Err("register_column_step called on a writer that has not opted into column-aware mode \
                        (call enable_column_aware_steps before begin_writing_trace_events)"
                .to_string());
        }
        let Some(encoder) = self.exec_encoder.as_mut() else {
            return Err("register_column_step called before begin_writing_trace_events".to_string());
        };
        let event = self.step_encoder.column_step(column_delta)?;
        encoder.write_event(event)?;
        if let Some(builder) = self.value_stream_builder.as_mut() {
            builder.open_step_record();
        }
        Ok(())
    }

    /// The per-file `line_lengths` tables registered so far, in interning-id
    /// order — what a reader's `GlobalPositionDecoder::from_line_lengths`
    /// consumes to resolve this trace's positions.
    pub fn line_lengths(&self) -> &[Vec<u32>] {
        self.position_space.line_lengths()
    }

    /// Enable or disable the dedicated `calls.dat` call stream (M17a / M20).
    ///
    /// As of M20 the call stream is emitted BY DEFAULT (see `with_options`), so
    /// this method is primarily a DISABLE lever — pass `false` to reproduce the
    /// pre-M20 flag-off bundle (no `calls.dat`/`calls.idx`, `has_call_stream`
    /// clear), e.g. when regenerating a legacy golden fixture.
    ///
    /// When enabled, `finish_writing_trace_events` writes, in addition to the
    /// unchanged `events.log`, a `calls.dat` stream of complete call records and
    /// its companion seekable index `calls.idx`, and stamps a `meta.dat` with
    /// the `has_call_stream` capability flag set. The call records are derived
    /// from the same Call/Return/Step events, so they are guaranteed consistent
    /// with the unified stream. This is additive: old readers ignore the extra
    /// files. Returns `self` for builder-style chaining.
    pub fn with_call_stream(mut self, enable: bool) -> Self {
        self.emit_call_stream = enable;
        self
    }

    /// Set the records-per-chunk for `calls.dat` (seek granularity). Smaller
    /// chunks give finer seeks at a slightly lower compression ratio.
    pub fn with_calls_chunk_size(mut self, chunk_size: usize) -> Self {
        self.calls_chunk_size = chunk_size.max(1);
        self
    }

    /// Whether the dedicated call stream is enabled.
    pub fn call_stream_enabled(&self) -> bool {
        self.emit_call_stream
    }

    /// Enable or disable the dedicated `steps.dat` execution stream (M23a / M23e-4).
    ///
    /// As of M23e-4 the step stream is emitted BY DEFAULT (see `with_options`),
    /// so this method is primarily a DISABLE lever — pass `false` to reproduce a
    /// legacy `events.log`-only bundle (no `steps.dat`/`steps.idx`,
    /// `has_step_stream` clear), e.g. for tests that exercise the old-format
    /// postprocessing path.
    ///
    /// When enabled, `finish_writing_trace_events` writes, in addition to the
    /// unchanged `events.log`, a `steps.dat` compact execution stream
    /// (AbsoluteStep/DeltaStep + Raise/Catch/ThreadSwitch) and its companion
    /// seekable index `steps.idx`, and sets the `has_step_stream` capability
    /// flag in `meta.dat`. The step records are derived from the same
    /// Step/Call/Return/ThreadSwitch events, so they are guaranteed consistent
    /// with the unified stream. This is additive: old readers ignore the extra
    /// files. Returns `self` for builder-style chaining.
    pub fn with_step_stream(mut self, enable: bool) -> Self {
        self.emit_step_stream = enable;
        self
    }

    /// Set the records-per-chunk for `steps.dat` (seek granularity). Smaller
    /// chunks give finer seeks at a slightly lower compression ratio.
    pub fn with_steps_chunk_size(mut self, chunk_size: usize) -> Self {
        self.steps_chunk_size = chunk_size.max(1);
        self
    }

    /// Whether the dedicated execution (step) stream is enabled.
    pub fn step_stream_enabled(&self) -> bool {
        self.emit_step_stream
    }

    /// Enable or disable the dedicated `values.dat` parallel value stream
    /// (M23b / M23e-4).
    ///
    /// As of M23e-4 the value stream is emitted BY DEFAULT (see `with_options`),
    /// so this method is primarily a DISABLE lever — pass `false` for a legacy
    /// `events.log`-only bundle.
    ///
    /// When enabled, `finish_writing_trace_events` writes, in addition to the
    /// unchanged `events.log`, a `values.dat` parallel value stream
    /// (StepValues / BindVariable / Cell / Assign… per step) and its companion
    /// seekable index `values.idx`, and sets the `has_value_stream` capability
    /// flag in `meta.dat`. The value records are derived from the same value
    /// events, parallel-indexed to the execution stream (value record N ↔ step
    /// N), so they are guaranteed consistent with the unified stream. This is
    /// additive: old readers ignore the extra files. Returns `self` for
    /// builder-style chaining.
    pub fn with_value_stream(mut self, enable: bool) -> Self {
        self.emit_value_stream = enable;
        self
    }

    /// Set the records-per-chunk for `values.dat` (seek granularity). Smaller
    /// chunks give finer seeks at a slightly lower compression ratio.
    pub fn with_values_chunk_size(mut self, chunk_size: usize) -> Self {
        self.values_chunk_size = chunk_size.max(1);
        self
    }

    /// Whether the dedicated parallel value stream is enabled.
    pub fn value_stream_enabled(&self) -> bool {
        self.emit_value_stream
    }

    /// Enable or disable the dedicated `events.dat` I/O event stream
    /// (M23c / M23e-4).
    ///
    /// As of M23e-4 the I/O event stream is emitted BY DEFAULT (see
    /// `with_options`), so this method is primarily a DISABLE lever — pass
    /// `false` for a legacy `events.log`-only bundle.
    ///
    /// When enabled, `finish_writing_trace_events` writes, in addition to the
    /// unchanged `events.log`, an `events.dat` I/O event stream (the
    /// `EventLogKind`-tagged stdout/stderr/file/network/error/log events, each
    /// record carrying kind / step_id / metadata / content) and its companion
    /// seekable index `events.idx`, and sets the `has_io_event_stream`
    /// capability flag in `meta.dat`. The I/O event records are derived from the
    /// same `Event` records, so they are guaranteed consistent with the unified
    /// stream. This is additive: old readers ignore the extra files. NOTE the
    /// distinct file naming — `events.dat` is NOT the legacy `events.log`.
    /// Returns `self` for builder-style chaining.
    pub fn with_io_event_stream(mut self, enable: bool) -> Self {
        self.emit_io_event_stream = enable;
        self
    }

    /// Set the records-per-chunk for `events.dat` (the event-log page
    /// granularity). Smaller chunks give finer pages at a slightly lower
    /// compression ratio.
    pub fn with_events_chunk_size(mut self, chunk_size: usize) -> Self {
        self.events_chunk_size = chunk_size.max(1);
        self
    }

    /// Whether the dedicated I/O event stream is enabled.
    pub fn io_event_stream_enabled(&self) -> bool {
        self.emit_io_event_stream
    }

    /// Enable or disable the binary varint interning tables (M23d / M23e-4).
    ///
    /// As of M23e-4 the interning tables are emitted BY DEFAULT (see
    /// `with_options`), so this method is primarily a DISABLE lever — pass
    /// `false` for a legacy `events.log`-only bundle.
    ///
    /// When enabled, `finish_writing_trace_events` writes, in addition to the
    /// unchanged `events.log` / `paths.json`, the four interning tables
    /// (`paths.dat`+`paths.off`, `funcs.dat`+`funcs.off`, `types.dat`+`types.off`,
    /// `varnames.dat`+`varnames.off`) using the Variable-Size Record Table
    /// (`.dat` + `.off`) pattern, and sets the `has_interning_tables` capability
    /// flag in `meta.dat`. The records are derived from the same
    /// Path/Function/Type/VariableName interning events, so they resolve exactly
    /// the ids the event streams reference. This is additive: old readers ignore
    /// the extra files; the existing `paths.json` interning is untouched. Returns
    /// `self` for builder-style chaining.
    pub fn with_interning_tables(mut self, enable: bool) -> Self {
        self.emit_interning_tables = enable;
        self
    }

    /// Whether the binary varint interning tables are enabled.
    pub fn interning_tables_enabled(&self) -> bool {
        self.emit_interning_tables
    }

    /// Create a CTFS trace writer that builds the container **in memory**
    /// instead of on disk.
    ///
    /// This is the constructor to use from WebAssembly, where there is no
    /// filesystem — but nothing about it is wasm-specific, and on a host it
    /// produces the same container the file-backed writer would.
    ///
    /// Usage is otherwise identical to [`new`](Self::new). The `path` handed
    /// to `begin_writing_trace_events` is ignored (pass anything, e.g.
    /// `Path::new("trace")`); after `finish_writing_trace_events` the bytes
    /// come out of [`take_container_bytes`](Self::take_container_bytes):
    ///
    /// ```no_run
    /// use codetracer_trace_writer::{ctfs_writer::CtfsTraceWriter, trace_writer::TraceWriter};
    /// use std::path::Path;
    ///
    /// let mut writer = CtfsTraceWriter::new_in_memory("program", &[]);
    /// writer.begin_writing_trace_events(Path::new("trace"))?;
    /// // ... register steps/calls/values ...
    /// writer.finish_writing_trace_events()?;
    /// let ct_bytes: Vec<u8> = writer.take_container_bytes().expect("in-memory writer");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// `ct_bytes` is a complete `.ct` container — write it to a file, hand it
    /// to a `Blob`, upload it. On `wasm32-unknown-unknown` you will usually
    /// also want [`set_recording_id`](Self::set_recording_id), since the
    /// module cannot mint a real UUIDv7 without a clock or a CSPRNG.
    pub fn new_in_memory(program: &str, args: &[String]) -> Self {
        let mut writer = Self::new(program, args);
        writer.output = CtfsOutput::Memory;
        writer
    }

    /// Choose file-backed or in-memory output. Must be set before
    /// `begin_writing_trace_events`.
    pub fn with_output(mut self, output: CtfsOutput) -> Self {
        self.output = output;
        self
    }

    /// Where this writer lays the container out.
    pub fn output(&self) -> CtfsOutput {
        self.output
    }

    /// Take the finished container bytes.
    ///
    /// Returns `Some` only for an in-memory writer whose
    /// `finish_writing_trace_events` has completed; `None` for a file-backed
    /// writer (whose bytes are on disk) or before the trace is finished. The
    /// bytes are moved out, so a second call returns `None`.
    pub fn take_container_bytes(&mut self) -> Option<Vec<u8>> {
        self.container_bytes.take()
    }

    /// Borrow the finished container bytes without consuming them.
    pub fn container_bytes(&self) -> Option<&[u8]> {
        self.container_bytes.as_deref()
    }

    /// Pin the `recording_id` stamped into `meta.json` and `meta.dat`.
    ///
    /// By default the writer mints a fresh UUIDv7 at
    /// `finish_writing_trace_events`. Set it explicitly when the identity is
    /// decided elsewhere — an import pinning a pre-existing id, a test that
    /// wants a reproducible container, or a browser host minting the id in
    /// JavaScript because `wasm32-unknown-unknown` has neither a wall clock
    /// nor an entropy source.
    pub fn set_recording_id(&mut self, recording_id: impl Into<String>) {
        self.recording_id = Some(recording_id.into());
    }

    /// Create a new CTFS trace writer using the legacy CBOR format.
    pub fn new_cbor(program: &str, args: &[String]) -> Self {
        Self::with_options(program, args, EventSerializationFormat::Cbor, DEFAULT_FLUSH_THRESHOLD, DEFAULT_CHUNK_SIZE)
    }

    /// Write the HEADERV1 prefix to the CTFS events.log if not already done.
    fn ensure_header_written(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.header_written {
            if let (Some(writer), Some(handle)) = (&mut self.ctfs_writer, self.events_handle) {
                writer.write(handle, HEADERV1)?;
                self.header_written = true;
            }
        }
        Ok(())
    }

    /// Flush the current Zstd frame to the CTFS container (CBOR mode).
    ///
    /// Ends the current Zstd frame (producing a complete, independently
    /// decompressible frame), drains the compressed output buffer, and
    /// writes it to the CTFS `events.log` file.
    fn flush_events_cbor(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.unflushed_bytes == 0 {
            return Ok(());
        }

        if let Some(ref mut encoder) = self.encoder {
            // End the current Zstd frame so it can be decompressed independently.
            encoder.end_frame()?;
            // Flush the encoder's internal output buffer to the shared sink.
            encoder.flush()?;
        }

        // Drain compressed bytes from the shared sink and write to CTFS.
        if let Some(ref sink) = self.compressed_sink {
            let data = sink.drain();
            if !data.is_empty() {
                self.ensure_header_written()?;
                if let (Some(writer), Some(handle)) = (&mut self.ctfs_writer, self.events_handle) {
                    writer.write(handle, &data)?;
                    // Sync the file entry to disk so concurrent readers can see
                    // the updated events.log size.
                    writer.sync_entry(handle)?;
                }
            }
        }

        self.unflushed_bytes = 0;
        self.flush_count += 1;
        Ok(())
    }

    /// Flush buffered events as a compressed chunk (SplitBinary mode).
    fn flush_chunk(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.unflushed_events == 0 {
            return Ok(());
        }

        let chunked_writer = ChunkedWriter::new(CompressionMethod::Zstd, self.unflushed_events);
        let chunk_data = chunked_writer.write_chunked(&self.event_buffer, &self.event_sizes, &self.event_geids)?;

        self.ensure_header_written()?;
        if let (Some(writer), Some(handle)) = (&mut self.ctfs_writer, self.events_handle) {
            writer.write(handle, &chunk_data)?;
            writer.sync_entry(handle)?;
        }

        self.event_buffer.clear();
        self.event_sizes.clear();
        self.event_geids.clear();
        self.unflushed_events = 0;
        self.flush_count += 1;
        Ok(())
    }

    /// Returns the number of flushes performed so far.
    pub fn flush_count(&self) -> usize {
        self.flush_count
    }

    /// Returns the serialization format in use.
    pub fn serialization_format(&self) -> EventSerializationFormat {
        self.serialization_format
    }
}

impl AbstractTraceWriter for CtfsTraceWriter {
    fn get_data(&self) -> &AbstractTraceWriterData {
        &self.base
    }

    fn get_mut_data(&mut self) -> &mut AbstractTraceWriterData {
        &mut self.base
    }

    /// Record a step at `(path, line, column)`.
    ///
    /// This overrides the trait's column-dropping shim. In column-aware mode
    /// the column is folded into the step's `global_position_index`, so the
    /// wire carries ONE record at `(line, column)` — matching the canonical Nim
    /// FFI, whose `trace_writer_register_delta_column` folds into the pending
    /// step for the same reason.
    ///
    /// Outside column-aware mode the column is still dropped, because there is
    /// nowhere in a line-only address space to put it; the difference from the
    /// old behaviour is that a caller can now detect that case up front through
    /// [`CtfsTraceWriter::column_aware_steps_enabled`] instead of discovering it
    /// in the decoded trace.
    fn register_step_with_column(
        &mut self,
        path: &std::path::Path,
        line: codetracer_trace_types::Line,
        column: Option<codetracer_trace_types::Line>,
    ) {
        if self.column_aware_active {
            // CTFS columns are 1-based, so column 1 is a zero delta.
            self.pending_column_delta = column.map(|c| c.0 - 1).unwrap_or(0);
        }
        AbstractTraceWriter::register_step(self, path, line);
        // Defensive: `register_step` always emits a `Step` event, which
        // consumes the delta. Clearing it anyway means a future refactor that
        // suppresses the event cannot leak a column onto an unrelated step.
        self.pending_column_delta = 0;
    }

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        // Column-aware mode intercepts the two events that carry source
        // positions BEFORE the line-only builders see them. `Path` grows the
        // position space; `Step` is encoded through Nim's delta policy into the
        // exec encoder instead of through `StepStreamBuilder`. Everything else
        // flows on unchanged, so `events.log`, `calls.dat`, `values.dat` and
        // `events.dat` are produced identically in both modes.
        if self.column_aware_active {
            match &event {
                TraceLowLevelEvent::Path(_) => {
                    let lls = self.pending_line_lengths.take().unwrap_or_default();
                    let path_id = self.position_space.push_path(&lls) as usize;
                    if let Some(ref mut builder) = self.interning_tables_builder {
                        builder.set_path_line_lengths(path_id, &lls);
                    }
                }
                TraceLowLevelEvent::Step(step) => {
                    let position = self.position_space.position_of(step.path_id.0 as u64, step.line.0.max(0) as u64);
                    // A bare `StepRecord` carries no column, so the delta is 0
                    // and the step addresses column 1 of the line.
                    // `register_step_with_column` stages a non-zero delta here
                    // so the `(line, column)` pair becomes ONE record rather
                    // than a line step followed by a column step — that folding
                    // is what the canonical Nim FFI does, and the reason is
                    // behavioural rather than aesthetic: an intermediate
                    // column-1 step carries no variables, so a line-granular
                    // step-over lands on it and `variables_at` answers empty.
                    let column_delta = std::mem::replace(&mut self.pending_column_delta, 0);
                    let step_event = self.step_encoder.step_at(position, column_delta);
                    if let Some(encoder) = self.exec_encoder.as_mut() {
                        // A failure here is a zstd failure, which the
                        // line-only path also swallows (`let _ =
                        // self.flush_chunk()`). Keep the shapes the same
                        // rather than introducing a panic on one path only.
                        let _ = encoder.write_event(step_event);
                    }
                }
                TraceLowLevelEvent::ThreadSwitch(codetracer_trace_types::ThreadId(tid)) => {
                    // A thread switch is a record in the execution stream and
                    // occupies a step slot: the Nim writer writes an empty
                    // value record beside it and increments `stepCount`. Both
                    // matter — the value record keeps `values.dat` parallel,
                    // and the counter is what decides whether the NEXT step is
                    // forced absolute.
                    if let Some(encoder) = self.exec_encoder.as_mut() {
                        let _ = encoder.write_event(crate::column_aware::StepEvent::ThreadSwitch { thread_id: *tid });
                    }
                    self.step_encoder.note_non_step_event();
                    if let Some(builder) = self.value_stream_builder.as_mut() {
                        builder.open_step_record();
                    }
                }
                _ => {}
            }
        }
        // M17a: feed the dedicated call-stream builder from the SAME event
        // sequence that produces events.log, so calls.dat stays consistent.
        if let Some(ref mut builder) = self.call_stream_builder {
            builder.observe(&event);
        }
        // M23a: feed the dedicated step-stream builder from the SAME event
        // sequence that produces events.log, so steps.dat stays consistent.
        // Armed only in line-only mode; the column-aware path above owns
        // `steps.dat` instead.
        if let Some(ref mut builder) = self.step_stream_builder {
            builder.observe(&event);
        }
        // M23b: feed the dedicated value-stream builder from the SAME event
        // sequence that produces events.log, so values.dat stays consistent and
        // parallel-indexed to the step stream.
        if let Some(ref mut builder) = self.value_stream_builder {
            builder.observe(&event);
        }
        // M23c: feed the dedicated I/O event-stream builder from the SAME event
        // sequence that produces events.log, so events.dat stays consistent.
        if let Some(ref mut builder) = self.io_event_stream_builder {
            builder.observe(&event);
        }
        // M23d: feed the interning-tables builder from the SAME
        // Path/Function/Type/VariableName events that intern into events.log /
        // paths.json, so the binary tables resolve exactly the ids the streams
        // reference.
        if let Some(ref mut builder) = self.interning_tables_builder {
            builder.observe(&event);
        }
        match self.serialization_format {
            EventSerializationFormat::Cbor => {
                let buf: Vec<u8> = Vec::new();
                let cbor_bytes = cbor4ii::serde::to_vec(buf, &event).unwrap();

                if let Some(ref mut encoder) = self.encoder {
                    encoder.write_all(&cbor_bytes).unwrap();
                }
                self.unflushed_bytes += cbor_bytes.len();

                // Auto-flush when uncompressed data exceeds threshold.
                if self.unflushed_bytes >= self.flush_threshold {
                    let _ = self.flush_events_cbor();
                }
            }
            EventSerializationFormat::SplitBinary => {
                let start = self.event_buffer.len();
                crate::split_binary::encode_event(&event, &mut self.event_buffer).unwrap();
                let size = self.event_buffer.len() - start;
                self.event_sizes.push(size);
                self.event_geids.push(self.total_events);
                self.total_events += 1;
                self.unflushed_events += 1;

                if self.unflushed_events >= self.chunk_size {
                    let _ = self.flush_chunk();
                }
            }
        }
    }

    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        for e in events {
            AbstractTraceWriter::add_event(self, e.clone());
        }
    }
}

impl TraceWriter for CtfsTraceWriter {
    // ---------------------------------------------------------------------
    // THE COLUMN-AWARE FAMILY IS HONOURED NOW, AND THESE OVERRIDES ARE WHAT
    // DELIVERS THAT TO A CALLER HOLDING THE TRAIT.
    //
    // They used to set a `column_aware_requested` flag and nothing else,
    // because this writer had no column-bearing step encoder. It has one.
    // Each override therefore FORWARDS to the inherent method of the same
    // name, which arms the position space, the step policy and the `meta.dat`
    // capability bits.
    //
    // DELETING THEM WOULD NOT BE A SIMPLIFICATION, IT WOULD BE A SILENT
    // NO-OP. `TraceWriter`'s defaults for this family are empty bodies, so a
    // caller that reaches the writer through the trait — `ct_writer_open` in
    // `aztec-avm-runtime/ct-writer` does exactly that — would get columns
    // accepted, ignored, and `dropped_column_awareness()` answering `false`
    // because nobody recorded that anybody asked. That is the campaign's
    // silent-wrong-answer shape, so the forwarding is deliberate and is
    // covered by `the_trait_column_family_reaches_the_real_implementation`.
    // ---------------------------------------------------------------------
    fn enable_column_aware_steps(&mut self) {
        CtfsTraceWriter::enable_column_aware_steps(self);
    }

    fn enable_column_breakpoints_support(&mut self) {
        CtfsTraceWriter::enable_column_breakpoints_support(self);
    }

    fn enable_column_motions_support(&mut self) {
        CtfsTraceWriter::enable_column_motions_support(self);
    }

    fn write_delta_column(&mut self, column_delta: i64) {
        let _ = CtfsTraceWriter::register_column_step(self, column_delta);
    }

    fn register_path_with_line_lengths(
        &mut self,
        path: &Path,
        line_lengths: &[u32],
    ) -> Result<codetracer_trace_types::PathId, Box<dyn std::error::Error>> {
        Ok(CtfsTraceWriter::register_path_with_line_lengths(self, path, line_lengths))
    }

    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        // The legacy CBOR mode streams through zeekstd (libzstd, C), which
        // needs a libc and so does not exist on `wasm32-unknown-unknown`.
        // Refuse it up front rather than letting the stub encoder fail deeper
        // in. `wasm32-wasip1` has wasi-libc and is not gated here.
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if self.serialization_format == EventSerializationFormat::Cbor {
            return Err("the CTFS `Cbor` serialization mode is not available on wasm32-unknown-unknown, which has no libc \
                        for zeekstd to link against; use `SplitBinary` (the default), or build for wasm32-wasip1, where \
                        zeekstd does link"
                .into());
        }

        let mut writer = match self.output {
            // Create .ct file at path (replace any existing extension)
            CtfsOutput::File => CtfsWriter::create(&path.with_extension("ct"), 4096, 31)?,
            CtfsOutput::Memory => CtfsWriter::create_in_memory(4096, 31, codetracer_ctfs::CompressionMethod::None)?,
        };
        let events_handle = writer.add_file("events.log")?;
        self.container_bytes = None;
        self.ctfs_writer = Some(writer);
        self.events_handle = Some(events_handle);

        match self.serialization_format {
            EventSerializationFormat::Cbor => {
                // Initialize the Zstd encoder writing to a shared in-memory buffer.
                let sink = SharedBuffer::new();
                let encoder = EncodeOptions::new()
                    .frame_size_policy(FrameSizePolicy::Uncompressed(self.flush_threshold as u32))
                    .compression_level(3)
                    .into_encoder(sink.clone())?;
                self.encoder = Some(encoder);
                self.compressed_sink = Some(sink);
            }
            EventSerializationFormat::SplitBinary => {
                // SplitBinary mode: event_buffer/event_sizes/event_geids are already initialized.
                self.event_buffer.clear();
                self.event_sizes.clear();
                self.event_geids.clear();
                self.total_events = 0;
                self.unflushed_events = 0;
            }
        }

        self.unflushed_bytes = 0;
        self.flush_count = 0;
        self.header_written = false;

        // Column-aware mode: arm the Nim-parity position space, step policy and
        // exec-stream encoder, and leave `StepStreamBuilder` disarmed so only
        // one of the two owns `steps.dat`.
        self.position_space = PositionSpace::new(self.column_aware_active);
        self.step_encoder = StepEncoder::new();
        self.exec_encoder = if self.column_aware_active && self.emit_step_stream {
            Some(ExecStreamEncoder::new(self.steps_chunk_size, EXEC_COMPRESSION_LEVEL))
        } else {
            None
        };
        self.pending_line_lengths = None;

        // M17a: arm the call-stream builder when the dedicated stream is enabled.
        self.call_stream_builder = if self.emit_call_stream { Some(CallStreamBuilder::new()) } else { None };
        // M23a: arm the step-stream builder when the dedicated stream is enabled.
        self.step_stream_builder = if self.emit_step_stream && !self.column_aware_active {
            Some(StepStreamBuilder::new())
        } else {
            None
        };
        // M23b: arm the value-stream builder when the dedicated stream is enabled.
        self.value_stream_builder = if self.emit_value_stream { Some(ValueStreamBuilder::new()) } else { None };
        // M23c: arm the I/O event-stream builder when the dedicated stream is enabled.
        self.io_event_stream_builder = if self.emit_io_event_stream {
            Some(IoEventStreamBuilder::new())
        } else {
            None
        };
        // M23d: arm the interning-tables builder when the tables are enabled.
        // In column-aware mode its `paths.dat` records switch to spec Layout A.
        self.interning_tables_builder = if self.emit_interning_tables {
            let mut builder = InterningTablesBuilder::new();
            builder.set_column_aware(self.column_aware_active);
            Some(builder)
        } else {
            None
        };

        Ok(())
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match self.serialization_format {
            EventSerializationFormat::Cbor => {
                // Finish the encoder: flushes any remaining data and writes the seek table.
                if let Some(encoder) = self.encoder.take() {
                    encoder.finish()?;
                }

                // Drain any remaining compressed data from the sink.
                if let Some(ref sink) = self.compressed_sink.take() {
                    let remaining = sink.drain();
                    if !remaining.is_empty() {
                        self.ensure_header_written()?;
                        if let (Some(writer), Some(handle)) = (&mut self.ctfs_writer, self.events_handle) {
                            writer.write(handle, &remaining)?;
                        }
                    }
                }

                // Count final flush if there was unflushed data.
                if self.unflushed_bytes > 0 {
                    self.flush_count += 1;
                    self.unflushed_bytes = 0;
                }
            }
            EventSerializationFormat::SplitBinary => {
                // Flush any remaining buffered events as a final chunk.
                self.flush_chunk()?;
            }
        }

        if let Some(ref mut writer) = self.ctfs_writer {
            // Write the format marker file.
            let format_name = match self.serialization_format {
                EventSerializationFormat::SplitBinary => b"split-binary" as &[u8],
                EventSerializationFormat::Cbor => b"cbor" as &[u8],
            };
            let format_handle = writer.add_file("events.fmt")?;
            writer.write(format_handle, format_name)?;

            // Write metadata as meta.json.
            // M-REC-1: mint a UUIDv7 recording_id for this trace.
            // Recorders that need to pin a pre-existing id (the
            // import flow, M-REC-7) should construct TraceMetadata
            // directly with their own id and then write it out.
            let trace_metadata = match &self.recording_id {
                Some(id) => codetracer_trace_types::TraceMetadata::with_recording_id(
                    id.clone(),
                    self.base.program.clone(),
                    self.base.args.clone(),
                    self.base.workdir.clone(),
                ),
                None => codetracer_trace_types::TraceMetadata::new(self.base.program.clone(), self.base.args.clone(), self.base.workdir.clone()),
            };
            let meta_json = serde_json::to_string(&trace_metadata)?;
            let meta_handle = writer.add_file("meta.json")?;
            writer.write(meta_handle, meta_json.as_bytes())?;

            // Write paths as paths.json
            let paths_json = serde_json::to_string(&self.base.path_list)?;
            let paths_handle = writer.add_file("paths.json")?;
            writer.write(paths_handle, paths_json.as_bytes())?;

            // M17a/M23a: emit the dedicated call stream and/or the dedicated
            // execution (step) stream, each with its companion seekable index,
            // plus a single meta.dat carrying the corresponding capability
            // flags. This is ADDITIVE: events.log / events.fmt / meta.json /
            // paths.json above are unchanged, and a reader that does not know a
            // flag simply ignores the extra dat/idx files and meta.dat.
            let mut stream_flags: u16 = 0;

            // M17a: the dedicated call stream + companion index.
            if self.emit_call_stream {
                let records = self.call_stream_builder.take().map(|b| b.finish()).unwrap_or_default();
                let encoded = encode_call_stream(&records, self.calls_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

                let calls_handle = writer.add_file("calls.dat")?;
                writer.write(calls_handle, &encoded.dat)?;
                let calls_idx_handle = writer.add_file("calls.idx")?;
                writer.write(calls_idx_handle, &encoded.idx)?;
                stream_flags |= FLAG_HAS_CALL_STREAM;
            }

            // M23a: the dedicated execution (step) stream + companion index.
            //
            // Two producers, one file. In column-aware mode the Nim-parity
            // `ExecStreamEncoder` has been streaming records since `begin`, so
            // its buffers are simply flushed here; in line-only mode the
            // records are encoded now from `StepStreamBuilder`. The `.idx`
            // framing is identical either way.
            if self.emit_step_stream {
                let (dat, idx) = if let Some(encoder) = self.exec_encoder.take() {
                    let encoded = encoder.finish().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
                    (encoded.dat, encoded.idx)
                } else {
                    let stream = self
                        .step_stream_builder
                        .take()
                        .map(|b| b.finish())
                        .unwrap_or_else(|| StepStreamBuilder::new().finish());
                    let encoded = encode_step_stream(&stream, self.steps_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL)
                        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
                    (encoded.dat, encoded.idx)
                };

                let steps_handle = writer.add_file("steps.dat")?;
                writer.write(steps_handle, &dat)?;
                let steps_idx_handle = writer.add_file("steps.idx")?;
                writer.write(steps_idx_handle, &idx)?;
                stream_flags |= FLAG_HAS_STEP_STREAM;
            }

            // M23b: the dedicated parallel value stream + companion index.
            // Parallel-indexed to the step stream (value record N ↔ step N).
            if self.emit_value_stream {
                let records = self.value_stream_builder.take().map(|b| b.finish()).unwrap_or_default();
                let encoded = encode_value_stream(&records, self.values_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

                let values_handle = writer.add_file("values.dat")?;
                writer.write(values_handle, &encoded.dat)?;
                let values_idx_handle = writer.add_file("values.idx")?;
                writer.write(values_idx_handle, &encoded.idx)?;
                stream_flags |= FLAG_HAS_VALUE_STREAM;
            }

            // M23c: the dedicated I/O event stream + companion index. Holds the
            // EventLogKind-tagged I/O / log events split out of events.log; each
            // record carries kind / step_id (cross-ref to the execution stream)
            // / metadata / content. NOTE: this `events.dat` is DISTINCT from the
            // legacy `events.log` written above — do not collide the names.
            if self.emit_io_event_stream {
                let records = self.io_event_stream_builder.take().map(|b| b.finish()).unwrap_or_default();
                let encoded = encode_io_event_stream(&records, self.events_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

                let events_handle = writer.add_file("events.dat")?;
                writer.write(events_handle, &encoded.dat)?;
                let events_idx_handle = writer.add_file("events.idx")?;
                writer.write(events_idx_handle, &encoded.idx)?;
                stream_flags |= FLAG_HAS_IO_EVENT_STREAM;
            }

            // M23d: the binary varint interning tables. Each is a Variable-Size
            // Record Table — a `.dat` of serialized records plus a `.off` u64-LE
            // offset index — built from the SAME Path/Function/Type/VariableName
            // interning that feeds events.log / paths.json, so the i-th record in
            // each `.dat` resolves the id the event streams reference. ADDITIVE:
            // the existing paths.json interning above is untouched.
            if self.emit_interning_tables {
                let tables = self
                    .interning_tables_builder
                    .take()
                    .map(|b| b.finish())
                    .unwrap_or_else(|| InterningTablesBuilder::new().finish());

                let paths_dat_handle = writer.add_file("paths.dat")?;
                writer.write(paths_dat_handle, &tables.paths_dat)?;
                let paths_off_handle = writer.add_file("paths.off")?;
                writer.write(paths_off_handle, &tables.paths_off)?;

                let funcs_dat_handle = writer.add_file("funcs.dat")?;
                writer.write(funcs_dat_handle, &tables.funcs_dat)?;
                let funcs_off_handle = writer.add_file("funcs.off")?;
                writer.write(funcs_off_handle, &tables.funcs_off)?;

                let types_dat_handle = writer.add_file("types.dat")?;
                writer.write(types_dat_handle, &tables.types_dat)?;
                let types_off_handle = writer.add_file("types.off")?;
                writer.write(types_off_handle, &tables.types_off)?;

                let varnames_dat_handle = writer.add_file("varnames.dat")?;
                writer.write(varnames_dat_handle, &tables.varnames_dat)?;
                let varnames_off_handle = writer.add_file("varnames.off")?;
                writer.write(varnames_off_handle, &tables.varnames_off)?;

                stream_flags |= FLAG_HAS_INTERNING_TABLES;
            }

            // The column bits. Bit 4 says the wire format changed — `paths.dat`
            // is Layout A and `steps.dat` positions address (line, column) —
            // and a reader that does not know it is required by spec to refuse
            // the container rather than misdecode it. Bits 6 and 7 are
            // capability claims about the recorder, and are meaningless without
            // bit 4, so they are only ever set alongside it.
            if self.column_aware_active {
                stream_flags |= FLAG_HAS_COLUMN_AWARE_STEPS;
                if self.column_breakpoints_requested {
                    stream_flags |= FLAG_SUPPORTS_COLUMN_BREAKPOINTS;
                }
                if self.column_motions_requested {
                    stream_flags |= FLAG_SUPPORTS_COLUMN_MOTIONS;
                }
            }

            // Stamp meta.dat with the combined stream-capability flags. The
            // recording_id mirrors the meta.json minted above so the two
            // metadata files agree on the recording identity. Only written when
            // at least one dedicated stream is present, so a flags-off bundle is
            // byte-for-byte the legacy container.
            if stream_flags != 0 {
                let meta_dat = encode_meta_dat(
                    &trace_metadata.recording_id,
                    &self.base.program,
                    &self.base.args,
                    &self.base.workdir.to_string_lossy(),
                    "",
                    &self.base.path_list.iter().map(|p| p.to_string_lossy().into_owned()).collect::<Vec<_>>(),
                    stream_flags,
                );
                let meta_dat_handle = writer.add_file("meta.dat")?;
                writer.write(meta_dat_handle, &meta_dat)?;
            }
        }

        // Close the CTFS container (takes ownership)
        if let Some(writer) = self.ctfs_writer.take() {
            match self.output {
                CtfsOutput::File => writer.close()?,
                CtfsOutput::Memory => self.container_bytes = Some(writer.finish_to_bytes()?),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codetracer_trace_types::*;

    /// Create a simple step event for testing.
    fn make_step_event(line: i64) -> TraceLowLevelEvent {
        TraceLowLevelEvent::Step(StepRecord {
            path_id: PathId(0),
            line: Line(line),
        })
    }

    #[test]
    fn test_ctfs_cbor_streaming_flushes_incrementally() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace");

        // Use CBOR mode with a small flush threshold (1 KiB) to force multiple flushes.
        let mut writer = CtfsTraceWriter::with_options("test", &[], EventSerializationFormat::Cbor, 1024, DEFAULT_CHUNK_SIZE);
        writer.begin_writing_trace_events(&path).unwrap();

        // Register a path event first (so Step events reference a valid path).
        AbstractTraceWriter::add_event(&mut writer, TraceLowLevelEvent::Path(std::path::PathBuf::from("/test/file.rs")));

        // Write 200 step events -- each serializes to ~10-15 bytes of CBOR,
        // so 200 events should be ~2-3 KiB, triggering at least 1-2 flushes.
        let num_events = 200;
        for i in 0..num_events {
            AbstractTraceWriter::add_event(&mut writer, make_step_event(i + 1));
        }

        // Verify that at least one intermediate flush occurred.
        assert!(
            writer.flush_count() >= 1,
            "Expected at least 1 flush with 1KB threshold over 200 events, got {}",
            writer.flush_count()
        );
        let flush_count_before_finish = writer.flush_count();

        writer.finish_writing_trace_events().unwrap();

        // Now read back all events and verify correctness.
        let ct_path = path.with_extension("ct");
        let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
        let events = reader.load_trace_events(&ct_path).unwrap();

        // Count step events.
        let step_events: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                TraceLowLevelEvent::Step(s) => Some(s),
                _ => None,
            })
            .collect();

        assert_eq!(
            step_events.len(),
            num_events as usize,
            "Expected {} step events, got {}",
            num_events,
            step_events.len()
        );

        // Verify step line numbers.
        for (i, step) in step_events.iter().enumerate() {
            assert_eq!(step.line, Line(i as i64 + 1));
        }

        eprintln!(
            "CBOR streaming test passed: {} flushes before finish, {} total events round-tripped",
            flush_count_before_finish,
            step_events.len()
        );
    }

    #[test]
    fn test_ctfs_split_binary_flushes_incrementally() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace");

        // Use SplitBinary mode with a small chunk size to force multiple flushes.
        let mut writer = CtfsTraceWriter::with_options(
            "test",
            &[],
            EventSerializationFormat::SplitBinary,
            DEFAULT_FLUSH_THRESHOLD,
            50, // 50 events per chunk
        );
        writer.begin_writing_trace_events(&path).unwrap();

        AbstractTraceWriter::add_event(&mut writer, TraceLowLevelEvent::Path(std::path::PathBuf::from("/test/file.rs")));

        let num_events = 200;
        for i in 0..num_events {
            AbstractTraceWriter::add_event(&mut writer, make_step_event(i + 1));
        }

        // With 201 events and chunk_size=50, expect 4 flushes (50+50+50+51 remaining)
        assert!(
            writer.flush_count() >= 3,
            "Expected at least 3 chunk flushes with chunk_size=50 over 201 events, got {}",
            writer.flush_count()
        );

        writer.finish_writing_trace_events().unwrap();

        // Read back and verify.
        let ct_path = path.with_extension("ct");
        let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
        let events = reader.load_trace_events(&ct_path).unwrap();

        let step_events: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                TraceLowLevelEvent::Step(s) => Some(s),
                _ => None,
            })
            .collect();

        assert_eq!(
            step_events.len(),
            num_events as usize,
            "Expected {} step events, got {}",
            num_events,
            step_events.len()
        );

        for (i, step) in step_events.iter().enumerate() {
            assert_eq!(step.line, Line(i as i64 + 1));
        }
    }
}
