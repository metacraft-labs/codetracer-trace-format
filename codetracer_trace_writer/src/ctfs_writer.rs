use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use codetracer_ctfs::{ChunkedWriter, CompressionMethod, CtfsWriter};
use codetracer_trace_format_cbor_zstd::HEADERV1;
use zeekstd::{EncodeOptions, Encoder, FrameSizePolicy};

use crate::{
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
    call_stream::{encode_call_stream, CallStreamBuilder, DEFAULT_CALLS_CHUNK_SIZE},
    meta_dat::{encode_meta_dat, FLAG_HAS_CALL_STREAM, FLAG_HAS_STEP_STREAM},
    step_stream::{encode_step_stream, StepStreamBuilder, DEFAULT_STEPS_CHUNK_SIZE},
    trace_writer::TraceWriter,
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

/// A trace writer that outputs a single `.ct` CTFS container file.
///
/// The container holds:
/// - `events.log` — encoded events (CBOR+Zstd or split-binary+chunked-Zstd)
/// - `events.fmt` — format marker ("cbor" or "split-binary")
/// - `meta.json`  — trace metadata (program, args, workdir)
/// - `paths.json` — registered source paths
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

    // --- M23a: dedicated execution (step) stream ---
    /// When set, the writer ALSO emits a dedicated `steps.dat` compact
    /// execution stream (plus its companion `steps.idx`), derived from the same
    /// Step/Call/Return/ThreadSwitch events that feed `events.log`, and sets the
    /// `has_step_stream` meta.dat flag. Additive and backward-compatible exactly
    /// like the call stream.
    emit_step_stream: bool,
    /// Builds the compact step records from the observed event sequence (present
    /// only while `emit_step_stream` is on and a trace is being written).
    step_stream_builder: Option<StepStreamBuilder>,
    /// Records-per-chunk for `steps.dat`.
    steps_chunk_size: usize,
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
            // M23a: the dedicated `steps.dat` execution stream is OPT-IN (off by
            // default), exactly like the M17a `calls.dat` split was before M20
            // flipped it on. This keeps every existing recorder's `.ct` output
            // byte-for-byte unchanged (no `steps.dat`/`steps.idx`, `has_step_stream`
            // clear) until the consumer migration (M22) is ready. Enable
            // explicitly with `with_step_stream(true)`.
            emit_step_stream: false,
            step_stream_builder: None,
            steps_chunk_size: DEFAULT_STEPS_CHUNK_SIZE,
        }
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

    /// Enable or disable the dedicated `steps.dat` execution stream (M23a).
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

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        // M17a: feed the dedicated call-stream builder from the SAME event
        // sequence that produces events.log, so calls.dat stays consistent.
        if let Some(ref mut builder) = self.call_stream_builder {
            builder.observe(&event);
        }
        // M23a: feed the dedicated step-stream builder from the SAME event
        // sequence that produces events.log, so steps.dat stays consistent.
        if let Some(ref mut builder) = self.step_stream_builder {
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
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        // Create .ct file at path (replace any existing extension)
        let ct_path = path.with_extension("ct");
        let mut writer = CtfsWriter::create(&ct_path, 4096, 31)?;
        let events_handle = writer.add_file("events.log")?;
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

        // M17a: arm the call-stream builder when the dedicated stream is enabled.
        self.call_stream_builder = if self.emit_call_stream { Some(CallStreamBuilder::new()) } else { None };
        // M23a: arm the step-stream builder when the dedicated stream is enabled.
        self.step_stream_builder = if self.emit_step_stream { Some(StepStreamBuilder::new()) } else { None };

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
            let trace_metadata =
                codetracer_trace_types::TraceMetadata::new(self.base.program.clone(), self.base.args.clone(), self.base.workdir.clone());
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
                let encoded = encode_call_stream(&records, self.calls_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL).map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

                let calls_handle = writer.add_file("calls.dat")?;
                writer.write(calls_handle, &encoded.dat)?;
                let calls_idx_handle = writer.add_file("calls.idx")?;
                writer.write(calls_idx_handle, &encoded.idx)?;
                stream_flags |= FLAG_HAS_CALL_STREAM;
            }

            // M23a: the dedicated execution (step) stream + companion index.
            if self.emit_step_stream {
                let stream = self.step_stream_builder.take().map(|b| b.finish()).unwrap_or_else(|| StepStreamBuilder::new().finish());
                let encoded = encode_step_stream(&stream, self.steps_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL).map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

                let steps_handle = writer.add_file("steps.dat")?;
                writer.write(steps_handle, &encoded.dat)?;
                let steps_idx_handle = writer.add_file("steps.idx")?;
                writer.write(steps_idx_handle, &encoded.idx)?;
                stream_flags |= FLAG_HAS_STEP_STREAM;
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
            writer.close()?;
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
