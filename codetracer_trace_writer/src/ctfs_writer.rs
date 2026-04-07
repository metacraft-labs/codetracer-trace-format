use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use codetracer_ctfs::CtfsWriter;
use codetracer_trace_format_cbor_zstd::HEADERV1;
use zeekstd::{EncodeOptions, Encoder, FrameSizePolicy};

use crate::{
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
    trace_writer::TraceWriter,
};
use codetracer_trace_types::TraceLowLevelEvent;

/// Default flush threshold: 64 KiB of uncompressed CBOR data triggers a flush.
const DEFAULT_FLUSH_THRESHOLD: usize = 64 * 1024;

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
/// - `events.log` — CBOR+Zstd encoded events (same encoding as the Binary format)
/// - `meta.json`  — trace metadata (program, args, workdir)
/// - `paths.json` — registered source paths
///
/// Events are CBOR-serialized and streamed to the CTFS container incrementally.
/// When the internal buffer exceeds `flush_threshold` bytes of uncompressed CBOR
/// data, the writer ends the current Zstd frame and flushes the compressed output
/// to the CTFS file.  Each flush produces a complete Zstd frame that a concurrent
/// reader can decompress independently using `zeekstd::Decoder`.
pub struct CtfsTraceWriter {
    base: AbstractTraceWriterData,
    ctfs_writer: Option<CtfsWriter>,
    events_handle: Option<codetracer_ctfs::FileHandle>,

    /// Zstd encoder that compresses CBOR data into `compressed_sink`.
    encoder: Option<Encoder<'static, SharedBuffer>>,
    /// Shared buffer that the encoder writes compressed data into.
    compressed_sink: Option<SharedBuffer>,
    /// Tracks uncompressed CBOR bytes written since the last flush.
    unflushed_bytes: usize,
    /// Flush when uncompressed bytes exceed this threshold (default 64 KiB).
    flush_threshold: usize,
    /// Number of flushes performed so far (visible for testing).
    flush_count: usize,
    /// Whether HEADERV1 has been written to the CTFS file.
    header_written: bool,
}

impl CtfsTraceWriter {
    /// Create a new CTFS trace writer for the given program and arguments.
    pub fn new(program: &str, args: &[String]) -> Self {
        Self::with_flush_threshold(program, args, DEFAULT_FLUSH_THRESHOLD)
    }

    /// Create a new CTFS trace writer with a custom flush threshold.
    pub fn with_flush_threshold(program: &str, args: &[String], flush_threshold: usize) -> Self {
        CtfsTraceWriter {
            base: AbstractTraceWriterData::new(program, args),
            ctfs_writer: None,
            events_handle: None,
            encoder: None,
            compressed_sink: None,
            unflushed_bytes: 0,
            flush_threshold,
            flush_count: 0,
            header_written: false,
        }
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

    /// Flush the current Zstd frame to the CTFS container.
    ///
    /// Ends the current Zstd frame (producing a complete, independently
    /// decompressible frame), drains the compressed output buffer, and
    /// writes it to the CTFS `events.log` file.
    fn flush_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
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
                }
            }
        }

        self.unflushed_bytes = 0;
        self.flush_count += 1;
        Ok(())
    }

    /// Returns the number of flushes performed so far.
    pub fn flush_count(&self) -> usize {
        self.flush_count
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
        let buf: Vec<u8> = Vec::new();
        let cbor_bytes = cbor4ii::serde::to_vec(buf, &event).unwrap();

        if let Some(ref mut encoder) = self.encoder {
            encoder.write_all(&cbor_bytes).unwrap();
        }
        self.unflushed_bytes += cbor_bytes.len();

        // Auto-flush when uncompressed data exceeds threshold.
        if self.unflushed_bytes >= self.flush_threshold {
            let _ = self.flush_events();
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

        // Initialize the Zstd encoder writing to a shared in-memory buffer.
        let sink = SharedBuffer::new();
        let encoder = EncodeOptions::new()
            .frame_size_policy(FrameSizePolicy::Uncompressed(self.flush_threshold as u32))
            .compression_level(3)
            .into_encoder(sink.clone())?;
        self.encoder = Some(encoder);
        self.compressed_sink = Some(sink);

        self.unflushed_bytes = 0;
        self.flush_count = 0;
        self.header_written = false;

        Ok(())
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Finish the encoder: flushes any remaining data and writes the seek table.
        // This consumes the encoder.
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

        if let Some(ref mut writer) = self.ctfs_writer {
            // Write metadata as meta.json
            let trace_metadata = codetracer_trace_types::TraceMetadata {
                program: self.base.program.clone(),
                args: self.base.args.clone(),
                workdir: self.base.workdir.clone(),
            };
            let meta_json = serde_json::to_string(&trace_metadata)?;
            let meta_handle = writer.add_file("meta.json")?;
            writer.write(meta_handle, meta_json.as_bytes())?;

            // Write paths as paths.json
            let paths_json = serde_json::to_string(&self.base.path_list)?;
            let paths_handle = writer.add_file("paths.json")?;
            writer.write(paths_handle, paths_json.as_bytes())?;
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
    fn test_ctfs_streaming_flushes_incrementally() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace");

        // Use a small flush threshold (1 KiB) to force multiple flushes.
        let mut writer = CtfsTraceWriter::with_flush_threshold("test", &[], 1024);
        writer.begin_writing_trace_events(&path).unwrap();

        // Register a path event first (so Step events reference a valid path).
        AbstractTraceWriter::add_event(
            &mut writer,
            TraceLowLevelEvent::Path(std::path::PathBuf::from("/test/file.rs")),
        );

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
        let mut reader = codetracer_trace_reader::create_trace_reader(
            codetracer_trace_reader::TraceEventsFileFormat::Ctfs,
        );
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
            "Streaming test passed: {} flushes before finish, {} total events round-tripped",
            flush_count_before_finish,
            step_events.len()
        );
    }
}
