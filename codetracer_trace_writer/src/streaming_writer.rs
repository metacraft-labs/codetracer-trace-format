//! Streaming seekable trace writer using zeekstd with smaller frame sizes.
//!
//! This module provides [`StreamingTraceWriter`], which writes CBOR+Zstd traces
//! with 64 KiB uncompressed frame sizes (instead of the default 2 MiB) for
//! better seek granularity. It also builds an event offset index mapping each
//! event to its decompressed byte position, enabling random access via the
//! seekable reader.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use codetracer_trace_format_cbor_zstd::HEADERV1;
use codetracer_trace_types::TraceLowLevelEvent;
use zeekstd::{EncodeOptions, Encoder, FrameSizePolicy};

/// Default uncompressed frame size: 64 KiB for fine-grained seeking.
pub const DEFAULT_FRAME_SIZE: u32 = 64 * 1024;

/// An entry in the event offset index.
#[derive(Debug, Clone, Copy)]
pub struct EventOffset {
    /// Zero-based index of the event in the trace.
    pub event_index: usize,
    /// Decompressed byte offset where this event starts (relative to stream start,
    /// i.e. after the 8-byte file header).
    pub decompressed_offset: u64,
}

/// A streaming trace writer that uses smaller Zstd frames and tracks event offsets.
///
/// Unlike [`crate::cbor_zstd_writer::CborZstdTraceWriter`] which uses the default
/// 2 MiB frame size, this writer creates 64 KiB frames by default, allowing the
/// seekable reader to jump close to any event without decompressing much extra data.
pub struct StreamingTraceWriter<'a> {
    trace_events_path: Option<PathBuf>,
    encoder: Option<Encoder<'a, File>>,
    event_offsets: Vec<EventOffset>,
    current_decompressed_offset: u64,
    event_count: usize,
    frame_size: u32,
}

impl<'a> StreamingTraceWriter<'a> {
    /// Create a new streaming writer with the default 64 KiB frame size.
    pub fn new() -> Self {
        Self::with_frame_size(DEFAULT_FRAME_SIZE)
    }

    /// Create a new streaming writer with a custom uncompressed frame size.
    pub fn with_frame_size(frame_size: u32) -> Self {
        Self {
            trace_events_path: None,
            encoder: None,
            event_offsets: Vec::new(),
            current_decompressed_offset: 0,
            event_count: 0,
            frame_size,
        }
    }

    /// Begin writing trace events to the given path.
    ///
    /// Creates the file, writes the 8-byte header, and initializes the Zstd encoder
    /// with the configured frame size policy.
    pub fn begin(&mut self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let pb = path.to_path_buf();
        self.trace_events_path = Some(pb.clone());
        let mut file_output = File::create(pb)?;
        file_output.write_all(HEADERV1)?;

        let encoder = EncodeOptions::new()
            .frame_size_policy(FrameSizePolicy::Uncompressed(self.frame_size))
            .compression_level(3)
            .into_encoder(file_output)?;

        self.encoder = Some(encoder);
        Ok(())
    }

    /// Write a single event, tracking its decompressed byte offset.
    pub fn write_event(
        &mut self,
        event: &TraceLowLevelEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let buf: Vec<u8> = Vec::new();
        let serialized = cbor4ii::serde::to_vec(buf, event)?;

        self.event_offsets.push(EventOffset {
            event_index: self.event_count,
            decompressed_offset: self.current_decompressed_offset,
        });

        if let Some(enc) = &mut self.encoder {
            enc.write_all(&serialized)?;
        } else {
            return Err("Writer not initialized: call begin() first".into());
        }

        self.current_decompressed_offset += serialized.len() as u64;
        self.event_count += 1;
        Ok(())
    }

    /// Flush the current Zstd frame to disk.
    ///
    /// After this call, any completed frames are readable by a concurrent seekable
    /// reader (e.g. for live debugging). The encoder continues accepting data for
    /// subsequent frames.
    pub fn flush_frame(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(enc) = &mut self.encoder {
            enc.end_frame()?;
            Ok(())
        } else {
            Err("Writer not initialized: call begin() first".into())
        }
    }

    /// Finish writing: flush the last frame and append the seek table.
    ///
    /// Consumes the internal encoder. After this call, the file is a valid
    /// seekable Zstd stream.
    pub fn finish(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(enc) = self.encoder.take() {
            enc.finish()?;
            Ok(())
        } else {
            Err("Writer not initialized or already finished".into())
        }
    }

    /// Returns the event offset index built during writing.
    ///
    /// Each entry maps an event index to its decompressed byte offset. This can be
    /// used with [`crate::seekable_reader::read_events_at_offset`] (in the reader
    /// crate) to seek directly to a specific event.
    pub fn event_offsets(&self) -> &[EventOffset] {
        &self.event_offsets
    }

    /// Returns the total number of events written so far.
    pub fn event_count(&self) -> usize {
        self.event_count
    }

    /// Returns the current decompressed byte offset (the end of all written data).
    pub fn current_decompressed_offset(&self) -> u64 {
        self.current_decompressed_offset
    }

    /// Returns the path to the trace events file, if set.
    pub fn trace_events_path(&self) -> Option<&Path> {
        self.trace_events_path.as_deref()
    }
}

impl Default for StreamingTraceWriter<'_> {
    fn default() -> Self {
        Self::new()
    }
}
