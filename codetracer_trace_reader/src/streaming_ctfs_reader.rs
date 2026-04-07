//! Streaming CTFS reader that follows a growing `.ct` file during live recording.
//!
//! The writer ([`codetracer_trace_writer::ctfs_writer::CtfsTraceWriter`]) flushes
//! events incrementally — every N bytes of uncompressed CBOR data it ends the
//! current Zstd frame and writes the compressed output to the CTFS container.
//! This reader polls the container for new data and decompresses complete frames
//! as they appear.
//!
//! CBOR events may span Zstd frame boundaries (the encoder does not align
//! frames to event boundaries), so the reader concatenates decompressed data
//! from multiple frames and tracks leftover bytes across polls.
//!
//! # Usage
//!
//! ```ignore
//! let mut reader = StreamingCtfsReader::open(path)?;
//! loop {
//!     let new_events = reader.poll_new_events()?;
//!     if !new_events.is_empty() {
//!         process(new_events);
//!     }
//!     if reader.is_finalized() { break; }
//!     std::thread::sleep(Duration::from_millis(50));
//! }
//! ```

use std::io::Cursor;
use std::path::{Path, PathBuf};

use codetracer_ctfs::ConcurrentCtfsReader;
use codetracer_trace_format_cbor_zstd::HEADERV1;
use codetracer_trace_types::TraceLowLevelEvent;

/// Zstd standard frame magic number (little-endian bytes).
const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// A reader that follows a growing `.ct` CTFS file during live recording.
pub struct StreamingCtfsReader {
    path: PathBuf,
    /// Byte offset in events.log (compressed) we have read up to.
    last_read_offset: u64,
    /// Last known size of events.log in the CTFS container.
    last_file_size: u64,
    /// Total events deserialized so far.
    events_read: usize,
    /// Whether we have verified the HEADERV1 prefix.
    header_verified: bool,
    /// Whether recording has finished (meta.json present with non-zero size).
    finalized: bool,
    /// Leftover decompressed bytes from the previous poll. A partial CBOR
    /// event may sit at the end of one batch of frames and be completed by
    /// the next batch.
    leftover: Vec<u8>,
}

/// Deserialize as many complete CBOR events as possible from `data`.
/// Returns the events and the number of bytes consumed.
fn deserialize_cbor_events(data: &[u8]) -> (Vec<TraceLowLevelEvent>, usize) {
    let mut events = Vec::new();
    let mut consumed = 0usize;

    while consumed < data.len() {
        let remaining = &data[consumed..];
        // cbor4ii::serde::from_reader reads exactly one CBOR value.
        // We wrap the remaining slice in a Cursor so we can track position.
        let mut cursor = Cursor::new(remaining);
        match cbor4ii::serde::from_reader::<TraceLowLevelEvent, _>(&mut cursor) {
            Ok(event) => {
                let bytes_used = cursor.position() as usize;
                events.push(event);
                consumed += bytes_used;
            }
            Err(_) => {
                // Incomplete CBOR — stop here.
                break;
            }
        }
    }

    (events, consumed)
}

impl StreamingCtfsReader {
    /// Open a CTFS container for streaming reads.
    ///
    /// Verifies the CTFS magic (but does not read events yet).
    pub fn open(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        // Verify the file exists and is a valid CTFS container.
        let _reader = ConcurrentCtfsReader::open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            last_read_offset: 0,
            last_file_size: 0,
            events_read: 0,
            header_verified: false,
            finalized: false,
            leftover: Vec::new(),
        })
    }

    /// Poll for new events that have been flushed since the last call.
    ///
    /// Returns an empty vec if no new complete Zstd frames are available yet.
    /// Handles partial frames and partial CBOR events gracefully.
    pub fn poll_new_events(
        &mut self,
    ) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
        let ctfs = ConcurrentCtfsReader::open(&self.path)?;

        let current_size = match ctfs.file_size("events.log") {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        if current_size == 0 {
            return Ok(Vec::new());
        }

        // Verify HEADERV1 on first access.
        if !self.header_verified {
            if current_size < HEADERV1.len() as u64 {
                return Ok(Vec::new());
            }
            let mut header_buf = [0u8; 8];
            ctfs.read_at("events.log", 0, &mut header_buf)?;
            if header_buf[..HEADERV1.len()] != *HEADERV1 {
                return Err("CTFS events.log: invalid or missing CBOR+Zstd header".into());
            }
            self.header_verified = true;
            if self.last_read_offset == 0 {
                self.last_read_offset = HEADERV1.len() as u64;
            }
        }

        if current_size <= self.last_read_offset {
            self.last_file_size = current_size;
            return Ok(Vec::new());
        }

        // Read all new compressed bytes from events.log.
        let new_bytes_len = (current_size - self.last_read_offset) as usize;
        let mut new_bytes = vec![0u8; new_bytes_len];
        let bytes_read = ctfs.read_at("events.log", self.last_read_offset, &mut new_bytes)?;
        if bytes_read == 0 {
            return Ok(Vec::new());
        }
        new_bytes.truncate(bytes_read);

        // Decompress all complete Zstd frames, concatenating the output.
        let mut decompressed_new = Vec::new();
        let mut compressed_offset = 0usize;

        while compressed_offset < new_bytes.len() {
            let remaining = &new_bytes[compressed_offset..];

            match zstd_safe::find_frame_compressed_size(remaining) {
                Ok(frame_len) => {
                    let frame_data = &remaining[..frame_len];

                    // Only decompress standard Zstd frames (skip seek tables).
                    if frame_data.len() >= 4 && frame_data[0..4] == ZSTD_FRAME_MAGIC {
                        let decompressed = zstd::decode_all(Cursor::new(frame_data))?;
                        decompressed_new.extend_from_slice(&decompressed);
                    }

                    compressed_offset += frame_len;
                }
                Err(_) => {
                    // Incomplete frame — stop and wait for more data.
                    break;
                }
            }
        }

        // Advance the compressed read offset past all complete frames.
        if compressed_offset > 0 {
            self.last_read_offset += compressed_offset as u64;
        }
        self.last_file_size = current_size;

        // Combine leftover decompressed bytes from previous poll with new data.
        let combined = if !self.leftover.is_empty() {
            let mut combined = std::mem::take(&mut self.leftover);
            combined.extend_from_slice(&decompressed_new);
            combined
        } else {
            decompressed_new
        };

        if combined.is_empty() {
            return Ok(Vec::new());
        }

        // Deserialize complete CBOR events from the combined buffer.
        let (events, consumed) = deserialize_cbor_events(&combined);

        // Save any remaining bytes (partial CBOR event) for the next poll.
        if consumed < combined.len() {
            self.leftover = combined[consumed..].to_vec();
        } else {
            self.leftover.clear();
        }

        self.events_read += events.len();
        Ok(events)
    }

    /// Check whether the recording has been finalized.
    ///
    /// A finalized container has a `meta.json` file with non-zero size, which
    /// the writer creates in `finish_writing_trace_events()`.
    pub fn check_finalized(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        if self.finalized {
            return Ok(true);
        }

        let ctfs = ConcurrentCtfsReader::open(&self.path)?;

        if let Some(size) = ctfs.file_size("meta.json") {
            if size > 0 {
                self.finalized = true;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns true if recording is finished.
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Returns the total number of events read so far.
    pub fn events_read(&self) -> usize {
        self.events_read
    }
}
