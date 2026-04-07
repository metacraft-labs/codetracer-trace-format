//! Seekable trace reader using zeekstd's seeking capabilities.
//!
//! Unlike [`crate::cbor_zstd_reader::read_trace`] which reads all events into memory,
//! this module provides:
//! - [`read_events_at_offset`]: reads events from a specific decompressed byte range
//! - [`EventIterator`]: lazy iterator over events without full materialization

use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};

use codetracer_trace_format_cbor_zstd::HEADERV1;
use fscommon::StreamSlice;
use zeekstd::{DecodeOptions, Decoder};

use codetracer_trace_types::TraceLowLevelEvent;

fn is_at_eof<R: BufRead>(reader: &mut R) -> io::Result<bool> {
    let buffer = reader.fill_buf()?;
    Ok(buffer.is_empty())
}

/// Read events from a specific decompressed byte offset range.
///
/// Uses zeekstd's seeking capability to skip directly to the frame containing
/// `offset` instead of decompressing everything from the start.
///
/// # Arguments
/// - `input`: a seekable stream containing a CBOR+Zstd trace file (with 8-byte header)
/// - `offset`: decompressed byte offset to start reading from
/// - `limit`: decompressed byte offset to stop reading at
///
/// # Errors
/// Returns an error if the file header is invalid, seeking fails, or CBOR decoding fails.
pub fn read_events_at_offset(
    input: &mut (impl Read + Write + Seek),
    offset: u64,
    limit: u64,
) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    // Validate header
    let stream_len = input.seek(SeekFrom::End(0))?;
    input.seek(SeekFrom::Start(0))?;

    let mut header_buf = [0u8; 8];
    input.read_exact(&mut header_buf)?;
    if header_buf != HEADERV1 {
        return Err("Invalid file header (wrong file format or incompatible version)".into());
    }

    input.seek(SeekFrom::Start(0))?;
    let input2 = StreamSlice::new(input, 8, stream_len)?;

    let decoder = DecodeOptions::new(input2)
        .offset(offset)
        .offset_limit(limit)
        .into_decoder()?;

    let mut buf_reader = BufReader::new(decoder);
    let mut result = vec![];
    while !is_at_eof(&mut buf_reader)? {
        let obj = cbor4ii::serde::from_reader::<TraceLowLevelEvent, _>(&mut buf_reader)?;
        result.push(obj);
    }
    Ok(result)
}

/// Read all events using the seekable decoder (full decompression, no offset).
///
/// Functionally equivalent to [`crate::cbor_zstd_reader::read_trace`] but goes through
/// the `DecodeOptions` path so seekable features are available.
pub fn read_all_events(
    input: &mut (impl Read + Write + Seek),
) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    let stream_len = input.seek(SeekFrom::End(0))?;
    input.seek(SeekFrom::Start(0))?;

    let mut header_buf = [0u8; 8];
    input.read_exact(&mut header_buf)?;
    if header_buf != HEADERV1 {
        return Err("Invalid file header (wrong file format or incompatible version)".into());
    }

    input.seek(SeekFrom::Start(0))?;
    let input2 = StreamSlice::new(input, 8, stream_len)?;

    let decoder = DecodeOptions::new(input2).into_decoder()?;

    let mut buf_reader = BufReader::new(decoder);
    let mut result = vec![];
    while !is_at_eof(&mut buf_reader)? {
        let obj = cbor4ii::serde::from_reader::<TraceLowLevelEvent, _>(&mut buf_reader)?;
        result.push(obj);
    }
    Ok(result)
}

/// Iterator over trace events without full materialization.
///
/// Instead of collecting all events into a `Vec`, this iterator yields events
/// one at a time from the decompressed stream. Useful for processing large traces
/// where only a subset of events is needed.
pub struct EventIterator<R: Read> {
    reader: BufReader<R>,
    done: bool,
}

impl<R: Read> EventIterator<R> {
    /// Create a new `EventIterator` wrapping a reader.
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            done: false,
        }
    }
}

impl<R: Read> Iterator for EventIterator<R> {
    type Item = Result<TraceLowLevelEvent, Box<dyn std::error::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match is_at_eof(&mut self.reader) {
            Ok(true) => {
                self.done = true;
                None
            }
            Ok(false) => {
                match cbor4ii::serde::from_reader::<TraceLowLevelEvent, _>(&mut self.reader) {
                    Ok(event) => Some(Ok(event)),
                    Err(e) => {
                        self.done = true;
                        Some(Err(Box::new(e) as Box<dyn std::error::Error>))
                    }
                }
            }
            Err(e) => {
                self.done = true;
                Some(Err(Box::new(e) as Box<dyn std::error::Error>))
            }
        }
    }
}

/// Create an [`EventIterator`] over all events in a CBOR+Zstd trace file.
///
/// The returned iterator lazily decompresses and decodes events one at a time.
///
/// # Errors
/// Returns an error if the file header is invalid or the decoder cannot be created.
pub fn into_event_iter(
    input: &mut (impl Read + Write + Seek),
) -> Result<
    EventIterator<Decoder<'_, StreamSlice<&mut (impl Read + Write + Seek)>>>,
    Box<dyn std::error::Error>,
> {
    let stream_len = input.seek(SeekFrom::End(0))?;
    input.seek(SeekFrom::Start(0))?;

    let mut header_buf = [0u8; 8];
    input.read_exact(&mut header_buf)?;
    if header_buf != HEADERV1 {
        return Err("Invalid file header (wrong file format or incompatible version)".into());
    }

    input.seek(SeekFrom::Start(0))?;
    let input2 = StreamSlice::new(input, 8, stream_len)?;
    let decoder = DecodeOptions::new(input2).into_decoder()?;

    Ok(EventIterator::new(decoder))
}

/// Create an [`EventIterator`] over events in a specific decompressed byte range.
///
/// # Errors
/// Returns an error if the file header is invalid or the decoder cannot be created.
pub fn into_event_iter_at_offset(
    input: &mut (impl Read + Write + Seek),
    offset: u64,
    limit: u64,
) -> Result<
    EventIterator<Decoder<'_, StreamSlice<&mut (impl Read + Write + Seek)>>>,
    Box<dyn std::error::Error>,
> {
    let stream_len = input.seek(SeekFrom::End(0))?;
    input.seek(SeekFrom::Start(0))?;

    let mut header_buf = [0u8; 8];
    input.read_exact(&mut header_buf)?;
    if header_buf != HEADERV1 {
        return Err("Invalid file header (wrong file format or incompatible version)".into());
    }

    input.seek(SeekFrom::Start(0))?;
    let input2 = StreamSlice::new(input, 8, stream_len)?;
    let decoder = DecodeOptions::new(input2)
        .offset(offset)
        .offset_limit(limit)
        .into_decoder()?;

    Ok(EventIterator::new(decoder))
}
