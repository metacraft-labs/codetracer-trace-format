use std::io::{BufRead, BufReader, Cursor};

use codetracer_ctfs::{ChunkedReader, CtfsReader};
use codetracer_trace_format_cbor_zstd::HEADERV1;
use codetracer_trace_types::TraceLowLevelEvent;
use codetracer_trace_writer::ctfs_writer::EventSerializationFormat;
use zeekstd::Decoder;

fn is_at_eof<R: BufRead>(reader: &mut R) -> std::io::Result<bool> {
    let buffer = reader.fill_buf()?;
    Ok(buffer.is_empty())
}

/// Detect the serialization format from the CTFS container.
///
/// Reads the `events.fmt` file if present. Falls back to `Cbor` for
/// containers written before the format marker was introduced.
fn detect_format(reader: &mut CtfsReader) -> EventSerializationFormat {
    if let Ok(data) = reader.read_file("events.fmt") {
        match data.as_slice() {
            b"split-binary" => EventSerializationFormat::SplitBinary,
            _ => EventSerializationFormat::Cbor,
        }
    } else {
        // Legacy: no format file means CBOR
        EventSerializationFormat::Cbor
    }
}

/// Deserialize events from decompressed data using CBOR.
fn deserialize_cbor(data: &[u8]) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(data);
    let mut buf_reader = BufReader::new(&mut cursor);
    let mut result: Vec<TraceLowLevelEvent> = Vec::new();
    while !is_at_eof(&mut buf_reader)? {
        let obj = cbor4ii::serde::from_reader::<TraceLowLevelEvent, _>(&mut buf_reader)?;
        result.push(obj);
    }
    Ok(result)
}

/// Read trace events from a CTFS container's `events.log` file.
///
/// Supports both legacy CBOR+Zstd (zeekstd) encoding and the newer
/// split-binary+chunked-Zstd encoding. The format is detected automatically
/// from the `events.fmt` marker file.
pub fn read_trace_from_ctfs(
    path: &std::path::Path,
) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    let mut reader = CtfsReader::open(path)?;
    let format = detect_format(&mut reader);
    let events_data = reader.read_file("events.log")?;

    // Verify HEADERV1 prefix
    if events_data.len() < HEADERV1.len() || &events_data[..HEADERV1.len()] != HEADERV1 {
        return Err("CTFS events.log: invalid or missing CBOR+Zstd header".into());
    }

    // Skip the header
    let data = &events_data[HEADERV1.len()..];

    match format {
        EventSerializationFormat::SplitBinary => {
            // SplitBinary uses chunked Zstd -- decompress all chunks then decode.
            let decompressed = ChunkedReader::decompress_all(data)?;
            Ok(codetracer_trace_writer::split_binary::decode_events(
                &decompressed,
            ))
        }
        EventSerializationFormat::Cbor => {
            // Try chunked format first (for future CBOR+chunked combinations),
            // then fall back to zeekstd streaming.
            let headers = ChunkedReader::scan_headers(data);
            if !headers.is_empty() {
                let decompressed = ChunkedReader::decompress_all(data)?;
                deserialize_cbor(&decompressed)
            } else {
                // Legacy zeekstd streaming format.
                let cursor = Cursor::new(data);
                let decoder = Decoder::new(cursor)?;
                let mut buf_reader = BufReader::new(decoder);

                let mut result: Vec<TraceLowLevelEvent> = Vec::new();
                while !is_at_eof(&mut buf_reader)? {
                    let obj = cbor4ii::serde::from_reader::<TraceLowLevelEvent, _>(
                        &mut buf_reader,
                    )?;
                    result.push(obj);
                }
                Ok(result)
            }
        }
    }
}

/// Seek to a specific event range within a CTFS container.
///
/// Decompresses only the chunk containing `target_event` and returns
/// `count` events starting from that position. Only supported for
/// SplitBinary format with chunked Zstd; for CBOR, falls back to
/// decompressing the entire target chunk.
pub fn seek_events_in_ctfs(
    path: &std::path::Path,
    target_event: usize,
    count: usize,
) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    let mut reader = CtfsReader::open(path)?;
    let format = detect_format(&mut reader);
    let events_data = reader.read_file("events.log")?;

    if events_data.len() < HEADERV1.len() || &events_data[..HEADERV1.len()] != HEADERV1 {
        return Err("CTFS events.log: invalid or missing header".into());
    }

    let data = &events_data[HEADERV1.len()..];

    // Seek to the chunk containing target_event.
    let (chunk_data, header) = ChunkedReader::seek_to_geid(data, target_event as u64)?;
    let offset_in_chunk = target_event - header.first_geid as usize;

    match format {
        EventSerializationFormat::SplitBinary => {
            // Lazy scanning within the chunk -- build offset index then decode
            // only the requested range.
            let offsets =
                codetracer_trace_writer::split_binary::scan_event_offsets(&chunk_data);
            let end = (offset_in_chunk + count).min(offsets.len());
            let mut events = Vec::with_capacity(end - offset_in_chunk);
            for i in offset_in_chunk..end {
                let start = offsets[i] as usize;
                let event_end = if i + 1 < offsets.len() {
                    offsets[i + 1] as usize
                } else {
                    chunk_data.len()
                };
                let mut cursor = Cursor::new(&chunk_data[start..event_end]);
                events.push(
                    codetracer_trace_writer::split_binary::decode_event(&mut cursor)?,
                );
            }
            Ok(events)
        }
        EventSerializationFormat::Cbor => {
            // For CBOR, must deserialize all events in the chunk then slice.
            let all_events = deserialize_cbor(&chunk_data)?;
            let end = (offset_in_chunk + count).min(all_events.len());
            Ok(all_events[offset_in_chunk..end].to_vec())
        }
    }
}
