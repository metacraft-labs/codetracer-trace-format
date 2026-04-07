use std::io::{BufRead, BufReader, Cursor};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_format_cbor_zstd::HEADERV1;
use codetracer_trace_types::TraceLowLevelEvent;
use zeekstd::Decoder;

fn is_at_eof<R: BufRead>(reader: &mut R) -> std::io::Result<bool> {
    let buffer = reader.fill_buf()?;
    Ok(buffer.is_empty())
}

/// Read trace events from a CTFS container's `events.log` file.
///
/// The events data is expected to be HEADERV1 ++ Zstd(CBOR stream),
/// matching the Binary format encoding.
pub fn read_trace_from_ctfs(path: &std::path::Path) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    let mut reader = CtfsReader::open(path)?;
    let events_data = reader.read_file("events.log")?;

    // Verify HEADERV1 prefix
    if events_data.len() < HEADERV1.len() || &events_data[..HEADERV1.len()] != HEADERV1 {
        return Err("CTFS events.log: invalid or missing CBOR+Zstd header".into());
    }

    // Skip the header, decode the Zstd stream
    let compressed = &events_data[HEADERV1.len()..];
    let cursor = Cursor::new(compressed);
    let decoder = Decoder::new(cursor)?;
    let mut buf_reader = BufReader::new(decoder);

    let mut result: Vec<TraceLowLevelEvent> = Vec::new();
    while !is_at_eof(&mut buf_reader)? {
        let obj = cbor4ii::serde::from_reader::<TraceLowLevelEvent, _>(&mut buf_reader)?;
        result.push(obj);
    }

    Ok(result)
}
