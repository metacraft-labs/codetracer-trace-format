use std::io::{self, BufRead, BufReader, Read, Seek, Write};

use fscommon::StreamSlice;

use zeekstd::Decoder;

//use crate::{cbor_zstd_writer::HEADERV1};
use codetracer_trace_types::TraceLowLevelEvent;

/// The next 3 bytes are reserved/version info.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADERV1: &[u8] = &[
    0xC0, 0xDE, 0x72, 0xAC, 0xE2, // The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
    0x01, // Indicates version 1 of the file format
    0x00, 0x00,
]; // Reserved, must be zero in this version.

fn is_at_eof<R: BufRead>(reader: &mut R) -> io::Result<bool> {
    let buffer = reader.fill_buf()?;
    Ok(buffer.is_empty())
}

pub fn read_trace(input: &mut (impl Read + Write + Seek)) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    let end_pos = input.seek(io::SeekFrom::End(0))?;
    input.seek(io::SeekFrom::Start(0))?;

    let mut header_buf = [0; 8];
    let mut buf_reader = BufReader::new(&mut *input);
    buf_reader.read_exact(&mut header_buf)?;
    if header_buf != HEADERV1 {
        panic!("Invalid file header (wrong file format or incompatible version)");
    }

    input.seek(io::SeekFrom::Start(0))?;
    let input2 = StreamSlice::new(&mut *input, 8, end_pos)?;

    let decoder = Decoder::new(input2)?;
    let mut buf_reader = BufReader::new(decoder);

    let mut result: Vec<TraceLowLevelEvent> = vec![];

    while !is_at_eof(&mut buf_reader)? {
        let obj = cbor4ii::serde::from_reader::<TraceLowLevelEvent, _>(&mut buf_reader)?;
        result.push(obj);
    }

    Ok(result)
}
