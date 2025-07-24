use std::io::{self, BufRead, BufReader, Read, Seek, Write};

use fscommon::StreamSlice;
use zeekstd::Decoder;

use crate::{TraceLowLevelEvent, cbor_zstd_writer::HEADERV1};

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
