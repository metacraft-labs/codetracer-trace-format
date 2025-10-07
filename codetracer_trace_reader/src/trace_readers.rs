use std::{
    error::Error,
    fs::{self, File},
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use crate::TraceEventsFileFormat;
use codetracer_trace_types::TraceLowLevelEvent;
use codetracer_trace_format_capnp::capnptrace::HEADER;

/// The next 3 bytes are reserved/version info.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADERV1: &[u8] = &[
    0xC0, 0xDE, 0x72, 0xAC, 0xE2, // The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
    0x01, // Indicates version 1 of the file format
    0x00, 0x00,
]; // Reserved, must be zero in this version.

pub trait TraceReader {
    fn load_trace_events(&mut self, path: &Path) -> Result<Vec<TraceLowLevelEvent>, Box<dyn Error>>;
}

pub struct JsonTraceReader {}

impl TraceReader for JsonTraceReader {
    fn load_trace_events(&mut self, path: &Path) -> Result<Vec<TraceLowLevelEvent>, Box<dyn Error>> {
        let json = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&json)?)
    }
}

pub struct BinaryTraceReader {}

fn detect_bin_file_version(input: &mut File) -> Result<Option<TraceEventsFileFormat>, Box<dyn Error>> {
    input.seek(SeekFrom::Start(0))?;
    let mut header_buf = [0; 8];
    input.read_exact(&mut header_buf)?;
    input.seek(SeekFrom::Start(0))?;

    if header_buf == HEADER {
        Ok(Some(TraceEventsFileFormat::BinaryV0))
    } else if header_buf == HEADERV1 {
        Ok(Some(TraceEventsFileFormat::Binary))
    } else {
        Ok(None)
    }
}

impl TraceReader for BinaryTraceReader {
    fn load_trace_events(&mut self, path: &Path) -> Result<Vec<TraceLowLevelEvent>, Box<dyn Error>> {
        let mut file = fs::File::open(path)?;
        let ver = detect_bin_file_version(&mut file)?;
        match ver {
            Some(TraceEventsFileFormat::BinaryV0) => {
                let mut buf_reader = BufReader::new(file);
                Ok(codetracer_trace_format_capnp::capnptrace::read_trace(&mut buf_reader)?)
            }
            Some(TraceEventsFileFormat::Binary) => Ok(crate::cbor_zstd_reader::read_trace(&mut file)?),
            Some(TraceEventsFileFormat::Json) => {
                unreachable!()
            }
            None => {
                panic!("Invalid file header (wrong file format or incompatible version)");
            }
        }
    }
}
