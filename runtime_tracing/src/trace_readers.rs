use std::{
    error::Error,
    fs::{self, File},
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use crate::{TraceEventsFileFormat, capnptrace::HEADER, cbor_zstd_writer::HEADERV1};
use codetracer_trace_types::TraceLowLevelEvent;

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
                Ok(crate::capnptrace::read_trace(&mut buf_reader)?)
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
