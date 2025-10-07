use std::{
    fs::File,
    io::{Cursor, Write},
    path::PathBuf,
};

use ruzstd::encoding::{CompressionLevel, compress};

use crate::{
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
    trace_writer::TraceWriter,
};
use codetracer_trace_types::TraceLowLevelEvent;

/// The next 3 bytes are reserved/version info.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADERV1: &[u8] = &[
    0xC0, 0xDE, 0x72, 0xAC, 0xE2, // The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
    0x01, // Indicates version 1 of the file format
    0x00, 0x00,
]; // Reserved, must be zero in this version.

pub struct CborZstdTraceWriter {
    base: AbstractTraceWriterData,

    trace_events_path: Option<PathBuf>,
    trace_events_file: Option<File>,
    uncompressed_buf: Vec<u8>,
}

impl CborZstdTraceWriter {
    /// Create a new tracer instance for the given program and arguments.
    pub fn new(program: &str, args: &[String]) -> Self {
        CborZstdTraceWriter {
            base: AbstractTraceWriterData::new(program, args),

            trace_events_path: None,
            trace_events_file: None,
            uncompressed_buf: vec![],
        }
    }
}

impl AbstractTraceWriter for CborZstdTraceWriter {
    fn get_data(&self) -> &AbstractTraceWriterData {
        &self.base
    }

    fn get_mut_data(&mut self) -> &mut AbstractTraceWriterData {
        &mut self.base
    }

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        let buf: Vec<u8> = Vec::new();
        let q = cbor4ii::serde::to_vec(buf, &event).expect("CBOR encode failed");
        self.uncompressed_buf.extend_from_slice(&q);
    }

    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        for e in events.drain(..) {
            <Self as AbstractTraceWriter>::add_event(self, e);
        }
    }
}

impl TraceWriter for CborZstdTraceWriter {
    fn begin_writing_trace_events(&mut self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let pb = path.to_path_buf();
        self.trace_events_path = Some(pb.clone());

        let mut file_output = std::fs::File::create(pb)?;
        file_output.write_all(HEADERV1)?;
        self.trace_events_file = Some(file_output);

        Ok(())
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut file) = self.trace_events_file.take() {
            let mut cursor = Cursor::new(&self.uncompressed_buf);
            compress(&mut cursor, &mut file, CompressionLevel::Fastest);

            file.flush()?;

            self.uncompressed_buf.clear();
            Ok(())
        } else {
            panic!("finish_writing_trace_events() called without previous call to begin_writing_trace_events()");
        }
    }
}
