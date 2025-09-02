use std::{fs::File, io::Write, path::PathBuf};

use zeekstd::Encoder;

use crate::{
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
    trace_writer::TraceWriter,
    TraceLowLevelEvent,
};

/// The next 3 bytes are reserved/version info.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADERV1: &[u8] = &[
    0xC0, 0xDE, 0x72, 0xAC, 0xE2, // The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
    0x01, // Indicates version 1 of the file format
    0x00, 0x00,
]; // Reserved, must be zero in this version.

pub struct CborZstdTraceWriter<'a> {
    base: AbstractTraceWriterData,

    trace_events_path: Option<PathBuf>,
    trace_events_file_zstd_encoder: Option<Encoder<'a, File>>,
}

impl CborZstdTraceWriter<'_> {
    /// Create a new tracer instance for the given program and arguments.
    pub fn new(program: &str, args: &[String]) -> Self {
        CborZstdTraceWriter {
            base: AbstractTraceWriterData::new(program, args),

            trace_events_path: None,
            trace_events_file_zstd_encoder: None,
        }
    }
}

impl AbstractTraceWriter for CborZstdTraceWriter<'_> {
    fn get_data(&self) -> &AbstractTraceWriterData {
        &self.base
    }

    fn get_mut_data(&mut self) -> &mut AbstractTraceWriterData {
        &mut self.base
    }

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        let buf: Vec<u8> = Vec::new();
        let q = cbor4ii::serde::to_vec(buf, &event).unwrap();
        if let Some(enc) = &mut self.trace_events_file_zstd_encoder {
            enc.write_all(&q).unwrap();
        }
    }

    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        for e in events {
            AbstractTraceWriter::add_event(self, e.clone());
        }
    }
}

impl TraceWriter for CborZstdTraceWriter<'_> {
    fn begin_writing_trace_events(&mut self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let pb = path.to_path_buf();
        self.trace_events_path = Some(pb.clone());
        let mut file_output = std::fs::File::create(pb)?;
        file_output.write_all(HEADERV1)?;
        self.trace_events_file_zstd_encoder = Some(Encoder::new(file_output)?);

        Ok(())
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(enc) = self.trace_events_file_zstd_encoder.take() {
            enc.finish()?;

            Ok(())
        } else {
            panic!("finish_writing_trace_events() called without previous call to begin_writing_trace_events()");
        }
    }
}
