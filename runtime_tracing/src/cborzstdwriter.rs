use std::{collections::HashMap, env, fs::{self, File}, io::Write, path::PathBuf};
use zeekstd::Encoder;

use crate::{abstracttracewriter::{AbstractTraceWriter, AbstractTraceWriterData}, TraceLowLevelEvent, TraceMetadata, TraceWriter};

/// The next 3 bytes are reserved/version info.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADERV1: &[u8] = &[
    0xC0, 0xDE, 0x72, 0xAC, 0xE2,  // The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
    0x01,                          // Indicates version 1 of the file format
    0x00, 0x00];                   // Reserved, must be zero in this version.

pub struct StreamingTraceWriter<'a> {
    base: AbstractTraceWriterData,

    trace_metadata_path: Option<PathBuf>,
    trace_events_path: Option<PathBuf>,
    trace_events_file_zstd_encoder: Option<Encoder<'a, File>>,
    trace_paths_path: Option<PathBuf>,
}

impl<'a> StreamingTraceWriter<'a> {
    /// Create a new tracer instance for the given program and arguments.
    pub fn new(program: &str, args: &[String]) -> Self {
        StreamingTraceWriter {
            base: AbstractTraceWriterData {
                workdir: env::current_dir().expect("can access the current dir"),
                program: program.to_string(),
                args: args.to_vec(),

                path_list: vec![],
                function_list: vec![],
                paths: HashMap::new(),
                functions: HashMap::new(),
                variables: HashMap::new(),
                types: HashMap::new(),
            },

            trace_metadata_path: None,
            trace_events_path: None,
            trace_events_file_zstd_encoder: None,
            trace_paths_path: None,
        }
    }
}

impl<'a> AbstractTraceWriter for StreamingTraceWriter<'a> {
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
            enc.write(&q).unwrap();
        }
    }

    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        for e in events {
            AbstractTraceWriter::add_event(self, e.clone());
        }
    }
}

impl<'a> TraceWriter for StreamingTraceWriter<'a> {
    fn begin_writing_trace_metadata(&mut self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        self.trace_metadata_path = Some(path.to_path_buf());
        Ok(())
    }

    fn begin_writing_trace_events(&mut self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let pb = path.to_path_buf();
        self.trace_events_path = Some(pb.clone());
        let mut file_output = std::fs::File::create(pb)?;
        file_output.write_all(HEADERV1)?;
        self.trace_events_file_zstd_encoder = Some(Encoder::new(file_output)?);

        Ok(())
    }

    fn begin_writing_trace_paths(&mut self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        self.trace_paths_path = Some(path.to_path_buf());
        Ok(())
    }

    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(path) = &self.trace_metadata_path {
            let trace_metadata = TraceMetadata {
                program: self.get_data().program.clone(),
                args: self.get_data().args.clone(),
                workdir: self.get_data().workdir.clone(),
            };
            let json = serde_json::to_string(&trace_metadata)?;
            fs::write(path, json)?;
            Ok(())
        } else {
            panic!("finish_writing_trace_metadata() called without previous call to begin_writing_trace_metadata()");
        }
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(enc) = self.trace_events_file_zstd_encoder.take() {
            enc.finish()?;

            Ok(())
        } else {
            panic!("finish_writing_trace_events() called without previous call to begin_writing_trace_events()");
        }
    }

    fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(path) = &self.trace_paths_path {
            let json = serde_json::to_string(&self.get_data().path_list)?;
            fs::write(path, json)?;
            Ok(())
        } else {
            panic!("finish_writing_trace_paths() called without previous call to begin_writing_trace_paths()");
        }
    }
}
