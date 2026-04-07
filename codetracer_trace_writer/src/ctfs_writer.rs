use std::io::{Cursor, Write};
use std::path::Path;

use codetracer_ctfs::CtfsWriter;
use codetracer_trace_format_cbor_zstd::HEADERV1;

use crate::{
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
    trace_writer::TraceWriter,
};
use codetracer_trace_types::TraceLowLevelEvent;

/// A trace writer that outputs a single `.ct` CTFS container file.
///
/// The container holds:
/// - `events.log` — CBOR+Zstd encoded events (same encoding as the Binary format)
/// - `meta.json`  — trace metadata (program, args, workdir)
/// - `paths.json` — registered source paths
///
/// Events are CBOR-serialized into an in-memory buffer as they arrive.
/// On `finish_writing_trace_events` the buffer is Zstd-compressed and
/// written into the CTFS container along with metadata.
pub struct CtfsTraceWriter {
    base: AbstractTraceWriterData,
    ctfs_writer: Option<CtfsWriter>,
    events_handle: Option<codetracer_ctfs::FileHandle>,
    /// Accumulates CBOR-serialized event bytes.
    cbor_buffer: Vec<u8>,
}

impl CtfsTraceWriter {
    /// Create a new CTFS trace writer for the given program and arguments.
    pub fn new(program: &str, args: &[String]) -> Self {
        CtfsTraceWriter {
            base: AbstractTraceWriterData::new(program, args),
            ctfs_writer: None,
            events_handle: None,
            cbor_buffer: Vec::new(),
        }
    }
}

impl AbstractTraceWriter for CtfsTraceWriter {
    fn get_data(&self) -> &AbstractTraceWriterData {
        &self.base
    }

    fn get_mut_data(&mut self) -> &mut AbstractTraceWriterData {
        &mut self.base
    }

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        let buf: Vec<u8> = Vec::new();
        let cbor_bytes = cbor4ii::serde::to_vec(buf, &event).unwrap();
        self.cbor_buffer.extend_from_slice(&cbor_bytes);
    }

    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        for e in events {
            AbstractTraceWriter::add_event(self, e.clone());
        }
    }
}

impl TraceWriter for CtfsTraceWriter {
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        // Create .ct file at path (replace any existing extension)
        let ct_path = path.with_extension("ct");
        let mut writer = CtfsWriter::create(&ct_path, 4096, 31)?;
        let events_handle = writer.add_file("events.log")?;
        self.ctfs_writer = Some(writer);
        self.events_handle = Some(events_handle);
        self.cbor_buffer.clear();

        Ok(())
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Compress the CBOR buffer with Zstd, prefixed by HEADERV1
        let mut compressed = Vec::new();
        compressed.extend_from_slice(HEADERV1);

        {
            let mut cursor = Cursor::new(&mut compressed);
            // Position cursor at end so encoder appends after the header
            cursor.set_position(HEADERV1.len() as u64);
            let mut encoder = zeekstd::Encoder::new(cursor)?;
            encoder.write_all(&self.cbor_buffer)?;
            encoder.finish()?;
        }

        if let (Some(writer), Some(handle)) = (&mut self.ctfs_writer, self.events_handle) {
            // Write compressed events data into the CTFS container
            writer.write(handle, &compressed)?;

            // Write metadata as meta.json
            let trace_metadata = codetracer_trace_types::TraceMetadata {
                program: self.base.program.clone(),
                args: self.base.args.clone(),
                workdir: self.base.workdir.clone(),
            };
            let meta_json = serde_json::to_string(&trace_metadata)?;
            let meta_handle = writer.add_file("meta.json")?;
            writer.write(meta_handle, meta_json.as_bytes())?;

            // Write paths as paths.json
            let paths_json = serde_json::to_string(&self.base.path_list)?;
            let paths_handle = writer.add_file("paths.json")?;
            writer.write(paths_handle, paths_json.as_bytes())?;
        }

        // Close the CTFS container (takes ownership)
        if let Some(writer) = self.ctfs_writer.take() {
            writer.close()?;
        }

        self.cbor_buffer.clear();

        Ok(())
    }
}
