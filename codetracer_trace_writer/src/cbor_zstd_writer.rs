use std::{fs::File, io::Write, path::PathBuf};

use codetracer_trace_format_cbor_zstd::HEADERV1;
use zeekstd::Encoder;

use crate::{
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
    trace_writer::TraceWriter,
};
use codetracer_trace_types::TraceLowLevelEvent;

pub struct CborZstdTraceWriter<'a> {
    base: AbstractTraceWriterData,

    trace_events_path: Option<PathBuf>,
    trace_events_file_zstd_encoder: Option<Encoder<'a, File>>,
    /// The first event that could not be encoded or written. Record calls
    /// return nothing, so it is held here and
    /// [`finish_writing_trace_events`](TraceWriter::finish_writing_trace_events)
    /// fails with it; once set, later events are not written.
    write_error: Option<String>,
}

impl CborZstdTraceWriter<'_> {
    /// Create a new tracer instance for the given program and arguments.
    pub fn new(program: &str, args: &[String]) -> Self {
        CborZstdTraceWriter {
            base: AbstractTraceWriterData::new(program, args),

            trace_events_path: None,
            trace_events_file_zstd_encoder: None,
            write_error: None,
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
        if self.write_error.is_some() {
            return;
        }
        let Some(enc) = &mut self.trace_events_file_zstd_encoder else {
            return;
        };
        let result = cbor4ii::serde::to_vec(Vec::new(), &event)
            .map_err(|e| format!("encoding an event: {e}"))
            .and_then(|q| enc.write_all(&q).map_err(|e| format!("writing an event: {e}")));
        if let Err(e) = result {
            self.write_error = Some(e);
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
        self.write_error = None;

        Ok(())
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(enc) = self.trace_events_file_zstd_encoder.take() {
            // The first failure while recording is the one reported: a later
            // failure to finish the stream is its consequence.
            let finished = enc.finish();
            if let Some(err) = self.write_error.take() {
                return Err(format!("the trace could not be written: {err}").into());
            }
            finished?;
            Ok(())
        } else {
            panic!("finish_writing_trace_events() called without previous call to begin_writing_trace_events()");
        }
    }
}
