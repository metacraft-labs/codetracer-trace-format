use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    TraceEventsFileFormat, trace_writer::TraceWriter,
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
};
use codetracer_trace_format_capnp::capnptrace::write_trace;
use codetracer_trace_types::TraceLowLevelEvent;

/// State machine used to record [`TraceLowLevelEvent`]s.
///
/// A `NonStreamingTraceWriter` instance accumulates events in memory and stores them on
/// disk when the `finish_writing_trace_*` methods are called. The in-memory event list
/// is exposed publicly.
pub struct NonStreamingTraceWriter {
    base: AbstractTraceWriterData,

    // trace events
    pub events: Vec<TraceLowLevelEvent>,

    format: TraceEventsFileFormat,
    trace_events_path: Option<PathBuf>,
}

impl NonStreamingTraceWriter {
    /// Create a new tracer instance for the given program and arguments.
    pub fn new(program: &str, args: &[String]) -> Self {
        NonStreamingTraceWriter {
            base: AbstractTraceWriterData::new(program, args),

            events: vec![],

            format: TraceEventsFileFormat::Binary,
            trace_events_path: None,
        }
    }

    pub fn set_format(&mut self, format: TraceEventsFileFormat) {
        self.format = format;
    }
}

impl AbstractTraceWriter for NonStreamingTraceWriter {
    fn get_data(&self) -> &AbstractTraceWriterData {
        &self.base
    }

    fn get_mut_data(&mut self) -> &mut AbstractTraceWriterData {
        &mut self.base
    }

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        self.events.push(event)
    }

    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        self.events.append(events)
    }
}

impl TraceWriter for NonStreamingTraceWriter {
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        self.trace_events_path = Some(path.to_path_buf());
        Ok(())
    }

    fn events(&self) -> &[TraceLowLevelEvent] {
        &self.events
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(path) = &self.trace_events_path {
            match self.format {
                TraceEventsFileFormat::Json => {
                    let json = serde_json::to_string(&self.events)?;
                    fs::write(path, json)?;
                }
                TraceEventsFileFormat::BinaryV0 => {
                    let mut file = fs::File::create(path)?;
                    write_trace(&self.events, &mut file)?;
                }
                TraceEventsFileFormat::Binary => {
                    unreachable!()
                }
            }
            Ok(())
        } else {
            panic!("finish_writing_trace_events() called without previous call to begin_writing_trace_events()");
        }
    }
}
