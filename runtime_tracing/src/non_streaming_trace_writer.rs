use std::{collections::HashMap, env, error::Error, fs, path::{Path, PathBuf}};

use crate::{abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData}, TraceEventsFileFormat, TraceLowLevelEvent, TraceWriter};

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

                trace_metadata_path: None,
                trace_paths_path: None,
            },

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

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(path) = &self.trace_events_path {
            match self.format {
                TraceEventsFileFormat::Json => {
                    let json = serde_json::to_string(&self.events)?;
                    fs::write(path, json)?;
                }
                TraceEventsFileFormat::BinaryV0 => {
                    let mut file = fs::File::create(path)?;
                    crate::capnptrace::write_trace(&self.events, &mut file)?;
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
