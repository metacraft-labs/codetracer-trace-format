//! Helper for generating trace events from a running program or interpreter.

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::capnptrace::HEADER;
use crate::cborzstdwriter::HEADERV1;
use crate::abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData};
use crate::types::{
    EventLogKind, FullValueRecord,
    FunctionId, Line, PassBy, PathId, Place, TraceLowLevelEvent, TraceMetadata, TypeId,
    TypeKind, TypeRecord, ValueRecord, VariableId,
};
use crate::RValue;

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
            Some(TraceEventsFileFormat::Binary) => {
                Ok(crate::cborzstdreader::read_trace(&mut file)?)
            }
            Some(TraceEventsFileFormat::Json) => {
                unreachable!()
            }
            None => {
                panic!("Invalid file header (wrong file format or incompatible version)");
            }
        }
    }
}

pub trait TraceWriter: AbstractTraceWriter {
    fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
    fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;

    fn start(&mut self, path: &Path, line: Line) {
        AbstractTraceWriter::start(self, path, line)
    }
    fn ensure_path_id(&mut self, path: &Path) -> PathId {
        AbstractTraceWriter::ensure_path_id(self, path)
    }
    fn ensure_function_id(&mut self, function_name: &str, path: &Path, line: Line) -> FunctionId {
        AbstractTraceWriter::ensure_function_id(self, function_name, path, line)
    }
    fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
        AbstractTraceWriter::ensure_type_id(self, kind, lang_type)
    }
    fn ensure_raw_type_id(&mut self, typ: TypeRecord) -> TypeId {
        AbstractTraceWriter::ensure_raw_type_id(self, typ)
    }
    fn ensure_variable_id(&mut self, variable_name: &str) -> VariableId {
        AbstractTraceWriter::ensure_variable_id(self, variable_name)
    }
    fn register_path(&mut self, path: &Path) {
        AbstractTraceWriter::register_path(self, path)
    }
    fn register_function(&mut self, name: &str, path: &Path, line: Line) {
        AbstractTraceWriter::register_function(self, name, path, line)
    }
    fn register_step(&mut self, path: &Path, line: Line) {
        AbstractTraceWriter::register_step(self, path, line)
    }
    fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>) {
        AbstractTraceWriter::register_call(self, function_id, args)
    }
    fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord {
        AbstractTraceWriter::arg(self, name, value)
    }
    fn register_return(&mut self, return_value: ValueRecord) {
        AbstractTraceWriter::register_return(self, return_value)
    }
    // TODO: add metadata arg
    fn register_special_event(&mut self, kind: EventLogKind, content: &str) {
        AbstractTraceWriter::register_special_event(self, kind, content)
    }
    fn to_raw_type(&self, kind: TypeKind, lang_type: &str) -> TypeRecord {
        AbstractTraceWriter::to_raw_type(self, kind, lang_type)
    }
    fn register_type(&mut self, kind: TypeKind, lang_type: &str) {
        AbstractTraceWriter::register_type(self, kind, lang_type)
    }
    fn register_raw_type(&mut self, typ: TypeRecord) {
        AbstractTraceWriter::register_raw_type(self, typ)
    }
    fn register_asm(&mut self, instructions: &[String]) {
        AbstractTraceWriter::register_asm(self, instructions)
    }
    fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
        AbstractTraceWriter::register_variable_with_full_value(self, name, value)
    }
    fn register_variable_name(&mut self, variable_name: &str) {
        AbstractTraceWriter::register_variable_name(self, variable_name)
    }
    fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord) {
        AbstractTraceWriter::register_full_value(self, variable_id, value)
    }
    fn register_compound_value(&mut self, place: Place, value: ValueRecord) {
        AbstractTraceWriter::register_compound_value(self, place, value)
    }
    fn register_cell_value(&mut self, place: Place, value: ValueRecord) {
        AbstractTraceWriter::register_cell_value(self, place, value)
    }
    fn assign_compound_item(&mut self, place: Place, index: usize, item_place: Place) {
        AbstractTraceWriter::assign_compound_item(self, place, index, item_place)
    }
    fn assign_cell(&mut self, place: Place, new_value: ValueRecord) {
        AbstractTraceWriter::assign_cell(self, place, new_value)
    }
    fn register_variable(&mut self, variable_name: &str, place: Place) {
        AbstractTraceWriter::register_variable(self, variable_name, place)
    }
    fn drop_variable(&mut self, variable_name: &str) {
        AbstractTraceWriter::drop_variable(self, variable_name)
    }
    // history event helpers
    fn assign(&mut self, variable_name: &str, rvalue: RValue, pass_by: PassBy) {
        AbstractTraceWriter::assign(self, variable_name, rvalue, pass_by)
    }
    fn bind_variable(&mut self, variable_name: &str, place: Place) {
        AbstractTraceWriter::bind_variable(self, variable_name, place)
    }
    fn drop_variables(&mut self, variable_names: &[String]) {
        AbstractTraceWriter::drop_variables(self, variable_names)
    }
    fn simple_rvalue(&mut self, variable_name: &str) -> RValue {
        AbstractTraceWriter::simple_rvalue(self, variable_name)
    }
    fn compound_rvalue(&mut self, variable_dependencies: &[String]) -> RValue {
        AbstractTraceWriter::compound_rvalue(self, variable_dependencies)
    }
    fn drop_last_step(&mut self) {
        AbstractTraceWriter::drop_last_step(self)
    }

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        AbstractTraceWriter::add_event(self, event)
    }
    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        AbstractTraceWriter::append_events(self, events)
    }

    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>>;
    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>>;
    fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>>;
}

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
    trace_metadata_path: Option<PathBuf>,
    trace_events_path: Option<PathBuf>,
    trace_paths_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
    Json,
    BinaryV0,
    Binary
}

// we ensure in start they are registered with those id-s

// pub const EXAMPLE_INT_TYPE_ID: TypeId = TypeId(0);
// pub const EXAMPLE_FLOAT_TYPE_ID: TypeId = TypeId(1);
// pub const EXAMPLE_BOOL_TYPE_ID: TypeId = TypeId(2);
// pub const EXAMPLE_STRING_TYPE_ID: TypeId = TypeId(3);
pub const NONE_TYPE_ID: TypeId = TypeId(0);
pub const NONE_VALUE: ValueRecord = ValueRecord::None { type_id: NONE_TYPE_ID };

pub const TOP_LEVEL_FUNCTION_ID: FunctionId = FunctionId(0);

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
            },

            events: vec![],

            format: TraceEventsFileFormat::Binary,
            trace_metadata_path: None,
            trace_events_path: None,
            trace_paths_path: None,
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
    fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        self.trace_metadata_path = Some(path.to_path_buf());
        Ok(())
    }

    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        self.trace_events_path = Some(path.to_path_buf());
        Ok(())
    }

    fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        self.trace_paths_path = Some(path.to_path_buf());
        Ok(())
    }

    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>> {
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

    fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(path) = &self.trace_paths_path {
            let json = serde_json::to_string(&self.get_data().path_list)?;
            fs::write(path, json)?;
            Ok(())
        } else {
            panic!("finish_writing_trace_paths() called without previous call to begin_writing_trace_paths()");
        }
    }
}

pub fn create_trace_reader(format: TraceEventsFileFormat) -> Box<dyn TraceReader> {
    match format {
        TraceEventsFileFormat::Json => Box::new(JsonTraceReader {}),
        TraceEventsFileFormat::BinaryV0 |
        TraceEventsFileFormat::Binary => Box::new(BinaryTraceReader {}),
    }
}

pub fn create_trace_writer(program: &str, args: &[String], format: TraceEventsFileFormat) -> Box<dyn TraceWriter> {
    match format {
        TraceEventsFileFormat::Json |
        TraceEventsFileFormat::BinaryV0 => {
            let mut result = Box::new(NonStreamingTraceWriter::new(program, args));
            result.set_format(format);
            result
        }
        TraceEventsFileFormat::Binary => {
            Box::new(crate::cborzstdwriter::StreamingTraceWriter::new(program, args))
        }
    }
}
