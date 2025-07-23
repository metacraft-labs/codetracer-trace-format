//! Helper for generating trace events from a running program or interpreter.

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::capnptrace::HEADER;
use crate::cborzstdwriter::HEADERV1;
use crate::types::{
    AssignCellRecord, AssignCompoundItemRecord, AssignmentRecord, CallRecord, CellValueRecord, CompoundValueRecord, EventLogKind, FullValueRecord,
    FunctionId, FunctionRecord, Line, PassBy, PathId, Place, RecordEvent, ReturnRecord, StepRecord, TraceLowLevelEvent, TraceMetadata, TypeId,
    TypeKind, TypeRecord, TypeSpecificInfo, ValueRecord, VariableCellRecord, VariableId,
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

pub trait TraceWriter {
    fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
    fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;

    fn start(&mut self, path: &Path, line: Line);
    fn ensure_path_id(&mut self, path: &Path) -> PathId;
    fn ensure_function_id(&mut self, function_name: &str, path: &Path, line: Line) -> FunctionId;
    fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId;
    fn ensure_raw_type_id(&mut self, typ: TypeRecord) -> TypeId;
    fn ensure_variable_id(&mut self, variable_name: &str) -> VariableId;
    fn register_path(&mut self, path: &Path);
    fn register_function(&mut self, name: &str, path: &Path, line: Line);
    fn register_step(&mut self, path: &Path, line: Line);
    fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>);
    fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord;
    fn register_return(&mut self, return_value: ValueRecord);
    // TODO: add metadata arg
    fn register_special_event(&mut self, kind: EventLogKind, content: &str);
    fn to_raw_type(&self, kind: TypeKind, lang_type: &str) -> TypeRecord;
    fn register_type(&mut self, kind: TypeKind, lang_type: &str);
    fn register_raw_type(&mut self, typ: TypeRecord);
    fn register_asm(&mut self, instructions: &[String]);
    fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord);
    fn register_variable_name(&mut self, variable_name: &str);
    fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord);
    fn register_compound_value(&mut self, place: Place, value: ValueRecord);
    fn register_cell_value(&mut self, place: Place, value: ValueRecord);
    fn assign_compound_item(&mut self, place: Place, index: usize, item_place: Place);
    fn assign_cell(&mut self, place: Place, new_value: ValueRecord);
    fn register_variable(&mut self, variable_name: &str, place: Place);
    fn drop_variable(&mut self, variable_name: &str);
    // history event helpers
    fn assign(&mut self, variable_name: &str, rvalue: RValue, pass_by: PassBy);
    fn bind_variable(&mut self, variable_name: &str, place: Place);
    fn drop_variables(&mut self, variable_names: &[String]);
    fn simple_rvalue(&mut self, variable_name: &str) -> RValue;
    fn compound_rvalue(&mut self, variable_dependencies: &[String]) -> RValue;
    fn drop_last_step(&mut self);

    fn add_event(&mut self, event: TraceLowLevelEvent);
    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>);

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
    // trace metadata:
    workdir: PathBuf,
    program: String,
    args: Vec<String>,
    // trace events
    pub events: Vec<TraceLowLevelEvent>,
    // internal tracer state:
    path_list: Vec<PathBuf>,
    function_list: Vec<(String, PathId, Line)>,

    paths: HashMap<PathBuf, PathId>,
    functions: HashMap<String, FunctionId>,
    variables: HashMap<String, VariableId>,
    types: HashMap<String, TypeId>,

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
            workdir: env::current_dir().expect("can access the current dir"),
            program: program.to_string(),
            args: args.to_vec(),
            events: vec![],

            path_list: vec![],
            function_list: vec![],
            paths: HashMap::new(),
            functions: HashMap::new(),
            variables: HashMap::new(),
            types: HashMap::new(),

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

    /// Begin tracing of a program starting at the given source location.
    fn start(&mut self, path: &Path, line: Line) {
        let function_id = self.ensure_function_id("<toplevel>", path, line);
        self.register_call(function_id, vec![]);
        assert!(function_id == TOP_LEVEL_FUNCTION_ID);

        // probably we let the user choose, as different languages have
        // different base types/names
        // assert!(EXAMPLE_INT_TYPE_ID == self.load_type_id(TypeKind::Int, "Int"));
        // assert!(EXAMPLE_FLOAT_TYPE_ID == self.load_type_id(TypeKind::Float, "Float"));
        // assert!(EXAMPLE_BOOL_TYPE_ID == self.load_type_id(TypeKind::Bool, "Bool"));
        // assert!(EXAMPLE_STRING_TYPE_ID == self.load_type_id(TypeKind::Bool, "String"));
        assert!(NONE_TYPE_ID == self.ensure_type_id(TypeKind::None, "None"));
    }

    fn ensure_path_id(&mut self, path: &Path) -> PathId {
        if !self.paths.contains_key(path) {
            self.paths.insert(path.to_path_buf(), PathId(self.paths.len()));
            self.register_path(path);
        }
        *self.paths.get(path).unwrap()
    }

    fn ensure_function_id(&mut self, function_name: &str, path: &Path, line: Line) -> FunctionId {
        if !self.functions.contains_key(function_name) {
            // same function names for different path line? TODO
            self.functions.insert(function_name.to_string(), FunctionId(self.functions.len()));
            self.register_function(function_name, path, line);
        }
        *self.functions.get(function_name).unwrap()
    }

    fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
        let typ = self.to_raw_type(kind, lang_type);
        self.ensure_raw_type_id(typ)
    }

    fn ensure_raw_type_id(&mut self, typ: TypeRecord) -> TypeId {
        if !self.types.contains_key(&typ.lang_type) {
            self.types.insert(typ.lang_type.clone(), TypeId(self.types.len()));
            self.register_raw_type(typ.clone());
        }
        *self.types.get(&typ.lang_type).unwrap()
    }

    fn ensure_variable_id(&mut self, variable_name: &str) -> VariableId {
        if !self.variables.contains_key(variable_name) {
            self.variables.insert(variable_name.to_string(), VariableId(self.variables.len()));
            self.register_variable_name(variable_name);
        }
        *self.variables.get(variable_name).unwrap()
    }

    fn register_path(&mut self, path: &Path) {
        self.path_list.push(path.to_path_buf());
        self.events.push(TraceLowLevelEvent::Path(path.to_path_buf()));
    }

    fn register_function(&mut self, name: &str, path: &Path, line: Line) {
        let path_id = self.ensure_path_id(path);
        self.function_list.push((name.to_string(), path_id, line));
        self.events.push(TraceLowLevelEvent::Function(FunctionRecord {
            name: name.to_string(),
            path_id,
            line,
        }));
    }

    fn register_step(&mut self, path: &Path, line: Line) {
        let path_id = self.ensure_path_id(path);
        self.events.push(TraceLowLevelEvent::Step(StepRecord { path_id, line }));
    }

    fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>) {
        // register a step for each call, the backend expects this for
        // non-toplevel calls, so
        // we ensure it directly from register_call
        if function_id != TOP_LEVEL_FUNCTION_ID {
            for arg in &args {
                self.register_full_value(arg.variable_id, arg.value.clone());
            }
            let function = &self.function_list[function_id.0];
            self.events.push(TraceLowLevelEvent::Step(StepRecord {
                path_id: function.1,
                line: function.2,
            }));
        }
        // the actual call event:
        self.events.push(TraceLowLevelEvent::Call(CallRecord { function_id, args }));
    }

    fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord {
        let variable_id = self.ensure_variable_id(name);
        FullValueRecord { variable_id, value }
    }

    fn register_return(&mut self, return_value: ValueRecord) {
        self.events.push(TraceLowLevelEvent::Return(ReturnRecord { return_value }));
    }

    fn register_special_event(&mut self, kind: EventLogKind, content: &str) {
        self.events.push(TraceLowLevelEvent::Event(RecordEvent {
            kind,
            metadata: "".to_string(),
            content: content.to_string(),
        }));
    }

    fn to_raw_type(&self, kind: TypeKind, lang_type: &str) -> TypeRecord {
        TypeRecord {
            kind,
            lang_type: lang_type.to_string(),
            specific_info: TypeSpecificInfo::None,
        }
    }

    fn register_type(&mut self, kind: TypeKind, lang_type: &str) {
        let typ = self.to_raw_type(kind, lang_type);
        self.events.push(TraceLowLevelEvent::Type(typ));
    }

    fn register_raw_type(&mut self, typ: TypeRecord) {
        self.events.push(TraceLowLevelEvent::Type(typ));
    }

    fn register_asm(&mut self, instructions: &[String]) {
        self.events.push(TraceLowLevelEvent::Asm(instructions.to_vec()));
    }

    fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
        let variable_id = self.ensure_variable_id(name);
        self.register_full_value(variable_id, value);
    }

    fn register_variable_name(&mut self, variable_name: &str) {
        self.events.push(TraceLowLevelEvent::VariableName(variable_name.to_string()));
    }

    fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord) {
        self.events.push(TraceLowLevelEvent::Value(FullValueRecord { variable_id, value }));
    }

    fn register_compound_value(&mut self, place: Place, value: ValueRecord) {
        self.events.push(TraceLowLevelEvent::CompoundValue(CompoundValueRecord { place, value }));
    }

    fn register_cell_value(&mut self, place: Place, value: ValueRecord) {
        self.events.push(TraceLowLevelEvent::CellValue(CellValueRecord { place, value }));
    }

    fn assign_compound_item(&mut self, place: Place, index: usize, item_place: Place) {
        self.events.push(TraceLowLevelEvent::AssignCompoundItem(AssignCompoundItemRecord {
            place,
            index,
            item_place,
        }));
    }
    fn assign_cell(&mut self, place: Place, new_value: ValueRecord) {
        self.events.push(TraceLowLevelEvent::AssignCell(AssignCellRecord { place, new_value }));
    }

    fn register_variable(&mut self, variable_name: &str, place: Place) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.events
            .push(TraceLowLevelEvent::VariableCell(VariableCellRecord { variable_id, place }));
    }

    fn drop_variable(&mut self, variable_name: &str) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.events.push(TraceLowLevelEvent::DropVariable(variable_id));
    }

    // history event helpers
    fn assign(&mut self, variable_name: &str, rvalue: RValue, pass_by: PassBy) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.events.push(TraceLowLevelEvent::Assignment(AssignmentRecord {
            to: variable_id,
            from: rvalue,
            pass_by,
        }));
    }

    fn bind_variable(&mut self, variable_name: &str, place: Place) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.events
            .push(TraceLowLevelEvent::BindVariable(crate::BindVariableRecord { variable_id, place }));
    }

    fn drop_variables(&mut self, variable_names: &[String]) {
        let variable_ids: Vec<VariableId> = variable_names
            .to_vec()
            .iter()
            .map(|variable_name| self.ensure_variable_id(variable_name))
            .collect();
        self.events.push(TraceLowLevelEvent::DropVariables(variable_ids))
    }

    fn simple_rvalue(&mut self, variable_name: &str) -> RValue {
        let variable_id = self.ensure_variable_id(variable_name);
        RValue::Simple(variable_id)
    }

    fn compound_rvalue(&mut self, variable_dependencies: &[String]) -> RValue {
        let variable_ids: Vec<VariableId> = variable_dependencies
            .to_vec()
            .iter()
            .map(|variable_dependency| self.ensure_variable_id(variable_dependency))
            .collect();
        RValue::Compound(variable_ids)
    }

    fn drop_last_step(&mut self) {
        self.events.push(TraceLowLevelEvent::DropLastStep);
    }

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        self.events.push(event)
    }

    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        self.events.append(events);
    }

    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(path) = &self.trace_metadata_path {
            let trace_metadata = TraceMetadata {
                program: self.program.clone(),
                args: self.args.clone(),
                workdir: self.workdir.clone(),
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
            let json = serde_json::to_string(&self.path_list)?;
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
