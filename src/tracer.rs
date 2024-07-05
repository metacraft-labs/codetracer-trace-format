use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use crate::types::{
    ArgRecord, CallRecord, EventLogKind, FullValueRecord, FunctionId, FunctionRecord, Line, PathId, RecordEvent, ReturnRecord, StepRecord,
    TraceLowLevelEvent, TraceMetadata, TypeId, TypeKind, TypeRecord, TypeSpecificInfo, ValueRecord, VariableId,
};

pub struct Tracer {
    // trace metadata:
    workdir: PathBuf,
    program: String,
    args: Vec<String>,
    // trace events
    pub events: Vec<TraceLowLevelEvent>,

    // internal tracer state:
    paths: HashMap<PathBuf, PathId>,
    functions: HashMap<String, FunctionId>,
    variables: HashMap<String, VariableId>,
    types: HashMap<String, TypeId>,
}

// we ensure in start they are registered with those id-s

// pub const EXAMPLE_INT_TYPE_ID: TypeId = TypeId(0);
// pub const EXAMPLE_FLOAT_TYPE_ID: TypeId = TypeId(1);
// pub const EXAMPLE_BOOL_TYPE_ID: TypeId = TypeId(2);
// pub const EXAMPLE_STRING_TYPE_ID: TypeId = TypeId(3);
pub const NONE_TYPE_ID: TypeId = TypeId(0);
pub const NONE_VALUE: ValueRecord = ValueRecord::None { type_id: NONE_TYPE_ID };

impl Tracer {
    pub fn new(program: &str, args: &[String]) -> Self {
        Tracer {
            workdir: env::current_dir().expect("can access the current dir"),
            program: program.to_string(),
            args: args.to_vec(),
            events: vec![],

            paths: HashMap::new(),
            functions: HashMap::new(),
            variables: HashMap::new(),
            types: HashMap::new(),
        }
    }

    pub fn start(&mut self, path: &Path, line: Line) {
        let function_id = self.ensure_function_id("<toplevel>", path, line);
        self.register_call(function_id, vec![]);

        // probably we let the user choose, as different languages have
        // different base types/names
        // assert!(EXAMPLE_INT_TYPE_ID == self.load_type_id(TypeKind::Int, "Int"));
        // assert!(EXAMPLE_FLOAT_TYPE_ID == self.load_type_id(TypeKind::Float, "Float"));
        // assert!(EXAMPLE_BOOL_TYPE_ID == self.load_type_id(TypeKind::Bool, "Bool"));
        // assert!(EXAMPLE_STRING_TYPE_ID == self.load_type_id(TypeKind::Bool, "String"));
        assert!(NONE_TYPE_ID == self.ensure_type_id(TypeKind::None, "None"));
    }

    pub fn ensure_path_id(&mut self, path: &Path) -> PathId {
        if !self.paths.contains_key(path) {
            self.paths.insert(path.to_path_buf(), PathId(self.paths.len()));
            self.register_path(path);
        }
        *self.paths.get(path).unwrap()
    }

    pub fn ensure_function_id(&mut self, function_name: &str, path: &Path, line: Line) -> FunctionId {
        if !self.functions.contains_key(function_name) {
            // same function names for different path line? TODO
            self.functions.insert(function_name.to_string(), FunctionId(self.functions.len()));
            self.register_function(function_name, path, line);
        }
        *self.functions.get(function_name).unwrap()
    }

    pub fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
        if !self.types.contains_key(lang_type) {
            self.types.insert(lang_type.to_string(), TypeId(self.types.len()));
            self.register_type(kind, lang_type);
        }
        *self.types.get(lang_type).unwrap()
    }

    pub fn ensure_variable_id(&mut self, variable_name: &str) -> VariableId {
        if !self.variables.contains_key(variable_name) {
            self.variables.insert(variable_name.to_string(), VariableId(self.variables.len()));
            self.register_variable_event(variable_name);
        }
        *self.variables.get(variable_name).unwrap()
    }

    pub fn register_path(&mut self, path: &Path) {
        self.events.push(TraceLowLevelEvent::Path(path.to_path_buf()));
    }

    pub fn register_function(&mut self, name: &str, path: &Path, line: Line) {
        let path_id = self.ensure_path_id(path);
        self.events.push(TraceLowLevelEvent::Function(FunctionRecord {
            name: name.to_string(),
            path_id,
            line,
        }));
    }

    pub fn register_step(&mut self, path: &Path, line: Line) {
        let path_id = self.ensure_path_id(path);
        self.events.push(TraceLowLevelEvent::Step(StepRecord { path_id, line: line }));
    }

    pub fn register_call(&mut self, function_id: FunctionId, args: Vec<ArgRecord>) {
        self.events.push(TraceLowLevelEvent::Call(CallRecord { function_id, args }));
    }

    pub fn register_return(&mut self, return_value: ValueRecord) {
        self.events.push(TraceLowLevelEvent::Return(ReturnRecord { return_value }));
    }

    pub fn register_special_event(&mut self, kind: EventLogKind, content: &str) {
        self.events.push(TraceLowLevelEvent::Event(RecordEvent {
            kind,
            content: content.to_string(),
        }));
    }

    pub fn register_type(&mut self, kind: TypeKind, lang_type: &str) {
        let typ = TypeRecord {
            kind,
            lang_type: lang_type.to_string(),
            specific_info: TypeSpecificInfo::None,
        };
        self.events.push(TraceLowLevelEvent::Type(typ));
    }

    pub fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
        let variable_id = self.ensure_variable_id(name);
        self.register_full_value(variable_id, value);
    }

    pub fn register_variable_event(&mut self, variable_name: &str) {
        self.events.push(TraceLowLevelEvent::Variable(variable_name.to_string()));
    }

    pub fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord) {
        self.events.push(TraceLowLevelEvent::Value(FullValueRecord { variable_id, value }));
    }

    pub fn store_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let trace_metadata = TraceMetadata {
            program: self.program.clone(),
            args: self.args.clone(),
            workdir: self.workdir.clone(),
        };
        let json = serde_json::to_string(&trace_metadata)?;
        fs::write(path, json)?;
        Ok(())
    }

    pub fn store_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        // TODO: probably change format
        let json = serde_json::to_string(&self.events)?;
        fs::write(path, json)?;
        Ok(())
    }
}
