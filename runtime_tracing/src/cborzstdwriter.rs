use std::{collections::HashMap, env, fs::{self, File}, io::Write, path::PathBuf};
use zeekstd::Encoder;

use crate::{tracer::TOP_LEVEL_FUNCTION_ID, AssignCellRecord, AssignCompoundItemRecord, AssignmentRecord, CallRecord, CellValueRecord, CompoundValueRecord, FullValueRecord, FunctionId, FunctionRecord, Line, PathId, RValue, RecordEvent, ReturnRecord, StepRecord, TraceLowLevelEvent, TraceMetadata, TraceWriter, TypeId, TypeKind, TypeRecord, TypeSpecificInfo, VariableCellRecord, VariableId, NONE_TYPE_ID};

/// The next 3 bytes are reserved/version info.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADERV1: &[u8] = &[
    0xC0, 0xDE, 0x72, 0xAC, 0xE2,  // The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
    0x01,                          // Indicates version 1 of the file format
    0x00, 0x00];                   // Reserved, must be zero in this version.

pub struct StreamingTraceWriter<'a> {
    // trace metadata:
    workdir: PathBuf,
    program: String,
    args: Vec<String>,
    // internal tracer state:
    path_list: Vec<PathBuf>,
    function_list: Vec<(String, PathId, Line)>,

    paths: HashMap<PathBuf, PathId>,
    functions: HashMap<String, FunctionId>,
    variables: HashMap<String, VariableId>,
    types: HashMap<String, TypeId>,

    trace_metadata_path: Option<PathBuf>,
    trace_events_path: Option<PathBuf>,
    trace_events_file_zstd_encoder: Option<Encoder<'a, File>>,
    trace_paths_path: Option<PathBuf>,
}

impl<'a> StreamingTraceWriter<'a> {
    /// Create a new tracer instance for the given program and arguments.
    pub fn new(program: &str, args: &[String]) -> Self {
        StreamingTraceWriter {
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
            trace_events_path: None,
            trace_events_file_zstd_encoder: None,
            trace_paths_path: None,
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

    fn start(&mut self, path: &std::path::Path, line: Line) {
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

    fn ensure_path_id(&mut self, path: &std::path::Path) -> PathId {
        if !self.paths.contains_key(path) {
            self.paths.insert(path.to_path_buf(), PathId(self.paths.len()));
            self.register_path(path);
        }
        *self.paths.get(path).unwrap()
    }

    fn ensure_function_id(&mut self, function_name: &str, path: &std::path::Path, line: Line) -> FunctionId {
        if !self.functions.contains_key(function_name) {
            // same function names for different path line? TODO
            self.functions.insert(function_name.to_string(), FunctionId(self.functions.len()));
            self.register_function(function_name, path, line);
        }
        *self.functions.get(function_name).unwrap()
    }

    fn ensure_type_id(&mut self, kind: crate::TypeKind, lang_type: &str) -> TypeId {
        let typ = self.to_raw_type(kind, lang_type);
        self.ensure_raw_type_id(typ)
    }

    fn ensure_raw_type_id(&mut self, typ: crate::TypeRecord) -> TypeId {
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

    fn register_path(&mut self, path: &std::path::Path) {
        self.path_list.push(path.to_path_buf());
        self.add_event(TraceLowLevelEvent::Path(path.to_path_buf()));
    }

    fn register_function(&mut self, name: &str, path: &std::path::Path, line: Line) {
        let path_id = self.ensure_path_id(path);
        self.function_list.push((name.to_string(), path_id, line));
        self.add_event(TraceLowLevelEvent::Function(FunctionRecord {
            name: name.to_string(),
            path_id,
            line,
        }));
    }

    fn register_step(&mut self, path: &std::path::Path, line: Line) {
        let path_id = self.ensure_path_id(path);
        self.add_event(TraceLowLevelEvent::Step(StepRecord { path_id, line }));
    }

    fn register_call(&mut self, function_id: FunctionId, args: Vec<crate::FullValueRecord>) {
        // register a step for each call, the backend expects this for
        // non-toplevel calls, so
        // we ensure it directly from register_call
        if function_id != TOP_LEVEL_FUNCTION_ID {
            for arg in &args {
                self.register_full_value(arg.variable_id, arg.value.clone());
            }
            let function = &self.function_list[function_id.0];
            self.add_event(TraceLowLevelEvent::Step(StepRecord {
                path_id: function.1,
                line: function.2,
            }));
        }
        // the actual call event:
        self.add_event(TraceLowLevelEvent::Call(CallRecord { function_id, args }));
    }

    fn arg(&mut self, name: &str, value: crate::ValueRecord) -> FullValueRecord {
        let variable_id = self.ensure_variable_id(name);
        FullValueRecord { variable_id, value }
    }

    fn register_return(&mut self, return_value: crate::ValueRecord) {
        self.add_event(TraceLowLevelEvent::Return(ReturnRecord { return_value }));
    }

    fn register_special_event(&mut self, kind: crate::EventLogKind, content: &str) {
        self.add_event(TraceLowLevelEvent::Event(RecordEvent {
            kind,
            metadata: "".to_string(),
            content: content.to_string(),
        }));
    }

    fn to_raw_type(&self, kind: crate::TypeKind, lang_type: &str) -> crate::TypeRecord {
        TypeRecord {
            kind,
            lang_type: lang_type.to_string(),
            specific_info: TypeSpecificInfo::None,
        }
    }

    fn register_type(&mut self, kind: crate::TypeKind, lang_type: &str) {
        let typ = self.to_raw_type(kind, lang_type);
        self.add_event(TraceLowLevelEvent::Type(typ));
    }

    fn register_raw_type(&mut self, typ: crate::TypeRecord) {
        self.add_event(TraceLowLevelEvent::Type(typ));
    }

    fn register_asm(&mut self, instructions: &[String]) {
        self.add_event(TraceLowLevelEvent::Asm(instructions.to_vec()));
    }

    fn register_variable_with_full_value(&mut self, name: &str, value: crate::ValueRecord) {
        let variable_id = self.ensure_variable_id(name);
        self.register_full_value(variable_id, value);
    }

    fn register_variable_name(&mut self, variable_name: &str) {
        self.add_event(TraceLowLevelEvent::VariableName(variable_name.to_string()));
    }

    fn register_full_value(&mut self, variable_id: VariableId, value: crate::ValueRecord) {
        self.add_event(TraceLowLevelEvent::Value(FullValueRecord { variable_id, value }));
    }

    fn register_compound_value(&mut self, place: crate::Place, value: crate::ValueRecord) {
        self.add_event(TraceLowLevelEvent::CompoundValue(CompoundValueRecord { place, value }));
    }

    fn register_cell_value(&mut self, place: crate::Place, value: crate::ValueRecord) {
        self.add_event(TraceLowLevelEvent::CellValue(CellValueRecord { place, value }));
    }

    fn assign_compound_item(&mut self, place: crate::Place, index: usize, item_place: crate::Place) {
        self.add_event(TraceLowLevelEvent::AssignCompoundItem(AssignCompoundItemRecord {
            place,
            index,
            item_place,
        }));
    }

    fn assign_cell(&mut self, place: crate::Place, new_value: crate::ValueRecord) {
        self.add_event(TraceLowLevelEvent::AssignCell(AssignCellRecord { place, new_value }));
    }

    fn register_variable(&mut self, variable_name: &str, place: crate::Place) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.add_event(TraceLowLevelEvent::VariableCell(VariableCellRecord { variable_id, place }));
    }

    fn drop_variable(&mut self, variable_name: &str) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.add_event(TraceLowLevelEvent::DropVariable(variable_id));
    }

    // history event helpers
    fn assign(&mut self, variable_name: &str, rvalue: crate::RValue, pass_by: crate::PassBy) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.add_event(TraceLowLevelEvent::Assignment(AssignmentRecord {
            to: variable_id,
            from: rvalue,
            pass_by,
        }));
    }

    fn bind_variable(&mut self, variable_name: &str, place: crate::Place) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.add_event(TraceLowLevelEvent::BindVariable(crate::BindVariableRecord { variable_id, place }));
    }

    fn drop_variables(&mut self, variable_names: &[String]) {
        let variable_ids: Vec<VariableId> = variable_names
            .to_vec()
            .iter()
            .map(|variable_name| self.ensure_variable_id(variable_name))
            .collect();
        self.add_event(TraceLowLevelEvent::DropVariables(variable_ids))
    }

    fn simple_rvalue(&mut self, variable_name: &str) -> crate::RValue {
        let variable_id = self.ensure_variable_id(variable_name);
        RValue::Simple(variable_id)
    }

    fn compound_rvalue(&mut self, variable_dependencies: &[String]) -> crate::RValue {
        let variable_ids: Vec<VariableId> = variable_dependencies
            .to_vec()
            .iter()
            .map(|variable_dependency| self.ensure_variable_id(variable_dependency))
            .collect();
        RValue::Compound(variable_ids)
    }

    fn drop_last_step(&mut self) {
        self.add_event(TraceLowLevelEvent::DropLastStep);
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
            self.add_event(e.clone());
        }
    }

    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn std::error::Error>> {
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
            let json = serde_json::to_string(&self.path_list)?;
            fs::write(path, json)?;
            Ok(())
        } else {
            panic!("finish_writing_trace_paths() called without previous call to begin_writing_trace_paths()");
        }
    }
}
