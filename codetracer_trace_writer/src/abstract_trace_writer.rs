use std::{
    collections::HashMap,
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use codetracer_trace_types::{
    AssignCellRecord, AssignCompoundItemRecord, AssignmentRecord, BindVariableRecord, CallRecord, CellValueRecord, CompoundValueRecord, EventLogKind,
    FullValueRecord, FunctionId, FunctionRecord, Line, NONE_TYPE_ID, PassBy, PathId, Place, RValue, RecordEvent, ReturnRecord, StepRecord,
    TOP_LEVEL_FUNCTION_ID, ThreadId, TraceLowLevelEvent, TraceMetadata, TypeId, TypeKind, TypeRecord, TypeSpecificInfo, ValueRecord,
    VariableCellRecord, VariableId,
};

pub struct AbstractTraceWriterData {
    // trace metadata:
    pub workdir: PathBuf,
    pub program: String,
    pub args: Vec<String>,
    // internal tracer state:
    pub path_list: Vec<PathBuf>,
    pub function_list: Vec<(String, PathId, Line)>,

    pub paths: HashMap<PathBuf, PathId>,
    pub functions: HashMap<String, FunctionId>,
    pub variables: HashMap<String, VariableId>,
    pub types: HashMap<String, TypeId>,

    pub trace_metadata_path: Option<PathBuf>,
    pub trace_paths_path: Option<PathBuf>,
}

impl AbstractTraceWriterData {
    pub fn new(program: &str, args: &[String]) -> Self {
        AbstractTraceWriterData {
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
        }
    }
}

pub trait AbstractTraceWriter {
    fn get_data(&self) -> &AbstractTraceWriterData;
    fn get_mut_data(&mut self) -> &mut AbstractTraceWriterData;

    fn add_event(&mut self, event: TraceLowLevelEvent);
    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>);

    fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        self.get_mut_data().trace_metadata_path = Some(path.to_path_buf());
        Ok(())
    }

    fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        self.get_mut_data().trace_paths_path = Some(path.to_path_buf());
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
        if !self.get_data().paths.contains_key(path) {
            let mut_data = self.get_mut_data();
            mut_data.paths.insert(path.to_path_buf(), PathId(mut_data.paths.len()));
            self.register_path(path);
        }
        *self.get_data().paths.get(path).unwrap()
    }

    fn ensure_function_id(&mut self, function_name: &str, path: &std::path::Path, line: Line) -> FunctionId {
        if !self.get_data().functions.contains_key(function_name) {
            // same function names for different path line? TODO
            let mut_data = self.get_mut_data();
            mut_data.functions.insert(function_name.to_string(), FunctionId(mut_data.functions.len()));
            self.register_function(function_name, path, line);
        }
        *self.get_data().functions.get(function_name).unwrap()
    }

    fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
        let typ = self.to_raw_type(kind, lang_type);
        self.ensure_raw_type_id(typ)
    }

    fn ensure_raw_type_id(&mut self, typ: TypeRecord) -> TypeId {
        if !self.get_data().types.contains_key(&typ.lang_type) {
            let mut_data = self.get_mut_data();
            mut_data.types.insert(typ.lang_type.clone(), TypeId(mut_data.types.len()));
            self.register_raw_type(typ.clone());
        }
        *self.get_data().types.get(&typ.lang_type).unwrap()
    }

    fn ensure_variable_id(&mut self, variable_name: &str) -> VariableId {
        if !self.get_data().variables.contains_key(variable_name) {
            let mut_data = self.get_mut_data();
            mut_data.variables.insert(variable_name.to_string(), VariableId(mut_data.variables.len()));
            self.register_variable_name(variable_name);
        }
        *self.get_data().variables.get(variable_name).unwrap()
    }

    fn register_path(&mut self, path: &std::path::Path) {
        self.get_mut_data().path_list.push(path.to_path_buf());
        self.add_event(TraceLowLevelEvent::Path(path.to_path_buf()));
    }

    fn register_function(&mut self, name: &str, path: &std::path::Path, line: Line) {
        let path_id = self.ensure_path_id(path);
        self.get_mut_data().function_list.push((name.to_string(), path_id, line));
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

    fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>) {
        // register a step for each call, the backend expects this for
        // non-toplevel calls, so
        // we ensure it directly from register_call
        if function_id != TOP_LEVEL_FUNCTION_ID {
            for arg in &args {
                self.register_full_value(arg.variable_id, arg.value.clone());
            }
            let function = &self.get_data().function_list[function_id.0];
            self.add_event(TraceLowLevelEvent::Step(StepRecord {
                path_id: function.1,
                line: function.2,
            }));
        }
        // the actual call event:
        self.add_event(TraceLowLevelEvent::Call(CallRecord { function_id, args }));
    }

    fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord {
        let variable_id = self.ensure_variable_id(name);
        FullValueRecord { variable_id, value }
    }

    fn register_return(&mut self, return_value: ValueRecord) {
        self.add_event(TraceLowLevelEvent::Return(ReturnRecord { return_value }));
    }

    fn register_special_event(&mut self, kind: EventLogKind, content: &str) {
        self.add_event(TraceLowLevelEvent::Event(RecordEvent {
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
        self.add_event(TraceLowLevelEvent::Type(typ));
    }

    fn register_raw_type(&mut self, typ: TypeRecord) {
        self.add_event(TraceLowLevelEvent::Type(typ));
    }

    fn register_asm(&mut self, instructions: &[String]) {
        self.add_event(TraceLowLevelEvent::Asm(instructions.to_vec()));
    }

    fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
        let variable_id = self.ensure_variable_id(name);
        self.register_full_value(variable_id, value);
    }

    fn register_variable_name(&mut self, variable_name: &str) {
        self.add_event(TraceLowLevelEvent::VariableName(variable_name.to_string()));
    }

    fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord) {
        self.add_event(TraceLowLevelEvent::Value(FullValueRecord { variable_id, value }));
    }

    fn register_compound_value(&mut self, place: Place, value: ValueRecord) {
        self.add_event(TraceLowLevelEvent::CompoundValue(CompoundValueRecord { place, value }));
    }

    fn register_cell_value(&mut self, place: Place, value: ValueRecord) {
        self.add_event(TraceLowLevelEvent::CellValue(CellValueRecord { place, value }));
    }

    fn assign_compound_item(&mut self, place: Place, index: usize, item_place: Place) {
        self.add_event(TraceLowLevelEvent::AssignCompoundItem(AssignCompoundItemRecord {
            place,
            index,
            item_place,
        }));
    }

    fn assign_cell(&mut self, place: Place, new_value: ValueRecord) {
        self.add_event(TraceLowLevelEvent::AssignCell(AssignCellRecord { place, new_value }));
    }

    fn register_variable(&mut self, variable_name: &str, place: Place) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.add_event(TraceLowLevelEvent::VariableCell(VariableCellRecord { variable_id, place }));
    }

    fn drop_variable(&mut self, variable_name: &str) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.add_event(TraceLowLevelEvent::DropVariable(variable_id));
    }

    // history event helpers
    fn assign(&mut self, variable_name: &str, rvalue: RValue, pass_by: PassBy) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.add_event(TraceLowLevelEvent::Assignment(AssignmentRecord {
            to: variable_id,
            from: rvalue,
            pass_by,
        }));
    }

    fn bind_variable(&mut self, variable_name: &str, place: Place) {
        let variable_id = self.ensure_variable_id(variable_name);
        self.add_event(TraceLowLevelEvent::BindVariable(BindVariableRecord { variable_id, place }));
    }

    fn drop_variables(&mut self, variable_names: &[String]) {
        let variable_ids: Vec<VariableId> = variable_names
            .to_vec()
            .iter()
            .map(|variable_name| self.ensure_variable_id(variable_name))
            .collect();
        self.add_event(TraceLowLevelEvent::DropVariables(variable_ids))
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

    fn thread_start(&mut self, thread_id: ThreadId) {
        self.add_event(TraceLowLevelEvent::ThreadStart(thread_id));
    }

    fn thread_exit(&mut self, thread_id: ThreadId) {
        self.add_event(TraceLowLevelEvent::ThreadExit(thread_id));
    }

    fn thread_switch(&mut self, thread_id: ThreadId) {
        self.add_event(TraceLowLevelEvent::ThreadSwitch(thread_id));
    }

    fn drop_last_step(&mut self) {
        self.add_event(TraceLowLevelEvent::DropLastStep);
    }

    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(path) = &self.get_data().trace_metadata_path {
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

    fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(path) = &self.get_data().trace_paths_path {
            let json = serde_json::to_string(&self.get_data().path_list)?;
            fs::write(path, json)?;
            Ok(())
        } else {
            panic!("finish_writing_trace_paths() called without previous call to begin_writing_trace_paths()");
        }
    }
}
