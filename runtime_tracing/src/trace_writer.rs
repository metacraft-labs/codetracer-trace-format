use std::{error::Error, path::Path};

use crate::{
    EventLogKind, FullValueRecord, FunctionId, Line, PassBy, PathId, Place, RValue, TraceLowLevelEvent, TypeId, TypeKind, TypeRecord, ValueRecord,
    VariableId, abstract_trace_writer::AbstractTraceWriter,
};

pub trait TraceWriter: AbstractTraceWriter {
    fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        AbstractTraceWriter::begin_writing_trace_metadata(self, path)
    }
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
    fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        AbstractTraceWriter::begin_writing_trace_paths(self, path)
    }

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

    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>> {
        AbstractTraceWriter::finish_writing_trace_metadata(self)
    }
    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>>;
    fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>> {
        AbstractTraceWriter::finish_writing_trace_paths(self)
    }
}
