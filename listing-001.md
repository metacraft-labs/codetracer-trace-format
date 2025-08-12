# Listing 1: Trace format selection and writer/reader interfaces

This listing examines the public API surface that lets clients choose a trace format, create reader or writer instances, and record events. It covers `TraceEventsFileFormat` and related constants, factory functions, the `TraceReader` trait, `NonStreamingTraceWriter` structure, and the comprehensive `TraceWriter` trait.

**Start the enumeration of supported trace file formats.**
```rust
#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
```

**List JSON and two binary formats.**
```rust
    Json,
    BinaryV0,
    Binary,
}
```

**Provide constants for representing missing values and the top-level function.**
```rust
pub const NONE_TYPE_ID: TypeId = TypeId(0);
pub const NONE_VALUE: ValueRecord = ValueRecord::None { type_id: NONE_TYPE_ID };
pub const TOP_LEVEL_FUNCTION_ID: FunctionId = FunctionId(0);
```

**Construct a trace reader for a given format.**
```rust
pub fn create_trace_reader(format: TraceEventsFileFormat) -> Box<dyn TraceReader> {
```

**Match on the format to instantiate the proper reader.**
```rust
    match format {
        TraceEventsFileFormat::Json => Box::new(JsonTraceReader {}),
        TraceEventsFileFormat::BinaryV0 | TraceEventsFileFormat::Binary => Box::new(BinaryTraceReader {}),
    }
}
```

**Construct a trace writer for a program, arguments, and format.**
```rust
pub fn create_trace_writer(program: &str, args: &[String], format: TraceEventsFileFormat) -> Box<dyn TraceWriter> {
```

**Begin matching on the requested format.**
```rust
    match format {
```

**Use the in-memory writer for JSON and legacy binary formats.**
```rust
        TraceEventsFileFormat::Json | TraceEventsFileFormat::BinaryV0 => {
            let mut result = Box::new(NonStreamingTraceWriter::new(program, args));
            result.set_format(format);
            result
        }
```

**Produce a streaming CBOR writer for the current binary format and return.**
```rust
        TraceEventsFileFormat::Binary => Box::new(crate::cbor_zstd_writer::CborZstdTraceWriter::new(program, args)),
    }
}
```

**Define the trait for loading stored traces.**
```rust
pub trait TraceReader {
    fn load_trace_events(&mut self, path: &Path) -> Result<Vec<TraceLowLevelEvent>, Box<dyn Error>>;
}
```

**Begin declaration of a writer that accumulates events in memory.**
```rust
pub struct NonStreamingTraceWriter {
```

**Fields track metadata, collected events, format, and output path.**
```rust
    base: AbstractTraceWriterData,
    pub events: Vec<TraceLowLevelEvent>,
    format: TraceEventsFileFormat,
    trace_events_path: Option<PathBuf>,
}
```

**Implement constructor initializing storage and format.**
```rust
impl NonStreamingTraceWriter {
    pub fn new(program: &str, args: &[String]) -> Self {
        NonStreamingTraceWriter {
            base: AbstractTraceWriterData::new(program, args),
```

**Set up empty event list, default binary format, and unset path.**
```rust
            events: vec![],
            format: TraceEventsFileFormat::Binary,
            trace_events_path: None,
        }
    }
```

**Allow callers to override output format.**
```rust
    pub fn set_format(&mut self, format: TraceEventsFileFormat) {
        self.format = format;
    }
}
```

**Declare the `TraceWriter` trait which extends `AbstractTraceWriter`.**
```rust
pub trait TraceWriter: AbstractTraceWriter {
```

**Provide default metadata handling and leave event writing abstract.**
```rust
    fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        AbstractTraceWriter::begin_writing_trace_metadata(self, path)
    }
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
```

**Delegate path file initialization to the underlying writer.**
```rust
    fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        AbstractTraceWriter::begin_writing_trace_paths(self, path)
    }
```

**Start tracing and request identifiers for paths.**
```rust
    fn start(&mut self, path: &Path, line: Line) {
        AbstractTraceWriter::start(self, path, line)
    }
    fn ensure_path_id(&mut self, path: &Path) -> PathId {
        AbstractTraceWriter::ensure_path_id(self, path)
    }
```

**Allocate a unique identifier for a function.**
```rust
    fn ensure_function_id(&mut self, function_name: &str, path: &Path, line: Line) -> FunctionId {
        AbstractTraceWriter::ensure_function_id(self, function_name, path, line)
    }
```

**Allocate type identifiers, either by components or explicit record.**
```rust
    fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
        AbstractTraceWriter::ensure_type_id(self, kind, lang_type)
    }
    fn ensure_raw_type_id(&mut self, typ: TypeRecord) -> TypeId {
        AbstractTraceWriter::ensure_raw_type_id(self, typ)
    }
```

**Intern variable names for later reference.**
```rust
    fn ensure_variable_id(&mut self, variable_name: &str) -> VariableId {
        AbstractTraceWriter::ensure_variable_id(self, variable_name)
    }
```

**Register source file paths and function declarations.**
```rust
    fn register_path(&mut self, path: &Path) {
        AbstractTraceWriter::register_path(self, path)
    }
    fn register_function(&mut self, name: &str, path: &Path, line: Line) {
        AbstractTraceWriter::register_function(self, name, path, line)
    }
```

**Capture line execution and function invocation with arguments.**
```rust
    fn register_step(&mut self, path: &Path, line: Line) {
        AbstractTraceWriter::register_step(self, path, line)
    }
    fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>) {
        AbstractTraceWriter::register_call(self, function_id, args)
    }
```

**Helper to build an argument record and note function returns.**
```rust
    fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord {
        AbstractTraceWriter::arg(self, name, value)
    }
    fn register_return(&mut self, return_value: ValueRecord) {
        AbstractTraceWriter::register_return(self, return_value)
    }
```

**Record ad-hoc events within the trace.**
```rust
    // TODO: add metadata arg
    fn register_special_event(&mut self, kind: EventLogKind, content: &str) {
        AbstractTraceWriter::register_special_event(self, kind, content)
    }
```

**Convert type descriptions into records and register them.**
```rust
    fn to_raw_type(&self, kind: TypeKind, lang_type: &str) -> TypeRecord {
        AbstractTraceWriter::to_raw_type(self, kind, lang_type)
    }
    fn register_type(&mut self, kind: TypeKind, lang_type: &str) {
        AbstractTraceWriter::register_type(self, kind, lang_type)
    }
```

**Support registering arbitrary type records and assembly snippets.**
```rust
    fn register_raw_type(&mut self, typ: TypeRecord) {
        AbstractTraceWriter::register_raw_type(self, typ)
    }
    fn register_asm(&mut self, instructions: &[String]) {
        AbstractTraceWriter::register_asm(self, instructions)
    }
```

**Store a variable and its value in one step.**
```rust
    fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
        AbstractTraceWriter::register_variable_with_full_value(self, name, value)
    }
```

**Separate registration of variable names and associated values.**
```rust
    fn register_variable_name(&mut self, variable_name: &str) {
        AbstractTraceWriter::register_variable_name(self, variable_name)
    }
    fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord) {
        AbstractTraceWriter::register_full_value(self, variable_id, value)
    }
```

**Emit compound values and individual cells.**
```rust
    fn register_compound_value(&mut self, place: Place, value: ValueRecord) {
        AbstractTraceWriter::register_compound_value(self, place, value)
    }
    fn register_cell_value(&mut self, place: Place, value: ValueRecord) {
        AbstractTraceWriter::register_cell_value(self, place, value)
    }
```

**Describe updates to compound items or cell contents.**
```rust
    fn assign_compound_item(&mut self, place: Place, index: usize, item_place: Place) {
        AbstractTraceWriter::assign_compound_item(self, place, index, item_place)
    }
    fn assign_cell(&mut self, place: Place, new_value: ValueRecord) {
        AbstractTraceWriter::assign_cell(self, place, new_value)
    }
```

**Track variables pointing to places and their removal.**
```rust
    fn register_variable(&mut self, variable_name: &str, place: Place) {
        AbstractTraceWriter::register_variable(self, variable_name, place)
    }
    fn drop_variable(&mut self, variable_name: &str) {
        AbstractTraceWriter::drop_variable(self, variable_name)
    }
```

**Record an assignment with explicit pass-by semantics.**
```rust
    fn assign(&mut self, variable_name: &str, rvalue: RValue, pass_by: PassBy) {
        AbstractTraceWriter::assign(self, variable_name, rvalue, pass_by)
    }
```

**Bind variables to storage locations or drop multiple at once.**
```rust
    fn bind_variable(&mut self, variable_name: &str, place: Place) {
        AbstractTraceWriter::bind_variable(self, variable_name, place)
    }
    fn drop_variables(&mut self, variable_names: &[String]) {
        AbstractTraceWriter::drop_variables(self, variable_names)
    }
```

**Build an rvalue from a single variable.**
```rust
    fn simple_rvalue(&mut self, variable_name: &str) -> RValue {
        AbstractTraceWriter::simple_rvalue(self, variable_name)
    }
```

**Build an rvalue from multiple variable dependencies.**
```rust
    fn compound_rvalue(&mut self, variable_dependencies: &[String]) -> RValue {
        AbstractTraceWriter::compound_rvalue(self, variable_dependencies)
    }
```

**Remove the last step event when necessary.**
```rust
    fn drop_last_step(&mut self) {
        AbstractTraceWriter::drop_last_step(self)
    }
```

**Insert an event directly into the stream.**
```rust
    fn add_event(&mut self, event: TraceLowLevelEvent) {
        AbstractTraceWriter::add_event(self, event)
    }
```

**Append multiple events in one call.**
```rust
    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        AbstractTraceWriter::append_events(self, events)
    }
```

**Finish writing metadata before finalizing events and paths.**
```rust
    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>> {
        AbstractTraceWriter::finish_writing_trace_metadata(self)
    }
```

**Leave event completion abstract and provide default path finalization.**
```rust
    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>>;
    fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>> {
        AbstractTraceWriter::finish_writing_trace_paths(self)
    }
}
```
