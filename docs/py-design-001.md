# Python sys.monitoring Tracer Design

## Overview

This document outlines the design for integrating Python's `sys.monitoring` API with the `runtime_tracing` format. The goal is to produce CodeTracer-compatible traces for Python programs without modifying the interpreter.

The tracer collects `sys.monitoring` events, converts them to `runtime_tracing` events, and streams them to `trace.json`/`trace.bin` along with metadata and source snapshots.

## Architecture

### Tool Initialization
- Acquire a tool identifier via `sys.monitoring.use_tool_id`; store it for the lifetime of the tracer.
  ```rs
  pub const MONITORING_TOOL_NAME: &str = "codetracer";
  pub struct ToolId { pub id: u8 }
  pub fn acquire_tool_id() -> PyResult<ToolId>;
  ```
- Register one callback per event using `sys.monitoring.register_callback`.
  ```rs
  pub enum MonitoringEvent { PyStart, PyResume, PyReturn, PyYield, StopIteration, PyUnwind, PyThrow, Reraise, Call, Line, Instruction, Jump, Branch, Raise, ExceptionHandled, CReturn, CRaise }
  pub type CallbackFn = unsafe extern "C" fn(event: MonitoringEvent, frame: *mut PyFrameObject);
  pub fn register_callback(tool: &ToolId, event: MonitoringEvent, cb: CallbackFn);
  ```
- Enable all desired events by bitmask with `sys.monitoring.set_events`.
  ```rs
  pub const ALL_EVENTS_MASK: u64 = 0xffff;
  pub fn enable_events(tool: &ToolId, mask: u64);
  ```

### Writer Management
- Open a `runtime_tracing` writer (`trace.json` or `trace.bin`) during `start_tracing`.
  ```rs
  pub enum OutputFormat { Json, Binary }
  pub struct TraceWriter { pub format: OutputFormat }
  pub fn start_tracing(path: &Path, format: OutputFormat) -> io::Result<TraceWriter>;
  ```
- Expose methods to append metadata and file copies using existing `runtime_tracing` helpers.
  ```rs
  pub fn append_metadata(writer: &mut TraceWriter, meta: &TraceMetadata);
  pub fn copy_source_file(writer: &mut TraceWriter, path: &Path) -> io::Result<()>;
  ```
- Flush and close the writer when tracing stops.
  ```rs
  pub fn stop_tracing(writer: TraceWriter) -> io::Result<()>;
  ```

### Frame and Thread Tracking
- Maintain a per-thread stack of frame identifiers to correlate `CALL`, `PY_START`, and returns.
  ```rs
  pub type FrameId = u64;
  pub struct ThreadState { pub stack: Vec<FrameId> }
  pub fn current_thread_state() -> &'static mut ThreadState;
  ```
- Map `frame` objects to internal IDs for cross-referencing events.
  ```rs
  pub struct FrameRegistry { next: FrameId, map: HashMap<*mut PyFrameObject, FrameId> }
  pub fn intern_frame(reg: &mut FrameRegistry, frame: *mut PyFrameObject) -> FrameId;
  ```
- Record thread start/end events when a new thread registers callbacks.
  ```rs
  pub fn on_thread_start(thread_id: u64);
  pub fn on_thread_stop(thread_id: u64);
  ```

## Event Handling

Each bullet below represents a low-level operation translating a single `sys.monitoring` event into the `runtime_tracing` stream.

### Control Flow
- **PY_START** – Create a `Function` event for the code object and push a new frame ID onto the thread's stack.
  ```rs
  pub fn on_py_start(frame: *mut PyFrameObject);
  ```
- **PY_RESUME** – Emit an `Event` log noting resumption and update the current frame's state.
  ```rs
  pub fn on_py_resume(frame: *mut PyFrameObject);
  ```
- **PY_RETURN** – Pop the frame ID, write a `Return` event with the value (if retrievable), and link to the caller.
  ```rs
  pub struct ReturnRecord { pub frame: FrameId, pub value: Option<ValueRecord> }
  pub fn on_py_return(frame: *mut PyFrameObject, value: *mut PyObject);
  ```
- **PY_YIELD** – Record a `Return` event flagged as a yield and keep the frame on the stack for later resumes.
  ```rs
  pub fn on_py_yield(frame: *mut PyFrameObject, value: *mut PyObject);
  ```
- **STOP_ITERATION** – Emit an `Event` indicating iteration exhaustion for the current frame.
  ```rs
  pub fn on_stop_iteration(frame: *mut PyFrameObject);
  ```
- **PY_UNWIND** – Mark the beginning of stack unwinding and note the target handler in an `Event`.
  ```rs
  pub fn on_py_unwind(frame: *mut PyFrameObject);
  ```
- **PY_THROW** – Emit an `Event` describing the thrown value and the target generator/coroutine.
  ```rs
  pub fn on_py_throw(frame: *mut PyFrameObject, value: *mut PyObject);
  ```
- **RERAISE** – Log a re-raise event referencing the original exception.
  ```rs
  pub fn on_reraise(frame: *mut PyFrameObject, exc: *mut PyObject);
  ```

### Call and Line Tracking
- **CALL** – Record a `Call` event, capturing argument values and the callee's `Function` ID.
  ```rs
  pub fn on_call(callee: *mut PyObject, args: &PyTupleObject) -> FrameId;
  ```
- **LINE** – Write a `Step` event with current path and line number; ensure the path is registered.
  ```rs
  pub fn on_line(frame: *mut PyFrameObject, lineno: u32);
  ```
- **INSTRUCTION** – Optionally emit a fine-grained `Event` containing the opcode name for detailed traces.
  ```rs
  pub fn on_instruction(frame: *mut PyFrameObject, opcode: u8);
  ```
- **JUMP** – Append an `Event` describing the jump target offset for control-flow visualization.
  ```rs
  pub fn on_jump(frame: *mut PyFrameObject, target: u32);
  ```
- **BRANCH** – Record an `Event` with branch outcome (taken or not) to aid coverage analysis.
  ```rs
  pub fn on_branch(frame: *mut PyFrameObject, taken: bool);
  ```

### Exception Lifecycle
- **RAISE** – Emit an `Event` containing exception type and message when raised.
  ```rs
  pub fn on_raise(frame: *mut PyFrameObject, exc: *mut PyObject);
  ```
- **EXCEPTION_HANDLED** – Log an `Event` marking when an exception is caught.
  ```rs
  pub fn on_exception_handled(frame: *mut PyFrameObject);
  ```

### C API Boundary
- **C_RETURN** – On returning from a C function, emit a `Return` event tagged as foreign and include result summary.
  ```rs
  pub fn on_c_return(func: *mut PyObject, result: *mut PyObject);
  ```
- **C_RAISE** – When a C function raises, record an `Event` with the exception info and current frame ID.
  ```rs
  pub fn on_c_raise(func: *mut PyObject, exc: *mut PyObject);
  ```

### No Events
- **NO_EVENTS** – Special constant; used only to disable monitoring. No runtime event is produced.
  ```rs
  pub const NO_EVENTS: u64 = 0;
  ```

## Metadata and File Capture
- Collect the working directory, program name, and arguments and store them in `trace_metadata.json`.
  ```rs
  pub struct TraceMetadata { pub cwd: PathBuf, pub program: String, pub args: Vec<String> }
  pub fn write_metadata(writer: &mut TraceWriter, meta: &TraceMetadata);
  ```
- Track every file path referenced; copy each into the trace directory under `files/`.
  ```rs
  pub fn track_file(writer: &mut TraceWriter, path: &Path) -> io::Result<()>;
  ```
- Record `VariableName`, `Type`, and `Value` entries when variables are inspected or logged.
  ```rs
  pub struct VariableRecord { pub name: String, pub ty: TypeId, pub value: ValueRecord }
  pub fn record_variable(writer: &mut TraceWriter, rec: VariableRecord);
  ```

## Value Translation and Recording
- Maintain a type registry that maps Python `type` objects to `runtime_tracing` `Type` entries and assigns new `type_id` values on first encounter.
  ```rs
  pub type TypeId = u32;
  pub type ValueId = u64;
  pub enum ValueRecord { Int(i64), Float(f64), Bool(bool), None, Str(String), Raw(Vec<u8>), Sequence(Vec<ValueRecord>), Tuple(Vec<ValueRecord>), Struct(Vec<(String, ValueRecord)>), Reference(ValueId) }
  pub struct TypeRegistry { next: TypeId, map: HashMap<*mut PyTypeObject, TypeId> }
  pub fn intern_type(reg: &mut TypeRegistry, ty: *mut PyTypeObject) -> TypeId;
  ```
- Convert primitives (`int`, `float`, `bool`, `None`, `str`) directly to their corresponding `ValueRecord` variants.
  ```rs
  pub fn encode_primitive(obj: *mut PyObject) -> Option<ValueRecord>;
  ```
- Encode `bytes` and `bytearray` as `Raw` records containing base64 text to preserve binary data.
  ```rs
  pub fn encode_bytes(obj: *mut PyObject) -> ValueRecord;
  ```
- Represent lists and sets as `Sequence` records and tuples as `Tuple` records, converting each element recursively.
  ```rs
  pub fn encode_sequence(iter: &PySequence) -> ValueRecord;
  pub fn encode_tuple(tuple: &PyTupleObject) -> ValueRecord;
  ```
- Serialize dictionaries as a `Sequence` of two-element `Tuple` records for key/value pairs to avoid fixed field layouts.
  ```rs
  pub fn encode_dict(dict: &PyDictObject) -> ValueRecord;
  ```
- For objects with accessible attributes, emit a `Struct` record with sorted field names; fall back to `Raw` with `repr(obj)` when inspection is unsafe.
  ```rs
  pub fn encode_object(obj: *mut PyObject) -> ValueRecord;
  ```
- Track object identities to detect cycles and reuse `Reference` records with `id(obj)` for repeated structures.
  ```rs
  pub struct SeenSet { map: HashMap<usize, ValueId> }
  pub fn record_reference(seen: &mut SeenSet, obj: *mut PyObject) -> Option<ValueRecord>;
  ```

## Shutdown
- On `stop_tracing`, call `sys.monitoring.set_events` with `NO_EVENTS` for the tool ID.
  ```rs
  pub fn disable_events(tool: &ToolId);
  ```
- Unregister callbacks and free the tool ID with `sys.monitoring.free_tool_id`.
  ```rs
  pub fn unregister_callbacks(tool: ToolId);
  pub fn free_tool_id(tool: ToolId);
  ```
- Close the writer and ensure all buffered events are flushed to disk.
  ```rs
  pub fn finalize(writer: TraceWriter) -> io::Result<()>;
  ```

## Current Limitations
- **No structured support for threads or async tasks** – the trace format lacks explicit identifiers for concurrent execution.
  Distinguishing events emitted by different Python threads or `asyncio` tasks requires ad hoc `Event` entries, complicating
  analysis and preventing downstream tools from reasoning about scheduling.
- **Generic `Event` log** – several `sys.monitoring` notifications like resume, unwind, and branch outcomes have no dedicated
  `runtime_tracing` variant. They must be encoded as free‑form `Event` logs, which reduces machine readability and hinders
  automation.
- **Heavy value snapshots** – arguments and returns expect full `ValueRecord` structures. Serializing arbitrary Python objects is
  expensive and often degrades to lossy string dumps, limiting the visibility of rich runtime state.
- **Append‑only path and function tables** – `runtime_tracing` assumes files and functions are discovered once and never change.
  Dynamically generated code (`eval`, REPL snippets) forces extra bookkeeping and cannot update earlier entries, making
  dynamic features awkward to trace.
- **No built‑in compression or streaming** – traces are written as monolithic JSON or binary files. Long sessions quickly grow in
  size and cannot be streamed to remote consumers without additional tooling.

## Future Extensions
- Add filtering to enable subsets of events for performance-sensitive scenarios.
- Support streaming traces over a socket for live debugging.
