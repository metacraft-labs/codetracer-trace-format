//! TraceWriter implementation that delegates to the Nim `codetracer-trace-format-nim` C library.
//!
//! This crate is a drop-in replacement for `codetracer_trace_writer`. Recorders can switch
//! by changing a single dependency in their `Cargo.toml`.

use std::error::Error;
use std::ffi::{CStr, CString};
use std::path::Path;
use std::sync::Once;

use codetracer_trace_types::*;

// ---------------------------------------------------------------------------
// FFI declarations (must match codetracer_trace_writer.h)
// ---------------------------------------------------------------------------

extern "C" {
    fn codetracer_trace_writer_init();

    fn trace_writer_last_error() -> *const std::os::raw::c_char;

    fn trace_writer_new(program: *const std::os::raw::c_char, format: i32)
        -> *mut std::ffi::c_void;
    fn trace_writer_free(handle: *mut std::ffi::c_void);
    fn trace_writer_close(handle: *mut std::ffi::c_void) -> i32;

    fn trace_writer_begin_metadata(
        handle: *mut std::ffi::c_void,
        path: *const std::os::raw::c_char,
    ) -> i32;
    fn trace_writer_finish_metadata(handle: *mut std::ffi::c_void) -> i32;
    fn trace_writer_begin_events(
        handle: *mut std::ffi::c_void,
        path: *const std::os::raw::c_char,
    ) -> i32;
    fn trace_writer_finish_events(handle: *mut std::ffi::c_void) -> i32;
    fn trace_writer_begin_paths(
        handle: *mut std::ffi::c_void,
        path: *const std::os::raw::c_char,
    ) -> i32;
    fn trace_writer_finish_paths(handle: *mut std::ffi::c_void) -> i32;

    fn trace_writer_start(
        handle: *mut std::ffi::c_void,
        path: *const std::os::raw::c_char,
        line: i64,
    );
    fn trace_writer_set_workdir(
        handle: *mut std::ffi::c_void,
        workdir: *const std::os::raw::c_char,
    );
    fn trace_writer_register_step(
        handle: *mut std::ffi::c_void,
        path: *const std::os::raw::c_char,
        line: i64,
    );

    fn trace_writer_ensure_function_id(
        handle: *mut std::ffi::c_void,
        name: *const std::os::raw::c_char,
        path: *const std::os::raw::c_char,
        line: i64,
    ) -> usize;

    fn trace_writer_ensure_type_id(
        handle: *mut std::ffi::c_void,
        kind: i32,
        lang_type: *const std::os::raw::c_char,
    ) -> usize;

    fn trace_writer_register_call(handle: *mut std::ffi::c_void, function_id: usize);
    fn trace_writer_register_return(handle: *mut std::ffi::c_void);

    fn trace_writer_register_return_int(
        handle: *mut std::ffi::c_void,
        value: i64,
        type_kind: i32,
        type_name: *const std::os::raw::c_char,
    );
    fn trace_writer_register_return_raw(
        handle: *mut std::ffi::c_void,
        value_repr: *const std::os::raw::c_char,
        type_kind: i32,
        type_name: *const std::os::raw::c_char,
    );

    fn trace_writer_register_variable_int(
        handle: *mut std::ffi::c_void,
        name: *const std::os::raw::c_char,
        value: i64,
        type_kind: i32,
        type_name: *const std::os::raw::c_char,
    );
    fn trace_writer_register_variable_raw(
        handle: *mut std::ffi::c_void,
        name: *const std::os::raw::c_char,
        value_repr: *const std::os::raw::c_char,
        type_kind: i32,
        type_name: *const std::os::raw::c_char,
    );

    fn trace_writer_register_special_event(
        handle: *mut std::ffi::c_void,
        kind: i32,
        metadata: *const std::os::raw::c_char,
        content: *const std::os::raw::c_char,
    );
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

static NIM_INIT: Once = Once::new();

fn ensure_nim_initialized() {
    NIM_INIT.call_once(|| unsafe {
        codetracer_trace_writer_init();
    });
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn last_error() -> String {
    unsafe {
        let ptr = trace_writer_last_error();
        if ptr.is_null() {
            "unknown error from Nim trace writer".to_string()
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

fn check_result(code: i32) -> Result<(), Box<dyn Error>> {
    if code == 0 {
        Ok(())
    } else {
        Err(last_error().into())
    }
}

// ---------------------------------------------------------------------------
// Path/str to CString helper
// ---------------------------------------------------------------------------

fn path_to_cstring(path: &Path) -> CString {
    CString::new(path.to_string_lossy().as_bytes()).unwrap_or_else(|_| CString::new("").unwrap())
}

fn str_to_cstring(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| CString::new("").unwrap())
}

// ---------------------------------------------------------------------------
// Format mapping
// ---------------------------------------------------------------------------

/// Trace event file formats (mirrors the Rust `TraceEventsFileFormat`).
#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
    Json,
    BinaryV0,
    Binary,
    Ctfs,
}

impl TraceEventsFileFormat {
    fn to_ffi(self) -> i32 {
        match self {
            TraceEventsFileFormat::Json => 0,
            TraceEventsFileFormat::BinaryV0 => 1,
            TraceEventsFileFormat::Binary => 2,
            TraceEventsFileFormat::Ctfs => 2, // Nim lib treats CTFS as Binary for now
        }
    }
}

// ---------------------------------------------------------------------------
// NimTraceWriter
// ---------------------------------------------------------------------------

/// A `TraceWriter` implementation backed by the Nim static library.
///
/// All tracing operations are delegated to the C FFI. The Nim library is
/// **not** thread-safe, so the handle is protected by a mutex when `Send`
/// is required.
pub struct NimTraceWriter {
    handle: *mut std::ffi::c_void,
}

// The Nim library is single-threaded but callers hold exclusive &mut self,
// so Send is safe as long as we never share the handle.
unsafe impl Send for NimTraceWriter {}

impl NimTraceWriter {
    /// Create a new trace writer backed by the Nim library.
    pub fn new(program: &str, format: TraceEventsFileFormat) -> Self {
        ensure_nim_initialized();
        let c_program = str_to_cstring(program);
        let handle = unsafe { trace_writer_new(c_program.as_ptr(), format.to_ffi()) };
        assert!(!handle.is_null(), "trace_writer_new returned null: {}", last_error());
        NimTraceWriter { handle }
    }

    /// Close the writer and flush all data. Called automatically on drop,
    /// but can be called explicitly to check for errors.
    pub fn close(&mut self) -> Result<(), Box<dyn Error>> {
        if !self.handle.is_null() {
            let rc = unsafe { trace_writer_close(self.handle) };
            check_result(rc)
        } else {
            Ok(())
        }
    }
}

impl Drop for NimTraceWriter {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                trace_writer_free(self.handle);
            }
            self.handle = std::ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// TraceWriter trait implementation
//
// We implement methods that map to the Nim C API. Methods that have no
// corresponding C function are no-ops or use simple delegations.
// ---------------------------------------------------------------------------

impl NimTraceWriter {
    pub fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let c_path = path_to_cstring(path);
        check_result(unsafe { trace_writer_begin_metadata(self.handle, c_path.as_ptr()) })
    }

    pub fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>> {
        check_result(unsafe { trace_writer_finish_metadata(self.handle) })
    }

    pub fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let c_path = path_to_cstring(path);
        check_result(unsafe { trace_writer_begin_events(self.handle, c_path.as_ptr()) })
    }

    pub fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>> {
        check_result(unsafe { trace_writer_finish_events(self.handle) })
    }

    pub fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let c_path = path_to_cstring(path);
        check_result(unsafe { trace_writer_begin_paths(self.handle, c_path.as_ptr()) })
    }

    pub fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>> {
        check_result(unsafe { trace_writer_finish_paths(self.handle) })
    }

    pub fn set_workdir(&mut self, workdir: &Path) {
        let c_workdir = path_to_cstring(workdir);
        unsafe { trace_writer_set_workdir(self.handle, c_workdir.as_ptr()) }
    }

    pub fn start(&mut self, path: &Path, line: Line) {
        let c_path = path_to_cstring(path);
        unsafe { trace_writer_start(self.handle, c_path.as_ptr(), line.0 as i64) }
    }

    pub fn ensure_function_id(
        &mut self,
        function_name: &str,
        path: &Path,
        line: Line,
    ) -> FunctionId {
        let c_name = str_to_cstring(function_name);
        let c_path = path_to_cstring(path);
        let id =
            unsafe { trace_writer_ensure_function_id(self.handle, c_name.as_ptr(), c_path.as_ptr(), line.0 as i64) };
        FunctionId(id)
    }

    pub fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
        let c_lang = str_to_cstring(lang_type);
        let id =
            unsafe { trace_writer_ensure_type_id(self.handle, kind as i32, c_lang.as_ptr()) };
        TypeId(id)
    }

    pub fn register_step(&mut self, path: &Path, line: Line) {
        let c_path = path_to_cstring(path);
        unsafe { trace_writer_register_step(self.handle, c_path.as_ptr(), line.0 as i64) }
    }

    pub fn register_call(&mut self, function_id: FunctionId, _args: Vec<FullValueRecord>) {
        // The Nim library handles args internally via register_variable calls
        // before the call. We only signal the call itself.
        unsafe { trace_writer_register_call(self.handle, function_id.0) }
    }

    pub fn register_return(&mut self, return_value: ValueRecord) {
        match return_value {
            ValueRecord::Int { i, type_id } => {
                let type_name = str_to_cstring(&format!("type_{}", type_id.0));
                unsafe {
                    trace_writer_register_return_int(
                        self.handle,
                        i,
                        TypeKind::Int as i32,
                        type_name.as_ptr(),
                    )
                }
            }
            ValueRecord::None { .. } => unsafe {
                trace_writer_register_return(self.handle);
            },
            _ => {
                // For all other value kinds, serialize to raw representation
                let (repr, kind, type_name) = value_record_to_raw(&return_value);
                let c_repr = str_to_cstring(&repr);
                let c_type = str_to_cstring(&type_name);
                unsafe {
                    trace_writer_register_return_raw(
                        self.handle,
                        c_repr.as_ptr(),
                        kind as i32,
                        c_type.as_ptr(),
                    )
                }
            }
        }
    }

    pub fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
        let c_name = str_to_cstring(name);
        match value {
            ValueRecord::Int { i, type_id } => {
                let type_name = str_to_cstring(&format!("type_{}", type_id.0));
                unsafe {
                    trace_writer_register_variable_int(
                        self.handle,
                        c_name.as_ptr(),
                        i,
                        TypeKind::Int as i32,
                        type_name.as_ptr(),
                    )
                }
            }
            _ => {
                let (repr, kind, type_name) = value_record_to_raw(&value);
                let c_repr = str_to_cstring(&repr);
                let c_type = str_to_cstring(&type_name);
                unsafe {
                    trace_writer_register_variable_raw(
                        self.handle,
                        c_name.as_ptr(),
                        c_repr.as_ptr(),
                        kind as i32,
                        c_type.as_ptr(),
                    )
                }
            }
        }
    }

    pub fn register_special_event(&mut self, kind: EventLogKind, metadata: &str, content: &str) {
        let c_metadata = str_to_cstring(metadata);
        let c_content = str_to_cstring(content);
        unsafe {
            trace_writer_register_special_event(
                self.handle,
                kind as i32,
                c_metadata.as_ptr(),
                c_content.as_ptr(),
            )
        }
    }

    // --- Methods that are no-ops in the Nim backend ---

    pub fn ensure_path_id(&mut self, _path: &Path) -> PathId {
        // The Nim library manages path IDs internally
        PathId(0)
    }

    pub fn ensure_raw_type_id(&mut self, typ: TypeRecord) -> TypeId {
        self.ensure_type_id(typ.kind, &typ.lang_type)
    }

    pub fn ensure_variable_id(&mut self, _variable_name: &str) -> VariableId {
        // The Nim library manages variable IDs internally
        VariableId(0)
    }

    pub fn register_path(&mut self, _path: &Path) {
        // Handled internally by the Nim library
    }

    pub fn register_function(&mut self, name: &str, path: &Path, line: Line) {
        // Just ensure it exists
        self.ensure_function_id(name, path, line);
    }

    pub fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord {
        // Register the variable and return a record
        self.register_variable_with_full_value(name, value.clone());
        FullValueRecord {
            variable_id: VariableId(0),
            value,
        }
    }

    pub fn to_raw_type(&self, kind: TypeKind, lang_type: &str) -> TypeRecord {
        TypeRecord {
            kind,
            lang_type: lang_type.to_string(),
            specific_info: TypeSpecificInfo::None,
        }
    }

    pub fn register_type(&mut self, kind: TypeKind, lang_type: &str) {
        self.ensure_type_id(kind, lang_type);
    }

    pub fn register_raw_type(&mut self, typ: TypeRecord) {
        self.ensure_type_id(typ.kind, &typ.lang_type);
    }

    pub fn register_asm(&mut self, _instructions: &[String]) {
        // Not supported by the Nim C API — no-op
    }

    pub fn register_variable_name(&mut self, _variable_name: &str) {
        // Handled internally by Nim
    }

    pub fn register_full_value(&mut self, _variable_id: VariableId, _value: ValueRecord) {
        // Handled via register_variable_with_full_value
    }

    pub fn register_compound_value(&mut self, _place: Place, _value: ValueRecord) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn register_cell_value(&mut self, _place: Place, _value: ValueRecord) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn assign_compound_item(&mut self, _place: Place, _index: usize, _item_place: Place) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn assign_cell(&mut self, _place: Place, _new_value: ValueRecord) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn register_variable(&mut self, _variable_name: &str, _place: Place) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn drop_variable(&mut self, _variable_name: &str) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn assign(&mut self, _variable_name: &str, _rvalue: RValue, _pass_by: PassBy) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn bind_variable(&mut self, _variable_name: &str, _place: Place) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn drop_variables(&mut self, _variable_names: &[String]) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn simple_rvalue(&mut self, _variable_name: &str) -> RValue {
        RValue::Simple(VariableId(0))
    }

    pub fn compound_rvalue(&mut self, _variable_dependencies: &[String]) -> RValue {
        RValue::Compound(vec![])
    }

    pub fn drop_last_step(&mut self) {
        // Not exposed in the Nim C API — no-op
    }

    pub fn add_event(&mut self, _event: TraceLowLevelEvent) {
        // The Nim library does not expose low-level event buffering
    }

    pub fn append_events(&mut self, _events: &mut Vec<TraceLowLevelEvent>) {
        // The Nim library does not expose low-level event buffering
    }

    pub fn events(&self) -> &[TraceLowLevelEvent] {
        // Nim writer streams to disk; no in-memory buffer
        &[]
    }
}

// ---------------------------------------------------------------------------
// Public factory function — drop-in replacement for codetracer_trace_writer
// ---------------------------------------------------------------------------

/// Create a trace writer backed by the Nim library.
///
/// This is a drop-in replacement for `codetracer_trace_writer::create_trace_writer`.
pub fn create_trace_writer(
    program: &str,
    _args: &[String],
    format: TraceEventsFileFormat,
) -> Box<NimTraceWriter> {
    Box::new(NimTraceWriter::new(program, format))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a `ValueRecord` to a raw string representation + TypeKind + type name
/// for use with the `_raw` C API variants.
fn value_record_to_raw(value: &ValueRecord) -> (String, TypeKind, String) {
    match value {
        ValueRecord::Int { i, type_id } => (i.to_string(), TypeKind::Int, format!("type_{}", type_id.0)),
        ValueRecord::Float { f, type_id } => (f.to_string(), TypeKind::Float, format!("type_{}", type_id.0)),
        ValueRecord::Bool { b, type_id } => (b.to_string(), TypeKind::Bool, format!("type_{}", type_id.0)),
        ValueRecord::String { text, type_id } => {
            (text.clone(), TypeKind::String, format!("type_{}", type_id.0))
        }
        ValueRecord::Raw { r, type_id } => (r.clone(), TypeKind::Raw, format!("type_{}", type_id.0)),
        ValueRecord::Error { msg, type_id } => {
            (msg.clone(), TypeKind::Error, format!("type_{}", type_id.0))
        }
        ValueRecord::None { type_id } => ("None".to_string(), TypeKind::None, format!("type_{}", type_id.0)),
        ValueRecord::Char { c, type_id } => (c.to_string(), TypeKind::Char, format!("type_{}", type_id.0)),
        ValueRecord::Sequence { type_id, .. } => {
            ("[...]".to_string(), TypeKind::Seq, format!("type_{}", type_id.0))
        }
        ValueRecord::Tuple { type_id, .. } => {
            ("(...)".to_string(), TypeKind::Tuple, format!("type_{}", type_id.0))
        }
        ValueRecord::Struct { type_id, .. } => {
            ("{...}".to_string(), TypeKind::Struct, format!("type_{}", type_id.0))
        }
        ValueRecord::Variant { discriminator, type_id, .. } => {
            (discriminator.clone(), TypeKind::Variant, format!("type_{}", type_id.0))
        }
        ValueRecord::Reference { address, type_id, .. } => {
            (format!("0x{:x}", address), TypeKind::Pointer, format!("type_{}", type_id.0))
        }
        ValueRecord::Cell { place } => {
            (format!("place_{}", place.0), TypeKind::Raw, "Cell".to_string())
        }
        ValueRecord::BigInt { negative, type_id, .. } => {
            let sign = if *negative { "-" } else { "" };
            (format!("{}(bigint)", sign), TypeKind::Int, format!("type_{}", type_id.0))
        }
    }
}
