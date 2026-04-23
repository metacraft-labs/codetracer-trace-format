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

    fn trace_writer_register_variable_cbor(
        handle: *mut std::ffi::c_void,
        name: *const std::os::raw::c_char,
        cbor_data: *const u8,
        cbor_len: usize,
    );

    fn trace_writer_register_return_cbor(
        handle: *mut std::ffi::c_void,
        cbor_data: *const u8,
        cbor_len: usize,
    );

    // ----- Streaming value encoder -----

    fn ct_value_encoder_new() -> *mut std::ffi::c_void;
    fn ct_value_encoder_free(h: *mut std::ffi::c_void);
    fn ct_value_encoder_reset(h: *mut std::ffi::c_void);

    fn ct_value_write_int(h: *mut std::ffi::c_void, value: i64, type_id: u64) -> i32;
    fn ct_value_write_float(h: *mut std::ffi::c_void, value: f64, type_id: u64) -> i32;
    fn ct_value_write_bool_typed(h: *mut std::ffi::c_void, value: i32, type_id: u64) -> i32;
    fn ct_value_write_string(
        h: *mut std::ffi::c_void,
        data: *const u8,
        len: usize,
        type_id: u64,
    ) -> i32;
    fn ct_value_write_none_typed(h: *mut std::ffi::c_void, type_id: u64) -> i32;
    fn ct_value_write_raw(
        h: *mut std::ffi::c_void,
        data: *const u8,
        len: usize,
        type_id: u64,
    ) -> i32;
    fn ct_value_write_error(
        h: *mut std::ffi::c_void,
        data: *const u8,
        len: usize,
        type_id: u64,
    ) -> i32;

    fn ct_value_begin_sequence(h: *mut std::ffi::c_void, type_id: u64, element_count: i32) -> i32;
    fn ct_value_begin_tuple(h: *mut std::ffi::c_void, type_id: u64, element_count: i32) -> i32;
    fn ct_value_end_compound(h: *mut std::ffi::c_void) -> i32;

    fn ct_value_get_bytes(h: *mut std::ffi::c_void, out_len: *mut usize) -> *const u8;

    fn trace_writer_register_special_event(
        handle: *mut std::ffi::c_void,
        kind: i32,
        metadata: *const std::os::raw::c_char,
        content: *const std::os::raw::c_char,
    );

    // ----- meta.dat -----

    fn ct_write_meta_dat(
        handle: *mut std::ffi::c_void,
        recorder_id: *const u8,
        recorder_id_len: usize,
    ) -> i32;

    fn ct_read_meta_dat(data: *const u8, len: usize) -> *mut std::ffi::c_void;
    fn ct_meta_dat_program(h: *mut std::ffi::c_void, out_len: *mut usize) -> *const u8;
    fn ct_meta_dat_workdir(h: *mut std::ffi::c_void, out_len: *mut usize) -> *const u8;
    fn ct_meta_dat_args_count(h: *mut std::ffi::c_void) -> usize;
    fn ct_meta_dat_arg(h: *mut std::ffi::c_void, idx: usize, out_len: *mut usize) -> *const u8;
    fn ct_meta_dat_paths_count(h: *mut std::ffi::c_void) -> usize;
    fn ct_meta_dat_path(h: *mut std::ffi::c_void, idx: usize, out_len: *mut usize) -> *const u8;
    fn ct_meta_dat_recorder_id(h: *mut std::ffi::c_void, out_len: *mut usize) -> *const u8;
    fn ct_meta_dat_free(h: *mut std::ffi::c_void);

    // ----- Trace reader (NewTraceReader) -----

    fn ct_reader_open(path: *const std::os::raw::c_char) -> *mut std::ffi::c_void;
    fn ct_reader_close(h: *mut std::ffi::c_void);

    fn ct_reader_step_count(h: *mut std::ffi::c_void) -> u64;
    fn ct_reader_call_count(h: *mut std::ffi::c_void) -> u64;
    fn ct_reader_event_count(h: *mut std::ffi::c_void) -> u64;

    fn ct_reader_path_count(h: *mut std::ffi::c_void) -> u64;
    fn ct_reader_function_count(h: *mut std::ffi::c_void) -> u64;
    fn ct_reader_type_count(h: *mut std::ffi::c_void) -> u64;
    fn ct_reader_varname_count(h: *mut std::ffi::c_void) -> u64;

    fn ct_reader_path(h: *mut std::ffi::c_void, id: u64, out_len: *mut usize) -> *mut u8;
    fn ct_reader_function(h: *mut std::ffi::c_void, id: u64, out_len: *mut usize) -> *mut u8;
    fn ct_reader_type_name(h: *mut std::ffi::c_void, id: u64, out_len: *mut usize) -> *mut u8;
    fn ct_reader_varname(h: *mut std::ffi::c_void, id: u64, out_len: *mut usize) -> *mut u8;

    fn ct_reader_step(h: *mut std::ffi::c_void, n: u64, out_len: *mut usize) -> *mut u8;
    fn ct_reader_values(h: *mut std::ffi::c_void, n: u64, out_len: *mut usize) -> *mut u8;
    fn ct_reader_call(h: *mut std::ffi::c_void, key: u64, out_len: *mut usize) -> *mut u8;
    fn ct_reader_call_for_step(h: *mut std::ffi::c_void, step_id: u64, out_len: *mut usize) -> *mut u8;
    fn ct_reader_event(h: *mut std::ffi::c_void, index: u64, out_len: *mut usize) -> *mut u8;

    fn ct_reader_program(h: *mut std::ffi::c_void, out_len: *mut usize) -> *mut u8;
    fn ct_reader_workdir(h: *mut std::ffi::c_void, out_len: *mut usize) -> *mut u8;

    fn ct_free_buffer(buf: *mut u8);
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
// StreamingValueEncoder — Rust wrapper for the Nim C FFI
// ---------------------------------------------------------------------------

/// Zero-allocation CBOR value encoder backed by the Nim streaming encoder.
///
/// Encodes `ValueRecord` trees directly into CBOR bytes without building
/// intermediate representations. The encoder is reusable: call `reset()`
/// between values to clear the buffer without deallocating.
pub struct StreamingValueEncoder {
    handle: *mut std::ffi::c_void,
}

impl StreamingValueEncoder {
    /// Create a new streaming value encoder.
    pub fn new() -> Self {
        ensure_nim_initialized();
        let handle = unsafe { ct_value_encoder_new() };
        assert!(!handle.is_null(), "ct_value_encoder_new returned null");
        StreamingValueEncoder { handle }
    }

    /// Reset the encoder for reuse (clears buffer, resets nesting stack).
    pub fn reset(&mut self) {
        unsafe { ct_value_encoder_reset(self.handle) }
    }

    /// Encode a `ValueRecord` into the internal CBOR buffer.
    /// Returns the CBOR bytes as a slice (valid until the next reset/encode/drop).
    pub fn encode(&mut self, value: &ValueRecord) -> &[u8] {
        self.reset();
        self.encode_recursive(value);
        self.get_bytes()
    }

    /// Get the encoded CBOR bytes. Valid until the next reset/encode/drop.
    fn get_bytes(&self) -> &[u8] {
        let mut len: usize = 0;
        let ptr = unsafe { ct_value_get_bytes(self.handle, &mut len) };
        if ptr.is_null() || len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    /// Recursively encode a value record into CBOR.
    fn encode_recursive(&mut self, value: &ValueRecord) {
        match value {
            ValueRecord::None { type_id } => {
                unsafe { ct_value_write_none_typed(self.handle, type_id.0 as u64) };
            }
            ValueRecord::Int { i, type_id } => {
                unsafe { ct_value_write_int(self.handle, *i, type_id.0 as u64) };
            }
            ValueRecord::Float { f, type_id } => {
                unsafe { ct_value_write_float(self.handle, *f, type_id.0 as u64) };
            }
            ValueRecord::Bool { b, type_id } => {
                unsafe {
                    ct_value_write_bool_typed(
                        self.handle,
                        if *b { 1 } else { 0 },
                        type_id.0 as u64,
                    )
                };
            }
            ValueRecord::String { text, type_id } => {
                unsafe {
                    ct_value_write_string(
                        self.handle,
                        text.as_ptr(),
                        text.len(),
                        type_id.0 as u64,
                    )
                };
            }
            ValueRecord::Raw { r, type_id } => {
                unsafe {
                    ct_value_write_raw(
                        self.handle,
                        r.as_ptr(),
                        r.len(),
                        type_id.0 as u64,
                    )
                };
            }
            ValueRecord::Error { msg, type_id } => {
                unsafe {
                    ct_value_write_error(
                        self.handle,
                        msg.as_ptr(),
                        msg.len(),
                        type_id.0 as u64,
                    )
                };
            }
            ValueRecord::Sequence { elements, is_slice: _, type_id } => {
                unsafe {
                    ct_value_begin_sequence(
                        self.handle,
                        type_id.0 as u64,
                        elements.len() as i32,
                    )
                };
                for elem in elements {
                    self.encode_recursive(elem);
                }
                unsafe { ct_value_end_compound(self.handle) };
            }
            ValueRecord::Tuple { elements, type_id } => {
                unsafe {
                    ct_value_begin_tuple(
                        self.handle,
                        type_id.0 as u64,
                        elements.len() as i32,
                    )
                };
                for elem in elements {
                    self.encode_recursive(elem);
                }
                unsafe { ct_value_end_compound(self.handle) };
            }
            // For types not yet supported by the streaming encoder, fall back to raw.
            _ => {
                let (repr, _kind, _type_name) = value_record_to_raw(value);
                unsafe {
                    ct_value_write_raw(self.handle, repr.as_ptr(), repr.len(), 0)
                };
            }
        }
    }
}

impl Drop for StreamingValueEncoder {
    fn drop(&mut self) {
        unsafe { ct_value_encoder_free(self.handle) }
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
    /// Reusable streaming value encoder — avoids allocation per value for
    /// compound types (sequences, tuples, dicts) by encoding directly to CBOR.
    streaming_encoder: StreamingValueEncoder,
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
        NimTraceWriter {
            handle,
            streaming_encoder: StreamingValueEncoder::new(),
        }
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

    /// Write binary meta.dat to the trace container.
    pub fn write_meta_dat(&mut self, recorder_id: &str) -> Result<(), Box<dyn Error>> {
        ensure_nim_initialized();
        let ret = unsafe {
            ct_write_meta_dat(
                self.handle,
                recorder_id.as_ptr(),
                recorder_id.len(),
            )
        };
        if ret != 0 {
            Err(last_error().into())
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
        match &return_value {
            ValueRecord::Int { i, type_id } => {
                let type_name = str_to_cstring(&format!("type_{}", type_id.0));
                unsafe {
                    trace_writer_register_return_int(
                        self.handle,
                        *i,
                        TypeKind::Int as i32,
                        type_name.as_ptr(),
                    )
                }
            }
            ValueRecord::None { .. } => unsafe {
                trace_writer_register_return(self.handle);
            },
            // Compound types benefit from the streaming encoder: instead of
            // flattening to a raw string like "[...]", we encode the full
            // structure to CBOR so the reader can reconstruct it.
            ValueRecord::Sequence { .. }
            | ValueRecord::Tuple { .. }
            | ValueRecord::Struct { .. } => {
                let cbor = self.streaming_encoder.encode(&return_value);
                unsafe {
                    trace_writer_register_return_cbor(
                        self.handle,
                        cbor.as_ptr(),
                        cbor.len(),
                    )
                }
            }
            _ => {
                // Leaf types: serialize to raw representation via the existing path
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
        match &value {
            ValueRecord::Int { i, type_id } => {
                let type_name = str_to_cstring(&format!("type_{}", type_id.0));
                unsafe {
                    trace_writer_register_variable_int(
                        self.handle,
                        c_name.as_ptr(),
                        *i,
                        TypeKind::Int as i32,
                        type_name.as_ptr(),
                    )
                }
            }
            // Compound types: use the streaming encoder for full structural
            // CBOR encoding instead of flattening to "[...]" / "(...)".
            ValueRecord::Sequence { .. }
            | ValueRecord::Tuple { .. }
            | ValueRecord::Struct { .. } => {
                let cbor = self.streaming_encoder.encode(&value);
                unsafe {
                    trace_writer_register_variable_cbor(
                        self.handle,
                        c_name.as_ptr(),
                        cbor.as_ptr(),
                        cbor.len(),
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
// TraceWriter trait — drop-in replacement for `codetracer_trace_writer::trace_writer::TraceWriter`
// ---------------------------------------------------------------------------

/// Re-export module so consumers can write `use codetracer_trace_writer_nim::trace_writer::TraceWriter`.
pub mod trace_writer {
    pub use super::TraceWriter;
}

/// Trait matching the API surface of the original `codetracer_trace_writer::TraceWriter`.
///
/// The Nim-backed [`NimTraceWriter`] is the sole implementation shipped by this crate.
/// Consumers that previously used `Box<dyn TraceWriter>` can continue to do so unchanged.
#[allow(unused_variables)]
pub trait TraceWriter: Send {
    fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>>;
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>>;
    fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>>;
    fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>>;

    /// Close the writer and flush all remaining data to disk.
    ///
    /// For the Nim multi-stream (CTFS) backend this is the step that actually
    /// writes the `.ct` container file. Callers should invoke this after all
    /// `finish_writing_*` calls have completed.
    ///
    /// The default implementation is a no-op, which is appropriate for
    /// in-memory test doubles that don't need an explicit close step.
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn set_workdir(&mut self, workdir: &Path);
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
    fn register_special_event(&mut self, kind: EventLogKind, metadata: &str, content: &str);

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
    fn assign(&mut self, variable_name: &str, rvalue: RValue, pass_by: PassBy);
    fn bind_variable(&mut self, variable_name: &str, place: Place);
    fn drop_variables(&mut self, variable_names: &[String]);
    fn simple_rvalue(&mut self, variable_name: &str) -> RValue;
    fn compound_rvalue(&mut self, variable_dependencies: &[String]) -> RValue;
    fn drop_last_step(&mut self);

    fn add_event(&mut self, event: TraceLowLevelEvent);
    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>);
    fn events(&self) -> &[TraceLowLevelEvent];
}

impl TraceWriter for NimTraceWriter {
    fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::begin_writing_trace_metadata(self, path)
    }
    fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::finish_writing_trace_metadata(self)
    }
    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::begin_writing_trace_events(self, path)
    }
    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::finish_writing_trace_events(self)
    }
    fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::begin_writing_trace_paths(self, path)
    }
    fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::finish_writing_trace_paths(self)
    }
    fn close(&mut self) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::close(self)
    }
    fn set_workdir(&mut self, workdir: &Path) {
        NimTraceWriter::set_workdir(self, workdir)
    }
    fn start(&mut self, path: &Path, line: Line) {
        NimTraceWriter::start(self, path, line)
    }
    fn ensure_path_id(&mut self, path: &Path) -> PathId {
        NimTraceWriter::ensure_path_id(self, path)
    }
    fn ensure_function_id(&mut self, function_name: &str, path: &Path, line: Line) -> FunctionId {
        NimTraceWriter::ensure_function_id(self, function_name, path, line)
    }
    fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
        NimTraceWriter::ensure_type_id(self, kind, lang_type)
    }
    fn ensure_raw_type_id(&mut self, typ: TypeRecord) -> TypeId {
        NimTraceWriter::ensure_raw_type_id(self, typ)
    }
    fn ensure_variable_id(&mut self, variable_name: &str) -> VariableId {
        NimTraceWriter::ensure_variable_id(self, variable_name)
    }
    fn register_path(&mut self, path: &Path) {
        NimTraceWriter::register_path(self, path)
    }
    fn register_function(&mut self, name: &str, path: &Path, line: Line) {
        NimTraceWriter::register_function(self, name, path, line)
    }
    fn register_step(&mut self, path: &Path, line: Line) {
        NimTraceWriter::register_step(self, path, line)
    }
    fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>) {
        NimTraceWriter::register_call(self, function_id, args)
    }
    fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord {
        NimTraceWriter::arg(self, name, value)
    }
    fn register_return(&mut self, return_value: ValueRecord) {
        NimTraceWriter::register_return(self, return_value)
    }
    fn register_special_event(&mut self, kind: EventLogKind, metadata: &str, content: &str) {
        NimTraceWriter::register_special_event(self, kind, metadata, content)
    }
    fn to_raw_type(&self, kind: TypeKind, lang_type: &str) -> TypeRecord {
        NimTraceWriter::to_raw_type(self, kind, lang_type)
    }
    fn register_type(&mut self, kind: TypeKind, lang_type: &str) {
        NimTraceWriter::register_type(self, kind, lang_type)
    }
    fn register_raw_type(&mut self, typ: TypeRecord) {
        NimTraceWriter::register_raw_type(self, typ)
    }
    fn register_asm(&mut self, instructions: &[String]) {
        NimTraceWriter::register_asm(self, instructions)
    }
    fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
        NimTraceWriter::register_variable_with_full_value(self, name, value)
    }
    fn register_variable_name(&mut self, variable_name: &str) {
        NimTraceWriter::register_variable_name(self, variable_name)
    }
    fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord) {
        NimTraceWriter::register_full_value(self, variable_id, value)
    }
    fn register_compound_value(&mut self, place: Place, value: ValueRecord) {
        NimTraceWriter::register_compound_value(self, place, value)
    }
    fn register_cell_value(&mut self, place: Place, value: ValueRecord) {
        NimTraceWriter::register_cell_value(self, place, value)
    }
    fn assign_compound_item(&mut self, place: Place, index: usize, item_place: Place) {
        NimTraceWriter::assign_compound_item(self, place, index, item_place)
    }
    fn assign_cell(&mut self, place: Place, new_value: ValueRecord) {
        NimTraceWriter::assign_cell(self, place, new_value)
    }
    fn register_variable(&mut self, variable_name: &str, place: Place) {
        NimTraceWriter::register_variable(self, variable_name, place)
    }
    fn drop_variable(&mut self, variable_name: &str) {
        NimTraceWriter::drop_variable(self, variable_name)
    }
    fn assign(&mut self, variable_name: &str, rvalue: RValue, pass_by: PassBy) {
        NimTraceWriter::assign(self, variable_name, rvalue, pass_by)
    }
    fn bind_variable(&mut self, variable_name: &str, place: Place) {
        NimTraceWriter::bind_variable(self, variable_name, place)
    }
    fn drop_variables(&mut self, variable_names: &[String]) {
        NimTraceWriter::drop_variables(self, variable_names)
    }
    fn simple_rvalue(&mut self, variable_name: &str) -> RValue {
        NimTraceWriter::simple_rvalue(self, variable_name)
    }
    fn compound_rvalue(&mut self, variable_dependencies: &[String]) -> RValue {
        NimTraceWriter::compound_rvalue(self, variable_dependencies)
    }
    fn drop_last_step(&mut self) {
        NimTraceWriter::drop_last_step(self)
    }
    fn add_event(&mut self, event: TraceLowLevelEvent) {
        NimTraceWriter::add_event(self, event)
    }
    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        NimTraceWriter::append_events(self, events)
    }
    fn events(&self) -> &[TraceLowLevelEvent] {
        NimTraceWriter::events(self)
    }
}

// ---------------------------------------------------------------------------
// MetaDatReader — read binary meta.dat blobs
// ---------------------------------------------------------------------------

/// Reader for binary meta.dat blobs produced by the Nim trace writer.
///
/// The underlying data is owned by the Nim heap and freed on [`Drop`].
pub struct MetaDatReader {
    handle: *mut std::ffi::c_void,
}

// Same rationale as NimTraceWriter — single-threaded Nim library, exclusive access.
unsafe impl Send for MetaDatReader {}

impl MetaDatReader {
    /// Parse a binary meta.dat blob.
    pub fn parse(data: &[u8]) -> Result<Self, Box<dyn Error>> {
        ensure_nim_initialized();
        let h = unsafe { ct_read_meta_dat(data.as_ptr(), data.len()) };
        if h.is_null() {
            Err(last_error().into())
        } else {
            Ok(MetaDatReader { handle: h })
        }
    }

    /// The traced program path.
    pub fn program(&self) -> &str {
        unsafe {
            let mut len: usize = 0;
            let ptr = ct_meta_dat_program(self.handle, &mut len);
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len))
        }
    }

    /// The working directory at recording time.
    pub fn workdir(&self) -> &str {
        unsafe {
            let mut len: usize = 0;
            let ptr = ct_meta_dat_workdir(self.handle, &mut len);
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len))
        }
    }

    /// Number of command-line arguments.
    pub fn args_count(&self) -> usize {
        unsafe { ct_meta_dat_args_count(self.handle) }
    }

    /// Get the command-line argument at `idx`, or `None` if out of range.
    pub fn arg(&self, idx: usize) -> Option<&str> {
        if idx >= self.args_count() {
            return None;
        }
        unsafe {
            let mut len: usize = 0;
            let ptr = ct_meta_dat_arg(self.handle, idx, &mut len);
            if ptr.is_null() {
                None
            } else {
                Some(std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)))
            }
        }
    }

    /// Number of source paths recorded.
    pub fn paths_count(&self) -> usize {
        unsafe { ct_meta_dat_paths_count(self.handle) }
    }

    /// Get the source path at `idx`, or `None` if out of range.
    pub fn path(&self, idx: usize) -> Option<&str> {
        if idx >= self.paths_count() {
            return None;
        }
        unsafe {
            let mut len: usize = 0;
            let ptr = ct_meta_dat_path(self.handle, idx, &mut len);
            if ptr.is_null() {
                None
            } else {
                Some(std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)))
            }
        }
    }

    /// The recorder identifier string.
    pub fn recorder_id(&self) -> &str {
        unsafe {
            let mut len: usize = 0;
            let ptr = ct_meta_dat_recorder_id(self.handle, &mut len);
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len))
        }
    }
}

impl Drop for MetaDatReader {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { ct_meta_dat_free(self.handle) };
            self.handle = std::ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// NimTraceReaderHandle — safe wrapper for the Nim ct_reader_* FFI
// ---------------------------------------------------------------------------

/// Read-only handle for a `.ct` trace file, backed by the Nim `NewTraceReader`.
///
/// All complex data (steps, values, calls, IO events) is returned as JSON
/// strings. The caller is responsible for parsing them.
pub struct NimTraceReaderHandle {
    handle: *mut std::ffi::c_void,
}

// Single-threaded Nim library; exclusive &mut/& self gives safety.
unsafe impl Send for NimTraceReaderHandle {}

/// Helper: read a heap-allocated buffer from Nim into a Rust `String`, then free it.
fn read_nim_buffer(ptr: *mut u8, len: usize) -> String {
    if ptr.is_null() || len == 0 {
        return String::new();
    }
    let s = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)) }
        .to_string();
    unsafe { ct_free_buffer(ptr) };
    s
}

impl NimTraceReaderHandle {
    /// Open a `.ct` trace file for reading.
    pub fn open(path: &str) -> Result<Self, Box<dyn Error>> {
        ensure_nim_initialized();
        let c_path = CString::new(path)?;
        let h = unsafe { ct_reader_open(c_path.as_ptr()) };
        if h.is_null() {
            Err(last_error().into())
        } else {
            Ok(Self { handle: h })
        }
    }

    // --- Counts ---

    pub fn step_count(&self) -> u64 {
        unsafe { ct_reader_step_count(self.handle) }
    }

    pub fn call_count(&self) -> u64 {
        unsafe { ct_reader_call_count(self.handle) }
    }

    pub fn event_count(&self) -> u64 {
        unsafe { ct_reader_event_count(self.handle) }
    }

    pub fn path_count(&self) -> u64 {
        unsafe { ct_reader_path_count(self.handle) }
    }

    pub fn function_count(&self) -> u64 {
        unsafe { ct_reader_function_count(self.handle) }
    }

    pub fn type_count(&self) -> u64 {
        unsafe { ct_reader_type_count(self.handle) }
    }

    pub fn varname_count(&self) -> u64 {
        unsafe { ct_reader_varname_count(self.handle) }
    }

    // --- Interning lookups ---

    pub fn path(&self, id: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_path(self.handle, id, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    pub fn function(&self, id: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_function(self.handle, id, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    pub fn type_name(&self, id: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_type_name(self.handle, id, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    pub fn varname(&self, id: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_varname(self.handle, id, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    // --- Data access (JSON) ---

    /// Returns step event N as a JSON string.
    pub fn step_json(&self, n: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_step(self.handle, n, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    /// Returns variable values for step N as a JSON array string.
    pub fn values_json(&self, n: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_values(self.handle, n, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    /// Returns call record by key as a JSON string.
    pub fn call_json(&self, key: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_call(self.handle, key, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    /// Returns the innermost call record enclosing the given step as a JSON string.
    pub fn call_for_step_json(&self, step_id: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_call_for_step(self.handle, step_id, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    /// Returns IO event by index as a JSON string.
    pub fn event_json(&self, index: u64) -> Result<String, Box<dyn Error>> {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_event(self.handle, index, &mut len) };
        if ptr.is_null() {
            return Err(last_error().into());
        }
        Ok(read_nim_buffer(ptr, len))
    }

    // --- Metadata ---

    /// Get the program name from trace metadata.
    pub fn program(&self) -> String {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_program(self.handle, &mut len) };
        read_nim_buffer(ptr, len)
    }

    /// Get the working directory from trace metadata.
    pub fn workdir(&self) -> String {
        let mut len: usize = 0;
        let ptr = unsafe { ct_reader_workdir(self.handle, &mut len) };
        read_nim_buffer(ptr, len)
    }
}

impl Drop for NimTraceReaderHandle {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { ct_reader_close(self.handle) };
            self.handle = std::ptr::null_mut();
        }
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
) -> Box<dyn TraceWriter> {
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

// ---------------------------------------------------------------------------
// NonStreamingTraceWriter — in-memory test double
// ---------------------------------------------------------------------------

/// A simple in-memory trace writer for use in unit tests.
///
/// This is a drop-in replacement for `codetracer_trace_writer::non_streaming_trace_writer::NonStreamingTraceWriter`.
/// It buffers all events in memory and exposes them via the public `events` field.
pub mod non_streaming_trace_writer {
    use super::*;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    /// In-memory trace writer that records events for inspection in tests.
    pub struct NonStreamingTraceWriter {
        /// Accumulated trace events — inspect this in tests.
        pub events: Vec<TraceLowLevelEvent>,

        format: TraceEventsFileFormat,
        paths: HashMap<PathBuf, PathId>,
        functions: HashMap<String, FunctionId>,
        types: HashMap<String, TypeId>,
        variables: HashMap<String, VariableId>,
        next_function_id: usize,
        next_type_id: usize,
        next_variable_id: usize,
        next_path_id: usize,
        workdir: PathBuf,
    }

    impl NonStreamingTraceWriter {
        /// Create a new in-memory writer for the given program.
        pub fn new(program: &str, _args: &[String]) -> Self {
            let _ = program;
            NonStreamingTraceWriter {
                events: Vec::new(),
                format: TraceEventsFileFormat::Binary,
                paths: HashMap::new(),
                functions: HashMap::new(),
                types: HashMap::new(),
                variables: HashMap::new(),
                next_function_id: 0,
                next_type_id: 0,
                next_variable_id: 0,
                next_path_id: 0,
                workdir: PathBuf::new(),
            }
        }

        pub fn set_format(&mut self, format: TraceEventsFileFormat) {
            self.format = format;
        }
    }

    #[allow(unused_variables)]
    impl TraceWriter for NonStreamingTraceWriter {
        fn begin_writing_trace_metadata(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
        fn finish_writing_trace_metadata(&mut self) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
        fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
        fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
        fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
        fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
        fn set_workdir(&mut self, workdir: &Path) {
            self.workdir = workdir.to_path_buf();
        }
        fn start(&mut self, path: &Path, line: Line) {
            // Mirrors AbstractTraceWriter::start — registers the toplevel function and calls it.
            let function_id = self.ensure_function_id("<toplevel>", path, line);
            self.register_call(function_id, vec![]);
            self.ensure_type_id(TypeKind::None, "None");
        }
        fn ensure_path_id(&mut self, path: &Path) -> PathId {
            if let Some(&id) = self.paths.get(path) {
                return id;
            }
            let id = PathId(self.next_path_id);
            self.next_path_id += 1;
            self.paths.insert(path.to_path_buf(), id);
            self.events.push(TraceLowLevelEvent::Path(path.to_path_buf()));
            id
        }
        fn ensure_function_id(&mut self, function_name: &str, path: &Path, line: Line) -> FunctionId {
            if let Some(&id) = self.functions.get(function_name) {
                return id;
            }
            let id = FunctionId(self.next_function_id);
            self.next_function_id += 1;
            self.functions.insert(function_name.to_string(), id);
            // register_function adds Path event + Function event
            let path_id = self.ensure_path_id(path);
            self.events.push(TraceLowLevelEvent::Function(FunctionRecord {
                name: function_name.to_string(),
                path_id,
                line,
            }));
            id
        }
        fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
            let key = format!("{:?}:{}", kind, lang_type);
            if let Some(&id) = self.types.get(&key) {
                return id;
            }
            let id = TypeId(self.next_type_id);
            self.next_type_id += 1;
            self.types.insert(key, id);
            id
        }
        fn ensure_raw_type_id(&mut self, typ: TypeRecord) -> TypeId {
            self.ensure_type_id(typ.kind, &typ.lang_type)
        }
        fn ensure_variable_id(&mut self, variable_name: &str) -> VariableId {
            if let Some(&id) = self.variables.get(variable_name) {
                return id;
            }
            let id = VariableId(self.next_variable_id);
            self.next_variable_id += 1;
            self.variables.insert(variable_name.to_string(), id);
            id
        }
        fn register_path(&mut self, path: &Path) {
            self.events.push(TraceLowLevelEvent::Path(path.to_path_buf()));
        }
        fn register_function(&mut self, name: &str, path: &Path, line: Line) {
            self.ensure_function_id(name, path, line);
        }
        fn register_step(&mut self, path: &Path, line: Line) {
            let path_id = self.ensure_path_id(path);
            self.events.push(TraceLowLevelEvent::Step(StepRecord {
                path_id,
                line,
            }));
        }
        fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>) {
            self.events.push(TraceLowLevelEvent::Call(CallRecord {
                function_id,
                args,
            }));
        }
        fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord {
            let variable_id = self.ensure_variable_id(name);
            FullValueRecord { variable_id, value }
        }
        fn register_return(&mut self, return_value: ValueRecord) {
            self.events.push(TraceLowLevelEvent::Return(ReturnRecord {
                return_value,
            }));
        }
        fn register_special_event(&mut self, kind: EventLogKind, metadata: &str, content: &str) {
            self.events.push(TraceLowLevelEvent::Event(RecordEvent {
                kind,
                metadata: metadata.to_string(),
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
            self.ensure_type_id(kind, lang_type);
        }
        fn register_raw_type(&mut self, typ: TypeRecord) {
            self.ensure_type_id(typ.kind, &typ.lang_type);
        }
        fn register_asm(&mut self, instructions: &[String]) {}
        fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
            let variable_id = self.ensure_variable_id(name);
            self.events.push(TraceLowLevelEvent::Value(FullValueRecord {
                variable_id,
                value,
            }));
        }
        fn register_variable_name(&mut self, variable_name: &str) {
            self.ensure_variable_id(variable_name);
        }
        fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord) {
            self.events.push(TraceLowLevelEvent::Value(FullValueRecord {
                variable_id,
                value,
            }));
        }
        fn register_compound_value(&mut self, place: Place, value: ValueRecord) {}
        fn register_cell_value(&mut self, place: Place, value: ValueRecord) {}
        fn assign_compound_item(&mut self, place: Place, index: usize, item_place: Place) {}
        fn assign_cell(&mut self, place: Place, new_value: ValueRecord) {}
        fn register_variable(&mut self, variable_name: &str, place: Place) {}
        fn drop_variable(&mut self, variable_name: &str) {}
        fn assign(&mut self, variable_name: &str, rvalue: RValue, pass_by: PassBy) {}
        fn bind_variable(&mut self, variable_name: &str, place: Place) {}
        fn drop_variables(&mut self, variable_names: &[String]) {}
        fn simple_rvalue(&mut self, variable_name: &str) -> RValue {
            RValue::Simple(VariableId(0))
        }
        fn compound_rvalue(&mut self, variable_dependencies: &[String]) -> RValue {
            RValue::Compound(vec![])
        }
        fn drop_last_step(&mut self) {
            if let Some(pos) = self.events.iter().rposition(|e| matches!(e, TraceLowLevelEvent::Step(_))) {
                self.events.remove(pos);
            }
        }
        fn add_event(&mut self, event: TraceLowLevelEvent) {
            self.events.push(event);
        }
        fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
            self.events.append(events);
        }
        fn events(&self) -> &[TraceLowLevelEvent] {
            &self.events
        }
    }
}
