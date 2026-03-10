//! C FFI layer for `codetracer_trace_writer`.
//!
//! Exposes an opaque `TraceWriterHandle` and `extern "C"` functions so that
//! C / Go (cgo) callers can produce valid CodeTracer DB traces without
//! reimplementing the serialisation logic.
//!
//! # Error handling
//!
//! Functions that can fail return `false` (or a sentinel value) and store a
//! human-readable error string in a thread-local buffer. The caller retrieves
//! it via `trace_writer_last_error()`.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;

use codetracer_trace_types::{EventLogKind, Line, TypeKind, ValueRecord};
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer::{create_trace_writer, TraceEventsFileFormat};
use num_traits::FromPrimitive;

// ---------------------------------------------------------------------------
// Thread-local error buffer
// ---------------------------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn set_error(msg: &str) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).unwrap_or_default();
    });
}

/// Retrieve the last error message for the current thread.
///
/// Returns a pointer to a NUL-terminated UTF-8 string.  The pointer is valid
/// until the next FFI call **on the same thread**.  Returns an empty string
/// when no error has occurred.
#[unsafe(no_mangle)]
pub extern "C" fn trace_writer_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

// ---------------------------------------------------------------------------
// Opaque handle
// ---------------------------------------------------------------------------

/// Opaque handle passed across the FFI boundary.
pub struct TraceWriterHandle {
    inner: Box<dyn TraceWriter + Send>,
}

// ---------------------------------------------------------------------------
// FFI enum mirrors (C-compatible repr)
// ---------------------------------------------------------------------------

/// Trace file format — mirrors [`TraceEventsFileFormat`].
#[repr(C)]
pub enum FfiTraceFormat {
    Json = 0,
    BinaryV0 = 1,
    Binary = 2,
}

/// Type kind — mirrors [`TypeKind`] (subset used by the FFI).
#[repr(C)]
pub enum FfiTypeKind {
    Seq = 0,
    Set = 1,
    HashSet = 2,
    OrderedSet = 3,
    Array = 4,
    Varargs = 5,
    Struct = 6,
    Int = 7,
    Float = 8,
    String = 9,
    CString = 10,
    Char = 11,
    Bool = 12,
    Literal = 13,
    Ref = 14,
    Recursion = 15,
    Raw = 16,
    Enum = 17,
    Enum16 = 18,
    Enum32 = 19,
    C = 20,
    TableKind = 21,
    Union = 22,
    Pointer = 23,
    Error = 24,
    FunctionKind = 25,
    TypeValue = 26,
    Tuple = 27,
    Variant = 28,
    Html = 29,
    None = 30,
    NonExpanded = 31,
    Any = 32,
    Slice = 33,
}

/// Event-log kind — mirrors [`EventLogKind`].
#[repr(C)]
pub enum FfiEventLogKind {
    Write = 0,
    WriteFile = 1,
    WriteOther = 2,
    Read = 3,
    ReadFile = 4,
    ReadOther = 5,
    ReadDir = 6,
    OpenDir = 7,
    CloseDir = 8,
    Socket = 9,
    Open = 10,
    Error = 11,
    TraceLogEvent = 12,
    EvmEvent = 13,
}

// ---------------------------------------------------------------------------
// Helpers — convert FFI enums to Rust types
// ---------------------------------------------------------------------------

fn to_format(f: FfiTraceFormat) -> TraceEventsFileFormat {
    match f {
        FfiTraceFormat::Json => TraceEventsFileFormat::Json,
        FfiTraceFormat::BinaryV0 => TraceEventsFileFormat::BinaryV0,
        FfiTraceFormat::Binary => TraceEventsFileFormat::Binary,
    }
}

fn to_type_kind(k: FfiTypeKind) -> TypeKind {
    TypeKind::from_u8(k as u8).unwrap_or_default()
}

fn to_event_log_kind(k: FfiEventLogKind) -> EventLogKind {
    EventLogKind::from_u8(k as u8).unwrap_or_default()
}

/// Convert a C string pointer to `&str`.  Returns `""` for null pointers.
unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> &'a str {
    if ptr.is_null() {
        return "";
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().unwrap_or("")
}

/// Helper: get a mutable reference to the writer, disambiguating through TraceWriter.
fn w(handle: &mut TraceWriterHandle) -> &mut dyn TraceWriter {
    &mut *handle.inner
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Create a new trace writer.
///
/// `program` is a NUL-terminated C string identifying the program being
/// traced.  `format` selects the on-disk serialisation format.
///
/// Returns a heap-allocated handle that **must** be freed with
/// [`trace_writer_free`].  Returns `NULL` on failure (check
/// [`trace_writer_last_error`]).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_new(
    program: *const c_char,
    format: FfiTraceFormat,
) -> *mut TraceWriterHandle {
    let prog = unsafe { cstr_to_str(program) };
    let writer = create_trace_writer(prog, &[], to_format(format));
    Box::into_raw(Box::new(TraceWriterHandle { inner: writer }))
}

/// Free a trace writer handle.  Passing `NULL` is a no-op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_free(handle: *mut TraceWriterHandle) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle) });
    }
}

// ---------------------------------------------------------------------------
// File I/O — begin / finish
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_begin_metadata(
    handle: *mut TraceWriterHandle,
    path: *const c_char,
) -> bool {
    if handle.is_null() {
        set_error("NULL handle");
        return false;
    }
    let h = unsafe { &mut *handle };
    match TraceWriter::begin_writing_trace_metadata(w(h), Path::new(unsafe { cstr_to_str(path) })) {
        Ok(()) => true,
        Err(e) => {
            set_error(&e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_finish_metadata(handle: *mut TraceWriterHandle) -> bool {
    if handle.is_null() {
        set_error("NULL handle");
        return false;
    }
    let h = unsafe { &mut *handle };
    match TraceWriter::finish_writing_trace_metadata(w(h)) {
        Ok(()) => true,
        Err(e) => {
            set_error(&e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_begin_events(
    handle: *mut TraceWriterHandle,
    path: *const c_char,
) -> bool {
    if handle.is_null() {
        set_error("NULL handle");
        return false;
    }
    let h = unsafe { &mut *handle };
    match TraceWriter::begin_writing_trace_events(w(h), Path::new(unsafe { cstr_to_str(path) })) {
        Ok(()) => true,
        Err(e) => {
            set_error(&e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_finish_events(handle: *mut TraceWriterHandle) -> bool {
    if handle.is_null() {
        set_error("NULL handle");
        return false;
    }
    let h = unsafe { &mut *handle };
    match TraceWriter::finish_writing_trace_events(w(h)) {
        Ok(()) => true,
        Err(e) => {
            set_error(&e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_begin_paths(
    handle: *mut TraceWriterHandle,
    path: *const c_char,
) -> bool {
    if handle.is_null() {
        set_error("NULL handle");
        return false;
    }
    let h = unsafe { &mut *handle };
    match TraceWriter::begin_writing_trace_paths(w(h), Path::new(unsafe { cstr_to_str(path) })) {
        Ok(()) => true,
        Err(e) => {
            set_error(&e.to_string());
            false
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_finish_paths(handle: *mut TraceWriterHandle) -> bool {
    if handle.is_null() {
        set_error("NULL handle");
        return false;
    }
    let h = unsafe { &mut *handle };
    match TraceWriter::finish_writing_trace_paths(w(h)) {
        Ok(()) => true,
        Err(e) => {
            set_error(&e.to_string());
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Tracing primitives
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_start(
    handle: *mut TraceWriterHandle,
    path: *const c_char,
    line: i64,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::start(w(h), Path::new(unsafe { cstr_to_str(path) }), Line(line));
}

/// Override the working directory recorded in the trace metadata.
///
/// By default the workdir is set to the process's current directory at
/// the time [`trace_writer_new`] is called.  Call this before
/// [`trace_writer_finish_metadata`] to record a different directory.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_set_workdir(
    handle: *mut TraceWriterHandle,
    workdir: *const c_char,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::set_workdir(w(h), Path::new(unsafe { cstr_to_str(workdir) }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_step(
    handle: *mut TraceWriterHandle,
    path: *const c_char,
    line: i64,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::register_step(w(h), Path::new(unsafe { cstr_to_str(path) }), Line(line));
}

/// Register a function and return its ID.  Returns `usize::MAX` on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_ensure_function_id(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    path: *const c_char,
    line: i64,
) -> usize {
    if handle.is_null() {
        return usize::MAX;
    }
    let h = unsafe { &mut *handle };
    let fid = TraceWriter::ensure_function_id(
        w(h),
        unsafe { cstr_to_str(name) },
        Path::new(unsafe { cstr_to_str(path) }),
        Line(line),
    );
    fid.0
}

/// Register a type and return its ID.  Returns `usize::MAX` on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_ensure_type_id(
    handle: *mut TraceWriterHandle,
    kind: FfiTypeKind,
    lang_type: *const c_char,
) -> usize {
    if handle.is_null() {
        return usize::MAX;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::ensure_type_id(w(h), to_type_kind(kind), unsafe { cstr_to_str(lang_type) }).0
}

/// Register a call to the function identified by `function_id`.
///
/// For simplicity the FFI does not expose argument passing — call
/// `trace_writer_register_variable_with_full_value` for each arg before
/// this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_call(
    handle: *mut TraceWriterHandle,
    function_id: usize,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::register_call(w(h), codetracer_trace_types::FunctionId(function_id), vec![]);
}

/// Register a function return with no explicit return value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_return(handle: *mut TraceWriterHandle) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::register_return(w(h), codetracer_trace_types::NONE_VALUE);
}

/// Register a function return with an integer return value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_return_int(
    handle: *mut TraceWriterHandle,
    value: i64,
    type_kind: FfiTypeKind,
    type_name: *const c_char,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    let type_id = TraceWriter::ensure_type_id(w(h), to_type_kind(type_kind), unsafe {
        cstr_to_str(type_name)
    });
    TraceWriter::register_return(w(h), ValueRecord::Int { i: value, type_id });
}

/// Register a function return with a string (raw) return value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_return_raw(
    handle: *mut TraceWriterHandle,
    value_repr: *const c_char,
    type_kind: FfiTypeKind,
    type_name: *const c_char,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    let type_id = TraceWriter::ensure_type_id(w(h), to_type_kind(type_kind), unsafe {
        cstr_to_str(type_name)
    });
    TraceWriter::register_return(
        w(h),
        ValueRecord::Raw {
            r: unsafe { cstr_to_str(value_repr) }.to_string(),
            type_id,
        },
    );
}

/// Register a variable with an integer value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_int(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    value: i64,
    type_kind: FfiTypeKind,
    type_name: *const c_char,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    let type_id = TraceWriter::ensure_type_id(w(h), to_type_kind(type_kind), unsafe {
        cstr_to_str(type_name)
    });
    TraceWriter::register_variable_with_full_value(
        w(h),
        unsafe { cstr_to_str(name) },
        ValueRecord::Int { i: value, type_id },
    );
}

/// Register a variable with a string (raw) value representation.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_raw(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    value_repr: *const c_char,
    type_kind: FfiTypeKind,
    type_name: *const c_char,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    let type_id = TraceWriter::ensure_type_id(w(h), to_type_kind(type_kind), unsafe {
        cstr_to_str(type_name)
    });
    TraceWriter::register_variable_with_full_value(
        w(h),
        unsafe { cstr_to_str(name) },
        ValueRecord::Raw {
            r: unsafe { cstr_to_str(value_repr) }.to_string(),
            type_id,
        },
    );
}

/// Register an I/O or special event with optional metadata.
///
/// `metadata` is an arbitrary NUL-terminated string attached to the event
/// (for example a file descriptor or channel name).  Pass `NULL` or an empty
/// string when no metadata is needed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_special_event(
    handle: *mut TraceWriterHandle,
    kind: FfiEventLogKind,
    metadata: *const c_char,
    content: *const c_char,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::register_special_event(
        w(h),
        to_event_log_kind(kind),
        unsafe { cstr_to_str(metadata) },
        unsafe { cstr_to_str(content) },
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::fs;

    #[test]
    fn test_roundtrip_json() {
        let tmp = std::env::temp_dir().join(format!("ffi_test_{}", std::process::id()));
        fs::create_dir_all(&tmp).unwrap();

        let program = CString::new("test_program").unwrap();
        let handle = unsafe { trace_writer_new(program.as_ptr(), FfiTraceFormat::Json) };
        assert!(!handle.is_null());

        // Begin writing
        let meta_path = CString::new(tmp.join("trace_metadata.json").to_str().unwrap()).unwrap();
        let events_path = CString::new(tmp.join("trace.json").to_str().unwrap()).unwrap();
        let paths_path = CString::new(tmp.join("trace_paths.json").to_str().unwrap()).unwrap();

        assert!(unsafe { trace_writer_begin_metadata(handle, meta_path.as_ptr()) });
        assert!(unsafe { trace_writer_begin_events(handle, events_path.as_ptr()) });
        assert!(unsafe { trace_writer_begin_paths(handle, paths_path.as_ptr()) });

        // Record some events
        let source = CString::new("/test/main.rs").unwrap();
        unsafe { trace_writer_start(handle, source.as_ptr(), 1) };
        unsafe { trace_writer_register_step(handle, source.as_ptr(), 2) };

        let fn_name = CString::new("main").unwrap();
        let fid = unsafe {
            trace_writer_ensure_function_id(handle, fn_name.as_ptr(), source.as_ptr(), 1)
        };
        assert_ne!(fid, usize::MAX);

        let var_name = CString::new("x").unwrap();
        let type_name = CString::new("i32").unwrap();
        unsafe {
            trace_writer_register_variable_int(
                handle,
                var_name.as_ptr(),
                42,
                FfiTypeKind::Int,
                type_name.as_ptr(),
            )
        };

        // Finish writing
        assert!(unsafe { trace_writer_finish_events(handle) });
        assert!(unsafe { trace_writer_finish_metadata(handle) });
        assert!(unsafe { trace_writer_finish_paths(handle) });

        // Verify files exist
        assert!(tmp.join("trace.json").exists());
        assert!(tmp.join("trace_metadata.json").exists());
        assert!(tmp.join("trace_paths.json").exists());

        // Verify trace.json is valid JSON with events
        let trace_content = fs::read_to_string(tmp.join("trace.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&trace_content).unwrap();
        assert!(parsed.as_array().map_or(false, |a| !a.is_empty()));

        unsafe { trace_writer_free(handle) };
        fs::remove_dir_all(&tmp).ok();
    }
}
