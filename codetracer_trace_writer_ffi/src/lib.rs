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

use codetracer_trace_types::{CallKey, EventLogKind, Line, PassBy, Place, RValue, TypeKind, ValueRecord, VariableId};
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer::{TraceEventsFileFormat, create_trace_writer};
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

/// `PassBy` mirror for the FFI.
#[repr(C)]
pub enum FfiPassBy {
    Value = 0,
    Reference = 1,
}

/// `RValue` kind discriminator for the FFI surface introduced in M14.
///
/// The discriminator is used by [`ct_assignment`] to pick the variant
/// inside the resulting `RValue` payload from the supplied scalar
/// arguments. The exact field semantics per discriminator are
/// documented on `ct_assignment`.
#[repr(C)]
pub enum FfiRValueKind {
    Simple = 0,
    Compound = 1,
    Literal = 2,
    FieldAccess = 3,
    IndexAccess = 4,
    FunctionReturn = 5,
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

fn to_pass_by(p: FfiPassBy) -> PassBy {
    match p {
        FfiPassBy::Value => PassBy::Value,
        FfiPassBy::Reference => PassBy::Reference,
    }
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
pub unsafe extern "C" fn trace_writer_new(program: *const c_char, format: FfiTraceFormat) -> *mut TraceWriterHandle {
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
pub unsafe extern "C" fn trace_writer_begin_metadata(handle: *mut TraceWriterHandle, path: *const c_char) -> bool {
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
pub unsafe extern "C" fn trace_writer_begin_events(handle: *mut TraceWriterHandle, path: *const c_char) -> bool {
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
pub unsafe extern "C" fn trace_writer_begin_paths(handle: *mut TraceWriterHandle, path: *const c_char) -> bool {
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
pub unsafe extern "C" fn trace_writer_start(handle: *mut TraceWriterHandle, path: *const c_char, line: i64) {
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
pub unsafe extern "C" fn trace_writer_set_workdir(handle: *mut TraceWriterHandle, workdir: *const c_char) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::set_workdir(w(h), Path::new(unsafe { cstr_to_str(workdir) }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_step(handle: *mut TraceWriterHandle, path: *const c_char, line: i64) {
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
    let fid = TraceWriter::ensure_function_id(w(h), unsafe { cstr_to_str(name) }, Path::new(unsafe { cstr_to_str(path) }), Line(line));
    fid.0
}

/// Register a type and return its ID.  Returns `usize::MAX` on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_ensure_type_id(handle: *mut TraceWriterHandle, kind: FfiTypeKind, lang_type: *const c_char) -> usize {
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
pub unsafe extern "C" fn trace_writer_register_call(handle: *mut TraceWriterHandle, function_id: usize) {
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
    let type_id = TraceWriter::ensure_type_id(w(h), to_type_kind(type_kind), unsafe { cstr_to_str(type_name) });
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
    let type_id = TraceWriter::ensure_type_id(w(h), to_type_kind(type_kind), unsafe { cstr_to_str(type_name) });
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
    let type_id = TraceWriter::ensure_type_id(w(h), to_type_kind(type_kind), unsafe { cstr_to_str(type_name) });
    TraceWriter::register_variable_with_full_value(w(h), unsafe { cstr_to_str(name) }, ValueRecord::Int { i: value, type_id });
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
    let type_id = TraceWriter::ensure_type_id(w(h), to_type_kind(type_kind), unsafe { cstr_to_str(type_name) });
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
    TraceWriter::register_special_event(w(h), to_event_log_kind(kind), unsafe { cstr_to_str(metadata) }, unsafe {
        cstr_to_str(content)
    });
}

// ---------------------------------------------------------------------------
// M14 — Assignment / BindVariable FFI surface
// ---------------------------------------------------------------------------

/// Helper: turn the FFI representation of an `RValue` into the Rust value.
///
/// The discriminator selects which of the supplied arguments are read.  The
/// arguments are interpreted as follows:
///
/// | discriminator    | source                                                                       |
/// | ---------------- | ---------------------------------------------------------------------------- |
/// | `Simple`         | `simple_variable_id`                                                         |
/// | `Compound`       | `compound_ids[0..compound_len]`                                              |
/// | `Literal`        | _(no arguments)_                                                             |
/// | `FieldAccess`    | `simple_variable_id` (= receiver), `field_name`                              |
/// | `IndexAccess`    | `simple_variable_id` (= receiver), `index`                                   |
/// | `FunctionReturn` | `call_key`                                                                   |
///
/// On unrecognised inputs the function returns `RValue::Compound(vec![])` so
/// callers cannot accidentally emit malformed events.
#[allow(clippy::too_many_arguments)]
fn build_rvalue(
    rvalue_kind: FfiRValueKind,
    simple_variable_id: usize,
    compound_ids: *const usize,
    compound_len: usize,
    field_name: *const c_char,
    index: i64,
    call_key: i64,
) -> RValue {
    match rvalue_kind {
        FfiRValueKind::Simple => RValue::Simple(VariableId(simple_variable_id)),
        FfiRValueKind::Compound => {
            if compound_ids.is_null() || compound_len == 0 {
                RValue::Compound(Vec::new())
            } else {
                let slice = unsafe { std::slice::from_raw_parts(compound_ids, compound_len) };
                RValue::Compound(slice.iter().copied().map(VariableId).collect())
            }
        }
        FfiRValueKind::Literal => RValue::Literal,
        FfiRValueKind::FieldAccess => RValue::FieldAccess {
            receiver: VariableId(simple_variable_id),
            field: unsafe { cstr_to_str(field_name) }.to_string(),
        },
        FfiRValueKind::IndexAccess => RValue::IndexAccess {
            receiver: VariableId(simple_variable_id),
            index,
        },
        FfiRValueKind::FunctionReturn => RValue::FunctionReturn { call_key: CallKey(call_key) },
    }
}

/// Emit an `Assignment` event.
///
/// `target_name` is the destination variable's display name (it is interned
/// by the writer if not already present). The other arguments describe the
/// RHS via [`FfiRValueKind`]; see [`build_rvalue`] for the per-discriminator
/// argument semantics.
///
/// # Safety
///
/// `handle` must be a writer obtained from [`trace_writer_new`].
/// `target_name` must be a valid NUL-terminated UTF-8 C string. The
/// `compound_ids` pointer (if non-null) must point at `compound_len`
/// contiguous `usize` values. `field_name` (if used) must be a valid
/// NUL-terminated UTF-8 C string.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn ct_assignment(
    handle: *mut TraceWriterHandle,
    target_name: *const c_char,
    pass_by: FfiPassBy,
    rvalue_kind: FfiRValueKind,
    simple_variable_id: usize,
    compound_ids: *const usize,
    compound_len: usize,
    field_name: *const c_char,
    index: i64,
    call_key: i64,
) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    let rvalue = build_rvalue(rvalue_kind, simple_variable_id, compound_ids, compound_len, field_name, index, call_key);
    let name = unsafe { cstr_to_str(target_name) };
    TraceWriter::assign(w(h), name, rvalue, to_pass_by(pass_by));
}

/// Emit a `BindVariable` event associating `variable_name` with `place`.
///
/// # Safety
///
/// `handle` must be a writer obtained from [`trace_writer_new`].
/// `variable_name` must be a valid NUL-terminated UTF-8 C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_bind_variable(handle: *mut TraceWriterHandle, variable_name: *const c_char, place: i64) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    TraceWriter::bind_variable(w(h), unsafe { cstr_to_str(variable_name) }, Place(place));
}

/// Emit a `Step` event at (path, line, column).
///
/// `column` is taken as-is when `has_column` is non-zero; otherwise the
/// event is recorded without column information. This matches the M14
/// back-compat rule (recorders without column data continue to emit the
/// legacy-shaped Step event).
///
/// # Safety
///
/// `handle` must be a writer obtained from [`trace_writer_new`].
/// `path` must be a valid NUL-terminated UTF-8 C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_assignment_with_column(handle: *mut TraceWriterHandle, path: *const c_char, line: i64, column: i64, has_column: bool) {
    if handle.is_null() {
        return;
    }
    let h = unsafe { &mut *handle };
    let column_opt = if has_column { Some(Line(column)) } else { None };
    TraceWriter::register_step_with_column(w(h), Path::new(unsafe { cstr_to_str(path) }), Line(line), column_opt);
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
        let fid = unsafe { trace_writer_ensure_function_id(handle, fn_name.as_ptr(), source.as_ptr(), 1) };
        assert_ne!(fid, usize::MAX);

        let var_name = CString::new("x").unwrap();
        let type_name = CString::new("i32").unwrap();
        unsafe { trace_writer_register_variable_int(handle, var_name.as_ptr(), 42, FfiTypeKind::Int, type_name.as_ptr()) };

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

    /// M14 verification: the FFI surface that Nim recorders use must produce
    /// `Assignment` events the Rust reader recognises. This exercises the
    /// JSON path so the assertion is deterministic across the workspace's
    /// available encoders (the CBOR-zstd path is exercised by the dedicated
    /// `m14_rvalue_roundtrip` reader test).
    #[test]
    fn test_ffi_emits_assignment_and_step_with_column() {
        let tmp = std::env::temp_dir().join(format!("ffi_m14_test_{}", std::process::id()));
        fs::create_dir_all(&tmp).unwrap();

        let program = CString::new("m14_test").unwrap();
        let handle = unsafe { trace_writer_new(program.as_ptr(), FfiTraceFormat::Json) };
        assert!(!handle.is_null());

        let meta_path = CString::new(tmp.join("trace_metadata.json").to_str().unwrap()).unwrap();
        let events_path = CString::new(tmp.join("trace.json").to_str().unwrap()).unwrap();
        let paths_path = CString::new(tmp.join("trace_paths.json").to_str().unwrap()).unwrap();

        assert!(unsafe { trace_writer_begin_metadata(handle, meta_path.as_ptr()) });
        assert!(unsafe { trace_writer_begin_events(handle, events_path.as_ptr()) });
        assert!(unsafe { trace_writer_begin_paths(handle, paths_path.as_ptr()) });

        let source = CString::new("/m14/main.rs").unwrap();
        unsafe { trace_writer_start(handle, source.as_ptr(), 1) };

        // Step with column 7
        unsafe { ct_assignment_with_column(handle, source.as_ptr(), 2, 7, true) };

        // Bind a variable to a place
        let var_name = CString::new("x").unwrap();
        unsafe { ct_bind_variable(handle, var_name.as_ptr(), 42) };

        // Emit an Assignment with RValue::Literal (x = 10)
        unsafe {
            ct_assignment(
                handle,
                var_name.as_ptr(),
                FfiPassBy::Value,
                FfiRValueKind::Literal,
                /* simple */ 0,
                /* compound */ std::ptr::null(),
                0,
                /* field */ std::ptr::null(),
                0,
                0,
            )
        };

        // Emit a FunctionReturn assignment (result = foo())
        let target_name = CString::new("result").unwrap();
        unsafe {
            ct_assignment(
                handle,
                target_name.as_ptr(),
                FfiPassBy::Value,
                FfiRValueKind::FunctionReturn,
                0,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                /* call_key */ 17,
            )
        };

        assert!(unsafe { trace_writer_finish_events(handle) });
        assert!(unsafe { trace_writer_finish_metadata(handle) });
        assert!(unsafe { trace_writer_finish_paths(handle) });
        unsafe { trace_writer_free(handle) };

        // Read back via serde_json and assert the M14 surface roundtripped.
        let trace_content = fs::read_to_string(tmp.join("trace.json")).unwrap();
        let events: Vec<codetracer_trace_types::TraceLowLevelEvent> =
            serde_json::from_str(&trace_content).expect("trace.json must be a valid event stream");

        // ct_assignment_with_column drops the column at the legacy layer
        // (the legacy StepRecord doesn't carry column metadata) — assert
        // the Step still lands with the correct line so the FFI's
        // column-dropping shim is exercised end-to-end.
        let step = events
            .iter()
            .find_map(|e| match e {
                codetracer_trace_types::TraceLowLevelEvent::Step(s) if s.line == Line(2) => Some(s),
                _ => None,
            })
            .expect("expected a Step at line 2 from ct_assignment_with_column");
        assert_eq!(step.line, Line(2));

        // Find the BindVariable event
        let bind = events
            .iter()
            .find_map(|e| match e {
                codetracer_trace_types::TraceLowLevelEvent::BindVariable(b) => Some(b),
                _ => None,
            })
            .expect("expected a BindVariable from ct_bind_variable");
        assert_eq!(bind.place, Place(42));

        // Find the Literal Assignment
        let literal_assignment = events
            .iter()
            .find_map(|e| match e {
                codetracer_trace_types::TraceLowLevelEvent::Assignment(a) if matches!(a.from, RValue::Literal) => Some(a),
                _ => None,
            })
            .expect("expected an Assignment with RValue::Literal");
        assert!(matches!(literal_assignment.from, RValue::Literal));

        // Find the FunctionReturn Assignment
        let fnret_assignment = events
            .iter()
            .find_map(|e| match e {
                codetracer_trace_types::TraceLowLevelEvent::Assignment(a) if matches!(a.from, RValue::FunctionReturn { .. }) => Some(a),
                _ => None,
            })
            .expect("expected an Assignment with RValue::FunctionReturn");
        match &fnret_assignment.from {
            RValue::FunctionReturn { call_key } => assert_eq!(*call_key, CallKey(17)),
            _ => unreachable!(),
        }

        fs::remove_dir_all(&tmp).ok();
    }
}
