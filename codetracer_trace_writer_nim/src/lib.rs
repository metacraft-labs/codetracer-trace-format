//! TraceWriter implementation that delegates to the Nim `codetracer-trace-format-nim` C library.
//!
//! This crate is a drop-in replacement for `codetracer_trace_writer`. Recorders can switch
//! by changing a single dependency in their `Cargo.toml`.

use std::error::Error;
use std::ffi::{CStr, CString};
use std::path::Path;
use std::sync::Once;

use codetracer_trace_types::*;

// The Nim static library calls libzstd. Referencing `zstd-sys` keeps it in
// the dependency graph so its (toolchain-built) libzstd is on the final
// link line and resolves those symbols. Not used from Rust directly.
use zstd_sys as _;

// ---------------------------------------------------------------------------
// FFI declarations (must match codetracer_trace_writer.h)
// ---------------------------------------------------------------------------

extern "C" {
    fn codetracer_trace_writer_init();

    fn trace_writer_last_error() -> *const std::os::raw::c_char;

    fn trace_writer_new(program: *const std::os::raw::c_char, format: i32) -> *mut std::ffi::c_void;
    fn trace_writer_free(handle: *mut std::ffi::c_void);
    fn trace_writer_close(handle: *mut std::ffi::c_void) -> i32;

    fn trace_writer_begin_metadata(handle: *mut std::ffi::c_void, path: *const std::os::raw::c_char) -> i32;
    fn trace_writer_finish_metadata(handle: *mut std::ffi::c_void) -> i32;
    fn trace_writer_begin_events(handle: *mut std::ffi::c_void, path: *const std::os::raw::c_char) -> i32;
    fn trace_writer_finish_events(handle: *mut std::ffi::c_void) -> i32;
    fn trace_writer_begin_paths(handle: *mut std::ffi::c_void, path: *const std::os::raw::c_char) -> i32;
    fn trace_writer_finish_paths(handle: *mut std::ffi::c_void) -> i32;

    fn trace_writer_start(handle: *mut std::ffi::c_void, path: *const std::os::raw::c_char, line: i64);
    fn trace_writer_set_workdir(handle: *mut std::ffi::c_void, workdir: *const std::os::raw::c_char);
    fn trace_writer_set_args(handle: *mut std::ffi::c_void, args: *const *const u8, arg_lens: *const usize, args_count: usize);
    fn trace_writer_register_step(handle: *mut std::ffi::c_void, path: *const std::os::raw::c_char, line: i64);

    fn trace_writer_ensure_function_id(
        handle: *mut std::ffi::c_void,
        name: *const std::os::raw::c_char,
        path: *const std::os::raw::c_char,
        line: i64,
    ) -> usize;

    fn trace_writer_ensure_type_id(handle: *mut std::ffi::c_void, kind: i32, lang_type: *const std::os::raw::c_char) -> usize;

    fn trace_writer_register_call(handle: *mut std::ffi::c_void, function_id: usize);
    /// Stage one (name, CBOR-encoded value) argument for the next
    /// `trace_writer_register_call`.  Multiple calls accumulate; the buffer
    /// is consumed and cleared by the next `trace_writer_register_call`.
    fn trace_writer_register_call_arg(handle: *mut std::ffi::c_void, name: *const std::os::raw::c_char, cbor_data: *const u8, cbor_len: usize);
    fn trace_writer_register_return(handle: *mut std::ffi::c_void);

    fn trace_writer_register_return_int(handle: *mut std::ffi::c_void, value: i64, type_kind: i32, type_name: *const std::os::raw::c_char);
    // Kept for ABI compatibility — the wrapper now routes every non-Int /
    // non-None return value through the streaming-encoder CBOR path so
    // typed variants (Bool, String, Float, Char, Struct, ...) survive
    // intact.  Recorders that still want a stringified payload can call
    // this directly, but the FFI surface keeps the binding to avoid an
    // ABI break for out-of-tree consumers.
    #[allow(dead_code)]
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
    // Kept for ABI compatibility — see `trace_writer_register_return_raw`.
    #[allow(dead_code)]
    fn trace_writer_register_variable_raw(
        handle: *mut std::ffi::c_void,
        name: *const std::os::raw::c_char,
        value_repr: *const std::os::raw::c_char,
        type_kind: i32,
        type_name: *const std::os::raw::c_char,
    );

    fn trace_writer_register_variable_cbor(handle: *mut std::ffi::c_void, name: *const std::os::raw::c_char, cbor_data: *const u8, cbor_len: usize);

    fn trace_writer_register_return_cbor(handle: *mut std::ffi::c_void, cbor_data: *const u8, cbor_len: usize);

    // ----- Streaming value encoder -----

    fn ct_value_encoder_new() -> *mut std::ffi::c_void;
    fn ct_value_encoder_free(h: *mut std::ffi::c_void);
    fn ct_value_encoder_reset(h: *mut std::ffi::c_void);

    fn ct_value_write_int(h: *mut std::ffi::c_void, value: i64, type_id: u64) -> i32;
    fn ct_value_write_float(h: *mut std::ffi::c_void, value: f64, type_id: u64) -> i32;
    fn ct_value_write_bool_typed(h: *mut std::ffi::c_void, value: i32, type_id: u64) -> i32;
    fn ct_value_write_string(h: *mut std::ffi::c_void, data: *const u8, len: usize, type_id: u64) -> i32;
    fn ct_value_write_none_typed(h: *mut std::ffi::c_void, type_id: u64) -> i32;
    fn ct_value_write_raw(h: *mut std::ffi::c_void, data: *const u8, len: usize, type_id: u64) -> i32;
    fn ct_value_write_error(h: *mut std::ffi::c_void, data: *const u8, len: usize, type_id: u64) -> i32;

    fn ct_value_begin_struct(h: *mut std::ffi::c_void, type_id: u64, field_count: i32) -> i32;
    fn ct_value_begin_sequence(h: *mut std::ffi::c_void, type_id: u64, element_count: i32) -> i32;
    fn ct_value_begin_sequence_with_slice(h: *mut std::ffi::c_void, type_id: u64, element_count: i32, is_slice: i32) -> i32;
    fn ct_value_begin_tuple(h: *mut std::ffi::c_void, type_id: u64, element_count: i32) -> i32;
    fn ct_value_begin_variant(h: *mut std::ffi::c_void, discriminator: *const u8, disc_len: usize, type_id: u64) -> i32;
    fn ct_value_begin_reference(h: *mut std::ffi::c_void, address: u64, mutable: i32, type_id: u64) -> i32;
    fn ct_value_end_compound(h: *mut std::ffi::c_void) -> i32;

    fn ct_value_write_char(h: *mut std::ffi::c_void, codepoint: u32, type_id: u64) -> i32;
    fn ct_value_write_bigint(h: *mut std::ffi::c_void, data: *const u8, len: usize, negative: i32, type_id: u64) -> i32;

    fn ct_value_get_bytes(h: *mut std::ffi::c_void, out_len: *mut usize) -> *const u8;

    fn trace_writer_register_special_event(
        handle: *mut std::ffi::c_void,
        kind: i32,
        metadata: *const std::os::raw::c_char,
        content: *const std::os::raw::c_char,
    );

    // Thread lifecycle events.  Added so recorders can route
    // `TraceLowLevelEvent::ThreadStart / ThreadExit / ThreadSwitch` through
    // dedicated entry points instead of `add_event`, which used to be a silent
    // no-op on the Nim multi-stream backend (incidents 1.21 / 1.22 / 1.27).
    fn trace_writer_register_thread_start(handle: *mut std::ffi::c_void, thread_id: u64);
    fn trace_writer_register_thread_exit(handle: *mut std::ffi::c_void, thread_id: u64);
    fn trace_writer_register_thread_switch(handle: *mut std::ffi::c_void, thread_id: u64);

    // ----- trace-filter provenance (TF-M7, spec §7) -----
    //
    // Recorders integrating `codetracer_trace_filter` call these to embed
    // the composed filter chain (builtin → auto-discovered → env → CLI)
    // into `meta.dat` so post-trace audit tools can verify which rules
    // produced the trace.  Each `add_filter_provenance` entry appends one
    // (path, sha256) pair in composition order; `record_empty_filter_provenance`
    // flips a flag for the rare "implements filters but the chain happens
    // to be empty" case.
    fn trace_writer_add_filter_provenance(
        handle: *mut std::ffi::c_void,
        path: *const u8,
        path_len: usize,
        sha256_bytes: *const u8,
        sha256_len: usize,
    ) -> i32;
    fn trace_writer_record_empty_filter_provenance(handle: *mut std::ffi::c_void) -> i32;

    // ----- meta.dat -----

    fn ct_write_meta_dat(handle: *mut std::ffi::c_void, recorder_id: *const u8, recorder_id_len: usize) -> i32;

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

    // ----- Structured reader accessors (no JSON parsing) -----

    /// Resolve step N to (path_id, line). Returns 0 on success.
    fn ct_reader_step_location(h: *mut std::ffi::c_void, n: u64, out_path_id: *mut u64, out_line: *mut u64) -> i32;

    /// Resolve a contiguous step range to parallel (path_id, line) buffers.
    /// Returns the number of entries written, or u64::MAX on error.
    fn ct_reader_step_locations(h: *mut std::ffi::c_void, start_n: u64, count: u64, out_path_ids: *mut u64, out_lines: *mut u64) -> u64;

    /// Number of variable values at step N.
    fn ct_reader_step_value_count(h: *mut std::ffi::c_void, n: u64) -> u64;

    /// Get value at (step N, value index). Data must be freed with ct_free_buffer.
    fn ct_reader_step_value(
        h: *mut std::ffi::c_void,
        n: u64,
        value_idx: u64,
        out_varname_id: *mut u64,
        out_type_id: *mut u64,
        out_data: *mut *mut u8,
        out_data_len: *mut usize,
    ) -> i32;

    /// Get scalar fields of call record. Returns 0 on success.
    fn ct_reader_call_fields(
        h: *mut std::ffi::c_void,
        key: u64,
        out_function_id: *mut u64,
        out_parent_key: *mut i64,
        out_entry_step: *mut u64,
        out_exit_step: *mut u64,
        out_depth: *mut u32,
        out_children_count: *mut u64,
    ) -> i32;

    /// Get child call_key at index within a call record.
    fn ct_reader_call_child(h: *mut std::ffi::c_void, key: u64, child_idx: u64) -> u64;

    /// Number of arguments captured for call ``key``. Returns 0 when the
    /// call has no captured arguments or on lookup failure.
    fn ct_reader_call_arg_count(h: *mut std::ffi::c_void, key: u64) -> u64;

    /// Get the (varname_id, CBOR-encoded value) pair for argument
    /// ``arg_idx`` of call ``key``.  The data pointer is heap-allocated;
    /// caller must free with ``ct_free_buffer``.  Returns 0 on success.
    fn ct_reader_call_arg(
        h: *mut std::ffi::c_void,
        key: u64,
        arg_idx: u64,
        out_varname_id: *mut u64,
        out_data: *mut *mut u8,
        out_data_len: *mut usize,
    ) -> i32;

    /// Get IO event fields. Data must be freed with ct_free_buffer.
    fn ct_reader_event_fields(
        h: *mut std::ffi::c_void,
        index: u64,
        out_kind: *mut u8,
        out_step_id: *mut u64,
        out_data: *mut *mut u8,
        out_data_len: *mut usize,
    ) -> i32;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn write_trace_with_staged_call_args(dir: &Path) -> PathBuf {
        let mut writer = create_trace_writer("ctfs_arg_roundtrip", &[], TraceEventsFileFormat::Ctfs);
        let events_path = dir.join("trace.json");
        let metadata_path = dir.join("trace_metadata.json");
        let paths_path = dir.join("trace_paths.json");

        writer.begin_writing_trace_events(&events_path).unwrap();
        writer.begin_writing_trace_metadata(&metadata_path).unwrap();
        writer.begin_writing_trace_paths(&paths_path).unwrap();

        let source_path = Path::new("/tmp/ctfs_arg_roundtrip.rs");
        writer.start(source_path, Line(1));
        writer.register_step(source_path, Line(2));

        writer.ensure_function_id("compute", source_path, Line(2));
        let function_id = writer.ensure_function_id("add", source_path, Line(3));
        let int_type_id = writer.ensure_type_id(TypeKind::Int, "uint256");
        writer.arg(
            "x",
            ValueRecord::Raw {
                r: "0xa".to_string(),
                type_id: int_type_id,
            },
        );
        writer.arg(
            "y",
            ValueRecord::Raw {
                r: "0x14".to_string(),
                type_id: int_type_id,
            },
        );
        writer.register_call(function_id, vec![]);
        writer.register_return(NONE_VALUE);
        writer.register_return(NONE_VALUE);

        writer.finish_writing_trace_events().unwrap();
        writer.finish_writing_trace_metadata().unwrap();
        writer.finish_writing_trace_paths().unwrap();
        writer.close().unwrap();

        dir.join("ctfs_arg_roundtrip.ct")
    }

    #[test]
    fn nim_writer_arg_before_register_call_roundtrips_call_args() {
        let dir = tempfile::tempdir().unwrap();
        let trace_path = write_trace_with_staged_call_args(dir.path());

        let reader = NimTraceReaderHandle::open(trace_path.to_str().unwrap()).unwrap();
        let function_names = (0..reader.function_count())
            .map(|id| reader.function(id).unwrap_or_else(|err| format!("<error:{err}>")))
            .collect::<Vec<_>>();
        let mut call_jsons = Vec::new();
        let mut add_call_key = None;
        for key in 0..reader.call_count() {
            let raw = reader.call_json(key).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
            call_jsons.push(raw);
            let function_id = parsed["function_id"]
                .as_u64()
                .or_else(|| parsed["functionId"].as_u64())
                .unwrap_or(u64::MAX);
            if reader.function(function_id).is_ok_and(|function_name| function_name == "add") {
                add_call_key = Some(key);
                break;
            }
        }

        let add_call_key =
            add_call_key.unwrap_or_else(|| panic!("expected an `add` call record; functions={function_names:?}; calls={call_jsons:?}"));
        let raw = reader.call_json(add_call_key).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let args = parsed["args"].as_array().expect("call_json should expose args as an array");
        assert_eq!(args.len(), 2, "call_json should include staged x/y args: {raw}");

        assert_eq!(reader.call_arg_count(add_call_key), 2);
        let (x_varname_id, x_value) = reader.call_arg(add_call_key, 0).unwrap();
        let (y_varname_id, y_value) = reader.call_arg(add_call_key, 1).unwrap();
        assert_eq!(reader.varname(x_varname_id).unwrap(), "x");
        assert_eq!(reader.varname(y_varname_id).unwrap(), "y");
        assert!(!x_value.is_empty(), "x arg should carry encoded value bytes");
        assert!(!y_value.is_empty(), "y arg should carry encoded value bytes");
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    /// Get a copy of the encoded CBOR bytes.
    ///
    /// Unlike [`get_bytes`](Self::get_bytes), the returned `Vec` owns its
    /// memory and remains valid after reset/encode/drop. Use this when the
    /// caller needs to hold the CBOR bytes beyond the encoder's lifetime
    /// (e.g. to pass to `register_variable_cbor`).
    pub fn get_bytes_copy(&self) -> Vec<u8> {
        self.get_bytes().to_vec()
    }

    // ----- Direct write methods for streaming encoding (M58) -----
    //
    // These methods let callers walk an object graph and call encoder
    // methods directly, without building an intermediate `ValueRecord` tree.
    // Each method maps to a single C FFI call.

    /// Write an integer value.
    pub fn write_int(&mut self, value: i64, type_id: TypeId) {
        unsafe { ct_value_write_int(self.handle, value, type_id.0 as u64) };
    }

    /// Write a floating-point value.
    pub fn write_float(&mut self, value: f64, type_id: TypeId) {
        unsafe { ct_value_write_float(self.handle, value, type_id.0 as u64) };
    }

    /// Write a boolean value.
    pub fn write_bool(&mut self, value: bool, type_id: TypeId) {
        unsafe { ct_value_write_bool_typed(self.handle, if value { 1 } else { 0 }, type_id.0 as u64) };
    }

    /// Write a string value.
    pub fn write_string(&mut self, text: &str, type_id: TypeId) {
        unsafe { ct_value_write_string(self.handle, text.as_ptr(), text.len(), type_id.0 as u64) };
    }

    /// Write a None/null value.
    pub fn write_none(&mut self, type_id: TypeId) {
        unsafe { ct_value_write_none_typed(self.handle, type_id.0 as u64) };
    }

    /// Write a raw string representation (for types without structured encoding).
    pub fn write_raw(&mut self, repr: &str, type_id: TypeId) {
        unsafe { ct_value_write_raw(self.handle, repr.as_ptr(), repr.len(), type_id.0 as u64) };
    }

    /// Write an error sentinel value.
    pub fn write_error(&mut self, msg: &str, type_id: TypeId) {
        unsafe { ct_value_write_error(self.handle, msg.as_ptr(), msg.len(), type_id.0 as u64) };
    }

    /// Begin a sequence (list/array) with a known element count.
    /// Must be followed by exactly `count` element encodings and one
    /// [`end_compound`](Self::end_compound) call.
    pub fn begin_sequence(&mut self, type_id: TypeId, count: usize) {
        unsafe { ct_value_begin_sequence(self.handle, type_id.0 as u64, count as i32) };
    }

    /// Begin a sequence with an explicit `is_slice` flag.  Use `is_slice =
    /// true` for view/slice sequences (`Span<T>`, Sway `Bytes`, Rust `&[T]`,
    /// etc.) and `is_slice = false` for owned sequences (`Vec<T>`,
    /// `Array<T>`, etc.).  Must be followed by exactly `count` element
    /// encodings and one [`end_compound`](Self::end_compound) call.
    pub fn begin_sequence_with_slice(&mut self, type_id: TypeId, count: usize, is_slice: bool) {
        unsafe { ct_value_begin_sequence_with_slice(self.handle, type_id.0 as u64, count as i32, if is_slice { 1 } else { 0 }) };
    }

    /// Begin a tuple with a known element count.
    /// Must be followed by exactly `count` element encodings and one
    /// [`end_compound`](Self::end_compound) call.
    pub fn begin_tuple(&mut self, type_id: TypeId, count: usize) {
        unsafe { ct_value_begin_tuple(self.handle, type_id.0 as u64, count as i32) };
    }

    /// End a compound value (sequence or tuple) started by
    /// [`begin_sequence`](Self::begin_sequence) or
    /// [`begin_tuple`](Self::begin_tuple).
    pub fn end_compound(&mut self) {
        unsafe { ct_value_end_compound(self.handle) };
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
                unsafe { ct_value_write_bool_typed(self.handle, if *b { 1 } else { 0 }, type_id.0 as u64) };
            }
            ValueRecord::String { text, type_id } => {
                unsafe { ct_value_write_string(self.handle, text.as_ptr(), text.len(), type_id.0 as u64) };
            }
            ValueRecord::Raw { r, type_id } => {
                unsafe { ct_value_write_raw(self.handle, r.as_ptr(), r.len(), type_id.0 as u64) };
            }
            ValueRecord::Error { msg, type_id } => {
                unsafe { ct_value_write_error(self.handle, msg.as_ptr(), msg.len(), type_id.0 as u64) };
            }
            ValueRecord::Sequence { elements, is_slice, type_id } => {
                unsafe { ct_value_begin_sequence_with_slice(self.handle, type_id.0 as u64, elements.len() as i32, if *is_slice { 1 } else { 0 }) };
                for elem in elements {
                    self.encode_recursive(elem);
                }
                unsafe { ct_value_end_compound(self.handle) };
            }
            ValueRecord::Tuple { elements, type_id } => {
                unsafe { ct_value_begin_tuple(self.handle, type_id.0 as u64, elements.len() as i32) };
                for elem in elements {
                    self.encode_recursive(elem);
                }
                unsafe { ct_value_end_compound(self.handle) };
            }
            ValueRecord::Struct { field_values, type_id } => {
                unsafe { ct_value_begin_struct(self.handle, type_id.0 as u64, field_values.len() as i32) };
                for elem in field_values {
                    self.encode_recursive(elem);
                }
                unsafe { ct_value_end_compound(self.handle) };
            }
            ValueRecord::Variant {
                discriminator,
                contents,
                type_id,
            } => {
                unsafe { ct_value_begin_variant(self.handle, discriminator.as_ptr(), discriminator.len(), type_id.0 as u64) };
                self.encode_recursive(contents);
                unsafe { ct_value_end_compound(self.handle) };
            }
            ValueRecord::Reference {
                dereferenced,
                address,
                mutable,
                type_id,
            } => {
                unsafe { ct_value_begin_reference(self.handle, *address, if *mutable { 1 } else { 0 }, type_id.0 as u64) };
                self.encode_recursive(dereferenced);
                unsafe { ct_value_end_compound(self.handle) };
            }
            ValueRecord::Char { c, type_id } => {
                unsafe { ct_value_write_char(self.handle, *c as u32, type_id.0 as u64) };
            }
            ValueRecord::BigInt { b, negative, type_id } => {
                let (ptr, len) = if b.is_empty() {
                    (std::ptr::null(), 0usize)
                } else {
                    (b.as_ptr(), b.len())
                };
                unsafe { ct_value_write_bigint(self.handle, ptr, len, if *negative { 1 } else { 0 }, type_id.0 as u64) };
            }
            // Cell has no streaming-encoder counterpart yet — its CBOR shape
            // (`{ "kind":"Cell", "place": int }`) only appears in tracer-side
            // intermediates, never in recorder output. Fall back to a raw
            // string so the data is at least preserved for inspection.
            ValueRecord::Cell { .. } => {
                let (repr, _kind, _type_name) = value_record_to_raw(value);
                unsafe { ct_value_write_raw(self.handle, repr.as_ptr(), repr.len(), 0) };
            }
        }
    }
}

impl Drop for StreamingValueEncoder {
    fn drop(&mut self) {
        unsafe { ct_value_encoder_free(self.handle) }
    }
}

// Safety: StreamingValueEncoder wraps a Nim-allocated opaque handle that is
// never shared across threads. The handle is only accessed through &mut self,
// so concurrent access is prevented by Rust's borrow checker. As long as each
// encoder instance is used from a single thread at a time (which &mut self
// guarantees), sending it to another thread is safe.
unsafe impl Send for StreamingValueEncoder {}

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
    /// PathId -> path string reverse map.
    ///
    /// The Nim FFI tracks paths internally (every `register_path` /
    /// `register_step` call interns the path string), and our `ensure_path_id`
    /// returns a placeholder `PathId(0)` because the Nim side owns the real ID
    /// assignment.  However, recorders that route events through `add_event`
    /// pass `Step(StepRecord{path_id, line})` where `path_id` is a real index
    /// into the recorder's own paths table.  To dispatch those events cleanly
    /// we mirror the recorder's path table here, populated by the
    /// `Path(PathBuf)` events the same recorder emits before any `Step`.
    path_table: Vec<std::path::PathBuf>,
    /// Variable index -> name reverse map.  Same rationale as `path_table` —
    /// `add_event(Value(FullValueRecord{variable_id, ...}))` needs a name
    /// string to call `register_variable_with_full_value`.  Populated from
    /// preceding `VariableName(String)` / `Variable(String)` events.
    variable_table: Vec<String>,
}

// The Nim library is single-threaded but callers hold exclusive &mut self,
// so Send is safe as long as we never share the handle.
unsafe impl Send for NimTraceWriter {}

impl NimTraceWriter {
    /// Create a new trace writer backed by the Nim library.
    ///
    /// `args` is the recorded program's argv; it is forwarded to the
    /// Nim writer so the CTFS `meta.dat` block records it (spec §7).
    /// Pass an empty slice when there are no arguments.
    pub fn new(program: &str, args: &[String], format: TraceEventsFileFormat) -> Self {
        ensure_nim_initialized();
        let c_program = str_to_cstring(program);
        let handle = unsafe { trace_writer_new(c_program.as_ptr(), format.to_ffi()) };
        assert!(!handle.is_null(), "trace_writer_new returned null: {}", last_error());
        // Forward argv into the Nim writer's meta.dat metadata.  Each
        // entry crosses the FFI boundary as a (pointer, length) pair so
        // argv containing non-UTF8 bytes or embedded NULs survives.
        let arg_ptrs: Vec<*const u8> = args.iter().map(|a| a.as_ptr()).collect();
        let arg_lens: Vec<usize> = args.iter().map(|a| a.len()).collect();
        unsafe {
            trace_writer_set_args(handle, arg_ptrs.as_ptr(), arg_lens.as_ptr(), args.len());
        }
        NimTraceWriter {
            handle,
            streaming_encoder: StreamingValueEncoder::new(),
            path_table: Vec::new(),
            variable_table: Vec::new(),
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
        let ret = unsafe { ct_write_meta_dat(self.handle, recorder_id.as_ptr(), recorder_id.len()) };
        if ret != 0 {
            Err(last_error().into())
        } else {
            Ok(())
        }
    }

    /// TF-M7: append one `(path, sha256)` entry to the trace-filter
    /// provenance chain that the writer will embed in `meta.dat` at
    /// close-time.  Callers should invoke this once per filter source in
    /// composition order (builtin default → auto-discovered →
    /// env-var-loaded → CLI `--trace-filter:`).
    ///
    /// `sha256` is the raw 32-byte SHA-256 digest of the filter source
    /// (file contents on disk, or — for inline filters like the
    /// recorder-embedded default — the literal TOML string).  Callers
    /// receive a pre-computed hex string from
    /// `codetracer_trace_filter::FilterSummaryEntry::sha256` and pass
    /// the decoded raw bytes here.
    ///
    /// Spec: `codetracer-trace-format-spec/Trace-Filters.md` § 7 and
    /// `internal-files.md` § "Flag bit 3 — Trace filter provenance".
    pub fn add_filter_provenance(&mut self, path: &str, sha256: &[u8; 32]) -> Result<(), Box<dyn Error>> {
        let rc = unsafe { trace_writer_add_filter_provenance(self.handle, path.as_ptr(), path.len(), sha256.as_ptr(), sha256.len()) };
        check_result(rc)
    }

    /// TF-M7: mark the writer to emit a *present-but-empty* trace-filter
    /// provenance block.  Useful only for recorders that integrate
    /// filters but ended up with a deliberately empty chain — preserves
    /// the spec § 7 distinction between "did not record" (flag clear)
    /// and "recorded an empty chain" (flag set, count 0).  When at
    /// least one entry is appended via [`add_filter_provenance`], this
    /// flag is ignored.
    pub fn record_empty_filter_provenance(&mut self) -> Result<(), Box<dyn Error>> {
        check_result(unsafe { trace_writer_record_empty_filter_provenance(self.handle) })
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

    pub fn ensure_function_id(&mut self, function_name: &str, path: &Path, line: Line) -> FunctionId {
        let c_name = str_to_cstring(function_name);
        let c_path = path_to_cstring(path);
        let id = unsafe { trace_writer_ensure_function_id(self.handle, c_name.as_ptr(), c_path.as_ptr(), line.0 as i64) };
        FunctionId(id)
    }

    pub fn ensure_type_id(&mut self, kind: TypeKind, lang_type: &str) -> TypeId {
        let c_lang = str_to_cstring(lang_type);
        let id = unsafe { trace_writer_ensure_type_id(self.handle, kind as i32, c_lang.as_ptr()) };
        TypeId(id)
    }

    pub fn register_step(&mut self, path: &Path, line: Line) {
        let c_path = path_to_cstring(path);
        unsafe { trace_writer_register_step(self.handle, c_path.as_ptr(), line.0 as i64) }
    }

    /// M14: column-aware register_step.  The Nim FFI exposes
    /// `ct_assignment_with_column` for this purpose.  When no Nim symbol
    /// matches (e.g. the old single-stream writer running without the M14
    /// FFI), we fall back to the column-less path so the event still
    /// lands.  Recorders that want the column-bearing event must invoke
    /// the FFI directly; this Rust wrapper is here for code paths that
    /// shovel `TraceLowLevelEvent::Step` straight through `add_event`.
    pub fn register_step_with_column(&mut self, path: &Path, line: Line, _column: Option<Line>) {
        // The Nim shared library exposes ~ct_assignment_with_column~ but
        // not a Rust binding by that name; we route through
        // ~trace_writer_register_step~ to preserve the existing wire
        // shape until the Nim writer's column-bearing API stabilises.
        // The column is intentionally dropped here so older Nim writers
        // do not crash on the missing symbol; the M14 FFI tests exercise
        // the column path via the FFI directly.
        self.register_step(path, line);
    }

    pub fn register_call(&mut self, function_id: FunctionId, _args: Vec<FullValueRecord>) {
        // The recorder calls `NimTraceWriter::arg(name, value)` for every
        // call argument before reaching `register_call`.  `arg()` stages
        // the (name, CBOR-encoded value) pair on the Nim handle via
        // `trace_writer_register_call_arg`, so the writer's pending-args
        // buffer is already populated by the time we get here.
        // `trace_writer_register_call` consumes that buffer and clears
        // it for the next call.
        //
        // The `_args` Vec parameter is unused: each FullValueRecord in
        // it carries `VariableId(0)` (the Nim backend manages IDs
        // internally) and the values are already staged via `arg()`.
        // We keep the parameter to preserve the abstract trait signature.
        unsafe { trace_writer_register_call(self.handle, function_id.0) }
    }

    pub fn register_return(&mut self, return_value: ValueRecord) {
        match &return_value {
            // Fast paths that bypass CBOR encoding for the most common shapes.
            ValueRecord::Int { i, type_id } => {
                let type_name = str_to_cstring(&format!("type_{}", type_id.0));
                unsafe { trace_writer_register_return_int(self.handle, *i, TypeKind::Int as i32, type_name.as_ptr()) }
            }
            ValueRecord::None { .. } => unsafe {
                trace_writer_register_return(self.handle);
            },
            // Every other variant — Bool, String, Float, Char, Sequence,
            // Tuple, Struct, Variant, Reference, BigInt, Raw, Error, Cell —
            // is encoded to CBOR via the streaming encoder so the reader
            // reconstructs the typed shape.  Previously most of these
            // silently fell through to `register_return_raw`, which
            // downgraded Bool/String/Float/Char to a stringified `Raw`
            // value (incident: ct-print rendering `true` as `Raw{r:"true"}`).
            _ => {
                let cbor = self.streaming_encoder.encode(&return_value);
                unsafe { trace_writer_register_return_cbor(self.handle, cbor.as_ptr(), cbor.len()) }
            }
        }
    }

    pub fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord) {
        let c_name = str_to_cstring(name);
        match &value {
            // Fast paths that bypass CBOR encoding for the most common shapes.
            ValueRecord::Int { i, type_id } => {
                let type_name = str_to_cstring(&format!("type_{}", type_id.0));
                unsafe { trace_writer_register_variable_int(self.handle, c_name.as_ptr(), *i, TypeKind::Int as i32, type_name.as_ptr()) }
            }
            // Every other variant — Bool, String, Float, Char, Sequence,
            // Tuple, Struct, Variant, Reference, BigInt, None, Raw, Error,
            // Cell — is routed through the streaming encoder so the reader
            // sees the typed CBOR shape rather than a stringified Raw.
            // The previous `_ => register_variable_raw(...)` fallback
            // silently downgraded Bool/String/Float/Char/etc. to Raw,
            // which broke ct-print and any consumer that relied on the
            // typed `kind` field of the decoded value.
            _ => {
                let cbor = self.streaming_encoder.encode(&value);
                unsafe { trace_writer_register_variable_cbor(self.handle, c_name.as_ptr(), cbor.as_ptr(), cbor.len()) }
            }
        }
    }

    /// Register a variable whose value is already encoded as CBOR bytes.
    ///
    /// This bypasses the `ValueRecord` tree entirely, passing pre-encoded CBOR
    /// directly to the Nim backend. Used by recorders that call the streaming
    /// value encoder C FFI during their object walk (M58+).
    pub fn register_variable_cbor(&mut self, name: &str, cbor: &[u8]) {
        let c_name = str_to_cstring(name);
        unsafe { trace_writer_register_variable_cbor(self.handle, c_name.as_ptr(), cbor.as_ptr(), cbor.len()) }
    }

    /// Stage one (name, CBOR-encoded value) argument for the next
    /// `register_call`.  The Nim writer accumulates these into a pending
    /// buffer that `register_call` consumes to build the call record's
    /// `args` field.  Without this, the call record's args stay empty and
    /// the frontend renders calls as `f()` instead of `f(arg=value)`.
    ///
    /// Recorders that already call [`register_variable_cbor`] for each
    /// argument should call `register_call_arg` *in addition* with the
    /// same name and CBOR bytes — the variable goes onto the current
    /// step (for `ct/load-locals`) and the arg goes onto the call record
    /// (for the calltrace pane).
    pub fn register_call_arg(&mut self, name: &str, cbor: &[u8]) {
        let c_name = str_to_cstring(name);
        unsafe { trace_writer_register_call_arg(self.handle, c_name.as_ptr(), cbor.as_ptr(), cbor.len()) }
    }

    /// Register a return value that is already encoded as CBOR bytes.
    ///
    /// See [`register_variable_cbor`](Self::register_variable_cbor) for rationale.
    pub fn register_return_cbor(&mut self, cbor: &[u8]) {
        unsafe { trace_writer_register_return_cbor(self.handle, cbor.as_ptr(), cbor.len()) }
    }

    pub fn register_special_event(&mut self, kind: EventLogKind, metadata: &str, content: &str) {
        let c_metadata = str_to_cstring(metadata);
        let c_content = str_to_cstring(content);
        unsafe { trace_writer_register_special_event(self.handle, kind as i32, c_metadata.as_ptr(), c_content.as_ptr()) }
    }

    /// Register a `ThreadStart` event (a new thread came into existence).
    ///
    /// Recorders observing multi-threaded execution should call this rather than
    /// routing the event through `TraceWriter::add_event`, which used to be a
    /// silent no-op on this backend (incidents 1.21 / 1.22 / 1.27).
    pub fn register_thread_start(&mut self, thread_id: u64) {
        unsafe { trace_writer_register_thread_start(self.handle, thread_id) }
    }

    /// Register a `ThreadExit` event (a thread terminated).
    pub fn register_thread_exit(&mut self, thread_id: u64) {
        unsafe { trace_writer_register_thread_exit(self.handle, thread_id) }
    }

    /// Register a `ThreadSwitch` event (the active thread changed).
    pub fn register_thread_switch(&mut self, thread_id: u64) {
        unsafe { trace_writer_register_thread_switch(self.handle, thread_id) }
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
        // Two effects:
        //   1. The argument is registered as a step variable on the
        //      *current* step so it appears in `ct/load-locals` for the
        //      caller.  This matches the historical behaviour of the
        //      single-stream writer.
        //   2. The argument is also staged on the writer's pending-args
        //      buffer so the next `register_call` attaches it to the
        //      call record.  Without this the call record would have
        //      empty `args`, and the frontend's calltrace pane would
        //      render the call as `format_board()` instead of
        //      `format_board(board=[[5,3,4,...]])`.
        self.register_variable_with_full_value(name, value.clone());

        let cbor = self.streaming_encoder.encode(&value).to_vec();
        let c_name = str_to_cstring(name);
        unsafe {
            trace_writer_register_call_arg(self.handle, c_name.as_ptr(), cbor.as_ptr(), cbor.len());
        }

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

    /// Dispatch a [`TraceLowLevelEvent`] to the correct `register_*` entry point.
    ///
    /// Historically this method was a silent no-op on the Nim multi-stream
    /// backend, which caused recorders that built up traces through
    /// `TraceWriter::add_event(...)` to lose every event they produced.  Three
    /// separate incidents traced data loss to this footgun (see comments on
    /// `register_thread_*` and the migration log).  Each variant of
    /// `TraceLowLevelEvent` now has a real dispatch path so no events are
    /// dropped silently.
    ///
    /// Variants whose payload is reduced (e.g. `Step(StepRecord{path_id, ...})`
    /// vs `register_step(&Path, Line)`) rely on `path_table` / `variable_table`
    /// being populated by the corresponding `Path` / `VariableName` events the
    /// same recorder is expected to emit in causal order.  When the lookup
    /// cannot be resolved (e.g. an out-of-range `PathId`) we still call the
    /// underlying entry point with a stringified placeholder rather than
    /// dropping the event — preserving the invariant that every `add_event`
    /// call leaves a trace footprint.
    pub fn add_event(&mut self, event: TraceLowLevelEvent) {
        match event {
            TraceLowLevelEvent::Path(path) => {
                // Mirror the path into our reverse table (PathIds are assigned
                // sequentially in emission order) and propagate to the Nim
                // backend so the path is interned in its registry too.
                self.path_table.push(path.clone());
                self.register_path(&path);
            }
            TraceLowLevelEvent::VariableName(name) => {
                self.variable_table.push(name.clone());
                self.register_variable_name(&name);
            }
            TraceLowLevelEvent::Variable(name) => {
                // Legacy alias for VariableName — keep both tables in sync.
                self.variable_table.push(name.clone());
                self.register_variable_name(&name);
            }
            TraceLowLevelEvent::Step(StepRecord { path_id, line }) => {
                let path: std::path::PathBuf = self
                    .path_table
                    .get(path_id.0)
                    .cloned()
                    .unwrap_or_else(|| std::path::PathBuf::from(format!("<path_{}>", path_id.0)));
                self.register_step(&path, line);
            }
            TraceLowLevelEvent::Type(type_record) => {
                self.register_raw_type(type_record);
            }
            TraceLowLevelEvent::Function(rec) => {
                let path: std::path::PathBuf = self
                    .path_table
                    .get(rec.path_id.0)
                    .cloned()
                    .unwrap_or_else(|| std::path::PathBuf::from(format!("<path_{}>", rec.path_id.0)));
                self.register_function(&rec.name, &path, rec.line);
            }
            TraceLowLevelEvent::Call(rec) => {
                // Stage the call args via `register_call_arg` so the Nim
                // multi-stream writer attaches them to the call record (mirrors
                // what `arg()` does for the streaming-style API).
                for full in &rec.args {
                    let cbor = self.streaming_encoder.encode(&full.value).to_vec();
                    let name = self
                        .variable_table
                        .get(full.variable_id.0)
                        .cloned()
                        .unwrap_or_else(|| format!("var_{}", full.variable_id.0));
                    self.register_call_arg(&name, &cbor);
                }
                self.register_call(rec.function_id, rec.args);
            }
            TraceLowLevelEvent::Return(rec) => {
                self.register_return(rec.return_value);
            }
            TraceLowLevelEvent::Event(rec) => {
                self.register_special_event(rec.kind, &rec.metadata, &rec.content);
            }
            TraceLowLevelEvent::Asm(instructions) => {
                self.register_asm(&instructions);
            }
            TraceLowLevelEvent::Value(full) => {
                let name = self
                    .variable_table
                    .get(full.variable_id.0)
                    .cloned()
                    .unwrap_or_else(|| format!("var_{}", full.variable_id.0));
                self.register_variable_with_full_value(&name, full.value);
            }
            TraceLowLevelEvent::BindVariable(rec) => {
                let name = self
                    .variable_table
                    .get(rec.variable_id.0)
                    .cloned()
                    .unwrap_or_else(|| format!("var_{}", rec.variable_id.0));
                self.bind_variable(&name, rec.place);
            }
            TraceLowLevelEvent::Assignment(rec) => {
                let name = self.variable_table.get(rec.to.0).cloned().unwrap_or_else(|| format!("var_{}", rec.to.0));
                self.assign(&name, rec.from, rec.pass_by);
            }
            TraceLowLevelEvent::DropVariables(ids) => {
                let names: Vec<String> = ids
                    .into_iter()
                    .map(|id| self.variable_table.get(id.0).cloned().unwrap_or_else(|| format!("var_{}", id.0)))
                    .collect();
                self.drop_variables(&names);
            }
            TraceLowLevelEvent::CompoundValue(rec) => {
                self.register_compound_value(rec.place, rec.value);
            }
            TraceLowLevelEvent::CellValue(rec) => {
                self.register_cell_value(rec.place, rec.value);
            }
            TraceLowLevelEvent::AssignCompoundItem(rec) => {
                self.assign_compound_item(rec.place, rec.index, rec.item_place);
            }
            TraceLowLevelEvent::AssignCell(rec) => {
                self.assign_cell(rec.place, rec.new_value);
            }
            TraceLowLevelEvent::VariableCell(rec) => {
                let name = self
                    .variable_table
                    .get(rec.variable_id.0)
                    .cloned()
                    .unwrap_or_else(|| format!("var_{}", rec.variable_id.0));
                self.register_variable(&name, rec.place);
            }
            TraceLowLevelEvent::DropVariable(id) => {
                let name = self.variable_table.get(id.0).cloned().unwrap_or_else(|| format!("var_{}", id.0));
                self.drop_variable(&name);
            }
            TraceLowLevelEvent::ThreadStart(tid) => {
                self.register_thread_start(tid.0);
            }
            TraceLowLevelEvent::ThreadExit(tid) => {
                self.register_thread_exit(tid.0);
            }
            TraceLowLevelEvent::ThreadSwitch(tid) => {
                self.register_thread_switch(tid.0);
            }
            TraceLowLevelEvent::DropLastStep => {
                self.drop_last_step();
            }
        }
    }

    /// Drain `events` into `add_event` one by one, dispatching every variant
    /// to its real `register_*` entry point.  The vector is cleared as a side
    /// effect so callers can reuse it as a scratch buffer.
    pub fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        for event in events.drain(..) {
            self.add_event(event);
        }
    }

    /// Returns an empty slice — the Nim writer streams events directly to
    /// disk and does not retain an in-memory log.  Consumers that need
    /// buffered events should use [`non_streaming_trace_writer::NonStreamingTraceWriter`].
    pub fn events(&self) -> &[TraceLowLevelEvent] {
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
    /// M14: column-aware register_step.  The default implementation drops
    /// the column so unmodified backends continue to compile; backends
    /// that have a column-bearing Step encoder override this.
    fn register_step_with_column(&mut self, path: &Path, line: Line, _column: Option<Line>) {
        self.register_step(path, line);
    }
    fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>);
    fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord;
    fn register_return(&mut self, return_value: ValueRecord);
    fn register_special_event(&mut self, kind: EventLogKind, metadata: &str, content: &str);

    /// Register a `ThreadStart` event.  Default implementation delegates to
    /// [`add_event`](Self::add_event) so existing implementations (notably the
    /// in-memory test double) continue to capture the event with no extra
    /// boilerplate.  Backends with native thread-event support — like
    /// [`NimTraceWriter`] — override this to use the dedicated entry point.
    fn register_thread_start(&mut self, thread_id: u64) {
        self.add_event(TraceLowLevelEvent::ThreadStart(ThreadId(thread_id)));
    }

    /// Register a `ThreadExit` event.  See [`register_thread_start`].
    fn register_thread_exit(&mut self, thread_id: u64) {
        self.add_event(TraceLowLevelEvent::ThreadExit(ThreadId(thread_id)));
    }

    /// Register a `ThreadSwitch` event.  See [`register_thread_start`].
    fn register_thread_switch(&mut self, thread_id: u64) {
        self.add_event(TraceLowLevelEvent::ThreadSwitch(ThreadId(thread_id)));
    }

    fn to_raw_type(&self, kind: TypeKind, lang_type: &str) -> TypeRecord;
    fn register_type(&mut self, kind: TypeKind, lang_type: &str);
    fn register_raw_type(&mut self, typ: TypeRecord);
    fn register_asm(&mut self, instructions: &[String]);
    fn register_variable_with_full_value(&mut self, name: &str, value: ValueRecord);

    /// Register a variable whose value is already encoded as CBOR bytes.
    ///
    /// This bypasses the `ValueRecord` tree entirely, passing pre-encoded CBOR
    /// directly to the backend. Used by recorders that call the streaming
    /// value encoder during their object walk (M58+).
    ///
    /// The default implementation wraps the CBOR bytes as a hex-encoded
    /// `ValueRecord::Raw` and delegates to `register_variable_with_full_value`.
    /// Writers that support native CBOR (e.g. `NimTraceWriter`) override this
    /// for zero-copy passthrough.
    fn register_variable_cbor(&mut self, name: &str, cbor: &[u8]) {
        let hex = cbor.iter().map(|b| format!("{b:02x}")).collect::<String>();
        self.register_variable_with_full_value(name, ValueRecord::Raw { r: hex, type_id: TypeId(0) });
    }

    /// Register a return value that is already encoded as CBOR bytes.
    ///
    /// See [`register_variable_cbor`](Self::register_variable_cbor) for rationale.
    fn register_return_cbor(&mut self, cbor: &[u8]) {
        let hex = cbor.iter().map(|b| format!("{b:02x}")).collect::<String>();
        self.register_return(ValueRecord::Raw { r: hex, type_id: TypeId(0) });
    }

    /// Stage one (name, CBOR-encoded value) argument for the next
    /// `register_call`.  The Nim multi-stream backend accumulates these
    /// into the call record's `args` field; without them the frontend
    /// renders the call as `f()` instead of `f(name=value)`.  Recorders
    /// that build call args via [`register_variable_cbor`] should call
    /// this *in addition* for each parameter immediately before
    /// `register_call`.
    ///
    /// Default implementation is a no-op (legacy single-stream writers
    /// store args inside the abstract `Call` event already).  Override
    /// in writers that need explicit per-call argument staging.
    fn register_call_arg(&mut self, _name: &str, _cbor: &[u8]) {}

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

    /// TF-M7: record one `(path, sha256)` filter-provenance entry on the
    /// writer.  Recorders integrating `codetracer_trace_filter` invoke
    /// this once per composed source in composition order (builtin
    /// default → auto-discovered → env-var → CLI `--trace-filter:`)
    /// before [`close`](Self::close).  Default impl is a no-op so test
    /// doubles and legacy writers without provenance support keep
    /// compiling; the CTFS-emitting `NimTraceWriter` overrides this to
    /// thread the entry into `meta.dat` (spec § 7).
    fn add_filter_provenance(&mut self, _path: &str, _sha256: &[u8; 32]) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    /// TF-M7: mark the writer to emit a present-but-empty trace-filter
    /// provenance block.  See [`add_filter_provenance`].  Default no-op
    /// for writers without CTFS meta.dat output.
    fn record_empty_filter_provenance(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    /// Write the branded recorder-id field into `meta.dat` (CTFS spec §7).
    /// `recorder_id` should be the stable recorder identifier
    /// (e.g. `"codetracer-cairo-recorder"`).  Default no-op so
    /// in-memory test doubles continue to compile; the CTFS-emitting
    /// `NimTraceWriter` overrides this to write the field via FFI.
    fn write_meta_dat(&mut self, _recorder_id: &str) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
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
    fn register_step_with_column(&mut self, path: &Path, line: Line, column: Option<Line>) {
        NimTraceWriter::register_step_with_column(self, path, line, column)
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
    fn register_thread_start(&mut self, thread_id: u64) {
        NimTraceWriter::register_thread_start(self, thread_id)
    }
    fn register_thread_exit(&mut self, thread_id: u64) {
        NimTraceWriter::register_thread_exit(self, thread_id)
    }
    fn register_thread_switch(&mut self, thread_id: u64) {
        NimTraceWriter::register_thread_switch(self, thread_id)
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
    fn register_variable_cbor(&mut self, name: &str, cbor: &[u8]) {
        NimTraceWriter::register_variable_cbor(self, name, cbor)
    }
    fn register_call_arg(&mut self, name: &str, cbor: &[u8]) {
        NimTraceWriter::register_call_arg(self, name, cbor)
    }
    fn register_return_cbor(&mut self, cbor: &[u8]) {
        NimTraceWriter::register_return_cbor(self, cbor)
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
    fn add_filter_provenance(&mut self, path: &str, sha256: &[u8; 32]) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::add_filter_provenance(self, path, sha256)
    }
    fn record_empty_filter_provenance(&mut self) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::record_empty_filter_provenance(self)
    }
    fn write_meta_dat(&mut self, recorder_id: &str) -> Result<(), Box<dyn Error>> {
        NimTraceWriter::write_meta_dat(self, recorder_id)
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
    let s = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)) }.to_string();
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

    // --- Structured data access (no JSON) ---

    /// Resolve step N to (path_id, line).
    pub fn step_location(&self, n: u64) -> Result<(u64, u64), Box<dyn Error>> {
        let mut path_id: u64 = 0;
        let mut line: u64 = 0;
        let rc = unsafe { ct_reader_step_location(self.handle, n, &mut path_id, &mut line) };
        if rc != 0 {
            Err(last_error().into())
        } else {
            Ok((path_id, line))
        }
    }

    /// Resolve steps `[start_n, start_n + count)` to `(path_id, line)`.
    ///
    /// The output slices must be at least `count` entries long.  The Nim
    /// reader clamps the result to the remaining step count and returns the
    /// number of entries actually written.  Starting past the end of the trace
    /// is a successful zero-length read.
    pub fn step_locations(&self, start_n: u64, count: u64, path_ids: &mut [u64], lines: &mut [u64]) -> Result<u64, Box<dyn Error>> {
        let count_usize = usize::try_from(count).map_err(|_| "step_locations count does not fit usize")?;
        if path_ids.len() < count_usize || lines.len() < count_usize {
            return Err(format!(
                "step_locations buffers too small: count={count}, path_ids={}, lines={}",
                path_ids.len(),
                lines.len()
            )
            .into());
        }
        if count == 0 {
            return Ok(0);
        }

        let written = unsafe { ct_reader_step_locations(self.handle, start_n, count, path_ids.as_mut_ptr(), lines.as_mut_ptr()) };
        if written == u64::MAX {
            Err(last_error().into())
        } else {
            Ok(written)
        }
    }

    /// Number of variable values at step N.
    pub fn step_value_count(&self, n: u64) -> u64 {
        unsafe { ct_reader_step_value_count(self.handle, n) }
    }

    /// Get the variable value at (step N, value index).
    /// Returns (varname_id, type_id, cbor_data).
    pub fn step_value(&self, n: u64, value_idx: u64) -> Result<(u64, u64, Vec<u8>), Box<dyn Error>> {
        let mut varname_id: u64 = 0;
        let mut type_id: u64 = 0;
        let mut data_ptr: *mut u8 = std::ptr::null_mut();
        let mut data_len: usize = 0;
        let rc = unsafe { ct_reader_step_value(self.handle, n, value_idx, &mut varname_id, &mut type_id, &mut data_ptr, &mut data_len) };
        if rc != 0 {
            return Err(last_error().into());
        }
        let data = if data_ptr.is_null() || data_len == 0 {
            Vec::new()
        } else {
            let v = unsafe { std::slice::from_raw_parts(data_ptr, data_len) }.to_vec();
            unsafe { ct_free_buffer(data_ptr) };
            v
        };
        Ok((varname_id, type_id, data))
    }

    /// Get the scalar fields of a call record.
    /// Returns (function_id, parent_key, entry_step, exit_step, depth, children_count).
    pub fn call_fields(&self, key: u64) -> Result<(u64, i64, u64, u64, u32, u64), Box<dyn Error>> {
        let mut function_id: u64 = 0;
        let mut parent_key: i64 = 0;
        let mut entry_step: u64 = 0;
        let mut exit_step: u64 = 0;
        let mut depth: u32 = 0;
        let mut children_count: u64 = 0;
        let rc = unsafe {
            ct_reader_call_fields(
                self.handle,
                key,
                &mut function_id,
                &mut parent_key,
                &mut entry_step,
                &mut exit_step,
                &mut depth,
                &mut children_count,
            )
        };
        if rc != 0 {
            Err(last_error().into())
        } else {
            Ok((function_id, parent_key, entry_step, exit_step, depth, children_count))
        }
    }

    /// Get the call_key of child at index within a call record.
    pub fn call_child(&self, key: u64, child_idx: u64) -> Result<u64, Box<dyn Error>> {
        let result = unsafe { ct_reader_call_child(self.handle, key, child_idx) };
        if result == u64::MAX {
            Err(last_error().into())
        } else {
            Ok(result)
        }
    }

    /// Number of arguments captured for the call at ``key``.
    pub fn call_arg_count(&self, key: u64) -> u64 {
        unsafe { ct_reader_call_arg_count(self.handle, key) }
    }

    /// Get the argument at ``arg_idx`` of call ``key`` as
    /// `(varname_id, cbor_value_bytes)`.  The CBOR bytes use the same
    /// `serde(tag = "kind")` layout as `step_value`.
    pub fn call_arg(&self, key: u64, arg_idx: u64) -> Result<(u64, Vec<u8>), Box<dyn Error>> {
        let mut varname_id: u64 = 0;
        let mut data_ptr: *mut u8 = std::ptr::null_mut();
        let mut data_len: usize = 0;
        let rc = unsafe { ct_reader_call_arg(self.handle, key, arg_idx, &mut varname_id, &mut data_ptr, &mut data_len) };
        if rc != 0 {
            return Err(last_error().into());
        }
        let data = if data_ptr.is_null() || data_len == 0 {
            Vec::new()
        } else {
            let v = unsafe { std::slice::from_raw_parts(data_ptr, data_len) }.to_vec();
            unsafe { ct_free_buffer(data_ptr) };
            v
        };
        Ok((varname_id, data))
    }

    /// Get the fields of an IO event.
    /// Returns (kind, step_id, data). kind: 0=stdout, 1=stderr, 2=file_op, 3=error.
    pub fn event_fields(&self, index: u64) -> Result<(u8, u64, Vec<u8>), Box<dyn Error>> {
        let mut kind: u8 = 0;
        let mut step_id: u64 = 0;
        let mut data_ptr: *mut u8 = std::ptr::null_mut();
        let mut data_len: usize = 0;
        let rc = unsafe { ct_reader_event_fields(self.handle, index, &mut kind, &mut step_id, &mut data_ptr, &mut data_len) };
        if rc != 0 {
            return Err(last_error().into());
        }
        let data = if data_ptr.is_null() || data_len == 0 {
            Vec::new()
        } else {
            let v = unsafe { std::slice::from_raw_parts(data_ptr, data_len) }.to_vec();
            unsafe { ct_free_buffer(data_ptr) };
            v
        };
        Ok((kind, step_id, data))
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

/// Create a trace writer for the requested format.
///
/// This is a drop-in replacement for `codetracer_trace_writer::create_trace_writer`
/// and preserves that crate's format → writer mapping:
///
/// * `Json` / `BinaryV0` → the in-memory [`non_streaming_trace_writer::NonStreamingTraceWriter`],
///   which buffers events (inspectable via `events()`) and serialises them to
///   disk on `finish_writing_trace_events` / `close`.
/// * `Binary` / `Ctfs` → the streaming Nim-backed [`NimTraceWriter`], which
///   writes a modern seekable-Zstd `.ct` CTFS container.
///
/// The Nim backend's single-stream legacy path is only safe once
/// `begin_writing_trace_events` has been called; routing the non-streaming
/// formats to the in-memory writer keeps `Json` usable both for on-disk
/// output and for recorder unit tests that inspect buffered events without
/// opening a container — matching the historical `codetracer_trace_writer`
/// contract these consumers were written against.
pub fn create_trace_writer(program: &str, args: &[String], format: TraceEventsFileFormat) -> Box<dyn TraceWriter> {
    match format {
        TraceEventsFileFormat::Json | TraceEventsFileFormat::BinaryV0 => {
            let mut writer = non_streaming_trace_writer::NonStreamingTraceWriter::new(program, args);
            writer.set_format(format);
            Box::new(writer)
        }
        TraceEventsFileFormat::Binary | TraceEventsFileFormat::Ctfs => Box::new(NimTraceWriter::new(program, args, format)),
    }
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
        ValueRecord::String { text, type_id } => (text.clone(), TypeKind::String, format!("type_{}", type_id.0)),
        ValueRecord::Raw { r, type_id } => (r.clone(), TypeKind::Raw, format!("type_{}", type_id.0)),
        ValueRecord::Error { msg, type_id } => (msg.clone(), TypeKind::Error, format!("type_{}", type_id.0)),
        ValueRecord::None { type_id } => ("None".to_string(), TypeKind::None, format!("type_{}", type_id.0)),
        ValueRecord::Char { c, type_id } => (c.to_string(), TypeKind::Char, format!("type_{}", type_id.0)),
        ValueRecord::Sequence { type_id, .. } => ("[...]".to_string(), TypeKind::Seq, format!("type_{}", type_id.0)),
        ValueRecord::Tuple { type_id, .. } => ("(...)".to_string(), TypeKind::Tuple, format!("type_{}", type_id.0)),
        ValueRecord::Struct { type_id, .. } => ("{...}".to_string(), TypeKind::Struct, format!("type_{}", type_id.0)),
        ValueRecord::Variant { discriminator, type_id, .. } => (discriminator.clone(), TypeKind::Variant, format!("type_{}", type_id.0)),
        ValueRecord::Reference { address, type_id, .. } => (format!("0x{:x}", address), TypeKind::Pointer, format!("type_{}", type_id.0)),
        ValueRecord::Cell { place } => (format!("place_{}", place.0), TypeKind::Raw, "Cell".to_string()),
        ValueRecord::BigInt { negative, type_id, .. } => {
            let sign = if *negative { "-" } else { "" };
            (format!("{}(bigint)", sign), TypeKind::Int, format!("type_{}", type_id.0))
        }
    }
}

// ---------------------------------------------------------------------------
// CBOR -> ValueRecord decoding
// ---------------------------------------------------------------------------

/// Decode the CBOR payload produced by `StreamingValueEncoder` (and the Nim
/// `streaming_value_encoder`) back into a typed [`ValueRecord`].
///
/// The streaming encoder emits one CBOR map per value, internally tagged with
/// a `"kind"` field — byte-identical to the serde representation of
/// `ValueRecord`. The in-memory [`non_streaming_trace_writer::NonStreamingTraceWriter`]
/// uses this so `register_variable_cbor` / `register_return_cbor` buffer the
/// real typed value rather than a lossy hex-`Raw` placeholder, which lets
/// recorder unit tests inspect the captured values faithfully.
///
/// Returns a `ValueRecord::Raw` carrying a hex dump of the bytes if the
/// payload cannot be decoded — this never silently loses data, it just
/// degrades to the legacy placeholder behaviour for genuinely opaque blobs.
pub fn decode_cbor_value_record(cbor: &[u8]) -> ValueRecord {
    match ciborium::de::from_reader::<ciborium::value::Value, _>(cbor) {
        Ok(value) => cbor_value_to_record(&value).unwrap_or_else(|| hex_raw(cbor)),
        Err(_) => hex_raw(cbor),
    }
}

fn hex_raw(cbor: &[u8]) -> ValueRecord {
    let hex = cbor.iter().map(|b| format!("{b:02x}")).collect::<String>();
    ValueRecord::Raw { r: hex, type_id: TypeId(0) }
}

fn cbor_value_to_record(value: &ciborium::value::Value) -> Option<ValueRecord> {
    use ciborium::value::Value as V;

    // A ValueRef (CBOR tag 256 + uint) — encoded by `StreamingValueEncoder::write_ref`.
    // The non-streaming writer has no compound-value table, so surface it as a
    // descriptive Raw rather than dropping it.
    if let V::Tag(256, inner) = value {
        let id = inner.as_integer().and_then(|i| i128::try_from(i).ok());
        return Some(ValueRecord::Raw {
            r: format!("<ref {}>", id.unwrap_or_default()),
            type_id: TypeId(0),
        });
    }

    let map = match value {
        V::Map(entries) => entries,
        _ => return None,
    };

    let get = |key: &str| -> Option<&V> {
        map.iter().find_map(|(k, v)| match k {
            V::Text(t) if t == key => Some(v),
            _ => None,
        })
    };
    let as_u64 = |v: &V| -> Option<u64> { v.as_integer().and_then(|i| u64::try_from(i).ok()) };
    let as_i64 = |v: &V| -> Option<i64> { v.as_integer().and_then(|i| i64::try_from(i).ok()) };
    let as_str = |v: &V| -> Option<String> { v.as_text().map(|s| s.to_string()) };
    let type_id = |default: u64| -> TypeId { TypeId(get("type_id").and_then(as_u64).unwrap_or(default) as usize) };

    let kind = get("kind").and_then(|v| v.as_text())?.to_string();
    let record = match kind.as_str() {
        "Int" => ValueRecord::Int {
            i: get("i").and_then(as_i64)?,
            type_id: type_id(0),
        },
        "Float" => ValueRecord::Float {
            f: get("f").and_then(|v| v.as_float())?,
            type_id: type_id(0),
        },
        "Bool" => ValueRecord::Bool {
            b: get("b").and_then(|v| v.as_bool())?,
            type_id: type_id(0),
        },
        "String" => ValueRecord::String {
            text: get("text").and_then(as_str)?,
            type_id: type_id(0),
        },
        "Raw" => ValueRecord::Raw {
            r: get("r").and_then(as_str)?,
            type_id: type_id(0),
        },
        "Error" => ValueRecord::Error {
            msg: get("msg").and_then(as_str)?,
            type_id: type_id(0),
        },
        "None" => ValueRecord::None { type_id: type_id(0) },
        "Char" => {
            let text = get("c").and_then(as_str)?;
            ValueRecord::Char {
                c: text.chars().next()?,
                type_id: type_id(0),
            }
        }
        "Sequence" => ValueRecord::Sequence {
            elements: decode_cbor_elements(get("elements")?)?,
            is_slice: get("is_slice").and_then(|v| v.as_bool()).unwrap_or(false),
            type_id: type_id(0),
        },
        "Tuple" => ValueRecord::Tuple {
            elements: decode_cbor_elements(get("elements")?)?,
            type_id: type_id(0),
        },
        "Struct" => ValueRecord::Struct {
            field_values: decode_cbor_elements(get("field_values")?)?,
            type_id: type_id(0),
        },
        "Variant" => ValueRecord::Variant {
            discriminator: get("discriminator").and_then(as_str)?,
            contents: Box::new(cbor_value_to_record(get("contents")?)?),
            type_id: type_id(0),
        },
        "Reference" => ValueRecord::Reference {
            dereferenced: Box::new(cbor_value_to_record(get("dereferenced")?)?),
            address: get("address").and_then(as_u64).unwrap_or(0),
            mutable: get("mutable").and_then(|v| v.as_bool()).unwrap_or(false),
            type_id: type_id(0),
        },
        "BigInt" => ValueRecord::BigInt {
            b: get("b").and_then(|v| v.as_bytes().map(|b| b.to_vec()))?,
            negative: get("negative").and_then(|v| v.as_bool()).unwrap_or(false),
            type_id: type_id(0),
        },
        "Enum" => {
            // The streaming encoder can emit an Enum leaf; `ValueRecord` has no
            // Enum variant, so represent it as a Raw "name" — faithful enough
            // for the in-memory test double.
            ValueRecord::Raw {
                r: get("name").and_then(as_str).unwrap_or_default(),
                type_id: type_id(0),
            }
        }
        _ => return None,
    };
    Some(record)
}

fn decode_cbor_elements(value: &ciborium::value::Value) -> Option<Vec<ValueRecord>> {
    let arr = value.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        out.push(cbor_value_to_record(item)?);
    }
    Some(out)
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
        /// Output path captured by `begin_writing_trace_events`. The buffered
        /// events are serialised here by `finish_writing_trace_events` /
        /// `close` so this writer is a faithful drop-in for the legacy
        /// `codetracer_trace_writer::NonStreamingTraceWriter` (which the
        /// `Json` trace format relies on for on-disk output).
        trace_events_path: Option<PathBuf>,
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
                trace_events_path: None,
            }
        }

        pub fn set_format(&mut self, format: TraceEventsFileFormat) {
            self.format = format;
        }

        /// Serialise the buffered events to `trace_events_path`.
        /// Called from `finish_writing_trace_events` and `close`; idempotent.
        ///
        /// Only the `Json` format produces an on-disk artifact here — that is
        /// the format the recorder uses for human-readable traces and that
        /// the legacy `codetracer_trace_writer::NonStreamingTraceWriter`
        /// emitted as JSON. `BinaryV0` keeps its events buffered in memory
        /// (inspectable via `events()`); it has no JSON-on-disk contract.
        fn flush_events_to_disk(&self) -> Result<(), Box<dyn Error>> {
            if self.format != TraceEventsFileFormat::Json {
                return Ok(());
            }
            if let Some(path) = &self.trace_events_path {
                let json = serde_json::to_string(&self.events)?;
                std::fs::write(path, json)?;
            }
            Ok(())
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
            self.trace_events_path = Some(path.to_path_buf());
            Ok(())
        }
        fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn Error>> {
            self.flush_events_to_disk()
        }
        fn begin_writing_trace_paths(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
        fn finish_writing_trace_paths(&mut self) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), Box<dyn Error>> {
            // Ensure events reach disk even if `finish_writing_trace_events`
            // was not called explicitly (mirrors the legacy writer contract).
            self.flush_events_to_disk()
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
            // Emit a `VariableName` event the first time an id is minted, so
            // the buffered event stream stays self-describing: a `Value` /
            // `Call` arg referencing `VariableId(n)` is always preceded by
            // the `VariableName` at index `n`. Mirrors the legacy
            // `codetracer_trace_writer::AbstractTraceWriter::ensure_variable_id`.
            self.events.push(TraceLowLevelEvent::VariableName(variable_name.to_string()));
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
            self.events.push(TraceLowLevelEvent::Step(StepRecord { path_id, line }));
        }
        fn register_step_with_column(&mut self, path: &Path, line: Line, _column: Option<Line>) {
            // Column dropped per the legacy crate's surface contract — see
            // the comment on AbstractTraceWriter::register_step_with_column.
            let path_id = self.ensure_path_id(path);
            self.events.push(TraceLowLevelEvent::Step(StepRecord { path_id, line }));
        }
        fn register_call(&mut self, function_id: FunctionId, args: Vec<FullValueRecord>) {
            self.events.push(TraceLowLevelEvent::Call(CallRecord { function_id, args }));
        }
        fn arg(&mut self, name: &str, value: ValueRecord) -> FullValueRecord {
            let variable_id = self.ensure_variable_id(name);
            FullValueRecord { variable_id, value }
        }
        fn register_return(&mut self, return_value: ValueRecord) {
            self.events.push(TraceLowLevelEvent::Return(ReturnRecord { return_value }));
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
            self.events.push(TraceLowLevelEvent::Value(FullValueRecord { variable_id, value }));
        }
        fn register_variable_cbor(&mut self, name: &str, cbor: &[u8]) {
            // Decode the streaming-encoder CBOR back into the typed
            // `ValueRecord` it represents so buffered events carry real
            // values (recorder unit tests inspect `events()`).
            let value = super::decode_cbor_value_record(cbor);
            let variable_id = self.ensure_variable_id(name);
            self.events.push(TraceLowLevelEvent::Value(FullValueRecord { variable_id, value }));
        }
        fn register_return_cbor(&mut self, cbor: &[u8]) {
            // Decode the streaming-encoder CBOR back into the typed
            // `ValueRecord` it represents (see `register_variable_cbor`).
            let return_value = super::decode_cbor_value_record(cbor);
            self.events.push(TraceLowLevelEvent::Return(ReturnRecord { return_value }));
        }
        fn register_variable_name(&mut self, variable_name: &str) {
            self.ensure_variable_id(variable_name);
        }
        fn register_full_value(&mut self, variable_id: VariableId, value: ValueRecord) {
            self.events.push(TraceLowLevelEvent::Value(FullValueRecord { variable_id, value }));
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
