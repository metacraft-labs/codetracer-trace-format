//! A failure inside a C ABI call reaches the caller: a panic is caught at the
//! boundary and reported through `trace_writer_last_error` with the call's
//! failure value, and a failure of a call that returns nothing makes
//! `trace_writer_close` fail.
//!
//! No mocks. The writer has no input that panics, so the boundary itself is
//! driven with a body that panics, through the same guard every entry point
//! runs in, on a real handle; the latching of a refused void call is driven
//! through the exported entry points exactly as a C host calls them.

use crate::writer::*;
use std::ffi::{CStr, CString};

fn last_error() -> String {
    unsafe { CStr::from_ptr(crate::trace_writer_last_error()) }.to_string_lossy().into_owned()
}

/// `FFI_TRACE_FORMAT_BINARY`.
const BINARY: i32 = 2;

fn begun_writer() -> *mut TraceWriterHandle {
    let program = CString::new("failures").unwrap();
    let handle = unsafe { trace_writer_new(program.as_ptr(), BINARY) };
    assert!(!handle.is_null(), "{}", last_error());
    assert_eq!(unsafe { trace_writer_begin_in_memory(handle) }, 0, "{}", last_error());
    handle
}

/// A panic inside an entry point's body returns its failure value with a
/// message naming the entry point and the panic, holds the failure on the
/// handle, and the process goes on running.
#[test]
fn a_panic_inside_a_call_is_caught_reported_and_held() {
    let handle = begun_writer();
    crate::trace_writer_clear_last_error();
    let r = crate::guarded("trace_writer_example", Some(handle), 7, || -> i32 { panic!("the body failed") });
    assert_eq!(r, 7, "the call returns its failure value");
    let err = last_error();
    assert!(err.contains("trace_writer_example"), "the error names the entry point: {err}");
    assert!(err.contains("the body failed"), "the error carries the panic message: {err}");
    assert_ne!(unsafe { trace_writer_close(handle) }, 0, "the close fails after a panicked call");
    assert!(last_error().contains("the body failed"), "{}", last_error());
    unsafe { trace_writer_free(handle) };
}

/// A void call that is refused — a value whose type id was never registered
/// — returns nothing, and the close fails naming it, although the close
/// itself succeeds.
#[test]
fn a_failed_void_call_fails_the_close() {
    let handle = begun_writer();
    let source = CString::new("/src/main.c").unwrap();
    let name = CString::new("x").unwrap();
    unsafe { trace_writer_start(handle, source.as_ptr(), 1) };
    unsafe { trace_writer_register_variable_int_by_type_id(handle, name.as_ptr(), 1, 99) };
    unsafe { trace_writer_register_step(handle, source.as_ptr(), 2) };
    assert_ne!(unsafe { trace_writer_close(handle) }, 0, "the close must fail after a failed call");
    let err = last_error();
    assert!(err.contains("earlier call failed"), "{err}");
    assert!(err.contains("type id 99"), "the error names the call that failed: {err}");
    assert_eq!(unsafe { trace_writer_container_ready(handle) }, 1, "the container was still finished");
    unsafe { trace_writer_free(handle) };
}

/// The same recording without the refused call closes, so the failure above
/// is the latched call's and not the recording's.
#[test]
fn a_recording_without_a_failed_call_closes() {
    let handle = begun_writer();
    let source = CString::new("/src/main.c").unwrap();
    let name = CString::new("x").unwrap();
    let int = CString::new("int").unwrap();
    unsafe { trace_writer_start(handle, source.as_ptr(), 1) };
    let tid = unsafe { trace_writer_ensure_type_id(handle, 7, int.as_ptr()) };
    unsafe { trace_writer_register_variable_int_by_type_id(handle, name.as_ptr(), 1, tid) };
    unsafe { trace_writer_register_step(handle, source.as_ptr(), 2) };
    assert_eq!(unsafe { trace_writer_close(handle) }, 0, "{}", last_error());
    unsafe { trace_writer_free(handle) };
}
