//! A failure inside a C ABI call reaches the caller: a panic is caught at the
//! boundary and reported through `trace_writer_last_error` with the call's
//! failure value, and a failure of a call that returns nothing makes
//! `trace_writer_close` fail.
//!
//! No mocks and no fault injection: each panic is one the writer raises on a
//! real misuse of the API, driven through the exported entry points exactly
//! as a C host calls them, writing real files.

use crate::writer::*;
use std::ffi::{CStr, CString};

fn last_error() -> String {
    unsafe { CStr::from_ptr(crate::trace_writer_last_error()) }.to_string_lossy().into_owned()
}

/// The JSON format, which `trace_writer_new` takes as 0.
const JSON: i32 = 0;

/// A JSON writer with all three outputs begun in a fresh directory.
fn begun_writer(dir: &std::path::Path) -> *mut TraceWriterHandle {
    let program = CString::new("failures").unwrap();
    let handle = unsafe { trace_writer_new(program.as_ptr(), JSON) };
    assert!(!handle.is_null());
    let c = |name: &str| CString::new(dir.join(name).to_str().unwrap()).unwrap();
    assert_eq!(unsafe { trace_writer_begin_metadata(handle, c("trace_metadata.json").as_ptr()) }, 0);
    assert_eq!(unsafe { trace_writer_begin_events(handle, c("trace.json").as_ptr()) }, 0);
    assert_eq!(unsafe { trace_writer_begin_paths(handle, c("trace_paths.json").as_ptr()) }, 0);
    handle
}

/// Finishing without beginning panics inside the writer. The call returns
/// its failure value naming the entry point and the writer's message, and the
/// process — this test — goes on running.
#[test]
fn a_panic_inside_a_call_is_caught_and_reported() {
    let program = CString::new("failures").unwrap();
    let handle = unsafe { trace_writer_new(program.as_ptr(), JSON) };
    assert!(!handle.is_null());

    assert_ne!(unsafe { trace_writer_finish_events(handle) }, 0, "a call that panicked must fail");
    let err = last_error();
    assert!(err.contains("trace_writer_finish_events"), "the error names the entry point: {err}");
    assert!(
        err.contains("without previous call to begin_writing_trace_events"),
        "the error carries the panic message: {err}"
    );

    unsafe { trace_writer_free(handle) };
}

/// `trace_writer_start` returns nothing, and panics when a type was
/// registered before it (the top-level ids it asserts are taken). The close
/// then fails, naming that call, although it succeeds on its own.
#[test]
fn a_failed_void_call_fails_the_close() {
    let dir = tempfile::tempdir().unwrap();
    let handle = begun_writer(dir.path());

    let type_name = CString::new("i32").unwrap();
    assert_ne!(unsafe { trace_writer_ensure_type_id(handle, 7, type_name.as_ptr()) }, usize::MAX);
    let source = CString::new("/src/main.c").unwrap();
    unsafe { trace_writer_start(handle, source.as_ptr(), 1) };
    unsafe { trace_writer_register_step(handle, source.as_ptr(), 2) };

    for (name, finished) in [
        ("events", unsafe { trace_writer_finish_events(handle) }),
        ("metadata", unsafe { trace_writer_finish_metadata(handle) }),
        ("paths", unsafe { trace_writer_finish_paths(handle) }),
    ] {
        assert_eq!(finished, 0, "finishing {name} writes what was recorded: {}", last_error());
    }
    assert_ne!(unsafe { trace_writer_close(handle) }, 0, "the close must fail after a failed call");
    let err = last_error();
    assert!(err.contains("earlier call failed"), "{err}");
    assert!(err.contains("trace_writer_start"), "the error names the call that failed: {err}");
    // The files were still written, as far as the recording got.
    assert!(dir.path().join("trace.json").exists());

    unsafe { trace_writer_free(handle) };
}

/// The same recording without the misuse closes, so the failure above is the
/// latched call's and not the recording's.
#[test]
fn a_recording_without_a_failed_call_closes() {
    let dir = tempfile::tempdir().unwrap();
    let handle = begun_writer(dir.path());

    let source = CString::new("/src/main.c").unwrap();
    unsafe { trace_writer_start(handle, source.as_ptr(), 1) };
    let type_name = CString::new("i32").unwrap();
    assert_ne!(unsafe { trace_writer_ensure_type_id(handle, 7, type_name.as_ptr()) }, usize::MAX);
    unsafe { trace_writer_register_step(handle, source.as_ptr(), 2) };

    assert_eq!(unsafe { trace_writer_finish_events(handle) }, 0, "{}", last_error());
    assert_eq!(unsafe { trace_writer_finish_metadata(handle) }, 0, "{}", last_error());
    assert_eq!(unsafe { trace_writer_finish_paths(handle) }, 0, "{}", last_error());
    assert_eq!(unsafe { trace_writer_close(handle) }, 0, "{}", last_error());

    unsafe { trace_writer_free(handle) };
}
