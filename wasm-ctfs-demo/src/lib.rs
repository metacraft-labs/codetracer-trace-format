//! A `.ct` CTFS container, built inside WebAssembly.
//!
//! This is the smallest end-to-end proof that `CtfsTraceWriter` works on
//! `wasm32-unknown-unknown`, and doubles as the reference for how a real
//! recorder should drive it from wasm. It builds a synthetic trace with the
//! ordinary writer API and hands the finished container back as bytes.
//!
//! The exported ABI is deliberately raw — four `extern "C"` functions over
//! linear memory, no `wasm-bindgen`, no JS glue, no imports at all — so the
//! module instantiates under a bare `WebAssembly.instantiate` in a browser
//! and runs unmodified under `wasmtime`.
//!
//! ```js
//! const { instance } = await WebAssembly.instantiate(bytes, {});
//! const ptr = instance.exports.ct_demo_build(64);
//! const len = instance.exports.ct_demo_len();
//! const ct  = new Uint8Array(instance.exports.memory.buffer, ptr, len).slice();
//! // `ct` is a complete .ct container: save it, Blob it, upload it.
//! ```
//!
//! Two things a wasm host must supply that a native recorder gets for free:
//!
//! * a `recording_id`, because the sandbox has neither a wall clock nor a
//!   CSPRNG to mint a UUIDv7 from (see `CtfsTraceWriter::set_recording_id`);
//! * a working directory, since there is no current directory to read (see
//!   `set_workdir`).

use codetracer_trace_types::{EventLogKind, Line, TypeKind, ValueRecord};
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use std::path::{Path, PathBuf};

/// Holds the container between `ct_demo_build` and the host reading it out.
///
/// A `static mut` is sound here in the way wasm actually runs this module:
/// `wasm32-unknown-unknown` is single-threaded, and the host calls
/// `ct_demo_build` then reads the bytes before calling anything else.
static mut CONTAINER: Vec<u8> = Vec::new();

/// Build a demo trace of `steps` steps and return a pointer to the finished
/// `.ct` container in linear memory. Pair with [`ct_demo_len`].
///
/// Returns a null pointer if the writer fails.
///
/// # Safety
/// The returned pointer is valid until the next call to `ct_demo_build`.
#[unsafe(no_mangle)]
pub extern "C" fn ct_demo_build(steps: u32) -> *const u8 {
    match build_container(steps) {
        Ok(bytes) => unsafe {
            let slot = &raw mut CONTAINER;
            *slot = bytes;
            (*slot).as_ptr()
        },
        Err(_) => core::ptr::null(),
    }
}

/// Length in bytes of the container `ct_demo_build` last produced.
#[unsafe(no_mangle)]
pub extern "C" fn ct_demo_len() -> usize {
    unsafe { (*(&raw const CONTAINER)).len() }
}

/// Reserve `len` bytes of linear memory and return a pointer to them, so a
/// host can pass data *in* (source text, a program to trace, …).
///
/// # Safety
/// Free with [`ct_demo_free`], passing the same length.
#[unsafe(no_mangle)]
pub extern "C" fn ct_demo_alloc(len: usize) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr();
    core::mem::forget(buf);
    ptr
}

/// Release memory obtained from [`ct_demo_alloc`].
///
/// # Safety
/// `ptr` must come from `ct_demo_alloc` and `len` must be the length passed
/// to it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_demo_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        drop(unsafe { Vec::from_raw_parts(ptr, 0, len) });
    }
}

/// The whole point, in ordinary Rust: drive the production
/// [`CtfsTraceWriter`] and collect the container it builds.
pub fn build_container(steps: u32) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut writer = CtfsTraceWriter::new_in_memory("wasm-demo", &[]);

    // A wasm sandbox can mint neither a UUIDv7 nor a working directory, so
    // both are supplied explicitly. A real host should pass a genuine v7 id
    // in from JavaScript; a fixed one here keeps the demo reproducible.
    writer.set_recording_id("01949fcc-7d92-7e9c-8000-00000000ce11");

    // The path is ignored by an in-memory writer — the container never
    // touches a filesystem — but the API still takes one.
    writer.begin_writing_trace_events(Path::new("trace"))?;

    let path = PathBuf::from("/demo/main.nr");
    TraceWriter::set_workdir(&mut writer, Path::new("/demo"));
    TraceWriter::start(&mut writer, &path, Line(1));

    let function_id = TraceWriter::ensure_function_id(&mut writer, "main", &path, Line(1));
    let int_type = TraceWriter::ensure_type_id(&mut writer, TypeKind::Int, "Field");

    let args = vec![TraceWriter::arg(&mut writer, "x", ValueRecord::Int { i: 3, type_id: int_type })];
    TraceWriter::register_call(&mut writer, function_id, args);

    let mut acc: i64 = 0;
    for step in 0..steps as i64 {
        acc += step;
        TraceWriter::register_step(&mut writer, &path, Line(step % 24 + 1));
        TraceWriter::register_variable_with_full_value(&mut writer, "acc", ValueRecord::Int { i: acc, type_id: int_type });
    }

    TraceWriter::register_special_event(&mut writer, EventLogKind::Write, "", "built inside WebAssembly\n");
    TraceWriter::register_return(&mut writer, ValueRecord::Int { i: acc, type_id: int_type });

    writer.finish_writing_trace_events()?;
    writer.take_container_bytes().ok_or_else(|| "the in-memory writer produced no container".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The same code path the wasm export takes, exercised natively so a
    /// failure points at the trace-building logic rather than at the wasm
    /// toolchain.
    #[test]
    fn builds_a_container_with_the_ctfs_magic() {
        let bytes = build_container(32).unwrap();
        assert!(bytes.len() > 4096, "container is implausibly small: {} bytes", bytes.len());
        assert_eq!(&bytes[..5], &[0xC0, 0xDE, 0x72, 0xAC, 0xE2], "missing the CTFS magic");
    }
}
