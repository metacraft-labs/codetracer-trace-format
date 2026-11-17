//! The CodeTracer trace-format C ABI, implemented over this repository's
//! writer and readers.
//!
//! The ABI is the one `codetracer-trace-format-nim` declares in
//! `include/codetracer_trace_writer.h`: the same symbols, signatures, return
//! conventions and observable behaviour, so a host built against that header
//! links against either library. `tests/c_abi_parity.rs` compiles one C host
//! against that header, links it separately against each library, and
//! compares what the two write and answer.
//!
//! # Families
//!
//! * [`writer`] — `trace_writer_*`: a recording, written as a split-stream
//!   CTFS container (`FFI_TRACE_FORMAT_BINARY`), to a file or in memory.
//! * [`value_encoder`] — `ct_value_*`: a streaming CBOR encoder for values.
//! * [`meta`] — `ct_write_meta_dat*`, `ct_read_meta_dat`, `ct_meta_dat_*`,
//!   `ct_free_buffer`, and the post-hoc container calls `ct_container_*`.
//! * [`reader`] — `ct_reader_*`: a container opened from a path or from bytes.
//!
//! # Errors
//!
//! A call that fails returns its failure value and leaves a message in a
//! per-thread buffer, read with [`trace_writer_last_error`]. The buffer is
//! sticky: nothing on a success path clears it, and
//! [`trace_writer_clear_last_error`] resets it. A panic inside an entry point
//! is caught at the boundary and reported the same way. A failure of a call on
//! a writer handle that returns nothing (and of the id-returning calls whose
//! sentinel a caller does not check) is also held on the handle, and
//! `trace_writer_close` then fails naming it.
//!
//! # Threads
//!
//! The error buffer is per thread. A handle is not synchronised: one handle
//! must not be used from two threads at once, but it may be used from one
//! thread and closed or freed from another. Distinct handles are independent.
//!
//! # Safety
//!
//! Every entry point dereferences pointers the caller owns. A handle must be
//! one its constructor returned and not yet freed; NULL is detected and
//! refused. A `const char*` must be NULL or NUL-terminated. A `(pointer,
//! length)` pair must describe readable memory of that length, or be NULL with
//! length 0. Each function's documentation in the header states the rest.

use std::cell::{Cell, RefCell};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

pub mod annotations;
pub mod meta;
pub mod reader;
pub mod value_encoder;
pub mod writer;

pub use writer::TraceWriterHandle;

// ---------------------------------------------------------------------------
// The per-thread error buffer
// ---------------------------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
    /// Incremented by every [`set_error`], so an entry point can tell whether
    /// the body it ran reported a failure without clearing the buffer.
    static ERROR_SERIAL: Cell<u64> = const { Cell::new(0) };
}

fn store_error(msg: &str) {
    let text = CString::new(msg.replace('\0', "\\0")).unwrap_or_default();
    LAST_ERROR.with(|e| *e.borrow_mut() = text);
}

/// Report a failure of the current call.
pub(crate) fn set_error(msg: &str) {
    store_error(msg);
    ERROR_SERIAL.with(|s| s.set(s.get() + 1));
}

/// Report something in the error buffer without it counting as a failure of
/// the call: it is not held on the handle.
pub(crate) fn set_notice(msg: &str) {
    store_error(msg);
}

fn error_serial() -> u64 {
    ERROR_SERIAL.with(|s| s.get())
}

fn last_error_string() -> String {
    LAST_ERROR.with(|e| e.borrow().to_string_lossy().into_owned())
}

/// This thread's last error: a NUL-terminated string, empty when no call has
/// reported one, valid until the next call on this thread.
#[unsafe(no_mangle)]
pub extern "C" fn trace_writer_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

/// Reset this thread's error buffer to the empty string.
#[unsafe(no_mangle)]
pub extern "C" fn trace_writer_clear_last_error() {
    store_error("");
}

/// How this library was compiled, as `key:value` pairs joined by `;`. Static,
/// never NULL.
#[unsafe(no_mangle)]
pub extern "C" fn trace_writer_build_config() -> *const c_char {
    #[cfg(debug_assertions)]
    const CONFIG: &CStr = c"impl:rust;app:staticlib;threads:on;release:off;processLock:off";
    #[cfg(not(debug_assertions))]
    const CONFIG: &CStr = c"impl:rust;app:staticlib;threads:on;release:on;processLock:off";
    CONFIG.as_ptr()
}

/// Initialize the library. Nothing needs initializing; the call exists so a
/// host written for the Nim library, which must call it first, links.
#[unsafe(no_mangle)]
pub extern "C" fn codetracer_trace_writer_init() {}

// ---------------------------------------------------------------------------
// The entry-point guard
// ---------------------------------------------------------------------------

fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s
    } else {
        "a panic with a non-string payload"
    }
}

/// Run the body of the entry point `name`. A panic becomes `fail` and a
/// message. When `latch` names a handle, any failure the body reported is
/// held on it for `trace_writer_close`.
pub(crate) fn guarded<R>(name: &str, latch: Option<*mut TraceWriterHandle>, fail: R, body: impl FnOnce() -> R) -> R {
    let serial = error_serial();
    let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(r) => r,
        Err(payload) => {
            set_error(&format!("{name}: internal error: {}", panic_message(&*payload)));
            fail
        }
    };
    if let Some(handle) = latch
        && !handle.is_null()
        && error_serial() != serial
    {
        // SAFETY: a non-NULL handle is live for the whole call (the handle
        // invariant), and the body's borrow of it has ended.
        unsafe { &mut *handle }.latch(last_error_string());
    }
    result
}

// ---------------------------------------------------------------------------
// Marshalling
// ---------------------------------------------------------------------------

/// The bytes of a NUL-terminated string; NULL is the empty string.
pub(crate) unsafe fn cstr_bytes<'a>(ptr: *const c_char) -> &'a [u8] {
    if ptr.is_null() {
        return &[];
    }
    unsafe { CStr::from_ptr(ptr) }.to_bytes()
}

/// A NUL-terminated string as text. Bytes that are not UTF-8 are replaced.
pub(crate) unsafe fn cstr_string(ptr: *const c_char) -> String {
    String::from_utf8_lossy(unsafe { cstr_bytes(ptr) }).into_owned()
}

/// The bytes of a `(pointer, length)` pair; NULL or a zero length is empty.
pub(crate) unsafe fn bytes<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// A path from its bytes.
pub(crate) fn path_from_bytes(bytes: &[u8]) -> std::path::PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        std::path::PathBuf::from(std::ffi::OsStr::from_bytes(bytes))
    }
    #[cfg(not(unix))]
    {
        std::path::PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
    }
}

// ---------------------------------------------------------------------------
// Buffers handed to the caller
// ---------------------------------------------------------------------------

/// Bytes in front of every buffer this library hands out: its total size, so
/// `ct_free_buffer`, which receives only the pointer, can release it.
const BUFFER_HEADER: usize = 16;

/// A heap copy of `data` the caller releases with [`meta::ct_free_buffer`].
/// Never NULL: an empty copy is a one-byte allocation.
pub(crate) fn alloc_buffer(data: &[u8]) -> *mut u8 {
    let total = BUFFER_HEADER + data.len().max(1);
    let Ok(layout) = std::alloc::Layout::from_size_align(total, BUFFER_HEADER) else {
        return std::ptr::null_mut();
    };
    // SAFETY: the layout is non-zero in size.
    let base = unsafe { std::alloc::alloc(layout) };
    if base.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: `base` holds `total` bytes, the header first.
    unsafe {
        (base as *mut usize).write(total);
        let out = base.add(BUFFER_HEADER);
        std::ptr::copy_nonoverlapping(data.as_ptr(), out, data.len());
        out
    }
}

/// Release a buffer [`alloc_buffer`] returned.
pub(crate) unsafe fn free_buffer(buf: *mut u8) {
    if buf.is_null() {
        return;
    }
    // SAFETY: `buf` is `BUFFER_HEADER` bytes into an allocation whose first
    // word is its total size.
    unsafe {
        let base = buf.sub(BUFFER_HEADER);
        let total = (base as *const usize).read();
        std::alloc::dealloc(base, std::alloc::Layout::from_size_align_unchecked(total, BUFFER_HEADER));
    }
}

#[cfg(test)]
mod failures_reach_the_caller;
