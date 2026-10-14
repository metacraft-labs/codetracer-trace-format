//! The Nim archive this crate links is the one the Nim library's own build
//! produces, flag for flag.
//!
//! Every Rust recorder links the archive `build.rs` makes (or the prebuilt one
//! it is pointed at). The C ABI is safe to call from several host threads only
//! when that archive is compiled `--threads:off` with its process lock: one
//! Nim heap, entered by one thread at a time. Compiled `--threads:on`, each
//! host thread gets its own heap and a writer freed after the thread that
//! used it exited crashes. So a command line copied into `build.rs` that
//! drifts from the library's (`codetracer-trace-format-nim/build_ffi.nims`)
//! ships a crash to every recorder while every single-threaded test passes.
//!
//! The archive reports its own compile configuration
//! (`trace_writer_build_config`); this asserts it is exactly what the
//! library's build gives a host archive. Falsifiability: build the archive
//! with `--threads:on` or `-d:ffiNoProcessLock` and this fails naming the
//! difference.
//!
//! No mocks: the real archive, linked as every recorder links it.

/// What `build_ffi.nims` gives a static host library; `tests/test_ffi.c` in the
/// Nim repository asserts the same string of the archive it builds.
const LIBRARY_BUILD: &str = "app:staticlib;threads:off;mm:arc;release:on;processLock:on";

#[test]
fn the_linked_archive_reports_the_library_build_configuration() {
    let config = codetracer_trace_writer_nim::build_config();
    assert_eq!(
        config, LIBRARY_BUILD,
        "the linked Nim archive was compiled as `{config}`, not as the Nim library's own build \
         (`{LIBRARY_BUILD}`): build.rs's compile has drifted from build_ffi.nims"
    );
}
