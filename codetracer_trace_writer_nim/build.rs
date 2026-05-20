//! Builds the Nim `codetracer-trace-format-nim` static library and links it.
//!
//! The Nim FFI library (`codetracer_trace_writer_ffi.nim`) is compiled to a
//! native static library at build time — it is **not** a committed artifact.
//! The repo previously expected a hand-built `libcodetracer_trace_writer.a`
//! to already exist (produced by `nimble buildStaticLib`), which meant a
//! fresh checkout could not build, and Windows could not build at all
//! (`.a` naming, `-fPIC`, `-lm` and `pkg-config` are all Unix-only).
//!
//! libzstd (which the Nim CTFS code links) is supplied by the `zstd` crate
//! dependency — zstd-sys compiles it from source with the active toolchain,
//! so it is MSVC-compatible. The prebuilt zstd release ships a MinGW
//! `libzstd_static.lib` that cannot be linked into an MSVC binary.

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo"),
    );
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by Cargo"));
    let windows = env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");

    // --- locate the Nim sources ------------------------------------------
    // Default: the `codetracer-trace-format-nim` sibling repo. Override with
    // CODETRACER_TRACE_FORMAT_NIM_DIR for non-standard checkouts.
    let nim_repo = env::var("CODETRACER_TRACE_FORMAT_NIM_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            manifest_dir
                .parent() // codetracer-trace-format
                .and_then(|p| p.parent()) // workspace root
                .map(|p| p.join("codetracer-trace-format-nim"))
                .unwrap_or_else(|| PathBuf::from("../../codetracer-trace-format-nim"))
        });
    let nim_src = nim_repo.join("src");
    let ffi_entry = nim_src.join("codetracer_trace_writer_ffi.nim");
    assert!(
        ffi_entry.is_file(),
        "Nim FFI entry point not found at {} — is the codetracer-trace-format-nim \
         repo checked out as a sibling? (override with CODETRACER_TRACE_FORMAT_NIM_DIR)",
        ffi_entry.display(),
    );

    // --- build the Nim static library into OUT_DIR -----------------------
    // MSVC's linker resolves `static=codetracer_trace_writer` to
    // `codetracer_trace_writer.lib`; Unix `ar` to
    // `libcodetracer_trace_writer.a`.
    let lib_name = if windows {
        "codetracer_trace_writer.lib"
    } else {
        "libcodetracer_trace_writer.a"
    };
    let lib_path = out_dir.join(lib_name);

    let mut nim = Command::new("nim");
    nim.arg("c")
        .arg("--app:staticlib")
        .arg("--mm:arc")
        .arg("--noMain")
        .arg("-d:release")
        // db-backend also links the Nim-compiled MCR emulator. Two
        // independently Nim-compiled artifacts in one binary both define
        // `NimMain`/`PreMain`/... — `--nimMainPrefix` renames this lib's
        // copy so the final link does not hit a duplicate-symbol error.
        // The prefix MUST match the `codetracerTraceWriterNimMain` importc
        // in `codetracer_trace_writer_ffi.nim` and the nimble build tasks.
        .arg("--nimMainPrefix:codetracerTraceWriter")
        .arg(format!("--path:{}", nim_src.display()))
        .arg(format!("--nimcache:{}", out_dir.join("nimcache").display()));
    if windows {
        // The consuming Rust crate uses the MSVC ABI, so the Nim objects
        // must too — Nim defaults to MinGW gcc on Windows.
        nim.arg("--cc:vcc");
        // The zstd bindings do `#include <zstd.h>`; put its header dir on
        // the C include path so the generated C compiles. (Header only —
        // the actual libzstd comes from the `zstd` crate dependency.)
        if let Some(inc) = zstd_include_dir() {
            nim.arg(format!("--passC:-I{}", inc.display()));
        }
    } else {
        // -fPIC so the .a can be linked into shared objects (PyO3 .so).
        nim.arg("--passC:-fPIC");
    }
    nim.arg(format!("-o:{}", lib_path.display())).arg(&ffi_entry);

    let status = nim
        .status()
        .expect("failed to run `nim` — the Nim compiler must be on PATH");
    assert!(status.success(), "nim static-library build failed");

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=codetracer_trace_writer");

    // libm is a separate library only on Unix (the Nim runtime / CTFS code
    // uses math symbols); the MSVC CRT folds the math symbols in, so
    // linking `m` on Windows fails with "could not find m".
    if !windows {
        println!("cargo:rustc-link-lib=dylib=m");
    }

    println!("cargo:rerun-if-changed={}", nim_src.display());
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CODETRACER_TRACE_FORMAT_NIM_DIR");
    println!("cargo:rerun-if-env-changed=ZSTD_DIR");
}

/// Locate a directory containing `zstd.h` for the Nim C compilation step.
///
/// On Windows `env.ps1` exports `ZSTD_DIR` (the prebuilt zstd release,
/// which has `include/zstd.h`). On Unix the system include path already
/// carries `zstd.h`, so no `-I` is needed and this returns `None`.
fn zstd_include_dir() -> Option<PathBuf> {
    let zstd_dir = env::var("ZSTD_DIR").ok()?;
    let inc = PathBuf::from(zstd_dir).join("include");
    if inc.join("zstd.h").is_file() {
        Some(inc)
    } else {
        None
    }
}
