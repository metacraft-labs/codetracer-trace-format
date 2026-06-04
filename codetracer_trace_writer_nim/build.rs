//! Builds the Nim `codetracer-trace-format-nim` static library and links it.
//!
//! The Nim FFI library (`codetracer_trace_writer_ffi.nim`) is compiled to a
//! native static library at build time -- it is **not** a committed
//! artifact. The repo previously expected a hand-built
//! `libcodetracer_trace_writer.a` to already exist, which meant a fresh
//! checkout could not build, and Windows could not build at all (`.a`
//! naming, `-fPIC`, `-lm` and `pkg-config` are all Unix-only).
//!
//! libzstd (which the Nim CTFS code links) is supplied by the `zstd-sys`
//! crate dependency: it builds libzstd from source with the active
//! toolchain (MSVC-clean on Windows) and exposes `DEP_ZSTD_ROOT`, where the
//! matching `zstd.h` lives.

use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by Cargo"));
    let windows = env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");
    // `msvc` vs `gnu` on Windows: the Nim objects must match the ABI of the
    // consuming Rust crate. The MSVC target needs `--cc:vcc` and a `.lib`;
    // the `x86_64-pc-windows-gnu` target (used by e.g. the Ruby recorder to
    // match MSYS2 Ruby) needs MinGW gcc and a `.a`.
    let msvc = windows && env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc");

    // --- locate the Nim sources ------------------------------------------
    // Default: the `codetracer-trace-format-nim` sibling repo. Override with
    // CODETRACER_TRACE_FORMAT_NIM_DIR for non-standard checkouts.
    let nim_repo = env::var("CODETRACER_TRACE_FORMAT_NIM_DIR").map(PathBuf::from).unwrap_or_else(|_| {
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
        "Nim FFI entry point not found at {} -- is the codetracer-trace-format-nim \
         repo checked out as a sibling? (override with CODETRACER_TRACE_FORMAT_NIM_DIR)",
        ffi_entry.display(),
    );

    // --- choose a short nimcache directory -------------------------------
    // When the nimcache sits inside a deeply nested OUT_DIR (e.g. a napi-rs
    // build: <repo>/crates/<x>/target/<triple>/release/build/<hash>/out),
    // Nim's per-file C compiler invocations fail with C1083 "cannot open
    // source file" -- Nim falls back to relative source paths that the C
    // compiler resolves against a different working directory. Keeping the
    // nimcache in a short directory under the system temp dir avoids that.
    // The directory is keyed by a deterministic hash of OUT_DIR so each
    // cargo build unit gets its own cache and incremental reuse still works.
    let mut hasher = DefaultHasher::new();
    out_dir.hash(&mut hasher);
    let nimcache = env::temp_dir().join("ctnw").join(format!("{:016x}", hasher.finish()));

    // --- resolve the Nim sources' nimble dependencies --------------------
    // `codetracer-trace-format-nim`'s `.nimble` declares `requires` entries
    // (`results`, `stew`, ...) that the FFI sources import. A bare `nim c`
    // (which is what this build.rs invokes) only finds those packages once
    // they are installed under the global nimble pkg dir -- a fresh checkout
    // has none of them, so `nim c` fails with `cannot open file: results`.
    //
    // `nimble install --depsOnly -y`, run in the nim repo directory, reads
    // that `.nimble` and installs exactly the declared dependencies into the
    // global nimble store (where `nim`'s default `nimblePath` then resolves
    // them). It is idempotent: already-satisfied requirements are a no-op,
    // so it is safe to run on every build. This mirrors the pattern the
    // sibling `codetracer-native-recorder` Justfile uses for its Nim FFI
    // libraries (`ct_interpose`). `nimble` ships with the Nim toolchain.
    let nimble_status = Command::new("nimble")
        .arg("install")
        .arg("--depsOnly")
        .arg("-y")
        .current_dir(&nim_repo)
        .status()
        .expect(
            "failed to run `nimble` -- it ships with the Nim toolchain and \
             must be on PATH alongside `nim`",
        );
    assert!(
        nimble_status.success(),
        "`nimble install --depsOnly -y` failed in {} -- could not resolve \
         the Nim FFI library's nimble dependencies",
        nim_repo.display(),
    );

    // --- build the Nim static library ------------------------------------
    // MSVC's linker resolves `static=codetracer_trace_writer` to
    // `codetracer_trace_writer.lib`; the GNU/Unix `ar` to
    // `libcodetracer_trace_writer.a`. The lib itself goes to OUT_DIR.
    let lib_name = if msvc {
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
        // `NimMain`/`PreMain`/... -- `--nimMainPrefix` renames this lib's
        // copy so the final link does not hit a duplicate-symbol error.
        // The prefix MUST match the `codetracerTraceWriterNimMain` importc
        // in `codetracer_trace_writer_ffi.nim` and the nimble build tasks.
        .arg("--nimMainPrefix:codetracerTraceWriter")
        .arg(format!("--path:{}", nim_src.display()))
        .arg(format!("--nimcache:{}", nimcache.display()));
    // The zstd bindings do `#include <zstd.h>`; put its header dir on the C
    // include path so the generated C compiles.
    if let Some(inc) = zstd_include_dir() {
        nim.arg(format!("--passC:-I{}", inc.display()));
    }
    if msvc {
        // MSVC-ABI consumer: Nim must emit MSVC objects (it defaults to
        // MinGW gcc on Windows).
        nim.arg("--cc:vcc");
    } else if windows {
        // windows-gnu consumer: MinGW gcc objects (ABI-compatible with the
        // x86_64-pc-windows-gnu Rust target).
        nim.arg("--cc:gcc");
    } else {
        // -fPIC so the .a can be linked into shared objects (PyO3 .so).
        nim.arg("--passC:-fPIC");
    }
    nim.arg(format!("-o:{}", lib_path.display())).arg(&ffi_entry);

    let status = nim.status().expect("failed to run `nim` -- the Nim compiler must be on PATH");
    assert!(status.success(), "nim static-library build failed");

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=codetracer_trace_writer");

    // The Nim runtime / CTFS code uses math symbols. Unix and MinGW
    // (windows-gnu) provide them in a separate `libm`; the MSVC CRT folds
    // them in, so linking `m` under MSVC fails with "could not find m".
    if !msvc {
        println!("cargo:rustc-link-lib=dylib=m");
    }
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-lib=framework=Security");
    }

    println!("cargo:rerun-if-changed={}", nim_src.display());
    // Re-resolve nimble dependencies whenever the `.nimble` requirements
    // change.
    println!("cargo:rerun-if-changed={}", nim_repo.join("codetracer_trace_format.nimble").display(),);
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CODETRACER_TRACE_FORMAT_NIM_DIR");
    println!("cargo:rerun-if-env-changed=ZSTD_DIR");
}

/// Locate a directory containing `zstd.h` for the Nim C compilation step.
///
/// Preference: the `zstd-sys` crate's exported root (`DEP_ZSTD_ROOT`, which
/// holds `include/zstd.h`) -- always present because this crate depends on
/// `zstd-sys`. Falls back to `ZSTD_DIR/include` (the prebuilt zstd release
/// exported by codetracer's env.ps1).
fn zstd_include_dir() -> Option<PathBuf> {
    if let Ok(root) = env::var("DEP_ZSTD_ROOT") {
        let inc = PathBuf::from(root).join("include");
        if inc.join("zstd.h").is_file() {
            return Some(inc);
        }
    }
    if let Ok(zstd_dir) = env::var("ZSTD_DIR") {
        let inc = PathBuf::from(zstd_dir).join("include");
        if inc.join("zstd.h").is_file() {
            return Some(inc);
        }
    }
    None
}
