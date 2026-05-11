fn main() {
    // Link against the pre-built Nim static library.
    // Users must set CODETRACER_NIM_LIB_DIR to the directory containing
    // libcodetracer_trace_writer.a, or it defaults to the sibling repo.
    let lib_dir = std::env::var("CODETRACER_NIM_LIB_DIR").unwrap_or_else(|_| {
        let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo"));
        manifest_dir
            .parent()
            .and_then(|path| path.parent())
            .map(|path| path.join("codetracer-trace-format-nim"))
            .unwrap_or_else(|| std::path::PathBuf::from("../../codetracer-trace-format-nim"))
            .to_string_lossy()
            .to_string()
    });
    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=static=codetracer_trace_writer");

    // Re-link whenever the static lib changes.  Without this, cargo treats
    // the .a as opaque and reuses the previous link, so dependents silently
    // ship a stale Nim writer when codetracer-trace-format-nim is rebuilt.
    // (Discovered after the May-10 meta.dat v2 schema update: the .a was
    // rebuilt but cargo refused to relink, so recorders kept emitting v1
    // bundles even after rebuilding the lib.)
    let static_lib = std::path::PathBuf::from(&lib_dir).join("libcodetracer_trace_writer.a");
    println!("cargo:rerun-if-changed={}", static_lib.display());
    println!("cargo:rerun-if-env-changed=CODETRACER_NIM_LIB_DIR");
    println!("cargo:rerun-if-env-changed=ZSTD_LIB_DIR");

    // Find zstd library via pkg-config or ZSTD_LIB_DIR env var
    if let Ok(zstd_dir) = std::env::var("ZSTD_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", zstd_dir);
    } else if let Ok(output) = std::process::Command::new("pkg-config").args(["--libs-only-L", "libzstd"]).output() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        for flag in stdout.split_whitespace() {
            if let Some(dir) = flag.strip_prefix("-L") {
                println!("cargo:rustc-link-search=native={}", dir);
            }
        }
    }

    println!("cargo:rustc-link-lib=dylib=zstd");
    println!("cargo:rustc-link-lib=dylib=m");
}
