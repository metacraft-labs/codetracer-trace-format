fn main() {
    // Link against the pre-built Nim static library.
    // Users must set CODETRACER_NIM_LIB_DIR to the directory containing
    // libcodetracer_trace_writer.a, or it defaults to the sibling repo.
    let lib_dir = std::env::var("CODETRACER_NIM_LIB_DIR")
        .unwrap_or_else(|_| "../../codetracer-trace-format-nim".to_string());
    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=static=codetracer_trace_writer");
    // Nim runtime and trace writer dependencies
    println!("cargo:rustc-link-lib=dylib=zstd");
    println!("cargo:rustc-link-lib=dylib=m");
}
