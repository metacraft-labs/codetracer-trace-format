fn main() {
    // Link against the pre-built Nim static library.
    // Users must set CODETRACER_NIM_LIB_DIR to the directory containing
    // libcodetracer_trace_writer.a, or it defaults to the sibling repo.
    let lib_dir = std::env::var("CODETRACER_NIM_LIB_DIR")
        .unwrap_or_else(|_| "../../codetracer-trace-format-nim".to_string());
    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=static=codetracer_trace_writer");

    // Find zstd library via pkg-config or ZSTD_LIB_DIR env var
    if let Ok(zstd_dir) = std::env::var("ZSTD_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", zstd_dir);
    } else if let Ok(output) = std::process::Command::new("pkg-config")
        .args(["--libs-only-L", "libzstd"])
        .output()
    {
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
