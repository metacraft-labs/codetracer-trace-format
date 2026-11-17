use std::env;

fn main() {
    // The integration host selects the same target compiler through cc's
    // supported discovery; these are Cargo's actual triples, not PATH guesses.
    for key in ["HOST", "TARGET"] {
        println!("cargo:rustc-env=CT_PARITY_{key}={}", env::var(key).expect("Cargo target metadata"));
    }
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(cbindgen::Config::from_file("cbindgen.toml").unwrap())
        .generate()
        .expect("Unable to generate C bindings")
        .write_to_file("codetracer_trace_writer.h");
}
