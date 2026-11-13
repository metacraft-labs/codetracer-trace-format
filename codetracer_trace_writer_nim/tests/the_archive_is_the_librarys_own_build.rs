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

/// Force the real dictionary-builder C objects into the same executable as
/// the Nim archive. These use DLL-import CRT symbols (including clock) under
/// Cargo's default MSVC mode: a static-CRT Nim archive must not silently change
/// their runtime. The Windows owner workflow runs this in both CRT contexts.
#[test]
fn the_archive_links_with_the_hosts_dictionary_builder() {
    use std::io::{Read, Write};
    assert_eq!(codetracer_trace_writer_nim::build_config(), LIBRARY_BUILD);
    let samples: Vec<Vec<u8>> = (0..256)
        .map(|i| {
            format!(
                "source_{i}.nim: function_{} variable_{} records value {} and returns {}\n",
                i % 37,
                i % 19,
                i * 13,
                i * 7
            )
            .into_bytes()
        })
        .collect();
    let dictionary = zstd::dict::from_samples(&samples, 1024).expect("real dictionary training");
    assert!(!dictionary.is_empty());
    let input = samples.concat();
    let mut encoder = zstd::stream::Encoder::with_dictionary(Vec::new(), 3, &dictionary).unwrap();
    encoder.write_all(&input).unwrap();
    let compressed = encoder.finish().unwrap();
    let mut decoder = zstd::stream::Decoder::with_dictionary(compressed.as_slice(), &dictionary).unwrap();
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, input);
}
