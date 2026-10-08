//! Byte-equality against the Nim reference encoder.
//!
//! `fixtures/nim_compact_golden.bin` was produced by actually RUNNING
//! `codetracer-trace-format-nim`'s `encodeCompactContainer`
//! (`src/codetracer_ctfs/compact.nim`) on the exact member list this test
//! builds below, via a small standalone Nim driver (not checked in here —
//! it imports only `codetracer_ctfs/compact` and `codetracer_ctfs/types`,
//! calls `encodeCompactContainer`, and writes the result to a file). That
//! driver was compiled and run against the Nim repository at
//! `/Users/zahary/m/dev/codetracer-trace-format-nim` (commit as of
//! 2026-10-09) with:
//!
//! ```text
//! nim c -o:gen_golden \
//!   --path:<nimble results pkg> --path:<nimble stew pkg> --path:src \
//!   gen_golden.nim
//! ./gen_golden fixtures/nim_compact_golden.bin
//! ```
//!
//! This test is the equivalence the task asked for: the Rust encoder is fed
//! the identical five-member list and its output is compared byte-for-byte
//! against that fixture.

use codetracer_ctfs::{encode_compact_container, CompactMember, EncryptionMethod, WholeFileCompression};

#[test]
fn rust_encoder_matches_nim_reference_byte_for_byte() {
    let members = vec![
        CompactMember::new("meta.dat", b"hello-metadata-0123456789".to_vec()),
        CompactMember::new("steps.dat", Vec::new()), // deliberately empty
        CompactMember::new(
            "values.dat",
            b"The quick brown fox jumps over the lazy dog. Repeated payload bytes to exercise a \
              non-trivial, multi-block-ish member length. 0123456789abcdefghijklmnopqrstuvwxyz./-"
                .to_vec(),
        ),
        CompactMember::new("t00000000001", vec![0u8, 1, 2, 3, 255, 254, 253, 0, 0, 10]),
        CompactMember::new("a.b/c-d", b"x".to_vec()),
    ];

    let rust_image = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None)
        .expect("Rust compact encoder must accept this member list");

    let golden = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/nim_compact_golden.bin"))
        .expect("golden fixture produced by the Nim reference encoder must be present");

    assert_eq!(
        rust_image.len(),
        golden.len(),
        "Rust and Nim compact containers differ in length: {} vs {}",
        rust_image.len(),
        golden.len()
    );
    assert_eq!(rust_image, golden, "Rust compact encoder output is not byte-identical to the Nim reference");
}
