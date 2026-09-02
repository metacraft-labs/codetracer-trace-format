//! Zstandard codec shim: libzstd (C) by default, pure-Rust `ruzstd` under the
//! `pure-rust-zstd` feature.
//!
//! Everything the CTFS container needs from Zstandard is one-shot
//! `encode_all`/`decode_all` over a byte slice, so the whole dependency
//! collapses to the two functions below and can be swapped by one feature.
//!
//! **The choice is an opt-in, not a property of the target.** `zstd-sys`
//! carries its own `wasm-shim/` and its build script enables it for
//! `wasm32-unknown-unknown` and for every `wasm32-wasi*` triple, so the
//! reference C library cross-builds for both wasm targets with any clang that
//! can emit wasm32 — no sysroot and no libc — and the resulting module keeps
//! the import count it had. Measured: a `cdylib` calling `zstd::bulk::compress`
//! built for `wasm32-unknown-unknown` with nothing but
//! `CC_wasm32_unknown_unknown` and
//! `CFLAGS_wasm32_unknown_unknown=--target=wasm32-unknown-unknown` links,
//! reports zero imports and instantiates against a literal `{}`.
//!
//! What the C backend does require is that clang, named through
//! `CC_<target>`/`CFLAGS_<target>` — a build-environment requirement Cargo has
//! no way to state, and the reason `pure-rust-zstd` exists as an opt-out. A
//! consumer that cannot supply one builds with
//! `default-features = false, features = ["pure-rust-zstd"]`.
//!
//! **Compatibility, not byte-identity.** `ruzstd`'s encoder emits
//! standard-conformant Zstandard frames, so a container written through it is
//! read back unchanged by the ordinary libzstd-backed readers (`ct-print`, the
//! Nim reader, `codetracer_trace_reader`). It does **not** emit the same bytes
//! as libzstd at a given level — the compressed payload differs, the
//! decompressed payload does not. The default build uses libzstd, so every
//! existing golden fixture stays byte-for-byte unchanged.

/// Compress `data` as a single Zstandard frame.
///
/// `level` is the libzstd compression level. The `pure-rust-zstd` variant
/// below accepts and ignores it, because `ruzstd` exposes no numeric levels —
/// it implements `Fastest` and nothing else.
#[cfg(not(feature = "pure-rust-zstd"))]
pub fn encode_all(data: &[u8], level: i32) -> std::io::Result<Vec<u8>> {
    zstd::encode_all(std::io::Cursor::new(data), level)
}

/// Compress `data` as a single Zstandard frame. See the libzstd variant above.
#[cfg(feature = "pure-rust-zstd")]
pub fn encode_all(data: &[u8], _level: i32) -> std::io::Result<Vec<u8>> {
    Ok(ruzstd::encoding::compress_to_vec(data, ruzstd::encoding::CompressionLevel::Fastest))
}

/// Decompress a Zstandard byte stream, which may hold several concatenated
/// frames.
#[cfg(not(feature = "pure-rust-zstd"))]
pub fn decode_all(data: &[u8]) -> std::io::Result<Vec<u8>> {
    zstd::decode_all(std::io::Cursor::new(data))
}

/// Decompress a Zstandard byte stream, which may hold several concatenated
/// frames. See the libzstd variant above.
#[cfg(feature = "pure-rust-zstd")]
pub fn decode_all(data: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Read;

    // `StreamingDecoder` consumes frame after frame from the same reader, so
    // concatenated frames decode exactly as libzstd's `decode_all` does.
    let mut out = Vec::new();
    let mut cursor = std::io::Cursor::new(data);
    while (cursor.position() as usize) < data.len() {
        let mut decoder = ruzstd::decoding::StreamingDecoder::new(&mut cursor).map_err(std::io::Error::other)?;
        decoder.read_to_end(&mut out)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_through_the_active_codec() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let compressed = encode_all(&data, 3).unwrap();
        assert_eq!(decode_all(&compressed).unwrap(), data);
    }

    #[test]
    fn decodes_concatenated_frames() {
        let a = encode_all(b"hello ", 3).unwrap();
        let b = encode_all(b"world", 3).unwrap();
        let mut joined = a;
        joined.extend_from_slice(&b);
        assert_eq!(decode_all(&joined).unwrap(), b"hello world");
    }

    /// A buffer compressible enough that libzstd's levels visibly disagree on
    /// it, and long enough that the difference cannot be one byte of framing.
    fn level_sensitive_buffer() -> Vec<u8> {
        (0..262_144u32).map(|i| ((i / 7) % 251) as u8).collect()
    }

    /// The selected backend is asserted by what it DOES, not by reading the
    /// feature back.
    ///
    /// The two codecs differ observably on one axis this crate's own API
    /// exposes: libzstd honours `level`, and `ruzstd` implements `Fastest` and
    /// nothing else, so it accepts the argument and ignores it. Each arm below
    /// asserts the behaviour of the backend its `cfg` claims is selected, so a
    /// selection that silently picked the other one is a red test rather than a
    /// container that is merely bigger than somebody expected.
    #[cfg(not(feature = "pure-rust-zstd"))]
    #[test]
    fn libzstd_is_selected_and_honours_the_level() {
        let data = level_sensitive_buffer();
        let fast = encode_all(&data, 1).unwrap();
        let slow = encode_all(&data, 19).unwrap();
        assert_ne!(
            fast.len(),
            slow.len(),
            "levels 1 and 19 produced the same {} bytes; the level is being ignored, which is the \
             pure-Rust backend's behaviour and not libzstd's",
            fast.len()
        );
        assert_eq!(decode_all(&fast).unwrap(), data);
        assert_eq!(decode_all(&slow).unwrap(), data);
    }

    #[cfg(feature = "pure-rust-zstd")]
    #[test]
    fn the_pure_rust_backend_is_selected_and_ignores_the_level() {
        let data = level_sensitive_buffer();
        let fast = encode_all(&data, 1).unwrap();
        let slow = encode_all(&data, 19).unwrap();
        assert_eq!(
            fast, slow,
            "levels 1 and 19 produced different output; `ruzstd` implements only `Fastest`, so \
             this is libzstd answering under the pure-Rust feature"
        );
        assert_eq!(decode_all(&fast).unwrap(), data);
    }
}
