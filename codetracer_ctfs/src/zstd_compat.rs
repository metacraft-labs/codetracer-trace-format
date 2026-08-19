//! Zstandard codec shim: libzstd (C) on hosts, pure-Rust `ruzstd` on wasm.
//!
//! `wasm32-unknown-unknown` has no libc, so `zstd-sys` — which builds the
//! reference C library with `cc` — cannot be linked there. Everything the
//! CTFS container needs from Zstandard is one-shot `encode_all`/`decode_all`
//! over a byte slice, so the whole dependency collapses to the two functions
//! below and can be swapped per target.
//!
//! **Compatibility, not byte-identity.** `ruzstd`'s encoder emits
//! standard-conformant Zstandard frames, so a container written from wasm is
//! read back unchanged by the ordinary libzstd-backed readers (`ct-print`, the
//! Nim reader, `codetracer_trace_reader`). It does **not** emit the same bytes
//! as libzstd at a given level — the compressed payload differs, the
//! decompressed payload does not. Host builds keep using libzstd, so every
//! existing golden fixture stays byte-for-byte unchanged.

/// Compress `data` as a single Zstandard frame.
///
/// `level` is the libzstd compression level on hosts. `ruzstd` does not expose
/// numeric levels — it only implements `Fastest` — so on wasm the level is
/// accepted and ignored.
#[cfg(not(target_arch = "wasm32"))]
pub fn encode_all(data: &[u8], level: i32) -> std::io::Result<Vec<u8>> {
    zstd::encode_all(std::io::Cursor::new(data), level)
}

/// Compress `data` as a single Zstandard frame. See the host variant above.
#[cfg(target_arch = "wasm32")]
pub fn encode_all(data: &[u8], _level: i32) -> std::io::Result<Vec<u8>> {
    Ok(ruzstd::encoding::compress_to_vec(data, ruzstd::encoding::CompressionLevel::Fastest))
}

/// Decompress a Zstandard byte stream, which may hold several concatenated
/// frames.
#[cfg(not(target_arch = "wasm32"))]
pub fn decode_all(data: &[u8]) -> std::io::Result<Vec<u8>> {
    zstd::decode_all(std::io::Cursor::new(data))
}

/// Decompress a Zstandard byte stream, which may hold several concatenated
/// frames. See the host variant above.
#[cfg(target_arch = "wasm32")]
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
}
