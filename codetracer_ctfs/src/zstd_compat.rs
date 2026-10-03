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

/// The largest content size a frame may pledge before [`Decoder`] stops
/// trusting the pledge to size its output, per compressed byte. Zstandard's
/// densest block (an RLE block of 128 KiB in 4 bytes) stays well under it, so
/// only a corrupt or hostile header crosses it, and such a frame is decoded by
/// growing the buffer instead of by one allocation of the claimed size.
#[cfg(not(feature = "pure-rust-zstd"))]
const MAX_PLEDGE_RATIO: u64 = 1 << 16;

/// A decompression context kept across calls, for a reader that inflates one
/// chunk after another.
///
/// [`decode_all`] builds a streaming decoder per call — a fresh context with
/// its own window buffers — and grows its output as it goes. A reader that
/// seeks around a stream pays that set-up per chunk. `Decoder` keeps the
/// context, and decodes a frame that pledges its content size (every stream
/// chunk does: `zstd_frame::compress_pledged`) in one call into a buffer the
/// caller reuses. Anything else — several frames back to back, a frame
/// without a pledge — decodes exactly as [`decode_all`] does.
pub struct Decoder {
    #[cfg(not(feature = "pure-rust-zstd"))]
    ctx: zstd::bulk::Decompressor<'static>,
}

impl std::fmt::Debug for Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Decoder")
    }
}

impl Decoder {
    pub fn new() -> std::io::Result<Decoder> {
        Ok(Decoder {
            #[cfg(not(feature = "pure-rust-zstd"))]
            ctx: zstd::bulk::Decompressor::new()?,
        })
    }

    /// Decompress `data` into `out`, replacing its contents. The bytes are the
    /// ones [`decode_all`] returns for the same input.
    #[cfg(not(feature = "pure-rust-zstd"))]
    pub fn decode_into(&mut self, data: &[u8], out: &mut Vec<u8>) -> std::io::Result<()> {
        use zstd::zstd_safe;
        out.clear();
        let single_frame = zstd_safe::find_frame_compressed_size(data).is_ok_and(|n| n == data.len());
        let pledge = match zstd_safe::get_frame_content_size(data) {
            Ok(Some(n)) if single_frame && n <= (data.len() as u64).saturating_mul(MAX_PLEDGE_RATIO) => n as usize,
            _ => {
                out.extend_from_slice(&decode_all(data)?);
                return Ok(());
            }
        };
        out.reserve(pledge);
        let written = self.ctx.decompress_to_buffer(data, out)?;
        if written != pledge {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("zstd frame pledges {pledge} bytes but decodes to {written}"),
            ));
        }
        Ok(())
    }

    /// Decompress `data` into `out`, replacing its contents.
    #[cfg(feature = "pure-rust-zstd")]
    pub fn decode_into(&mut self, data: &[u8], out: &mut Vec<u8>) -> std::io::Result<()> {
        *out = decode_all(data)?;
        Ok(())
    }
}

std::thread_local! {
    /// The context [`decode_into`] decodes with: one per thread, made on the
    /// first decode and freed when the thread ends.
    static SHARED: std::cell::RefCell<Option<Decoder>> = const { std::cell::RefCell::new(None) };
}

/// Decompress `data` into `out`, replacing its contents, with a context this
/// thread keeps for every caller: what [`Decoder::decode_into`] does, without
/// each reader holding a context of its own. The bytes are the ones
/// [`decode_all`] returns for the same input.
pub fn decode_into(data: &[u8], out: &mut Vec<u8>) -> std::io::Result<()> {
    SHARED.with(|shared| match shared.try_borrow_mut() {
        Ok(mut slot) => {
            if slot.is_none() {
                *slot = Some(Decoder::new()?);
            }
            slot.as_mut().map_or(Ok(()), |d| d.decode_into(data, out))
        }
        // Only reachable from inside a decode on this thread, which nothing
        // in this crate does; a context of its own keeps it correct.
        Err(_) => Decoder::new()?.decode_into(data, out),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reused_decoder_agrees_with_decode_all() {
        let mut d = Decoder::new().unwrap();
        let mut out = vec![0xAA; 7];
        let big: Vec<u8> = (0..300_000u32).map(|i| (i % 253) as u8).collect();
        let pledged = crate::zstd_frame::compress_pledged(&big, 3, "test").unwrap();
        let unpledged = encode_all(&big, 3).unwrap();
        let mut two = crate::zstd_frame::compress_pledged(b"hello ", 3, "test").unwrap();
        two.extend_from_slice(&crate::zstd_frame::compress_pledged(b"world", 3, "test").unwrap());
        let empty = crate::zstd_frame::compress_pledged(b"", 3, "test").unwrap();
        for input in [&pledged, &unpledged, &two, &empty, &pledged] {
            d.decode_into(input, &mut out).unwrap();
            assert_eq!(out, decode_all(input).unwrap());
        }
        assert_eq!(out, big);
    }

    #[test]
    fn the_shared_context_agrees_with_decode_all() {
        let big: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let pledged = crate::zstd_frame::compress_pledged(&big, 3, "test").unwrap();
        let unpledged = encode_all(b"no pledge", 3).unwrap();
        let mut out = vec![1, 2, 3];
        for input in [&pledged, &unpledged, &pledged] {
            decode_into(input, &mut out).unwrap();
            assert_eq!(out, decode_all(input).unwrap());
        }
        assert!(decode_into(&pledged[..pledged.len() - 3], &mut out).is_err());
        decode_into(&pledged, &mut out).unwrap();
        assert_eq!(out, big, "a refused frame leaves the context usable");
    }

    #[test]
    fn the_reused_decoder_refuses_what_decode_all_refuses() {
        let mut d = Decoder::new().unwrap();
        let mut out = Vec::new();
        let frame = crate::zstd_frame::compress_pledged(&[7u8; 5000], 3, "test").unwrap();
        let truncated = &frame[..frame.len() - 3];
        assert!(decode_all(truncated).is_err());
        assert!(d.decode_into(truncated, &mut out).is_err());
        let mut garbage = frame.clone();
        let mid = garbage.len() / 2;
        garbage[mid] ^= 0xFF;
        assert_eq!(decode_all(&garbage).is_err(), d.decode_into(&garbage, &mut out).is_err());
    }

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
