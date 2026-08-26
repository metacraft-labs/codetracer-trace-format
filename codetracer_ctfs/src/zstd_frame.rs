//! The one place a CTFS stream chunk is compressed.
//!
//! # Why this module exists rather than a `zstd::` call per stream
//!
//! libzstd has two entry points that produce a valid frame from a buffer, and
//! only one of them writes the **pledged content size** into the frame header:
//!
//! | call | pledge | what reads it |
//! |---|---|---|
//! | `ZSTD_compress` (Rust: `zstd::bulk::compress`) | present | everything |
//! | `ZSTD_compressStream` (Rust: `zstd::encode_all`) | absent | nothing can size its output buffer |
//!
//! The canonical Nim reader sizes every destination buffer from
//! `ZSTD_getFrameContentSize` and treats `ZSTD_CONTENTSIZE_UNKNOWN` as a hard
//! failure. It does this in **five** separate places, one per stream family:
//! `exec_stream.nim` (`steps.dat`), `value_stream.nim` (`values.dat`),
//! `call_stream.nim` (`calls.dat`), `io_event_stream.nim` (`events.dat`) and
//! `chunked_compressed_table.nim`; `codetracer_trace_reader.nim` does it once
//! more for `events.log`.
//!
//! So a stream written with the streaming API is not merely a few bytes
//! different from the reference writer's — **it is unreadable by the reference
//! reader**, and the failure is not always loud. Measured on a container this
//! repository's own `CtfsTraceWriter` produced, read back through the Nim FFI
//! reader:
//!
//! ```text
//!                 nim-written   rust-written (streaming frames)
//! step_count            12          0     <- silently empty, not refused
//! call_count             1          0     <- silently empty, not refused
//! event_count            1          0     <- silently empty, not refused
//! values_json(0)      Ok(..)     Err("cannot determine decompressed size…")
//! ```
//!
//! A recording that reads back as *empty* rather than as *broken* is the worst
//! outcome available here, which is why the choice of entry point is centralised
//! in one function with a name that says what it guarantees, instead of being a
//! `zstd::` call at each of the sites above where the next one added would
//! reintroduce it.
//!
//! `compress_pledged` is deliberately the only compression helper this crate
//! exports. `zstd::encode_all` must not appear in a stream-writing path; the
//! workspace test `no_stream_writer_uses_the_streaming_zstd_api` is the census
//! that says so and it can fail.

/// Compress `raw` into a single Zstd frame whose header pledges the
/// decompressed size, exactly as `ZSTD_compress` does — the call the reference
/// Nim writer makes.
///
/// `stream` names the stream for the error message only; it does not affect the
/// bytes.
///
/// Returns the frame. An empty input still produces a valid (pledged-zero)
/// frame, which is what `ZSTD_compress` does and what the readers expect.
#[cfg(not(target_arch = "wasm32"))]
pub fn compress_pledged(raw: &[u8], level: i32, stream: &str) -> Result<Vec<u8>, String> {
    zstd::bulk::compress(raw, level).map_err(|e| format!("{stream}: zstd compress failed: {e}"))
}

/// The `wasm32` arm. The contract is the host variant's — a frame whose header
/// pledges its decompressed size — and the route to it is different, because
/// there is no `ZSTD_compress` here to ask.
///
/// `zstd-sys` builds the reference C library with `cc` and there is no libc on
/// `wasm32-unknown-unknown`, so this crate swaps in the pure-Rust encoder
/// ([`crate::zstd_compat::encode_all`], `ruzstd`), which writes
/// `frame_content_size: None` unconditionally and exposes no way to ask for it.
/// The pledge is therefore added to the FINISHED frame by
/// [`pledge_frame_content_size`], which is a conformant edit for the reason set
/// out on that function. Eight bytes per frame, no new dependency, and the
/// module's wasm import count is unchanged at zero.
///
/// Without this arm a wasm-produced container has exactly the defect this
/// module exists to prevent, and the readers report it as an EMPTY stream.
#[cfg(target_arch = "wasm32")]
pub fn compress_pledged(raw: &[u8], level: i32, stream: &str) -> Result<Vec<u8>, String> {
    let frame = crate::zstd_compat::encode_all(raw, level).map_err(|e| format!("{stream}: zstd compress failed: {e}"))?;
    pledge_frame_content_size(frame, raw.len() as u64).map_err(|e| format!("{stream}: {e}"))
}

/// Add a pledged content size to a finished Zstd frame that lacks one.
///
/// # Who needs this
///
/// [`compress_pledged`] is the answer wherever libzstd is available. It is not
/// available on `wasm32-unknown-unknown`: `zstd-sys` builds the reference C
/// library and there is no libc to build it against, so the wasm build of this
/// crate swaps in a pure-Rust encoder. The one in use, `ruzstd`, writes
/// `frame_content_size: None` unconditionally —
/// `ruzstd-0.8.3/src/encoding/frame_compressor.rs:145`, under its own
/// `TODO: The Frame_Content_Size field isn't set at all, we should prefer to
/// include it always` — and exposes no way to ask for it. A container written
/// from wasm therefore has exactly the defect this module exists to prevent,
/// and the readers report it as an EMPTY stream.
///
/// Replacing or forking the encoder is not needed. RFC 8878 §3.1.1 lays a frame
/// out as
///
/// ```text
///   Magic_Number(4) Frame_Header_Descriptor(1) [Window_Descriptor(1)]
///   [Dictionary_ID(0..4)] [Frame_Content_Size(0..8)]  Block…  [Checksum]
/// ```
///
/// and **nothing in the blocks depends on the header's length**, so the field
/// can be inserted and the descriptor's `Frame_Content_Size_flag` flipped
/// afterwards. The result is an ordinary conformant frame: measured against
/// libzstd over empty, one-byte, incompressible-64-KiB and
/// highly-compressible-100-KB payloads, `ZSTD_getFrameContentSize` returns the
/// right value, `ZSTD_decompress` reproduces the payload, and the pure-Rust
/// decoder still reads its own patched frame. The cost is 8 bytes per frame.
///
/// Returns `Err` if `frame` is not a Zstd frame, or `Ok(frame)` unchanged if it
/// already pledges — so calling it on a [`compress_pledged`] frame is a no-op
/// rather than a corruption.
pub fn pledge_frame_content_size(frame: Vec<u8>, content_size: u64) -> Result<Vec<u8>, String> {
    const MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
    if frame.len() < 5 || frame[..4] != MAGIC {
        return Err("not a Zstd frame".to_string());
    }
    let descriptor = frame[4];
    let fcs_flag = descriptor >> 6;
    let single_segment = (descriptor >> 5) & 1 == 1;
    if fcs_flag != 0 || single_segment {
        // Already carries a Frame_Content_Size field (with Single_Segment_flag
        // set, flag 0 means a one-byte field rather than none).
        return Ok(frame);
    }
    // Single_Segment_flag is clear, so a Window_Descriptor byte is present.
    let dictionary_id_len = match descriptor & 0b11 {
        0 => 0usize,
        1 => 1,
        2 => 2,
        _ => 4,
    };
    let header_end = 5 + 1 + dictionary_id_len;
    if frame.len() < header_end {
        return Err("truncated Zstd frame header".to_string());
    }

    let mut out = Vec::with_capacity(frame.len() + 8);
    out.extend_from_slice(&frame[..4]);
    // Frame_Content_Size_flag = 3 -> an 8-byte field. The widest encoding is
    // chosen deliberately: a narrower one would have to be selected from the
    // size, and a size that crosses a width boundary is exactly where such a
    // choice goes wrong. Eight bytes is always legal and always correct.
    out.push((descriptor & 0b0011_1111) | (0b11 << 6));
    out.extend_from_slice(&frame[4 + 1..header_end]);
    out.extend_from_slice(&content_size.to_le_bytes());
    out.extend_from_slice(&frame[header_end..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the whole module exists for, with its negative control
    /// beside it: the helper pledges, and the call it replaced does not. Without
    /// the second assertion the first could pass against any encoder.
    #[test]
    fn compress_pledged_pledges_and_the_streaming_api_does_not() {
        let raw: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();

        let pledged = compress_pledged(&raw, 3, "test.dat").expect("compress");
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&pledged).expect("a valid frame"),
            Some(raw.len() as u64),
            "compress_pledged must write the content size into the frame header",
        );

        let streaming = zstd::encode_all(std::io::Cursor::new(&raw[..]), 3).expect("encode_all");
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&streaming).expect("a valid frame"),
            None,
            "control: the streaming API is still the thing this module replaces — if this ever \
             becomes Some, the module's reason for existing has changed and the comment above is stale",
        );
        assert_ne!(pledged, streaming, "control: the two encoders really do produce different bytes");

        // And it round-trips, so the pledge is not bought with a corrupt frame.
        assert_eq!(zstd::decode_all(&pledged[..]).expect("decode"), raw);
    }

    /// The wasm remedy, exercised against a REAL unpledged frame rather than a
    /// hand-built one.
    ///
    /// The frame `zstd::encode_all` produces is unpledged for the same reason
    /// the pure-Rust encoder's is, and has the same header shape
    /// (`Single_Segment_flag` clear, a `Window_Descriptor`, no dictionary), so
    /// it is the right stand-in on a host that cannot run the wasm encoder.
    /// Sizes are chosen to cross the 2-, 4- and 8-byte FCS width boundaries and
    /// to include the two degenerate payloads.
    #[test]
    fn an_unpledged_frame_can_be_made_to_pledge_after_the_fact() {
        let mut patched_count = 0usize;
        for len in [0usize, 1, 255, 256, 65_535, 65_536, 100_000] {
            let raw: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let unpledged = zstd::encode_all(std::io::Cursor::new(&raw[..]), 3).expect("encode_all");
            if zstd::zstd_safe::get_frame_content_size(&unpledged).expect("a valid frame").is_some() {
                // libzstd's streaming call emits a single-segment frame for an
                // EMPTY payload, and a single-segment frame always carries the
                // field. Nothing to patch, and the helper must say so by
                // returning the frame unchanged — which is the property that
                // makes it safe to apply unconditionally.
                assert_eq!(len, 0, "only the empty payload is expected to be pledged already, not len {len}");
                assert_eq!(pledge_frame_content_size(unpledged.clone(), 0).expect("no-op"), unpledged);
                continue;
            }

            patched_count += 1;
            let patched = pledge_frame_content_size(unpledged.clone(), raw.len() as u64).expect("patch");
            assert_eq!(
                zstd::zstd_safe::get_frame_content_size(&patched).expect("still a valid frame"),
                Some(raw.len() as u64),
                "len {len}: the patched frame must pledge the right size",
            );
            assert_eq!(
                zstd::decode_all(&patched[..]).expect("decode"),
                raw,
                "len {len}: the payload must survive"
            );
            assert_eq!(patched.len(), unpledged.len() + 8, "len {len}: the patch costs exactly the 8-byte field");
        }
        assert!(
            patched_count >= 6,
            "control: only {patched_count} of the payloads actually exercised the patch — if every case took the \
             already-pledged branch this test would prove nothing"
        );
    }

    /// It must be a no-op on a frame that already pledges, so a call site that
    /// cannot tell which encoder produced a frame is still safe.
    #[test]
    fn pledging_an_already_pledged_frame_changes_nothing() {
        let raw: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let pledged = compress_pledged(&raw, 3, "test.dat").expect("compress");
        let again = pledge_frame_content_size(pledged.clone(), raw.len() as u64).expect("patch");
        assert_eq!(again, pledged, "a frame that already pledges must come back byte-identical");
    }

    /// And it must refuse input that is not a frame, rather than producing
    /// something that looks like one.
    #[test]
    fn pledging_refuses_a_non_frame() {
        assert!(pledge_frame_content_size(b"not a frame at all".to_vec(), 5).is_err());
        assert!(pledge_frame_content_size(Vec::new(), 0).is_err());
    }

    /// An empty chunk must still be a frame that pledges zero rather than an
    /// error or an empty buffer — `chunked_compressed_table.nim` distinguishes
    /// "pledges 0" (malformed, refused by name) from "cannot tell" (refused
    /// with a different message), and only the writer can decide which it sees.
    #[test]
    fn an_empty_payload_still_produces_a_pledged_frame() {
        let frame = compress_pledged(&[], 3, "test.dat").expect("compress");
        assert!(!frame.is_empty());
        assert_eq!(zstd::zstd_safe::get_frame_content_size(&frame).expect("a valid frame"), Some(0));
    }
}
