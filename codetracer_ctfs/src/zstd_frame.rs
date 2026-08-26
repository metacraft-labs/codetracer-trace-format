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
pub fn compress_pledged(raw: &[u8], level: i32, stream: &str) -> Result<Vec<u8>, String> {
    zstd::bulk::compress(raw, level).map_err(|e| format!("{stream}: zstd compress failed: {e}"))
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
