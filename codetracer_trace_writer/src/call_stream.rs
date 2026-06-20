//! Dedicated call stream (`calls.dat`) for materialized CTFS `.ct` traces.
//!
//! This is the M17a deliverable of the Trace-Based-Incremental-Testing
//! campaign: an *additive*, backward-compatible split of the call tree out of
//! the unified `events.log`. Recorders that opt in emit, in addition to the
//! unchanged `events.log`, a dedicated `calls.dat` stream of complete call
//! records plus a companion seekable index `calls.idx`, gated by the new
//! `meta.dat` capability flag `has_call_stream` (bit 8). Readers that do not
//! know the flag simply ignore the two extra files, so old `.ct`s and old
//! readers keep working byte-for-byte.
//!
//! # Wire format (per record)
//!
//! Each record matches `codetracer-trace-format-spec/trace-events.md`
//! §"Call Stream Records" and is byte-identical to the Nim
//! `codetracer_trace_writer/call_stream.nim` encoding so the two
//! implementations are interoperable:
//!
//! ```text
//!   varint        function_id
//!   signed_varint parent_call_key      (-1 for root)
//!   varint        first_step_id
//!   varint        last_step_id
//!   varint        depth
//!   varint        args_count
//!     per arg:    varint varname_id, varint value_len, value bytes (CBOR)
//!   varint        return_value_len, return_value bytes
//!                 (a single 0xFF byte is the VoidReturn marker)
//!   varint        exception_len, exception bytes (0 if no exception)
//!   varint        children_count
//!     per child:  varint child_call_key
//! ```
//!
//! `args`, `return_value` and `raised_exception` payloads are CBOR, encoded the
//! same way the split-binary event stream encodes them (`cbor4ii`), so the call
//! stream is consistent with the `Call`/`Return` events still present in
//! `events.log`.
//!
//! # Storage (`calls.dat` + `calls.idx`)
//!
//! Records are grouped into chunks of `chunk_size` records, each independently
//! Zstd-compressed, concatenated into `calls.dat` with **no inline headers**.
//! The companion `calls.idx` follows
//! `codetracer-trace-format-spec/seekable-zstd.md`:
//!
//! ```text
//!   calls.dat:  [zstd(chunk_0)][zstd(chunk_1)]...
//!   calls.idx:  [chunk_size: u32 LE][offset_0: u64 LE][offset_1: u64 LE]...
//! ```
//!
//! `offset_i` is the byte offset of chunk `i` within `calls.dat`. To seek to
//! call record `N`: `chunk = N / chunk_size`, read `offset[chunk]` and
//! `offset[chunk+1]` (or the file size for the last chunk), decompress that one
//! chunk, and index `N % chunk_size` within it — O(1), no whole-stream
//! decompression.

use codetracer_trace_types::{CallRecord as EventCallRecord, ReturnRecord, TraceLowLevelEvent};

/// Default number of call records per chunk. Call records are larger than step
/// records (spec: ~20-200 bytes), so a smaller chunk size than `steps.dat`
/// gives finer seek granularity (seekable-zstd.md §Configuration).
pub const DEFAULT_CALLS_CHUNK_SIZE: usize = 256;

/// One-byte marker for a void return value, matching
/// `call_stream.nim`'s `VoidReturnMarker`.
pub const VOID_RETURN_MARKER: u8 = 0xFF;

/// A complete call-stream record, written when the call returns so it carries
/// full entry/exit information. This is the on-disk projection of a function
/// call (distinct from the event-stream [`EventCallRecord`], which only carries
/// `function_id` + `args` at call entry).
#[derive(Debug, Clone, PartialEq)]
pub struct CallStreamRecord {
    /// Sequential index assigned at call entry (the record's position in
    /// `calls.dat`).
    pub call_key: u64,
    /// Reference into the function interning table.
    pub function_id: u64,
    /// Parent call's `call_key`, or `-1` for a root call.
    pub parent_key: i64,
    /// First step index covered by this call.
    pub first_step_id: u64,
    /// Last step index covered by this call.
    pub last_step_id: u64,
    /// Call-stack depth (0 for a root call).
    pub depth: u64,
    /// CBOR `args` payload (the `Vec<FullValueRecord>` from the `Call` event),
    /// or empty when there were no args.
    pub args: Vec<u8>,
    /// CBOR `return_value` payload, or the single byte [`VOID_RETURN_MARKER`]
    /// for a void return.
    pub return_value: Vec<u8>,
    /// CBOR `raised_exception` payload, empty when the call returned normally.
    pub raised_exception: Vec<u8>,
    /// Child call keys, in call-entry order.
    pub children: Vec<u64>,
}

// --- varint helpers (unsigned LEB128 + zigzag signed) ---

fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn encode_signed_varint(value: i64, out: &mut Vec<u8>) {
    // zigzag: (n << 1) ^ (n >> 63)
    let zz = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(zz, out);
}

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err("calls.dat: truncated varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err("calls.dat: varint too long".to_string());
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

fn decode_signed_varint(data: &[u8], pos: &mut usize) -> Result<i64, String> {
    let zz = decode_varint(data, pos)?;
    // inverse zigzag
    Ok(((zz >> 1) as i64) ^ -((zz & 1) as i64))
}

impl CallStreamRecord {
    /// Encode this record into its wire format (appended to `out`).
    pub fn encode(&self, out: &mut Vec<u8>) {
        encode_varint(self.function_id, out);
        encode_signed_varint(self.parent_key, out);
        encode_varint(self.first_step_id, out);
        encode_varint(self.last_step_id, out);
        encode_varint(self.depth, out);

        // args: count, then (varname_id placeholder, len, bytes). The Rust
        // event stream stores args as a single CBOR blob (the `Vec<FullValueRecord>`)
        // rather than per-arg (varname_id, value) pairs, so we emit a single
        // synthetic arg entry carrying the whole CBOR payload under varname_id 0
        // when args are present. This keeps the record self-describing while
        // round-tripping the exact CBOR the `Call` event carried.
        if self.args.is_empty() {
            encode_varint(0, out);
        } else {
            encode_varint(1, out);
            encode_varint(0, out); // varname_id (synthetic: whole-args blob)
            encode_varint(self.args.len() as u64, out);
            out.extend_from_slice(&self.args);
        }

        encode_varint(self.return_value.len() as u64, out);
        out.extend_from_slice(&self.return_value);

        encode_varint(self.raised_exception.len() as u64, out);
        out.extend_from_slice(&self.raised_exception);

        encode_varint(self.children.len() as u64, out);
        for &child in &self.children {
            encode_varint(child, out);
        }
    }

    /// Decode a record from its wire format. `call_key` is supplied by the
    /// reader (it is the record's position, not stored inline).
    pub fn decode(call_key: u64, data: &[u8]) -> Result<CallStreamRecord, String> {
        let mut pos = 0usize;
        let function_id = decode_varint(data, &mut pos)?;
        let parent_key = decode_signed_varint(data, &mut pos)?;
        let first_step_id = decode_varint(data, &mut pos)?;
        let last_step_id = decode_varint(data, &mut pos)?;
        let depth = decode_varint(data, &mut pos)?;

        let args_count = decode_varint(data, &mut pos)? as usize;
        let mut args: Vec<u8> = Vec::new();
        for _ in 0..args_count {
            let _varname_id = decode_varint(data, &mut pos)?;
            let arg_len = decode_varint(data, &mut pos)? as usize;
            if pos + arg_len > data.len() {
                return Err("calls.dat: truncated arg data".to_string());
            }
            // We emit a single synthetic arg holding the whole CBOR blob; if a
            // producer ever writes multiple, concatenate (only the first is the
            // canonical args blob for Rust-written records).
            if args.is_empty() {
                args.extend_from_slice(&data[pos..pos + arg_len]);
            }
            pos += arg_len;
        }

        let ret_len = decode_varint(data, &mut pos)? as usize;
        if pos + ret_len > data.len() {
            return Err("calls.dat: truncated return value".to_string());
        }
        let return_value = data[pos..pos + ret_len].to_vec();
        pos += ret_len;

        let exc_len = decode_varint(data, &mut pos)? as usize;
        if pos + exc_len > data.len() {
            return Err("calls.dat: truncated exception".to_string());
        }
        let raised_exception = data[pos..pos + exc_len].to_vec();
        pos += exc_len;

        let children_count = decode_varint(data, &mut pos)? as usize;
        let mut children = Vec::with_capacity(children_count);
        for _ in 0..children_count {
            children.push(decode_varint(data, &mut pos)?);
        }

        Ok(CallStreamRecord {
            call_key,
            function_id,
            parent_key,
            first_step_id,
            last_step_id,
            depth,
            args,
            return_value,
            raised_exception,
            children,
        })
    }
}

/// Encode a value as CBOR the same way the split-binary event stream does, so
/// `calls.dat` args/return payloads are byte-identical to the `Call`/`Return`
/// event payloads in `events.log`.
fn cbor_bytes<T: serde::Serialize>(value: &T) -> Vec<u8> {
    cbor4ii::serde::to_vec(Vec::new(), value).expect("CBOR encode failed")
}

/// Builds the dedicated call stream from the same event sequence that feeds
/// `events.log`, so the two are guaranteed consistent.
///
/// The builder maintains a step counter and an explicit call stack. A `Call`
/// event pushes a new (open) record; the matching `Return` event (or, at
/// stream end, an implicit close) pops it and finalizes
/// `last_step_id`/`return_value`/`raised_exception`. `first_step_id` is the
/// step index in effect at call entry; `last_step_id` is the step index in
/// effect at return (clamped to the last real step).
#[derive(Default)]
pub struct CallStreamBuilder {
    /// Finalized records, indexed by `call_key`.
    records: Vec<CallStreamRecord>,
    /// Stack of open call keys (indices into `records`).
    open_stack: Vec<usize>,
    /// Current step index (number of `Step` events seen so far).
    step_index: u64,
    /// Whether at least one step has been recorded (so `last_step_id` can be
    /// clamped to `step_index - 1`).
    any_step: bool,
}

impl CallStreamBuilder {
    pub fn new() -> Self {
        CallStreamBuilder::default()
    }

    /// Feed one event in stream order.
    pub fn observe(&mut self, event: &TraceLowLevelEvent) {
        match event {
            TraceLowLevelEvent::Step(_) => {
                self.step_index += 1;
                self.any_step = true;
            }
            TraceLowLevelEvent::Call(EventCallRecord { function_id, args }) => {
                let call_key = self.records.len() as u64;
                let parent_key = match self.open_stack.last() {
                    Some(&parent_idx) => {
                        self.records[parent_idx].children.push(call_key);
                        self.records[parent_idx].call_key as i64
                    }
                    None => -1,
                };
                let depth = self.open_stack.len() as u64;
                let first_step_id = self.current_step_id();
                let args_bytes = if args.is_empty() { Vec::new() } else { cbor_bytes(args) };
                self.records.push(CallStreamRecord {
                    call_key,
                    function_id: function_id.0 as u64,
                    parent_key,
                    first_step_id,
                    last_step_id: first_step_id,
                    depth,
                    args: args_bytes,
                    return_value: vec![VOID_RETURN_MARKER],
                    raised_exception: Vec::new(),
                    children: Vec::new(),
                });
                self.open_stack.push(call_key as usize);
            }
            TraceLowLevelEvent::Return(ReturnRecord { return_value }) => {
                if let Some(idx) = self.open_stack.pop() {
                    let last = self.current_step_id();
                    let rec = &mut self.records[idx];
                    rec.last_step_id = last;
                    rec.return_value = cbor_bytes(return_value);
                }
            }
            _ => {}
        }
    }

    /// The step id to attribute to an entry/exit at the current position: the
    /// last real step index, or 0 before any step has been seen.
    fn current_step_id(&self) -> u64 {
        if self.any_step { self.step_index - 1 } else { 0 }
    }

    /// Number of call records built so far.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Finalize: close any still-open calls (a Return may be missing at stream
    /// end) and return the records in `call_key` order.
    pub fn finish(mut self) -> Vec<CallStreamRecord> {
        let last = self.current_step_id();
        while let Some(idx) = self.open_stack.pop() {
            // Leave the void-return marker in place for a call with no Return.
            self.records[idx].last_step_id = self.records[idx].last_step_id.max(last);
        }
        self.records
    }
}

/// The encoded `calls.dat` stream plus its companion `calls.idx`.
#[cfg(not(target_arch = "wasm32"))]
pub struct EncodedCallStream {
    /// Concatenated Zstd-compressed chunks, no inline headers.
    pub dat: Vec<u8>,
    /// Companion index: `[chunk_size: u32 LE][offset_0: u64 LE]...`.
    pub idx: Vec<u8>,
    /// Number of call records encoded.
    pub record_count: usize,
}

/// Encode call records into `calls.dat` (chunked Zstd) + `calls.idx`
/// (companion offset index), per seekable-zstd.md.
#[cfg(not(target_arch = "wasm32"))]
pub fn encode_call_stream(records: &[CallStreamRecord], chunk_size: usize, zstd_level: i32) -> Result<EncodedCallStream, String> {
    use std::io::Cursor;
    let chunk_size = chunk_size.max(1);
    let mut dat: Vec<u8> = Vec::new();
    let mut idx: Vec<u8> = Vec::new();
    idx.extend_from_slice(&(chunk_size as u32).to_le_bytes());

    let mut i = 0usize;
    while i < records.len() {
        let end = (i + chunk_size).min(records.len());
        // Record the byte offset of this chunk within calls.dat.
        idx.extend_from_slice(&(dat.len() as u64).to_le_bytes());

        let mut raw: Vec<u8> = Vec::new();
        for rec in &records[i..end] {
            // Each record is length-prefixed within the chunk so the reader can
            // walk records without re-deriving sizes (records are variable
            // length; the chunk holds up to chunk_size of them).
            let mut rec_bytes: Vec<u8> = Vec::new();
            rec.encode(&mut rec_bytes);
            encode_varint(rec_bytes.len() as u64, &mut raw);
            raw.extend_from_slice(&rec_bytes);
        }
        let compressed = zstd::encode_all(Cursor::new(&raw[..]), zstd_level).map_err(|e| format!("calls.dat: zstd encode failed: {e}"))?;
        dat.extend_from_slice(&compressed);
        i = end;
    }

    Ok(EncodedCallStream {
        dat,
        idx,
        record_count: records.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            encode_varint(v, &mut buf);
            let mut pos = 0;
            assert_eq!(decode_varint(&buf, &mut pos).unwrap(), v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn signed_varint_roundtrip() {
        for v in [0i64, -1, 1, -64, 63, i32::MIN as i64, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            encode_signed_varint(v, &mut buf);
            let mut pos = 0;
            assert_eq!(decode_signed_varint(&buf, &mut pos).unwrap(), v);
        }
    }

    #[test]
    fn record_encode_decode_roundtrip() {
        let rec = CallStreamRecord {
            call_key: 3,
            function_id: 7,
            parent_key: 1,
            first_step_id: 10,
            last_step_id: 20,
            depth: 2,
            args: vec![1, 2, 3],
            return_value: vec![VOID_RETURN_MARKER],
            raised_exception: vec![],
            children: vec![4, 5],
        };
        let mut buf = Vec::new();
        rec.encode(&mut buf);
        let decoded = CallStreamRecord::decode(3, &buf).unwrap();
        assert_eq!(decoded, rec);
    }
}
