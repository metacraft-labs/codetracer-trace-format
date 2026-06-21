//! Dedicated execution stream (`steps.dat`) for materialized CTFS `.ct` traces.
//!
//! This is the M23a deliverable of the Trace-Based-Incremental-Testing
//! campaign (the first sub-milestone of M23 — "finish the trace-events.md Event
//! Stream Redesign"): an *additive*, backward-compatible split of the compact
//! step (execution) timeline out of the unified `events.log`. It mirrors the
//! M17a `calls.dat` split exactly — recorders that opt in emit, in addition to
//! the unchanged `events.log`, a dedicated `steps.dat` stream of compact step
//! records plus a companion seekable index `steps.idx`, gated by the new
//! `meta.dat` capability flag `has_step_stream` (bit 9). Readers that do not
//! know the flag simply ignore the two extra files, so old `.ct`s and old
//! readers keep working byte-for-byte.
//!
//! # Compact step encoding (per record)
//!
//! Each record matches `codetracer-trace-format-spec/trace-events.md`
//! §"Compact Step Encoding" / §"Execution Stream Events (`steps.dat`)":
//!
//! ```text
//!   Tag 0  AbsoluteStep  : varint global_line_index
//!   Tag 1  DeltaStep     : signed (zigzag) varint delta from the previous
//!                          step's global_line_index
//!   Tag 2  Raise         : varint exception_type_id, varint message_len,
//!                          message bytes
//!   Tag 3  Catch         : varint exception_type_id
//!   Tag 4  ThreadSwitch  : varint thread_id
//! ```
//!
//! ## `global_line_index`
//!
//! The spec's canonical `global_line_index` is a per-file contiguous integer
//! over `(line, column)` pairs, interned through `funcs.dat`. That interning
//! table is a *later* sub-milestone (M23b+). For M23a — which only delivers the
//! seekable execution stream additively, WITHOUT touching the interning tables
//! or the value stream — the writer derives a deterministic `global_line_index`
//! directly from the same `Step{path_id, line}` events that feed `events.log`,
//! via [`global_line_index`]. The derivation is reversible by the reader: the
//! mapping packs `(path_id, line)` into a single u64 the same way on both sides,
//! so a decoded `global_line_index` recovers the exact `(path_id, line)` the
//! `events.log` step carried. When the canonical interning lands, this packing
//! is replaced by a `funcs.dat` lookup with no change to the stream's wire shape.
//!
//! # Encoding rules (spec §"Encoding Rules")
//!
//! 1. The first step in a trace is always AbsoluteStep.
//! 2. After a Call event, the next step is AbsoluteStep (new function context).
//! 3. After a Return event, the next step is AbsoluteStep (returning to caller).
//! 4. All other steps use DeltaStep when the signed delta fits in 3 varint
//!    bytes (±1048575), otherwise AbsoluteStep.
//! 5. The first step record of every chunk is AbsoluteStep so each chunk is
//!    independently decodable (the running absolute value never carries across a
//!    chunk boundary). See [`encode_step_stream`].
//!
//! # Storage (`steps.dat` + `steps.idx`)
//!
//! Records are grouped into chunks of `chunk_size` records, each independently
//! Zstd-compressed, concatenated into `steps.dat` with **no inline headers**.
//! The companion `steps.idx` follows
//! `codetracer-trace-format-spec/seekable-zstd.md`:
//!
//! ```text
//!   steps.dat:  [zstd(chunk_0)][zstd(chunk_1)]...
//!   steps.idx:  [chunk_size: u32 LE][offset_0: u64 LE][offset_1: u64 LE]...
//! ```
//!
//! `offset_i` is the byte offset of chunk `i` within `steps.dat`. To seek to
//! step record `N`: `chunk = N / chunk_size`, read `offset[chunk]` and
//! `offset[chunk+1]` (or the file size for the last chunk), decompress that one
//! chunk, and decode forward within it carrying the running absolute value from
//! the chunk's leading AbsoluteStep — O(1) chunks, no whole-stream decompression.

use codetracer_trace_types::{StepRecord, ThreadId, TraceLowLevelEvent};

/// Default number of step records per chunk. Step records are tiny (2-4 bytes,
/// spec §"Stream Summary"), so a larger chunk size than `calls.dat` keeps the
/// per-chunk overhead amortised while chunks still hold thousands of steps
/// (seekable-zstd.md §Configuration).
pub const DEFAULT_STEPS_CHUNK_SIZE: usize = 4096;

/// The maximum absolute delta encodable as a DeltaStep (3 varint bytes, spec
/// §"Compact Step Encoding"). Larger jumps fall back to AbsoluteStep.
pub const MAX_DELTA: i64 = 1_048_575;

// --- compact step record tags (trace-events.md §"Execution Stream Events") ---

/// Tag 0 — AbsoluteStep: full `global_line_index`.
pub const TAG_ABSOLUTE_STEP: u8 = 0;
/// Tag 1 — DeltaStep: signed delta from the previous step's `global_line_index`.
pub const TAG_DELTA_STEP: u8 = 1;
/// Tag 2 — Raise: exception raised (before unwinding).
pub const TAG_RAISE: u8 = 2;
/// Tag 3 — Catch: exception caught by a try/except handler.
pub const TAG_CATCH: u8 = 3;
/// Tag 4 — ThreadSwitch: execution switched to a different thread.
pub const TAG_THREAD_SWITCH: u8 = 4;

/// One decoded execution-stream record. This is the on-disk projection of the
/// compact step encoding; a [`StepStreamRecord::Step`] carries the recovered
/// `global_line_index` (which [`unpack_global_line_index`] turns back into the
/// `(path_id, line)` the `events.log` step held).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepStreamRecord {
    /// A source-line step at the given (decoded-to-absolute) `global_line_index`.
    Step { global_line_index: u64 },
    /// An exception was raised (before unwinding).
    Raise { exception_type_id: u64, message: Vec<u8> },
    /// An exception was caught by a try/except handler.
    Catch { exception_type_id: u64 },
    /// Execution switched to a different thread.
    ThreadSwitch { thread_id: u64 },
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
            return Err("steps.dat: truncated varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err("steps.dat: varint too long".to_string());
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

// --- global_line_index packing ------------------------------------------------
//
// M23a derivation (see module docs): pack a `Step{path_id, line}` into a single
// u64 `global_line_index` so the compact AbsoluteStep/DeltaStep encoding has the
// integer coordinate the spec defines, and the reader can recover the exact
// (path_id, line) the events.log step carried. `line` lives in the low 32 bits
// and `path_id` in the high 32 bits. Both are non-negative in practice (Step
// path/line are recorder-produced indices/line numbers); negative inputs are
// clamped to 0 so packing is total and never panics.

/// Number of bits reserved for the `line` component of a packed
/// `global_line_index`. `path_id` occupies the bits above it.
const GLI_LINE_BITS: u32 = 32;
const GLI_LINE_MASK: u64 = (1u64 << GLI_LINE_BITS) - 1;

/// Pack a `(path_id, line)` step location into the single `global_line_index`
/// integer the compact step encoding stores. Inverse of
/// [`unpack_global_line_index`].
pub fn pack_global_line_index(path_id: usize, line: i64) -> u64 {
    let p = path_id as u64;
    let l = (line.max(0) as u64) & GLI_LINE_MASK;
    (p << GLI_LINE_BITS) | l
}

/// Recover the `(path_id, line)` a packed `global_line_index` was built from.
/// Inverse of [`pack_global_line_index`].
pub fn unpack_global_line_index(gli: u64) -> (usize, i64) {
    let path_id = (gli >> GLI_LINE_BITS) as usize;
    let line = (gli & GLI_LINE_MASK) as i64;
    (path_id, line)
}

/// Derive the `global_line_index` for a `Step` event (M23a packing).
pub fn global_line_index(step: &StepRecord) -> u64 {
    pack_global_line_index(step.path_id.0, step.line.0)
}

/// A finalized execution stream: records in stream order plus, for each `Step`
/// record, whether it must be encoded AbsoluteStep (encoding rules 1-3). The
/// `forced_absolute` flags are positional over `Step` records only (the i-th
/// `true`/`false` applies to the i-th `StepStreamRecord::Step`).
pub struct StepStream {
    /// All execution-stream records in order.
    pub records: Vec<StepStreamRecord>,
    /// One flag per `Step` record (in `Step` order): force AbsoluteStep.
    pub forced_absolute: Vec<bool>,
}

impl StepStream {
    /// Number of records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Builds the dedicated execution stream from the same event sequence that
/// feeds `events.log`, so the two are guaranteed consistent.
///
/// Only the events that belong to the execution stream are observed: `Step`
/// (⇒ a step record carrying the derived `global_line_index`) and
/// `ThreadSwitch` (⇒ a ThreadSwitch record). `Call`/`Return` are observed only
/// to mark the *next* step as AbsoluteStep per the spec encoding rules; they do
/// not themselves produce execution-stream records. Raise/Catch have no
/// representation in the legacy `TraceLowLevelEvent` enum, so the builder never
/// emits them today — but the wire format and reader support their tags so the
/// stream is forward-compatible when recorders begin emitting them (M23b+).
#[derive(Default)]
pub struct StepStreamBuilder {
    /// Finalized records in stream order.
    records: Vec<StepStreamRecord>,
    /// Per-Step forced-absolute flags (parallel to the `Step` records).
    forced_absolute: Vec<bool>,
    /// Whether the next `Step` must be encoded as AbsoluteStep (the first step,
    /// or the step right after a Call/Return/ThreadSwitch). Starts true (rule 1).
    next_is_absolute: bool,
}

impl StepStreamBuilder {
    pub fn new() -> Self {
        StepStreamBuilder {
            records: Vec::new(),
            forced_absolute: Vec::new(),
            next_is_absolute: true,
        }
    }

    /// Feed one event in stream order.
    pub fn observe(&mut self, event: &TraceLowLevelEvent) {
        match event {
            TraceLowLevelEvent::Step(step) => {
                self.records.push(StepStreamRecord::Step {
                    global_line_index: global_line_index(step),
                });
                self.forced_absolute.push(self.next_is_absolute);
                self.next_is_absolute = false;
            }
            TraceLowLevelEvent::Call(_) | TraceLowLevelEvent::Return(_) => {
                // Rules 2 & 3: the next step starts a new function context.
                self.next_is_absolute = true;
            }
            TraceLowLevelEvent::ThreadSwitch(ThreadId(tid)) => {
                self.records.push(StepStreamRecord::ThreadSwitch { thread_id: *tid });
                // A thread switch breaks delta continuity: the next step's
                // previous-absolute belongs to a different thread, so force
                // AbsoluteStep (keeps deltas within a single thread).
                self.next_is_absolute = true;
            }
            _ => {}
        }
    }

    /// Number of records built so far.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Finalize and return the execution stream.
    pub fn finish(self) -> StepStream {
        StepStream {
            records: self.records,
            forced_absolute: self.forced_absolute,
        }
    }
}

/// Encode a single execution-stream record into the chunk buffer.
///
/// For a `Step`, `prev_abs` is the running absolute `global_line_index` of the
/// previous step in the same chunk (or `None` to force AbsoluteStep — the first
/// step of a chunk, or a step flagged forced-absolute). `force_absolute` honors
/// encoding rules 1-3. Returns the new running absolute value for the next step
/// (unchanged for non-Step records).
fn encode_record(record: &StepStreamRecord, prev_abs: Option<u64>, force_absolute: bool, out: &mut Vec<u8>) -> Option<u64> {
    match record {
        StepStreamRecord::Step { global_line_index } => {
            let gli = *global_line_index;
            let use_delta = match (prev_abs, force_absolute) {
                (Some(prev), false) => {
                    let delta = gli as i64 - prev as i64;
                    delta.abs() <= MAX_DELTA
                }
                _ => false,
            };
            if use_delta {
                let delta = gli as i64 - prev_abs.unwrap() as i64;
                out.push(TAG_DELTA_STEP);
                encode_signed_varint(delta, out);
            } else {
                out.push(TAG_ABSOLUTE_STEP);
                encode_varint(gli, out);
            }
            Some(gli)
        }
        StepStreamRecord::Raise { exception_type_id, message } => {
            out.push(TAG_RAISE);
            encode_varint(*exception_type_id, out);
            encode_varint(message.len() as u64, out);
            out.extend_from_slice(message);
            prev_abs
        }
        StepStreamRecord::Catch { exception_type_id } => {
            out.push(TAG_CATCH);
            encode_varint(*exception_type_id, out);
            prev_abs
        }
        StepStreamRecord::ThreadSwitch { thread_id } => {
            out.push(TAG_THREAD_SWITCH);
            encode_varint(*thread_id, out);
            prev_abs
        }
    }
}

/// Decode a single execution-stream record at `*pos`, carrying the running
/// absolute `global_line_index` `prev_abs` for delta resolution. Returns the
/// decoded record and the updated running absolute value.
pub fn decode_record(data: &[u8], pos: &mut usize, prev_abs: Option<u64>) -> Result<(StepStreamRecord, Option<u64>), String> {
    if *pos >= data.len() {
        return Err("steps.dat: truncated record (no tag)".to_string());
    }
    let tag = data[*pos];
    *pos += 1;
    match tag {
        TAG_ABSOLUTE_STEP => {
            let gli = decode_varint(data, pos)?;
            Ok((StepStreamRecord::Step { global_line_index: gli }, Some(gli)))
        }
        TAG_DELTA_STEP => {
            let prev = prev_abs.ok_or_else(|| "steps.dat: DeltaStep with no preceding AbsoluteStep in chunk".to_string())?;
            let delta = decode_signed_varint(data, pos)?;
            let gli = (prev as i64 + delta) as u64;
            Ok((StepStreamRecord::Step { global_line_index: gli }, Some(gli)))
        }
        TAG_RAISE => {
            let exception_type_id = decode_varint(data, pos)?;
            let msg_len = decode_varint(data, pos)? as usize;
            if *pos + msg_len > data.len() {
                return Err("steps.dat: truncated Raise message".to_string());
            }
            let message = data[*pos..*pos + msg_len].to_vec();
            *pos += msg_len;
            Ok((StepStreamRecord::Raise { exception_type_id, message }, prev_abs))
        }
        TAG_CATCH => {
            let exception_type_id = decode_varint(data, pos)?;
            Ok((StepStreamRecord::Catch { exception_type_id }, prev_abs))
        }
        TAG_THREAD_SWITCH => {
            let thread_id = decode_varint(data, pos)?;
            Ok((StepStreamRecord::ThreadSwitch { thread_id }, prev_abs))
        }
        other => Err(format!("steps.dat: unknown record tag {other}")),
    }
}

/// The encoded `steps.dat` stream plus its companion `steps.idx`.
#[cfg(not(target_arch = "wasm32"))]
pub struct EncodedStepStream {
    /// Concatenated Zstd-compressed chunks, no inline headers.
    pub dat: Vec<u8>,
    /// Companion index: `[chunk_size: u32 LE][offset_0: u64 LE]...`.
    pub idx: Vec<u8>,
    /// Number of execution-stream records encoded.
    pub record_count: usize,
}

/// Encode execution-stream records into `steps.dat` (chunked Zstd) + `steps.idx`
/// (companion offset index), per seekable-zstd.md and trace-events.md
/// §"Chunked Compression".
///
/// Each chunk is independently decodable: the running absolute
/// `global_line_index` resets at every chunk boundary, so the first `Step` in a
/// chunk is always AbsoluteStep (encoding rule 5). `forced_absolute` (one flag
/// per `Step` record, in `Step` order) additionally forces AbsoluteStep for the
/// first step and steps following a Call/Return/ThreadSwitch (rules 1-3).
#[cfg(not(target_arch = "wasm32"))]
pub fn encode_step_stream(stream: &StepStream, chunk_size: usize, zstd_level: i32) -> Result<EncodedStepStream, String> {
    use std::io::Cursor;
    let chunk_size = chunk_size.max(1);
    let records = &stream.records;
    let mut dat: Vec<u8> = Vec::new();
    let mut idx: Vec<u8> = Vec::new();
    idx.extend_from_slice(&(chunk_size as u32).to_le_bytes());

    // Walk forced-absolute flags in `Step` order as we encounter Step records.
    let mut step_index = 0usize;

    let mut i = 0usize;
    while i < records.len() {
        let end = (i + chunk_size).min(records.len());
        // Record the byte offset of this chunk within steps.dat.
        idx.extend_from_slice(&(dat.len() as u64).to_le_bytes());

        let mut raw: Vec<u8> = Vec::new();
        // Running absolute value resets per chunk for independent decode.
        let mut prev_abs: Option<u64> = None;
        for record in &records[i..end] {
            let force_absolute = match record {
                StepStreamRecord::Step { .. } => {
                    let flag = stream.forced_absolute.get(step_index).copied().unwrap_or(true);
                    step_index += 1;
                    // First Step of a chunk is always absolute (prev_abs is None
                    // there anyway); rules 1-3 force it elsewhere via `flag`.
                    flag
                }
                _ => false,
            };
            prev_abs = encode_record(record, prev_abs, force_absolute, &mut raw);
        }
        let compressed = zstd::encode_all(Cursor::new(&raw[..]), zstd_level).map_err(|e| format!("steps.dat: zstd encode failed: {e}"))?;
        dat.extend_from_slice(&compressed);
        i = end;
    }

    Ok(EncodedStepStream {
        dat,
        idx,
        record_count: records.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codetracer_trace_types::{Line, PathId};

    fn step(path_id: usize, line: i64) -> TraceLowLevelEvent {
        TraceLowLevelEvent::Step(StepRecord {
            path_id: PathId(path_id),
            line: Line(line),
        })
    }

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
        for v in [0i64, -1, 1, -64, 63, MAX_DELTA, -MAX_DELTA, i32::MIN as i64, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            encode_signed_varint(v, &mut buf);
            let mut pos = 0;
            assert_eq!(decode_signed_varint(&buf, &mut pos).unwrap(), v);
        }
    }

    #[test]
    fn gli_pack_unpack_roundtrip() {
        for (p, l) in [(0usize, 0i64), (0, 1), (3, 42), (1234, 999999), (u32::MAX as usize, 7)] {
            let gli = pack_global_line_index(p, l);
            assert_eq!(unpack_global_line_index(gli), (p, l));
        }
        // Negative line clamps to 0 (total, never panics).
        assert_eq!(unpack_global_line_index(pack_global_line_index(0, -9)), (0, 0));
    }

    #[test]
    fn record_roundtrip_through_chunk_codec() {
        // Build a stream with a forced-absolute first step, a delta step, an
        // absolute (post-call), a thread switch, and a synthetic Raise/Catch.
        let mut builder = StepStreamBuilder::new();
        builder.observe(&step(2, 10)); // first -> absolute
        builder.observe(&step(2, 11)); // delta +(1<<0)
        builder.observe(&TraceLowLevelEvent::Call(codetracer_trace_types::CallRecord {
            function_id: codetracer_trace_types::FunctionId(0),
            args: vec![],
        }));
        builder.observe(&step(2, 30)); // post-call -> absolute
        builder.observe(&TraceLowLevelEvent::ThreadSwitch(ThreadId(7)));
        builder.observe(&step(2, 31)); // post-thread-switch -> absolute
        let mut stream = builder.finish();
        // Inject a synthetic Raise + Catch (no legacy event emits these) to
        // exercise their tags through the codec.
        stream.records.push(StepStreamRecord::Raise {
            exception_type_id: 5,
            message: b"boom".to_vec(),
        });
        stream.records.push(StepStreamRecord::Catch { exception_type_id: 5 });

        // Encode all in a single chunk, decode forward, compare absolute lines.
        let encoded = encode_step_stream(&stream, 1024, 3).unwrap();
        // Decompress the single chunk.
        let raw = zstd::decode_all(std::io::Cursor::new(&encoded.dat[..])).unwrap();
        let mut pos = 0usize;
        let mut prev_abs: Option<u64> = None;
        let mut decoded = Vec::new();
        while pos < raw.len() {
            let (rec, next) = decode_record(&raw, &mut pos, prev_abs).unwrap();
            prev_abs = next;
            decoded.push(rec);
        }
        assert_eq!(decoded, stream.records);
    }
}
