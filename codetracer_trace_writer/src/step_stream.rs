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
//! A step's address is the spec's per-file contiguous range: the file's base in
//! the trace's global position space plus the line's 0-based in-file offset. The
//! arithmetic, its inverse and the reason there is no second scheme all live in
//! [`crate::line_position`]; this module holds only the wire encoding around it.
//!
//! [`StepStreamBuilder`] carries the trace's [`LinePositionSpace`] and addresses
//! each `Step{path_id, line}` event through it, so the execution stream and
//! anything else that addresses a source line — `funcs.dat`, `linehits.tc`, a
//! reader resolving a step back to a location — agree by construction rather
//! than by two implementations happening to match.
//!
//! # Encoding rule (spec §"Encoding Rules")
//!
//! The first position record of every chunk is an AbsoluteStep; every other
//! one is a DeltaStep exactly when its delta's varint is strictly shorter than
//! the position's, and an AbsoluteStep otherwise. Calls, returns and thread
//! switches force nothing. The rule lives in [`crate::step_rule`] and the
//! chunking in [`crate::column_aware::ExecStreamEncoder`], which both step
//! paths share; see [`encode_step_stream`].
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

use codetracer_trace_types::{ThreadId, TraceLowLevelEvent};

use crate::line_position::LinePositionSpace;

/// Default number of step records per chunk. Step records are tiny (2-4 bytes,
/// spec §"Stream Summary"), so a larger chunk size than `calls.dat` keeps the
/// per-chunk overhead amortised while chunks still hold thousands of steps
/// (seekable-zstd.md §Configuration).
pub const DEFAULT_STEPS_CHUNK_SIZE: usize = 4096;

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
/// Tag 5 — ThreadStart. Emitted by the canonical Nim writer; decoded here so a
/// Nim-written `steps.dat` does not fail this reader on an "unknown tag".
pub const TAG_THREAD_START: u8 = 5;
/// Tag 6 — ThreadExit. See [`TAG_THREAD_START`].
pub const TAG_THREAD_EXIT: u8 = 6;
/// Tag 7 — DeltaColumn: column-only motion inside the current line.
///
/// Legal only in a trace whose `meta.dat` carries
/// [`crate::meta_dat::FLAG_HAS_COLUMN_AWARE_STEPS`]. Written by
/// [`crate::column_aware`]; decoded here so this reader can consume a
/// column-aware stream from either writer.
pub const TAG_DELTA_COLUMN: u8 = 7;
/// Tag 8 — SourceReload: one or more source files were reloaded as new path
/// versions (`trace-events.md` §"Source Reload Marker (Tag 0x08)").
///
/// Legal only in a trace whose `meta.dat` declares
/// [`crate::meta_dat::FLAG_EXT_HAS_SOURCE_RELOAD`]; a decoder refuses it by
/// name otherwise, because skipping it would re-read its payload as records.
pub const TAG_SOURCE_RELOAD: u8 = 8;

/// One file's transition across a source reload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceReloadChange {
    /// The `paths.dat` id the file's steps resolved to before the reload.
    pub old_path_id: u64,
    /// The id they resolve to after. Distinct from `old_path_id`.
    pub new_path_id: u64,
    /// The observer's generation: 2 for the first reload of a file.
    pub generation: u64,
}

/// One decoded execution-stream record. This is the on-disk projection of the
/// compact step encoding; a [`StepStreamRecord::Step`] carries the recovered
/// `global_line_index`, which [`LinePositionSpace::resolve`] turns back into the
/// `(path_id, line)` the `events.log` step held.
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
    /// A thread started (tag 5). Produced by the canonical Nim writer.
    ThreadStart { thread_id: u64 },
    /// A thread exited (tag 6). Produced by the canonical Nim writer.
    ThreadExit { thread_id: u64 },
    /// Column-only motion inside the current line (tag 7).
    ///
    /// The record carries the *resolved* absolute position, like
    /// [`StepStreamRecord::Step`], because `global_position_index` is
    /// one-dimensional: a column move is a position move. `column_delta` is
    /// retained so a consumer can tell a column-only step from a line step
    /// without re-deriving it from the line table.
    DeltaColumn { global_position_index: u64, column_delta: i64 },
    /// A source reload marker (tag 8). Not a position: it does not move the
    /// running cursor and resolves to no source location.
    SourceReload {
        reload_ordinal: u64,
        changed: Vec<SourceReloadChange>,
        in_flight_frames: u64,
    },
}

/// Append a `SourceReload` record's payload (without the tag).
pub fn encode_source_reload_payload(reload_ordinal: u64, changed: &[SourceReloadChange], in_flight_frames: u64, out: &mut Vec<u8>) {
    encode_varint(reload_ordinal, out);
    encode_varint(changed.len() as u64, out);
    for ch in changed {
        encode_varint(ch.old_path_id, out);
        encode_varint(ch.new_path_id, out);
        encode_varint(ch.generation, out);
    }
    encode_varint(in_flight_frames, out);
}

/// Decode a `SourceReload` record's payload (the tag already consumed).
pub fn decode_source_reload_payload(data: &[u8], pos: &mut usize) -> Result<(u64, Vec<SourceReloadChange>, u64), String> {
    let reload_ordinal = decode_varint(data, pos)?;
    let count = decode_varint(data, pos)?;
    // A count read off the wire is bounded by the bytes that remain before
    // anything is allocated: the smallest change is three one-byte varints.
    let left = (data.len() - *pos) as u64;
    if count > left / 3 {
        return Err(format!(
            "steps.dat: source reload marker claims {count} changed files, more than the remaining {left} bytes can hold"
        ));
    }
    let mut changed = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let old_path_id = decode_varint(data, pos)?;
        let new_path_id = decode_varint(data, pos)?;
        let generation = decode_varint(data, pos)?;
        changed.push(SourceReloadChange {
            old_path_id,
            new_path_id,
            generation,
        });
    }
    let in_flight_frames = decode_varint(data, pos)?;
    Ok((reload_ordinal, changed, in_flight_frames))
}

/// The refusal for a delta record that comes before its chunk's first
/// `AbsoluteStep`. A caller that knows the chunk's number prefixes it.
fn unanchored_delta_error(tag: &str) -> String {
    format!(
        "steps.dat: a {tag} comes before the chunk's first AbsoluteStep, so it has nothing to be relative to; \
         every chunk's first position record must be absolute (trace-events.md \"Encoding Rules\")"
    )
}

/// The refusal for tag 8 in a container that does not declare it.
pub fn undeclared_source_reload_error() -> String {
    "steps.dat: record tag 8 (0x08, SourceReload) is present but meta.dat does not declare \
     FLAG_EXT_HAS_SOURCE_RELOAD (extended flag bit 0, schema version 5); the record cannot be \
     skipped because its length is only known by decoding it"
        .to_string()
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

#[cfg(test)]
fn encode_signed_varint(value: i64, out: &mut Vec<u8>) {
    // zigzag: (n << 1) ^ (n >> 63)
    let zz = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(zz, out);
}

/// A varint. One byte, most fields of a record, is read inline; longer
/// ones, and every refusal, by [`decode_varint_long`].
#[inline]
fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    if let Some(&byte) = data.get(*pos)
        && byte < 0x80
    {
        *pos += 1;
        return Ok(byte as u64);
    }
    decode_varint_long(data, pos)
}

fn decode_varint_long(data: &[u8], pos: &mut usize) -> Result<u64, String> {
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

/// A finalized execution stream: its records in stream order.
pub struct StepStream {
    /// All execution-stream records in order.
    pub records: Vec<StepStreamRecord>,
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
/// Only the events that belong to the execution stream are observed: `Path`
/// (⇒ a file joins the position space), `Step` (⇒ a step record carrying that
/// step's address) and the thread events (⇒ thread records).
/// Raise/Catch have no representation in the legacy `TraceLowLevelEvent` enum,
/// so no observed event produces them; a recorder writes them through
/// [`Self::push_raise`] / [`Self::push_catch`].
#[derive(Default)]
pub struct StepStreamBuilder {
    /// Records in stream order not yet handed out by [`Self::drain`].
    records: Vec<StepStreamRecord>,
    /// Records built so far, drained or not: the next record's exec index.
    total: u64,
    /// The trace's address space, grown as `Path` events intern files. A reader
    /// rebuilds the same space from `paths.dat` and inverts every address with
    /// it — see [`crate::line_position`].
    space: LinePositionSpace,
    /// The size the NEXT `Path` event's file is given, when the trace records
    /// real line counts (`meta.dat` bit 14). Consumed by that event.
    next_path_line_count: Option<u64>,
}

impl StepStreamBuilder {
    pub fn new() -> Self {
        StepStreamBuilder {
            records: Vec::new(),
            total: 0,
            space: LinePositionSpace::new(),
            next_path_line_count: None,
        }
    }

    /// Size the file the next `Path` event registers at `line_count`
    /// addresses instead of the conventional stride. Used by a trace that
    /// records per-file line counts (`meta.dat` bit 14).
    pub fn set_next_path_line_count(&mut self, line_count: u64) {
        self.next_path_line_count = Some(line_count);
    }

    /// Append a `SourceReload` record at the current point of the stream. It
    /// is not a position, so the running cursor is left where it was.
    pub fn push_source_reload(&mut self, reload_ordinal: u64, changed: Vec<SourceReloadChange>, in_flight_frames: u64) {
        self.push(StepStreamRecord::SourceReload {
            reload_ordinal,
            changed,
            in_flight_frames,
        });
    }

    /// Append a `Raise` record (tag 2) at the current point of the stream. It
    /// is not a position, so the running cursor is left where it was.
    pub fn push_raise(&mut self, exception_type_id: u64, message: Vec<u8>) {
        self.push(StepStreamRecord::Raise { exception_type_id, message });
    }

    /// Append a `Catch` record (tag 3) at the current point of the stream. It
    /// is not a position, so the running cursor is left where it was.
    pub fn push_catch(&mut self, exception_type_id: u64) {
        self.push(StepStreamRecord::Catch { exception_type_id });
    }

    fn push(&mut self, record: StepStreamRecord) {
        self.records.push(record);
        self.total += 1;
    }

    /// Hand out the records built since the last drain, in stream order.
    pub fn drain(&mut self) -> Vec<StepStreamRecord> {
        std::mem::take(&mut self.records)
    }

    /// The address space this builder has addressed its steps in. A file joins
    /// it when its `Path` event is observed, and appending never moves an
    /// earlier file's addresses.
    pub fn position_space(&self) -> &LinePositionSpace {
        &self.space
    }

    /// Feed one event in stream order.
    pub fn observe(&mut self, event: &TraceLowLevelEvent) {
        match event {
            TraceLowLevelEvent::Path(_) => {
                // Paths are interned in event order, so the id this event
                // assigns is the number of paths already seen.
                match self.next_path_line_count.take() {
                    Some(count) => {
                        self.space.push_file(count);
                    }
                    None => {
                        let next_id = self.space.file_count();
                        self.space.ensure_file(next_id);
                    }
                }
            }
            TraceLowLevelEvent::Step(step) => {
                let global_line_index = self.space.global_index(step.path_id.0, step.line.0);
                self.push(StepStreamRecord::Step { global_line_index });
            }
            TraceLowLevelEvent::ThreadSwitch(ThreadId(tid)) => {
                self.push(StepStreamRecord::ThreadSwitch { thread_id: *tid });
            }
            // Thread start/exit are exec records too (tags 5 and 6); the
            // canonical writer emits them, and a writer that dropped them
            // would shift every later exec-record index.
            TraceLowLevelEvent::ThreadStart(ThreadId(tid)) => {
                self.push(StepStreamRecord::ThreadStart { thread_id: *tid });
            }
            TraceLowLevelEvent::ThreadExit(ThreadId(tid)) => {
                self.push(StepStreamRecord::ThreadExit { thread_id: *tid });
            }
            _ => {}
        }
    }

    /// Number of records built so far, drained or not.
    pub fn len(&self) -> usize {
        self.total as usize
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Finalize and return the records not yet drained.
    pub fn finish(self) -> StepStream {
        StepStream { records: self.records }
    }
}

/// Decode a single execution-stream record at `*pos`, carrying the running
/// absolute `global_line_index` `prev_abs` for delta resolution. Returns the
/// decoded record and the updated running absolute value.
///
/// Refuses tag 8 (`SourceReload`); a caller that has read the container's
/// `meta.dat` uses [`decode_record_declared`] with what it declares.
pub fn decode_record(data: &[u8], pos: &mut usize, prev_abs: Option<u64>) -> Result<(StepStreamRecord, Option<u64>), String> {
    decode_record_declared(data, pos, prev_abs, false)
}

/// [`decode_record`], accepting tag 8 exactly when `allow_source_reload` —
/// which a caller sets from the container's
/// [`crate::meta_dat::FLAG_EXT_HAS_SOURCE_RELOAD`].
pub fn decode_record_declared(
    data: &[u8],
    pos: &mut usize,
    prev_abs: Option<u64>,
    allow_source_reload: bool,
) -> Result<(StepStreamRecord, Option<u64>), String> {
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
            // A delta before the chunk's first AbsoluteStep has nothing to be
            // relative to (`trace-events.md` §"Encoding Rules", "Reading").
            let prev = prev_abs.ok_or_else(|| unanchored_delta_error("DeltaStep"))?;
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
        TAG_THREAD_START => {
            let thread_id = decode_varint(data, pos)?;
            Ok((StepStreamRecord::ThreadStart { thread_id }, prev_abs))
        }
        TAG_THREAD_EXIT => {
            let thread_id = decode_varint(data, pos)?;
            Ok((StepStreamRecord::ThreadExit { thread_id }, prev_abs))
        }
        TAG_DELTA_COLUMN => {
            // Unlike Raise/Catch/ThreadSwitch, this one MOVES the cursor: the
            // position space is one-dimensional, so a column delta is a
            // position delta. Treating it as position-neutral would desync
            // every subsequent DeltaStep in the chunk.
            let prev = prev_abs.ok_or_else(|| unanchored_delta_error("DeltaColumn"))?;
            let column_delta = decode_signed_varint(data, pos)?;
            let gpi = (prev as i64 + column_delta) as u64;
            Ok((
                StepStreamRecord::DeltaColumn {
                    global_position_index: gpi,
                    column_delta,
                },
                Some(gpi),
            ))
        }
        TAG_SOURCE_RELOAD => {
            if !allow_source_reload {
                return Err(undeclared_source_reload_error());
            }
            let (reload_ordinal, changed, in_flight_frames) = decode_source_reload_payload(data, pos)?;
            Ok((
                StepStreamRecord::SourceReload {
                    reload_ordinal,
                    changed,
                    in_flight_frames,
                },
                prev_abs,
            ))
        }
        other => Err(format!("steps.dat: unknown record tag {other}")),
    }
}

/// The encoded `steps.dat` stream plus its companion `steps.idx`.
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
/// The records go through [`crate::column_aware::ExecStreamEncoder`], the
/// chunk encoder the column-aware path streams into, so both paths apply one
/// encoding rule ([`crate::step_rule`]).
pub fn encode_step_stream(stream: &StepStream, chunk_size: usize, zstd_level: i32) -> Result<EncodedStepStream, String> {
    let mut enc = crate::column_aware::ExecStreamEncoder::new(chunk_size, zstd_level);
    for record in &stream.records {
        write_record(&mut enc, record)?;
    }
    let encoded = enc.finish()?;
    Ok(EncodedStepStream {
        dat: encoded.dat,
        idx: encoded.idx,
        record_count: stream.records.len(),
    })
}

/// Write one execution-stream record into `enc`, whose chunk encoder chooses
/// each position's form by [`crate::step_rule`].
pub fn write_record(enc: &mut crate::column_aware::ExecStreamEncoder, record: &StepStreamRecord) -> Result<(), String> {
    use crate::column_aware::StepEvent;
    match record {
        StepStreamRecord::Step { global_line_index } => enc.write_position(*global_line_index, false),
        StepStreamRecord::DeltaColumn { global_position_index, .. } => enc.write_position(*global_position_index, true),
        StepStreamRecord::Raise { exception_type_id, message } => enc.write_event(StepEvent::Raise {
            exception_type_id: *exception_type_id,
            message: message.clone(),
        }),
        StepStreamRecord::Catch { exception_type_id } => enc.write_event(StepEvent::Catch {
            exception_type_id: *exception_type_id,
        }),
        StepStreamRecord::ThreadSwitch { thread_id } => enc.write_event(StepEvent::ThreadSwitch { thread_id: *thread_id }),
        StepStreamRecord::ThreadStart { thread_id } => enc.write_event(StepEvent::ThreadStart { thread_id: *thread_id }),
        StepStreamRecord::ThreadExit { thread_id } => enc.write_event(StepEvent::ThreadExit { thread_id: *thread_id }),
        StepStreamRecord::SourceReload {
            reload_ordinal,
            changed,
            in_flight_frames,
        } => enc.write_event(StepEvent::SourceReload {
            reload_ordinal: *reload_ordinal,
            changed: changed.clone(),
            in_flight_frames: *in_flight_frames,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codetracer_trace_types::{Line, PathId, StepRecord};

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
        for v in [0i64, -1, 1, -64, 63, 1_048_575, -1_048_575, i32::MIN as i64, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            encode_signed_varint(v, &mut buf);
            let mut pos = 0;
            assert_eq!(decode_signed_varint(&buf, &mut pos).unwrap(), v);
        }
    }

    /// The builder addresses a step through the trace's own position space, and
    /// the space inverts it back to the step's location. Two files, because one
    /// file cannot show how the addresses are apportioned.
    #[test]
    fn a_steps_address_resolves_to_the_step() {
        let mut builder = StepStreamBuilder::new();
        builder.observe(&TraceLowLevelEvent::Path("/a.rs".into()));
        builder.observe(&TraceLowLevelEvent::Path("/b.rs".into()));
        let recorded = [(0usize, 1i64), (0, 42), (1, 1), (1, 999)];
        for (path_id, line) in recorded {
            builder.observe(&step(path_id, line));
        }

        let space = builder.position_space().clone();
        assert_eq!(space.file_count(), 2, "both Path events joined the space");
        let stream = builder.finish();
        for (record, (path_id, line)) in stream.records.iter().zip(recorded) {
            let StepStreamRecord::Step { global_line_index } = record else {
                panic!("expected a Step record, got {record:?}");
            };
            assert_eq!(
                space.resolve(*global_line_index),
                Ok((path_id, line)),
                "address {global_line_index} was built for (path {path_id}, line {line})"
            );
        }
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
        let raw = codetracer_ctfs::zstd_compat::decode_all(&encoded.dat).unwrap();
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
