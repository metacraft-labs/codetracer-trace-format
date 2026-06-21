//! Dedicated value stream (`values.dat`) for materialized CTFS `.ct` traces.
//!
//! This is the M23b deliverable of the Trace-Based-Incremental-Testing
//! campaign (the second sub-milestone of M23 — "finish the trace-events.md
//! Event Stream Redesign"): an *additive*, backward-compatible split of the
//! per-step variable VALUES out of the unified `events.log`. It mirrors the
//! M23a `steps.dat` split and the M17a `calls.dat` split exactly — recorders
//! that opt in emit, in addition to the unchanged `events.log`, a dedicated
//! value stream plus a companion seekable index, gated by a new `meta.dat`
//! capability flag `has_value_stream` (bit 10). Readers that do not know the
//! flag simply ignore the two extra files, so old `.ct`s and old readers keep
//! working byte-for-byte.
//!
//! # File-naming decision: `values.dat` (NOT `steps.dat`)
//!
//! `codetracer-trace-format-spec/trace-events.md` §"Stream Summary" /
//! §"Value Stream Events" *labels* both the Execution stream and the Values
//! stream `steps.dat`. That label is a spec inconsistency: the two streams have
//! fundamentally different record characteristics and Zstd chunk sizing —
//!
//! | Stream    | Record size (spec) | Chunk sizing                       |
//! |-----------|--------------------|------------------------------------|
//! | Execution | 2-4 bytes (tiny)   | large chunks (thousands of steps)  |
//! | Values    | 50-500 bytes       | small chunks (different Zstd tuning)|
//!
//! and the spec itself says (§"Benefits" #5) "value-heavy `steps.dat` gets
//! different Zstd settings" — i.e. the value data is a SEPARATE seekable stream.
//! Two streams cannot share one CTFS file (a CTFS internal file is a single
//! seekable byte range with one companion `.idx`). So M23b implements the value
//! stream as its OWN CTFS file pair **`values.dat` + `values.idx`**, exactly the
//! shape M23a gave `steps.dat`/`steps.idx` and M17a gave `calls.dat`/`calls.idx`.
//!
//! ## Parallel-index invariant (record N ↔ step N)
//!
//! The value stream is **parallel-indexed to the execution stream**: value
//! record `N` holds the variable values visible at step `N` (the N-th `Step`
//! event in `events.log` / the N-th record in `steps.dat`). Steps with no
//! variable activity get an EMPTY record (a single `0x00` byte — `count = 0`,
//! no tagged events). This 1:1 alignment is what lets the reader fetch a step's
//! values by the same integer index it uses for the execution stream, with no
//! separate cross-reference table. The writer guarantees the invariant by
//! attributing every value-stream event that appears in `events.log` to the
//! step that was most recently emitted (value events before the very first
//! `Step` are attributed to step 0).
//!
//! # Per-record wire format
//!
//! Each value record is a sequence of tagged value-stream events, matching
//! `codetracer-trace-format-spec/trace-events.md` §"Value Stream Events
//! (`steps.dat`)":
//!
//! ```text
//!   Tag 0  StepValues         : varint count, then count × (varint name_id,
//!                                varint value_len, value bytes (CBOR ValueRecord))
//!   Tag 1  BindVariable       : varint variable_id, signed varint place
//!   Tag 2  DropVariable       : varint variable_id
//!   Tag 3  DropVariables      : varint count, count × varint variable_id
//!   Tag 4  CellValue          : signed varint place, varint value_len, value bytes (CBOR)
//!   Tag 5  CompoundValue      : signed varint place, varint value_len, value bytes (CBOR)
//!   Tag 6  AssignCell         : signed varint place, varint value_len, value bytes (CBOR)
//!   Tag 7  AssignCompoundItem : signed varint place, varint index, signed varint item_place
//!   Tag 8  VariableCell       : varint variable_id, signed varint place
//!   Tag 9  Assignment         : varint to, u8 pass_by, varint from_len, from bytes (CBOR RValue)
//! ```
//!
//! A record is the concatenation of zero-or-more such tagged events. The record
//! is self-delimiting because the chunk codec length-prefixes each record (see
//! [`encode_value_stream`]); within a record, every event is fully described by
//! its tag + fields, so decoding walks events until the record's byte length is
//! consumed.
//!
//! The `value` / `args` / `from` CBOR payloads use the EXISTING `ValueRecord` /
//! `RValue` CBOR encoding from `codetracer_trace_types` (the same `cbor4ii`
//! encoding the split-binary `events.log` uses), so the value stream is
//! byte-consistent with the value events still present in `events.log`. M23b does
//! NOT redesign `ValueRecord`; it only routes the per-step value events into the
//! parallel stream. (`name_id` is the variable-name interning id; M23b derives it
//! from the writer's variable-name table the same way `events.log` does — see
//! [`ValueStreamBuilder`].)
//!
//! # Storage (`values.dat` + `values.idx`)
//!
//! Records are grouped into chunks of `chunk_size` records, each independently
//! Zstd-compressed, concatenated into `values.dat` with **no inline headers**.
//! The companion `values.idx` follows
//! `codetracer-trace-format-spec/seekable-zstd.md`:
//!
//! ```text
//!   values.dat:  [zstd(chunk_0)][zstd(chunk_1)]...
//!   values.idx:  [chunk_size: u32 LE][offset_0: u64 LE][offset_1: u64 LE]...
//! ```
//!
//! `offset_i` is the byte offset of chunk `i` within `values.dat`. To seek to
//! value record `N`: `chunk = N / chunk_size`, read `offset[chunk]` and
//! `offset[chunk+1]` (or the file size for the last chunk), decompress that one
//! chunk, and index `N % chunk_size` within it — O(1) chunks, no whole-stream
//! decompression. This mirrors `calls.dat`/`steps.dat` exactly.

use codetracer_trace_types::{
    AssignCellRecord, AssignCompoundItemRecord, AssignmentRecord, BindVariableRecord, CellValueRecord, CompoundValueRecord, FullValueRecord, PassBy, RValue,
    TraceLowLevelEvent, ValueRecord, VariableCellRecord, VariableId,
};

/// Default number of value records per chunk. Value records are large (spec
/// §"Stream Summary": 50-500 bytes each), so a SMALLER chunk size than
/// `steps.dat` gives finer seek granularity without inflating per-seek
/// decompression work (seekable-zstd.md §Configuration). This is the
/// "different Zstd settings for value-heavy data" the spec calls out.
pub const DEFAULT_VALUES_CHUNK_SIZE: usize = 256;

// --- value-stream event tags (trace-events.md §"Value Stream Events") ---

/// Tag 0 — StepValues: all variable values visible at the step.
pub const TAG_STEP_VALUES: u8 = 0;
/// Tag 1 — BindVariable: bind a variable to a memory place.
pub const TAG_BIND_VARIABLE: u8 = 1;
/// Tag 2 — DropVariable: drop a single variable.
pub const TAG_DROP_VARIABLE: u8 = 2;
/// Tag 3 — DropVariables: drop multiple variables (end of scope).
pub const TAG_DROP_VARIABLES: u8 = 3;
/// Tag 4 — CellValue: cell value at a place.
pub const TAG_CELL_VALUE: u8 = 4;
/// Tag 5 — CompoundValue: compound value at a place.
pub const TAG_COMPOUND_VALUE: u8 = 5;
/// Tag 6 — AssignCell: assign to a cell.
pub const TAG_ASSIGN_CELL: u8 = 6;
/// Tag 7 — AssignCompoundItem: assign to a compound item.
pub const TAG_ASSIGN_COMPOUND_ITEM: u8 = 7;
/// Tag 8 — VariableCell: associate a variable with a cell.
pub const TAG_VARIABLE_CELL: u8 = 8;
/// Tag 9 — Assignment: variable assignment or parameter passing.
pub const TAG_ASSIGNMENT: u8 = 9;

/// One decoded value-stream event. This is the on-disk projection of the value
/// events that, in the legacy unified stream, appear interleaved between `Step`
/// events. M23b keeps the EXISTING `ValueRecord`/`RValue` CBOR payloads (carried
/// as opaque CBOR bytes here) so the value stream is byte-consistent with
/// `events.log`; a consumer decodes the bytes with the same `cbor4ii` decoder.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueStreamEvent {
    /// All variable values visible at this step: `(name_id, CBOR ValueRecord)`
    /// pairs.
    StepValues { values: Vec<(u64, Vec<u8>)> },
    /// Bind a variable to a memory place.
    BindVariable { variable_id: u64, place: i64 },
    /// Drop a single variable.
    DropVariable { variable_id: u64 },
    /// Drop multiple variables (end of scope).
    DropVariables { variable_ids: Vec<u64> },
    /// Cell value at a place (CBOR `ValueRecord`).
    CellValue { place: i64, value: Vec<u8> },
    /// Compound value at a place (CBOR `ValueRecord`).
    CompoundValue { place: i64, value: Vec<u8> },
    /// Assign to a cell (CBOR `ValueRecord`).
    AssignCell { place: i64, new_value: Vec<u8> },
    /// Assign to a compound item.
    AssignCompoundItem { place: i64, index: u64, item_place: i64 },
    /// Associate a variable with a cell.
    VariableCell { variable_id: u64, place: i64 },
    /// Variable assignment or parameter passing (`from` is a CBOR `RValue`).
    Assignment { to: u64, pass_by: u8, from: Vec<u8> },
}

/// One value record: the (possibly empty) sequence of value-stream events
/// attributed to a single step. Parallel-indexed — record `N` ↔ step `N`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValueRecordEntry {
    /// Value-stream events for this step, in stream order. Empty for a step
    /// with no variable activity.
    pub events: Vec<ValueStreamEvent>,
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
            return Err("values.dat: truncated varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err("values.dat: varint too long".to_string());
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

/// Read a varint-length-prefixed CBOR blob from `data` at `*pos`.
fn decode_blob(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, String> {
    let len = decode_varint(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err("values.dat: truncated value blob".to_string());
    }
    let blob = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(blob)
}

/// Encode a value as CBOR the same way the split-binary event stream does, so
/// `values.dat` payloads are byte-identical to the value-event payloads in
/// `events.log`.
fn cbor_bytes<T: serde::Serialize>(value: &T) -> Vec<u8> {
    cbor4ii::serde::to_vec(Vec::new(), value).expect("CBOR encode failed")
}

impl ValueStreamEvent {
    /// Encode this event (tag + fields) into `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            ValueStreamEvent::StepValues { values } => {
                out.push(TAG_STEP_VALUES);
                encode_varint(values.len() as u64, out);
                for (name_id, value) in values {
                    encode_varint(*name_id, out);
                    encode_varint(value.len() as u64, out);
                    out.extend_from_slice(value);
                }
            }
            ValueStreamEvent::BindVariable { variable_id, place } => {
                out.push(TAG_BIND_VARIABLE);
                encode_varint(*variable_id, out);
                encode_signed_varint(*place, out);
            }
            ValueStreamEvent::DropVariable { variable_id } => {
                out.push(TAG_DROP_VARIABLE);
                encode_varint(*variable_id, out);
            }
            ValueStreamEvent::DropVariables { variable_ids } => {
                out.push(TAG_DROP_VARIABLES);
                encode_varint(variable_ids.len() as u64, out);
                for id in variable_ids {
                    encode_varint(*id, out);
                }
            }
            ValueStreamEvent::CellValue { place, value } => {
                out.push(TAG_CELL_VALUE);
                encode_signed_varint(*place, out);
                encode_varint(value.len() as u64, out);
                out.extend_from_slice(value);
            }
            ValueStreamEvent::CompoundValue { place, value } => {
                out.push(TAG_COMPOUND_VALUE);
                encode_signed_varint(*place, out);
                encode_varint(value.len() as u64, out);
                out.extend_from_slice(value);
            }
            ValueStreamEvent::AssignCell { place, new_value } => {
                out.push(TAG_ASSIGN_CELL);
                encode_signed_varint(*place, out);
                encode_varint(new_value.len() as u64, out);
                out.extend_from_slice(new_value);
            }
            ValueStreamEvent::AssignCompoundItem { place, index, item_place } => {
                out.push(TAG_ASSIGN_COMPOUND_ITEM);
                encode_signed_varint(*place, out);
                encode_varint(*index, out);
                encode_signed_varint(*item_place, out);
            }
            ValueStreamEvent::VariableCell { variable_id, place } => {
                out.push(TAG_VARIABLE_CELL);
                encode_varint(*variable_id, out);
                encode_signed_varint(*place, out);
            }
            ValueStreamEvent::Assignment { to, pass_by, from } => {
                out.push(TAG_ASSIGNMENT);
                encode_varint(*to, out);
                out.push(*pass_by);
                encode_varint(from.len() as u64, out);
                out.extend_from_slice(from);
            }
        }
    }

    /// Decode a single event at `*pos`.
    pub fn decode(data: &[u8], pos: &mut usize) -> Result<ValueStreamEvent, String> {
        if *pos >= data.len() {
            return Err("values.dat: truncated event (no tag)".to_string());
        }
        let tag = data[*pos];
        *pos += 1;
        match tag {
            TAG_STEP_VALUES => {
                let count = decode_varint(data, pos)? as usize;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    let name_id = decode_varint(data, pos)?;
                    let value = decode_blob(data, pos)?;
                    values.push((name_id, value));
                }
                Ok(ValueStreamEvent::StepValues { values })
            }
            TAG_BIND_VARIABLE => {
                let variable_id = decode_varint(data, pos)?;
                let place = decode_signed_varint(data, pos)?;
                Ok(ValueStreamEvent::BindVariable { variable_id, place })
            }
            TAG_DROP_VARIABLE => {
                let variable_id = decode_varint(data, pos)?;
                Ok(ValueStreamEvent::DropVariable { variable_id })
            }
            TAG_DROP_VARIABLES => {
                let count = decode_varint(data, pos)? as usize;
                let mut variable_ids = Vec::with_capacity(count);
                for _ in 0..count {
                    variable_ids.push(decode_varint(data, pos)?);
                }
                Ok(ValueStreamEvent::DropVariables { variable_ids })
            }
            TAG_CELL_VALUE => {
                let place = decode_signed_varint(data, pos)?;
                let value = decode_blob(data, pos)?;
                Ok(ValueStreamEvent::CellValue { place, value })
            }
            TAG_COMPOUND_VALUE => {
                let place = decode_signed_varint(data, pos)?;
                let value = decode_blob(data, pos)?;
                Ok(ValueStreamEvent::CompoundValue { place, value })
            }
            TAG_ASSIGN_CELL => {
                let place = decode_signed_varint(data, pos)?;
                let new_value = decode_blob(data, pos)?;
                Ok(ValueStreamEvent::AssignCell { place, new_value })
            }
            TAG_ASSIGN_COMPOUND_ITEM => {
                let place = decode_signed_varint(data, pos)?;
                let index = decode_varint(data, pos)?;
                let item_place = decode_signed_varint(data, pos)?;
                Ok(ValueStreamEvent::AssignCompoundItem { place, index, item_place })
            }
            TAG_VARIABLE_CELL => {
                let variable_id = decode_varint(data, pos)?;
                let place = decode_signed_varint(data, pos)?;
                Ok(ValueStreamEvent::VariableCell { variable_id, place })
            }
            TAG_ASSIGNMENT => {
                let to = decode_varint(data, pos)?;
                if *pos >= data.len() {
                    return Err("values.dat: truncated Assignment pass_by".to_string());
                }
                let pass_by = data[*pos];
                *pos += 1;
                let from = decode_blob(data, pos)?;
                Ok(ValueStreamEvent::Assignment { to, pass_by, from })
            }
            other => Err(format!("values.dat: unknown value-event tag {other}")),
        }
    }
}

impl ValueRecordEntry {
    /// Decode all events of one value record from its raw bytes.
    pub fn decode(data: &[u8]) -> Result<ValueRecordEntry, String> {
        let mut pos = 0usize;
        let mut events = Vec::new();
        while pos < data.len() {
            events.push(ValueStreamEvent::decode(data, &mut pos)?);
        }
        Ok(ValueRecordEntry { events })
    }

    /// Encode all events of this record into `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        for ev in &self.events {
            ev.encode(out);
        }
    }
}

/// Builds the dedicated value stream from the same event sequence that feeds
/// `events.log`, so the two are guaranteed consistent and parallel-indexed.
///
/// The builder maintains a current step index (number of `Step` events seen so
/// far) and accumulates value-stream events into the record for the current
/// step. The parallel-index invariant is enforced thus:
///   * A `Step` event pushes the in-progress record (the values of the PREVIOUS
///     step) and starts a fresh empty record for the new step.
///   * Value events (`Value`, `BindVariable`, `CellValue`, …) append to the
///     current record. Value events seen before the first `Step` attribute to
///     step 0 (the record that the first `Step` will push).
///
/// At [`finish`](ValueStreamBuilder::finish), the in-progress record for the
/// last step is pushed, so the number of value records EQUALS the number of
/// `Step` events — i.e. record `N` ↔ step `N`.
///
/// `Value(FullValueRecord)` events are folded into the current step's
/// `StepValues` event (one per step) so the on-disk record matches the spec's
/// tag-0 "all variable values visible at this step". The `name_id` is the
/// variable id carried by the `FullValueRecord` (the canonical varname interning
/// is M23c+; for M23b the variable id IS the name reference, exactly as the
/// legacy `events.log` carries it).
pub struct ValueStreamBuilder {
    /// Finalized value records, indexed by step id.
    records: Vec<ValueRecordEntry>,
    /// The record being accumulated for the current step.
    current: ValueRecordEntry,
    /// Whether at least one `Step` has been seen (so the first `Step` does not
    /// push a spurious leading record — value events before the first step
    /// belong to step 0, which is the record the first `Step` pushes).
    seen_step: bool,
}

impl Default for ValueStreamBuilder {
    fn default() -> Self {
        ValueStreamBuilder::new()
    }
}

impl ValueStreamBuilder {
    pub fn new() -> Self {
        ValueStreamBuilder {
            records: Vec::new(),
            current: ValueRecordEntry::default(),
            seen_step: false,
        }
    }

    /// Append `FullValueRecord` values to the current step's `StepValues` event,
    /// creating it on first use so there is at most one `StepValues` per record.
    fn push_step_value(&mut self, fv: &FullValueRecord) {
        let name_id = fv.variable_id.0 as u64;
        let cbor = cbor_bytes(&fv.value);
        // Find or create the (single) StepValues event in the current record.
        if let Some(ValueStreamEvent::StepValues { values }) = self.current.events.iter_mut().find(|e| matches!(e, ValueStreamEvent::StepValues { .. })) {
            values.push((name_id, cbor));
        } else {
            self.current.events.push(ValueStreamEvent::StepValues {
                values: vec![(name_id, cbor)],
            });
        }
    }

    /// Feed one event in stream order.
    pub fn observe(&mut self, event: &TraceLowLevelEvent) {
        match event {
            TraceLowLevelEvent::Step(_) => {
                // Close the record for the previous step (or step 0's pre-step
                // values) and start a fresh one for the step just stepped to.
                if self.seen_step {
                    self.records.push(std::mem::take(&mut self.current));
                } else {
                    // First step: the in-progress record holds any pre-step
                    // values (attributed to step 0). Do NOT push it yet — the
                    // NEXT Step (or finish) closes step 0's record. We still need
                    // to start step 0 with whatever pre-step values accumulated,
                    // which is exactly `self.current`, so leave it in place.
                    self.seen_step = true;
                }
            }
            TraceLowLevelEvent::Value(fv) => self.push_step_value(fv),
            TraceLowLevelEvent::BindVariable(BindVariableRecord { variable_id, place }) => {
                self.current.events.push(ValueStreamEvent::BindVariable {
                    variable_id: variable_id.0 as u64,
                    place: place.0,
                });
            }
            TraceLowLevelEvent::DropVariable(VariableId(id)) => {
                self.current.events.push(ValueStreamEvent::DropVariable { variable_id: *id as u64 });
            }
            TraceLowLevelEvent::DropVariables(ids) => {
                self.current.events.push(ValueStreamEvent::DropVariables {
                    variable_ids: ids.iter().map(|v| v.0 as u64).collect(),
                });
            }
            TraceLowLevelEvent::CellValue(CellValueRecord { place, value }) => {
                self.current.events.push(ValueStreamEvent::CellValue {
                    place: place.0,
                    value: cbor_bytes(value),
                });
            }
            TraceLowLevelEvent::CompoundValue(CompoundValueRecord { place, value }) => {
                self.current.events.push(ValueStreamEvent::CompoundValue {
                    place: place.0,
                    value: cbor_bytes(value),
                });
            }
            TraceLowLevelEvent::AssignCell(AssignCellRecord { place, new_value }) => {
                self.current.events.push(ValueStreamEvent::AssignCell {
                    place: place.0,
                    new_value: cbor_bytes(new_value),
                });
            }
            TraceLowLevelEvent::AssignCompoundItem(AssignCompoundItemRecord { place, index, item_place }) => {
                self.current.events.push(ValueStreamEvent::AssignCompoundItem {
                    place: place.0,
                    index: *index as u64,
                    item_place: item_place.0,
                });
            }
            TraceLowLevelEvent::VariableCell(VariableCellRecord { variable_id, place }) => {
                self.current.events.push(ValueStreamEvent::VariableCell {
                    variable_id: variable_id.0 as u64,
                    place: place.0,
                });
            }
            TraceLowLevelEvent::Assignment(AssignmentRecord { to, pass_by, from }) => {
                self.current.events.push(ValueStreamEvent::Assignment {
                    to: to.0 as u64,
                    pass_by: pass_by_ord(pass_by),
                    from: cbor_bytes(from),
                });
            }
            _ => {}
        }
    }

    /// Number of value records built so far (excludes the in-progress record).
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.current.events.is_empty() && !self.seen_step
    }

    /// Finalize: push the in-progress record for the last step (so the record
    /// count equals the `Step` count) and return the records in step order.
    ///
    /// A trace with zero `Step` events yields zero value records (even if
    /// pre-step value events accumulated — there is no step to attach them to).
    pub fn finish(mut self) -> Vec<ValueRecordEntry> {
        if self.seen_step {
            self.records.push(self.current);
        }
        self.records
    }
}

/// Map a [`PassBy`] to its stable on-disk ordinal (matches the spec's `pass_by:
/// u8` field). The mapping is fixed so the byte is portable across readers.
fn pass_by_ord(pass_by: &PassBy) -> u8 {
    match pass_by {
        PassBy::Value => 0,
        PassBy::Reference => 1,
    }
}

/// Decode a CBOR `ValueRecord` blob (helper for tests / consumers that want the
/// typed value back). Uses the same `cbor4ii` decoder the event stream uses.
pub fn decode_value_record(blob: &[u8]) -> Result<ValueRecord, String> {
    cbor4ii::serde::from_slice(blob).map_err(|e| format!("values.dat: CBOR ValueRecord decode failed: {e}"))
}

/// Decode a CBOR `RValue` blob (helper for tests / consumers).
pub fn decode_rvalue(blob: &[u8]) -> Result<RValue, String> {
    cbor4ii::serde::from_slice(blob).map_err(|e| format!("values.dat: CBOR RValue decode failed: {e}"))
}

/// The encoded `values.dat` stream plus its companion `values.idx`.
#[cfg(not(target_arch = "wasm32"))]
pub struct EncodedValueStream {
    /// Concatenated Zstd-compressed chunks, no inline headers.
    pub dat: Vec<u8>,
    /// Companion index: `[chunk_size: u32 LE][offset_0: u64 LE]...`.
    pub idx: Vec<u8>,
    /// Number of value records encoded (== number of steps).
    pub record_count: usize,
}

/// Encode value records into `values.dat` (chunked Zstd) + `values.idx`
/// (companion offset index), per seekable-zstd.md.
///
/// Each record is length-prefixed within its chunk so the reader can walk to the
/// `N % chunk_size`-th record without re-deriving sizes (records are variable
/// length). Each chunk is independently Zstd-compressed.
#[cfg(not(target_arch = "wasm32"))]
pub fn encode_value_stream(records: &[ValueRecordEntry], chunk_size: usize, zstd_level: i32) -> Result<EncodedValueStream, String> {
    use std::io::Cursor;
    let chunk_size = chunk_size.max(1);
    let mut dat: Vec<u8> = Vec::new();
    let mut idx: Vec<u8> = Vec::new();
    idx.extend_from_slice(&(chunk_size as u32).to_le_bytes());

    let mut i = 0usize;
    while i < records.len() {
        let end = (i + chunk_size).min(records.len());
        // Record the byte offset of this chunk within values.dat.
        idx.extend_from_slice(&(dat.len() as u64).to_le_bytes());

        let mut raw: Vec<u8> = Vec::new();
        for rec in &records[i..end] {
            let mut rec_bytes: Vec<u8> = Vec::new();
            rec.encode(&mut rec_bytes);
            // Length-prefix each record so the reader can index within a chunk.
            encode_varint(rec_bytes.len() as u64, &mut raw);
            raw.extend_from_slice(&rec_bytes);
        }
        let compressed = zstd::encode_all(Cursor::new(&raw[..]), zstd_level).map_err(|e| format!("values.dat: zstd encode failed: {e}"))?;
        dat.extend_from_slice(&compressed);
        i = end;
    }

    Ok(EncodedValueStream {
        dat,
        idx,
        record_count: records.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codetracer_trace_types::{Place, TypeId, ValueRecord};

    fn int_value(i: i64) -> ValueRecord {
        ValueRecord::Int { i, type_id: TypeId(1) }
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
        for v in [0i64, -1, 1, -64, 63, i32::MIN as i64, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            encode_signed_varint(v, &mut buf);
            let mut pos = 0;
            assert_eq!(decode_signed_varint(&buf, &mut pos).unwrap(), v);
        }
    }

    #[test]
    fn every_event_variant_roundtrips() {
        let events = vec![
            ValueStreamEvent::StepValues {
                values: vec![(0, cbor_bytes(&int_value(1))), (2, cbor_bytes(&int_value(99)))],
            },
            ValueStreamEvent::BindVariable { variable_id: 3, place: -7 },
            ValueStreamEvent::DropVariable { variable_id: 4 },
            ValueStreamEvent::DropVariables { variable_ids: vec![1, 2, 3] },
            ValueStreamEvent::CellValue { place: 5, value: cbor_bytes(&int_value(2)) },
            ValueStreamEvent::CompoundValue { place: 6, value: cbor_bytes(&int_value(3)) },
            ValueStreamEvent::AssignCell { place: -1, new_value: cbor_bytes(&int_value(4)) },
            ValueStreamEvent::AssignCompoundItem { place: 0, index: 7, item_place: 9 },
            ValueStreamEvent::VariableCell { variable_id: 8, place: 100 },
            ValueStreamEvent::Assignment { to: 1, pass_by: 1, from: cbor_bytes(&RValue::Simple(VariableId(2))) },
        ];
        let rec = ValueRecordEntry { events: events.clone() };
        let mut buf = Vec::new();
        rec.encode(&mut buf);
        let decoded = ValueRecordEntry::decode(&buf).unwrap();
        assert_eq!(decoded.events, events);

        // The CBOR payloads decode back to the original typed values.
        if let ValueStreamEvent::CellValue { value, .. } = &decoded.events[4] {
            assert_eq!(decode_value_record(value).unwrap(), int_value(2));
        } else {
            panic!("expected CellValue");
        }
    }

    #[test]
    fn empty_record_is_one_zero_count_byte_after_step_values_absent() {
        // An empty record encodes to zero bytes (no events). Through the chunk
        // codec it is length-prefixed with a single 0x00, matching the spec's
        // "empty record" for a value-less step.
        let rec = ValueRecordEntry::default();
        let mut buf = Vec::new();
        rec.encode(&mut buf);
        assert!(buf.is_empty());
        let decoded = ValueRecordEntry::decode(&buf).unwrap();
        assert_eq!(decoded, rec);
    }

    #[test]
    fn builder_parallel_index_invariant() {
        use codetracer_trace_types::{Line, PathId, StepRecord};
        let step = |l: i64| TraceLowLevelEvent::Step(StepRecord { path_id: PathId(0), line: Line(l) });
        let value = |id: usize, v: i64| {
            TraceLowLevelEvent::Value(FullValueRecord {
                variable_id: VariableId(id),
                value: int_value(v),
            })
        };

        let mut b = ValueStreamBuilder::new();
        // Pre-step value attributes to step 0.
        b.observe(&value(0, 10));
        b.observe(&step(1)); // step 0
        b.observe(&value(1, 20)); // step 0's value
        b.observe(&step(2)); // step 1 — no values
        b.observe(&step(3)); // step 2
        b.observe(&CellValueEvent(Place(5), int_value(30)));
        let records = b.finish();

        assert_eq!(records.len(), 3, "record count must equal step count");
        // Step 0: pre-step value(0,10) + value(1,20) folded into one StepValues.
        match &records[0].events[0] {
            ValueStreamEvent::StepValues { values } => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0].0, 0);
                assert_eq!(values[1].0, 1);
            }
            other => panic!("expected StepValues, got {other:?}"),
        }
        // Step 1: empty.
        assert!(records[1].events.is_empty());
        // Step 2: a CellValue.
        assert!(matches!(records[2].events[0], ValueStreamEvent::CellValue { place: 5, .. }));
    }

    /// Helper so the builder test above can emit a CellValue event tersely.
    #[allow(non_snake_case)]
    fn CellValueEvent(place: Place, value: ValueRecord) -> TraceLowLevelEvent {
        TraceLowLevelEvent::CellValue(CellValueRecord { place, value })
    }
}
