//! Column-aware step encoding — a direct port of the canonical Nim writer.
//!
//! # Why this module exists as a port rather than a re-derivation
//!
//! The Rust `CtfsTraceWriter` and the Nim `MultiStreamTraceWriter` both produce
//! CTFS `.ct` containers, and the Nim one is the reference implementation. For
//! the column extension the requirement is not "spec-conformant" but *the same
//! bytes*: a `steps.dat` / `paths.dat` written here must be indistinguishable
//! from one the Nim writer would have produced for the same logical input.
//! Everything in this module is therefore a line-for-line port of a named Nim
//! procedure, and the differential test
//! `codetracer_trace_writer_nim/tests/writer_differential.rs` compares the two
//! byte-for-byte on every commit.
//!
//! | this module | Nim original |
//! |---|---|
//! | [`StepEvent`] / [`encode_step_event`] / [`decode_step_event`] | `codetracer_trace_writer/step_encoding.nim` |
//! | [`ExecStreamEncoder`] | `codetracer_trace_writer/exec_stream.nim` (`writeEvent` / `flushChunk` / `flush`) |
//! | [`PositionSpace`] | `multi_stream_writer.nim` (`rebuildGli` / `toGlobalLineIndex`) + `global_line_index.nim` |
//! | [`encode_path_record_layout_a`] | `interning_table.nim` (`ensurePathIdColumnAware`) |
//! | [`StepEncoder`] | `multi_stream_writer.nim` (`registerStep` / `registerStepWithColumn` / `registerColumnStep`) |
//!
//! # The two addressing modes, and why the legacy one is left alone
//!
//! Both writers address a step through one varint. What that varint *means*
//! depends on a trace-global flag:
//!
//! * **Line-only** (`FLAG_HAS_COLUMN_AWARE_STEPS` clear). Each integer addresses
//!   one line. The two writers **disagree** here and always have: this crate's
//!   [`crate::step_stream::pack_global_line_index`] uses `(path_id << 32) | line`
//!   while the Nim writer uses `path_id * DefaultLinesPerFile + line` with
//!   `DefaultLinesPerFile == 100_000`. That divergence is *not* repaired here.
//!   `pack_global_line_index` / `unpack_global_line_index` are a published API
//!   with consumers outside this repository — `codetracer/src/db-backend`'s
//!   `linehits_namespace`, `recreator_session`, `materialization_cache`,
//!   `follow_stream_source` and `step_value_stream_source` all round-trip
//!   through them — so changing it would invalidate every container those
//!   readers already hold, to no benefit for column support.
//!
//! * **Line + column** (`FLAG_HAS_COLUMN_AWARE_STEPS` set). Each integer
//!   addresses one `(line, column)` pair, laid out as the prefix sum of the
//!   per-file `line_lengths` tables. Here the two writers agree exactly,
//!   because this module reproduces the Nim allocation including its fallback
//!   for files whose `line_lengths` were not supplied.
//!
//! The Nim writer makes the same split for the same reason (see
//! `multi_stream_writer.nim` `rebuildGli`: *"In line-only mode every file gets
//! the legacy `DefaultLinesPerFile` allocation, preserving byte-for-byte output
//! of pre-P6 traces"*). Parity is therefore *additive*: a line-only trace's
//! bytes do not move, and a column-aware trace matches Nim.
//!
//! # Spec
//!
//! `codetracer-trace-format-spec/trace-events.md` §"Source Location
//! Addressing", §"Column Encoding — `DeltaColumn` (chosen)" and §"paths.dat
//! per-line offset table". **Those sections are not present at every revision
//! of the spec repository** — they landed in `spec: document column-aware
//! navigation campaign deliverables`. A checkout that predates it describes no
//! column format at all.

// --- varint helpers (unsigned LEB128 + zigzag signed) ------------------------
//
// Byte-for-byte the Nim `varint.nim` procedures. Kept local rather than shared
// with `step_stream.rs` so a change to one encoder cannot silently move the
// other's bytes.

/// Append `value` to `out` as an unsigned LEB128 varint.
/// Port of Nim `varint.nim` `encodeVarint`.
pub fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
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

/// Append `value` to `out` as a zigzag-encoded signed LEB128 varint.
/// Port of Nim `varint.nim` `encodeSignedVarint`.
pub fn encode_signed_varint(value: i64, out: &mut Vec<u8>) {
    // Nim spells this as a branch rather than the usual `(n << 1) ^ (n >> 63)`;
    // the two agree on every input, including `i64::MIN`, but the branch is
    // kept so a reader comparing the two files sees the same shape.
    let zigzag: u64 = if value >= 0 { (value as u64) << 1 } else { (((!value) as u64) << 1) | 1 };
    encode_varint(zigzag, out);
}

/// Decode an unsigned LEB128 varint at `*pos`, advancing `pos`.
pub fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err("varint: unexpected end of input".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint: too many bytes (>10)".to_string());
        }
    }
}

/// Decode a zigzag-encoded signed varint at `*pos`, advancing `pos`.
pub fn decode_signed_varint(data: &[u8], pos: &mut usize) -> Result<i64, String> {
    let v = decode_varint(data, pos)?;
    if v & 1 == 0 { Ok((v >> 1) as i64) } else { Ok(!((v >> 1) as i64)) }
}

// --- step event tags ---------------------------------------------------------
//
// Port of the `Tag*` constants in Nim `step_encoding.nim`. Tags 0x00-0x06 were
// already allocated when the column extension landed; `DeltaColumn` took 0x07,
// which is what makes the extension additive on the wire (spec §"Column
// Encoding — `DeltaColumn` (chosen)", "Tag allocation").

/// Tag 0x00 — `AbsoluteStep`: a full `global_position_index`.
pub const TAG_ABSOLUTE_STEP: u8 = 0x00;
/// Tag 0x01 — `DeltaStep`: signed delta over `global_position_index`.
pub const TAG_DELTA_STEP: u8 = 0x01;
/// Tag 0x02 — `Raise`: an exception was raised, before unwinding.
pub const TAG_RAISE: u8 = 0x02;
/// Tag 0x03 — `Catch`: an exception was caught by a handler.
pub const TAG_CATCH: u8 = 0x03;
/// Tag 0x04 — `ThreadSwitch`: execution moved to another thread.
pub const TAG_THREAD_SWITCH: u8 = 0x04;
/// Tag 0x05 — `ThreadStart`.
pub const TAG_THREAD_START: u8 = 0x05;
/// Tag 0x06 — `ThreadExit`.
pub const TAG_THREAD_EXIT: u8 = 0x06;
/// Tag 0x07 — `DeltaColumn`: column-only motion inside the current line.
///
/// Legal on the wire **only** when the trace's `meta.dat`
/// `FLAG_HAS_COLUMN_AWARE_STEPS` (bit 4) is set. A column-unaware reader is
/// required by the spec to reject such a trace at metadata-parse time via the
/// reserved-bits rule, rather than misdecode this tag.
pub const TAG_DELTA_COLUMN: u8 = 0x07;

/// One execution-stream event, in the shape the Nim writer buffers it —
/// *before* chunk-boundary promotion. Port of Nim `step_encoding.nim`
/// `StepEvent`.
///
/// The distinction from [`crate::step_stream::StepStreamRecord`] matters: that
/// type is the *decoded* projection, where every step carries a resolved
/// absolute position. This one is the *encoder's* view, where a step may still
/// be a delta whose meaning depends on the running cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepEvent {
    /// A step at an absolute `global_position_index`.
    AbsoluteStep { global_position_index: u64 },
    /// A step at a signed delta from the previous step's position.
    DeltaStep { delta: i64 },
    /// An exception was raised.
    Raise { exception_type_id: u64, message: Vec<u8> },
    /// An exception was caught.
    Catch { exception_type_id: u64 },
    /// Execution switched to another thread.
    ThreadSwitch { thread_id: u64 },
    /// A thread started.
    ThreadStart { thread_id: u64 },
    /// A thread exited.
    ThreadExit { thread_id: u64 },
    /// Column-only motion inside the current line (tag 0x07).
    DeltaColumn { column_delta: i64 },
}

/// Encode one step event. Port of Nim `step_encoding.nim` `encodeStepEvent`.
pub fn encode_step_event(event: &StepEvent, out: &mut Vec<u8>) {
    match event {
        StepEvent::AbsoluteStep { global_position_index } => {
            out.push(TAG_ABSOLUTE_STEP);
            encode_varint(*global_position_index, out);
        }
        StepEvent::DeltaStep { delta } => {
            out.push(TAG_DELTA_STEP);
            encode_signed_varint(*delta, out);
        }
        StepEvent::Raise { exception_type_id, message } => {
            out.push(TAG_RAISE);
            encode_varint(*exception_type_id, out);
            encode_varint(message.len() as u64, out);
            out.extend_from_slice(message);
        }
        StepEvent::Catch { exception_type_id } => {
            out.push(TAG_CATCH);
            encode_varint(*exception_type_id, out);
        }
        StepEvent::ThreadSwitch { thread_id } => {
            out.push(TAG_THREAD_SWITCH);
            encode_varint(*thread_id, out);
        }
        StepEvent::ThreadStart { thread_id } => {
            out.push(TAG_THREAD_START);
            encode_varint(*thread_id, out);
        }
        StepEvent::ThreadExit { thread_id } => {
            out.push(TAG_THREAD_EXIT);
            encode_varint(*thread_id, out);
        }
        StepEvent::DeltaColumn { column_delta } => {
            out.push(TAG_DELTA_COLUMN);
            encode_signed_varint(*column_delta, out);
        }
    }
}

/// Decode one step event at `*pos`. Port of Nim `step_encoding.nim`
/// `decodeStepEvent`.
pub fn decode_step_event(data: &[u8], pos: &mut usize) -> Result<StepEvent, String> {
    if *pos >= data.len() {
        return Err("unexpected end of step stream".to_string());
    }
    let tag = data[*pos];
    *pos += 1;
    match tag {
        TAG_ABSOLUTE_STEP => Ok(StepEvent::AbsoluteStep {
            global_position_index: decode_varint(data, pos)?,
        }),
        TAG_DELTA_STEP => Ok(StepEvent::DeltaStep {
            delta: decode_signed_varint(data, pos)?,
        }),
        TAG_RAISE => {
            let exception_type_id = decode_varint(data, pos)?;
            let msg_len = decode_varint(data, pos)? as usize;
            if *pos + msg_len > data.len() {
                return Err("raise message truncated".to_string());
            }
            let message = data[*pos..*pos + msg_len].to_vec();
            *pos += msg_len;
            Ok(StepEvent::Raise { exception_type_id, message })
        }
        TAG_CATCH => Ok(StepEvent::Catch {
            exception_type_id: decode_varint(data, pos)?,
        }),
        TAG_THREAD_SWITCH => Ok(StepEvent::ThreadSwitch {
            thread_id: decode_varint(data, pos)?,
        }),
        TAG_THREAD_START => Ok(StepEvent::ThreadStart {
            thread_id: decode_varint(data, pos)?,
        }),
        TAG_THREAD_EXIT => Ok(StepEvent::ThreadExit {
            thread_id: decode_varint(data, pos)?,
        }),
        TAG_DELTA_COLUMN => Ok(StepEvent::DeltaColumn {
            column_delta: decode_signed_varint(data, pos)?,
        }),
        other => Err(format!("unknown step event tag: {other}")),
    }
}

// --- the per-file position space --------------------------------------------

/// Nim `multi_stream_writer.nim` `DefaultLinesPerFile`. Every file that has no
/// per-line table is allocated this many addresses, in both modes.
pub const DEFAULT_LINES_PER_FILE: u64 = 100_000;

/// The trace's global position space: one contiguous, gap-free range per
/// registered file, in file-id order.
///
/// Port of Nim `multi_stream_writer.nim` `rebuildGli` + `toGlobalLineIndex`
/// and `global_line_index.nim` `buildGlobalLineIndex`.
///
/// A file's slot size is:
///
/// * `max(sum(line_lengths[f]), 1)` when the trace is column-aware **and** that
///   file has a non-empty `line_lengths` table, or
/// * [`DEFAULT_LINES_PER_FILE`] otherwise — which includes a column-aware trace
///   whose recorder did not surface per-line counts for *that* file. The mixed
///   case is real and the Nim writer handles it this way; reproducing the
///   fallback is what makes a partially-populated trace match.
#[derive(Debug, Clone, Default)]
pub struct PositionSpace {
    /// Per-file addressable column counts. Empty entry ⇒ no per-line data.
    line_lengths: Vec<Vec<u32>>,
    /// Whether the trace is column-aware. Decides the slot sizing above.
    column_aware: bool,
    /// Exclusive prefix sum of the per-file slot sizes: the first address of
    /// each file. Recomputed lazily whenever a path is added.
    prefix_sum: Vec<u64>,
    /// Set when `prefix_sum` needs rebuilding.
    dirty: bool,
}

impl PositionSpace {
    /// An empty space. `column_aware` is trace-global and must be decided
    /// before any path is registered, exactly as on the Nim side.
    pub fn new(column_aware: bool) -> Self {
        PositionSpace {
            line_lengths: Vec::new(),
            column_aware,
            prefix_sum: Vec::new(),
            dirty: true,
        }
    }

    /// Whether this space addresses `(line, column)` pairs.
    pub fn is_column_aware(&self) -> bool {
        self.column_aware
    }

    /// Register the next path's per-line table. Paths must be registered in
    /// interning-id order; the returned id is that order.
    ///
    /// `line_lengths` is ignored (stored empty) when the space is not
    /// column-aware — the Nim writer does the same, so a line-only trace's
    /// addresses do not move if a recorder happens to pass a table.
    pub fn push_path(&mut self, line_lengths: &[u32]) -> u64 {
        let id = self.line_lengths.len() as u64;
        if self.column_aware {
            self.line_lengths.push(line_lengths.to_vec());
        } else {
            self.line_lengths.push(Vec::new());
        }
        self.dirty = true;
        id
    }

    /// Number of registered paths.
    pub fn path_count(&self) -> usize {
        self.line_lengths.len()
    }

    /// The per-file tables, in id order — what a reader's
    /// `GlobalPositionDecoder::from_line_lengths` needs.
    pub fn line_lengths(&self) -> &[Vec<u32>] {
        &self.line_lengths
    }

    fn rebuild(&mut self) {
        let mut prefix = Vec::with_capacity(self.line_lengths.len());
        let mut running: u64 = 0;
        for lls in &self.line_lengths {
            prefix.push(running);
            let slot = if self.column_aware && !lls.is_empty() {
                let total: u64 = lls.iter().map(|l| u64::from(*l)).sum();
                total.max(1)
            } else {
                DEFAULT_LINES_PER_FILE
            };
            running = running.saturating_add(slot);
        }
        self.prefix_sum = prefix;
        self.dirty = false;
    }

    /// The `global_position_index` of column 1 on `line` in `path_id`.
    ///
    /// Port of Nim `toGlobalLineIndex`. `line` is 1-based. In column-aware mode
    /// this is `file_base + sum(line_lengths[0 .. line-2])`, clamped to the
    /// file's known line count; in line-only mode it is the legacy
    /// `file_base + line`.
    ///
    /// An unregistered `path_id` yields the legacy form against a zero base,
    /// which is what the Nim writer's bounds test degenerates to.
    pub fn position_of(&mut self, path_id: u64, line: u64) -> u64 {
        if self.dirty {
            self.rebuild();
        }
        let idx = path_id as usize;
        let base = self.prefix_sum.get(idx).copied().unwrap_or(0);
        if self.column_aware {
            if let Some(lls) = self.line_lengths.get(idx) {
                if !lls.is_empty() {
                    // `line` is 1-based, so line 1 sits at offset 0. Nim clamps
                    // `upTo` to the known line count and lets the reader's
                    // decoder handle a past-end address the same way.
                    let up_to = ((line.max(1) - 1) as usize).min(lls.len());
                    let line_offset: u64 = lls[..up_to].iter().map(|l| u64::from(*l)).sum();
                    return base + line_offset;
                }
            }
        }
        base + line
    }
}

// --- paths.dat Layout A ------------------------------------------------------

/// Encode one column-aware `paths.dat` record (spec Layout A).
///
/// Port of Nim `interning_table.nim` `ensurePathIdColumnAware`:
///
/// ```text
///   path_len:     varint
///   path_bytes:   [u8] × path_len
///   line_count:   varint
///   line_lengths: [zigzag varint] × line_count
///                 (entry 0 absolute; entry i a delta from entry i-1)
/// ```
///
/// An empty `line_lengths` still emits the `path_len` prefix and
/// `line_count = 0`, so the reader demarcates the path bytes uniformly whether
/// or not the recorder had per-line data. This is *not* the line-only record
/// shape — that one is the bare path bytes, with the length recovered from
/// `paths.off`.
pub fn encode_path_record_layout_a(path: &str, line_lengths: &[u32]) -> Vec<u8> {
    let mut record = Vec::with_capacity(path.len() + 8 + line_lengths.len());
    encode_varint(path.len() as u64, &mut record);
    record.extend_from_slice(path.as_bytes());
    encode_varint(line_lengths.len() as u64, &mut record);
    if !line_lengths.is_empty() {
        encode_signed_varint(i64::from(line_lengths[0]), &mut record);
        for i in 1..line_lengths.len() {
            let delta = i64::from(line_lengths[i]) - i64::from(line_lengths[i - 1]);
            encode_signed_varint(delta, &mut record);
        }
    }
    record
}

/// Decode a Layout A `paths.dat` record back to `(path, line_lengths)`.
/// The inverse of [`encode_path_record_layout_a`]; used by the differential
/// test and by any reader that wants the table without the Nim runtime.
pub fn decode_path_record_layout_a(record: &[u8]) -> Result<(String, Vec<u32>), String> {
    let mut pos = 0usize;
    let path_len = decode_varint(record, &mut pos)? as usize;
    if pos + path_len > record.len() {
        return Err("paths.dat: Layout A path bytes truncated".to_string());
    }
    let path = String::from_utf8(record[pos..pos + path_len].to_vec()).map_err(|e| format!("paths.dat: path is not UTF-8: {e}"))?;
    pos += path_len;
    let line_count = decode_varint(record, &mut pos)? as usize;
    let mut line_lengths = Vec::with_capacity(line_count);
    let mut previous: i64 = 0;
    for i in 0..line_count {
        let v = decode_signed_varint(record, &mut pos)?;
        let value = if i == 0 { v } else { previous + v };
        if value < 0 {
            return Err(format!("paths.dat: Layout A line length {i} decoded negative ({value})"));
        }
        line_lengths.push(value as u32);
        previous = value;
    }
    Ok((path, line_lengths))
}

// --- the execution-stream encoder -------------------------------------------

/// Nim `exec_stream.nim` `DefaultExecChunkSize`.
pub const DEFAULT_EXEC_CHUNK_SIZE: usize = 4096;
/// Nim `exec_stream.nim` `ExecCompressionLevel`.
pub const EXEC_COMPRESSION_LEVEL: i32 = 3;

/// An encoded `steps.dat` + `steps.idx` pair.
pub struct EncodedExecStream {
    /// `steps.dat`: concatenated Zstd frames, one per chunk, no inline headers.
    pub dat: Vec<u8>,
    /// `steps.idx`: `[chunk_size: u32 LE][offset_0: u64 LE]…`.
    pub idx: Vec<u8>,
    /// Number of events written.
    pub total_events: u64,
}

/// Streaming encoder for the execution stream, byte-compatible with the Nim
/// writer. Port of Nim `exec_stream.nim` `ExecStreamWriter`.
///
/// Two behaviours here are load-bearing for byte identity and are easy to get
/// wrong by re-deriving from the spec:
///
/// 1. **Chunk-boundary promotion happens at write time, not at flush time.**
///    A `DeltaStep` or `DeltaColumn` that lands first in a chunk is rewritten
///    to an `AbsoluteStep` carrying `running + delta`, so every chunk decodes
///    independently. The running cursor is *not* reset at the boundary — it
///    carries across, which is what makes the promoted absolute correct.
/// 2. **The chunk payload is compressed one-shot.** Nim calls `ZSTD_compress`,
///    which pledges the frame's content size in its header.
///    `zstd::encode_all` — the streaming call — does not, and produces
///    different bytes for the same input (measured: 104 vs 105 bytes on a
///    200-record chunk, `get_frame_content_size` `None` vs `Some(400)`). That
///    is not only a byte difference: the Nim reader's
///    `decodeSpecChunkRecordCount` and `chunkSlot` both *fail* on
///    `ZSTD_CONTENTSIZE_UNKNOWN`, so a stream compressed the streaming way is
///    unreadable by the reference reader. Use [`compress_chunk`].
pub struct ExecStreamEncoder {
    chunk_size: usize,
    zstd_level: i32,
    buffer: Vec<u8>,
    event_count: usize,
    total_events: u64,
    dat: Vec<u8>,
    idx: Vec<u8>,
    data_offset: u64,
    /// Running absolute position — Nim's `lastGlobalLineIndex`.
    last_position: u64,
}

/// Compress one chunk payload the way the Nim writer does.
///
/// Delegates to [`codetracer_ctfs::compress_pledged`], which is the single
/// place this workspace calls `ZSTD_compress` — `steps.dat` is one of *five*
/// stream families the reference reader refuses without a pledged content
/// size, and fixing them one at a time is how the other four stayed broken
/// after this one was fixed. See [`ExecStreamEncoder`] for the consequence.
pub fn compress_chunk(raw: &[u8], zstd_level: i32) -> Result<Vec<u8>, String> {
    codetracer_ctfs::compress_pledged(raw, zstd_level, "steps.dat")
}

impl ExecStreamEncoder {
    /// A new encoder. `chunk_size` is clamped to at least 1, as Nim rejects 0.
    pub fn new(chunk_size: usize, zstd_level: i32) -> Self {
        let chunk_size = chunk_size.max(1);
        let mut idx = Vec::new();
        // Nim `initExecStreamWriter`: the index header is the u32 chunk_size
        // and nothing else — no `total_events` placeholder, no trailer.
        idx.extend_from_slice(&(chunk_size as u32).to_le_bytes());
        ExecStreamEncoder {
            chunk_size,
            zstd_level,
            buffer: Vec::new(),
            event_count: 0,
            total_events: 0,
            dat: Vec::new(),
            idx,
            data_offset: 0,
            last_position: 0,
        }
    }

    /// Events written so far.
    pub fn total_events(&self) -> u64 {
        self.total_events
    }

    /// The running absolute position after the last event.
    pub fn last_position(&self) -> u64 {
        self.last_position
    }

    /// Write one event. Port of Nim `writeEvent`.
    pub fn write_event(&mut self, event: StepEvent) -> Result<(), String> {
        let mut ev = event;

        // At the start of a chunk, force an absolute so the chunk stands alone.
        if self.event_count == 0 {
            ev = match ev {
                StepEvent::DeltaStep { delta } => StepEvent::AbsoluteStep {
                    global_position_index: (self.last_position as i64).wrapping_add(delta) as u64,
                },
                StepEvent::DeltaColumn { column_delta } => StepEvent::AbsoluteStep {
                    global_position_index: (self.last_position as i64).wrapping_add(column_delta) as u64,
                },
                other => other,
            };
        }

        match &ev {
            StepEvent::AbsoluteStep { global_position_index } => self.last_position = *global_position_index,
            StepEvent::DeltaStep { delta } => self.last_position = (self.last_position as i64).wrapping_add(*delta) as u64,
            StepEvent::DeltaColumn { column_delta } => self.last_position = (self.last_position as i64).wrapping_add(*column_delta) as u64,
            _ => {}
        }

        encode_step_event(&ev, &mut self.buffer);
        self.event_count += 1;
        self.total_events += 1;

        if self.event_count >= self.chunk_size {
            self.flush_chunk()?;
        }
        Ok(())
    }

    /// Compress and emit the buffered chunk. Port of Nim `flushChunk`.
    fn flush_chunk(&mut self) -> Result<(), String> {
        if self.event_count == 0 {
            return Ok(());
        }
        let compressed = compress_chunk(&self.buffer, self.zstd_level)?;
        self.idx.extend_from_slice(&self.data_offset.to_le_bytes());
        self.dat.extend_from_slice(&compressed);
        self.data_offset += compressed.len() as u64;
        self.event_count = 0;
        self.buffer.clear();
        Ok(())
    }

    /// Flush the trailing partial chunk and return the two files.
    /// Port of Nim `flush`.
    pub fn finish(mut self) -> Result<EncodedExecStream, String> {
        self.flush_chunk()?;
        Ok(EncodedExecStream {
            dat: self.dat,
            idx: self.idx,
            total_events: self.total_events,
        })
    }
}

// --- the step encoder (delta-vs-absolute policy) -----------------------------

/// Nim's delta window. `registerStep` emits a `DeltaStep` only when the signed
/// position delta lies in `-64 ..= 63` — one zigzag varint byte — and an
/// `AbsoluteStep` otherwise.
///
/// This is **narrower than** [`crate::step_stream::MAX_DELTA`] (±1_048_575),
/// which the line-only Rust encoder uses. The two policies produce different
/// bytes for the same steps, so the column-aware path uses Nim's.
pub const NIM_DELTA_MIN: i64 = -64;
/// Upper end of Nim's delta window. See [`NIM_DELTA_MIN`].
pub const NIM_DELTA_MAX: i64 = 63;

/// Turns `(path_id, line[, column_delta])` calls into the [`StepEvent`]
/// sequence the Nim writer would buffer.
///
/// Port of Nim `multi_stream_writer.nim` `registerStep`,
/// `registerStepWithColumn` and `registerColumnStep`. It owns the running
/// cursor and the delta-vs-absolute decision; [`ExecStreamEncoder`] owns
/// framing and chunk-boundary promotion. Keeping the two separate mirrors the
/// Nim split and is why promotion cannot double-count: the encoder's promotion
/// preserves the absolute value this type computed.
#[derive(Debug, Clone, Default)]
pub struct StepEncoder {
    step_count: u64,
    last_position: u64,
}

impl StepEncoder {
    pub fn new() -> Self {
        StepEncoder {
            step_count: 0,
            last_position: 0,
        }
    }

    /// Steps emitted so far.
    pub fn step_count(&self) -> u64 {
        self.step_count
    }

    /// The event for a step at `position`, with `column_delta` folded in.
    ///
    /// `column_delta` is the offset from column 1 of the requested line, i.e.
    /// `column - 1`. Passing 0 reproduces `registerStep` exactly — Nim's
    /// `registerStepWithColumn` docs make the same guarantee.
    pub fn step_at(&mut self, position: u64, column_delta: i64) -> StepEvent {
        let combined = (position as i64).wrapping_add(column_delta) as u64;
        let event = if self.step_count == 0 {
            // Rule 1: the first step in a trace is always absolute.
            StepEvent::AbsoluteStep {
                global_position_index: combined,
            }
        } else {
            let delta = (combined as i64).wrapping_sub(self.last_position as i64);
            if (NIM_DELTA_MIN..=NIM_DELTA_MAX).contains(&delta) {
                StepEvent::DeltaStep { delta }
            } else {
                StepEvent::AbsoluteStep {
                    global_position_index: combined,
                }
            }
        };
        self.last_position = combined;
        self.step_count += 1;
        event
    }

    /// Account for an execution-stream record that occupies a step slot but
    /// carries no position — `ThreadSwitch`, `ThreadStart`, `ThreadExit`.
    ///
    /// The Nim writer increments `stepCount` for each of these (they also each
    /// write an empty value record, so `values.dat` stays parallel), and
    /// `stepCount` is what decides "the first step in a trace is always
    /// absolute". A trace that opens with a thread switch therefore encodes its
    /// FIRST real step as a `DeltaStep` from position 0, not as an
    /// `AbsoluteStep` — surprising, and exactly the sort of detail a
    /// re-derivation from the spec gets wrong. The cursor itself does not move.
    pub fn note_non_step_event(&mut self) {
        self.step_count += 1;
    }

    /// A column-only step. Port of `registerColumnStep`.
    ///
    /// Refuses to be the first step: the running cursor must be defined before
    /// a column delta can be applied, and Nim returns the same error.
    pub fn column_step(&mut self, column_delta: i64) -> Result<StepEvent, String> {
        if self.step_count == 0 {
            return Err(
                "registerColumnStep cannot be the first step — emit an AbsoluteStep (registerStep) first so the cursor position is defined"
                    .to_string(),
            );
        }
        self.last_position = (self.last_position as i64).wrapping_add(column_delta) as u64;
        self.step_count += 1;
        Ok(StepEvent::DeltaColumn { column_delta })
    }
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
    fn signed_varint_matches_the_zigzag_identity() {
        // Nim spells the zigzag as a branch; assert it agrees with the usual
        // shift form on every interesting input, i64::MIN included.
        for v in [0i64, -1, 1, -64, 63, -65, 64, i32::MIN as i64, i64::MAX, i64::MIN] {
            let mut branch = Vec::new();
            encode_signed_varint(v, &mut branch);
            let mut shift = Vec::new();
            encode_varint(((v << 1) ^ (v >> 63)) as u64, &mut shift);
            assert_eq!(branch, shift, "zigzag forms disagree for {v}");
            let mut pos = 0;
            assert_eq!(decode_signed_varint(&branch, &mut pos).unwrap(), v);
        }
    }

    #[test]
    fn every_tag_roundtrips_including_delta_column() {
        let events = vec![
            StepEvent::AbsoluteStep {
                global_position_index: 123456,
            },
            StepEvent::DeltaStep { delta: -17 },
            StepEvent::Raise {
                exception_type_id: 5,
                message: b"boom".to_vec(),
            },
            StepEvent::Catch { exception_type_id: 5 },
            StepEvent::ThreadSwitch { thread_id: 7 },
            StepEvent::ThreadStart { thread_id: 8 },
            StepEvent::ThreadExit { thread_id: 8 },
            StepEvent::DeltaColumn { column_delta: 3 },
            StepEvent::DeltaColumn { column_delta: -3 },
        ];
        let mut buf = Vec::new();
        for e in &events {
            encode_step_event(e, &mut buf);
        }
        let mut pos = 0usize;
        let mut decoded = Vec::new();
        while pos < buf.len() {
            decoded.push(decode_step_event(&buf, &mut pos).unwrap());
        }
        assert_eq!(decoded, events);
    }

    #[test]
    fn delta_column_is_tag_seven_and_two_bytes_for_small_deltas() {
        // Spec §"Column Encoding": "Total: 2 bytes typical (1 tag + 1 varint
        // for column delta ±63)". Pin both the tag and the size.
        let mut buf = Vec::new();
        encode_step_event(&StepEvent::DeltaColumn { column_delta: 1 }, &mut buf);
        assert_eq!(buf[0], 0x07);
        assert_eq!(buf.len(), 2, "a ±1 column delta must cost two bytes");
        let mut wide = Vec::new();
        encode_step_event(&StepEvent::DeltaColumn { column_delta: 63 }, &mut wide);
        assert_eq!(wide.len(), 2, "a +63 column delta must still cost two bytes");
    }

    #[test]
    fn position_space_column_aware_matches_the_prefix_sum_layout() {
        let mut space = PositionSpace::new(true);
        space.push_path(&[8]); // file 0: one line, 8 columns
        space.push_path(&[5, 12, 3]); // file 1: 20 columns total
        space.push_path(&[4]); // file 2
        // file_base: 0, 8, 28
        assert_eq!(space.position_of(0, 1), 0);
        assert_eq!(space.position_of(1, 1), 8);
        assert_eq!(space.position_of(1, 2), 8 + 5);
        assert_eq!(space.position_of(1, 3), 8 + 5 + 12);
        assert_eq!(space.position_of(2, 1), 8 + 20);
        // A line past the end clamps to the file's capacity rather than
        // running into the next file's range.
        assert_eq!(space.position_of(1, 99), 8 + 20);
    }

    #[test]
    fn position_space_falls_back_per_file_when_a_table_is_missing() {
        // The mixed case: a column-aware trace where one file has no per-line
        // data. Nim gives that file DEFAULT_LINES_PER_FILE and addresses it the
        // legacy way; anything else would silently shift every later file.
        let mut space = PositionSpace::new(true);
        space.push_path(&[8]);
        space.push_path(&[]); // no table
        space.push_path(&[4]);
        assert_eq!(space.position_of(0, 1), 0);
        assert_eq!(space.position_of(1, 7), 8 + 7);
        assert_eq!(space.position_of(2, 1), 8 + DEFAULT_LINES_PER_FILE);
    }

    #[test]
    fn position_space_line_only_is_the_legacy_allocation() {
        let mut space = PositionSpace::new(false);
        space.push_path(&[8]);
        space.push_path(&[5, 12, 3]);
        // line_lengths are ignored entirely when the trace is line-only.
        assert_eq!(space.position_of(0, 42), 42);
        assert_eq!(space.position_of(1, 42), DEFAULT_LINES_PER_FILE + 42);
    }

    #[test]
    fn layout_a_record_roundtrips_and_delta_encodes() {
        let (path, lls) = ("/src/main.rs", vec![20u32, 22, 21, 80, 4]);
        let rec = encode_path_record_layout_a(path, &lls);
        // path_len(1) + 12 path bytes + line_count(1) + the zigzag varints:
        // 20 -> 40 (1 byte), then deltas +2 -> 4, -1 -> 1, +59 -> 118 (1 byte
        // each) and -76 -> 151, which needs 2 bytes. Six bytes, not five —
        // spelled out because "one byte per line" is the tempting wrong answer
        // and the delta encoding is the whole point of the layout.
        assert_eq!(rec.len(), 1 + 12 + 1 + 6);
        let (back, back_lls) = decode_path_record_layout_a(&rec).unwrap();
        assert_eq!(back, path);
        assert_eq!(back_lls, lls);
    }

    #[test]
    fn layout_a_with_no_line_data_still_frames_the_path() {
        let rec = encode_path_record_layout_a("/a", &[]);
        assert_eq!(rec, vec![2u8, b'/', b'a', 0u8]);
        let (path, lls) = decode_path_record_layout_a(&rec).unwrap();
        assert_eq!(path, "/a");
        assert!(lls.is_empty());
    }

    #[test]
    fn step_encoder_uses_nims_narrow_delta_window() {
        let mut enc = StepEncoder::new();
        assert_eq!(enc.step_at(1000, 0), StepEvent::AbsoluteStep { global_position_index: 1000 });
        // +63 is inside the window.
        assert_eq!(enc.step_at(1063, 0), StepEvent::DeltaStep { delta: 63 });
        // +64 is outside it — the wider MAX_DELTA policy would have emitted a
        // DeltaStep here, which is exactly the byte divergence this pins.
        assert_eq!(enc.step_at(1127, 0), StepEvent::AbsoluteStep { global_position_index: 1127 });
        // -64 is inside.
        assert_eq!(enc.step_at(1063, 0), StepEvent::DeltaStep { delta: -64 });
    }

    #[test]
    fn column_step_refuses_to_open_a_trace() {
        let mut enc = StepEncoder::new();
        let err = enc.column_step(1).expect_err("a column step cannot be first");
        assert!(err.contains("cannot be the first step"), "{err}");
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    #[test]
    fn chunk_boundary_promotes_a_delta_to_an_absolute() {
        // Two events per chunk. The third event opens chunk 1 and must be
        // promoted, or chunk 1 cannot be decoded on its own.
        let mut enc = ExecStreamEncoder::new(2, EXEC_COMPRESSION_LEVEL);
        enc.write_event(StepEvent::AbsoluteStep { global_position_index: 100 }).unwrap();
        enc.write_event(StepEvent::DeltaStep { delta: 1 }).unwrap();
        enc.write_event(StepEvent::DeltaStep { delta: 1 }).unwrap();
        enc.write_event(StepEvent::DeltaColumn { column_delta: 1 }).unwrap();
        let out = enc.finish().unwrap();
        assert_eq!(out.total_events, 4);

        // idx: chunk_size + two chunk offsets.
        assert_eq!(out.idx.len(), 4 + 8 * 2);
        assert_eq!(u32::from_le_bytes(out.idx[0..4].try_into().unwrap()), 2);
        let off0 = u64::from_le_bytes(out.idx[4..12].try_into().unwrap());
        let off1 = u64::from_le_bytes(out.idx[12..20].try_into().unwrap());
        assert_eq!(off0, 0);
        assert!(off1 > 0 && (off1 as usize) < out.dat.len());

        // Chunk 1 decodes standalone and starts absolute at 102.
        let raw1 = zstd::decode_all(&out.dat[off1 as usize..]).unwrap();
        let mut pos = 0usize;
        assert_eq!(
            decode_step_event(&raw1, &mut pos).unwrap(),
            StepEvent::AbsoluteStep { global_position_index: 102 }
        );
        assert_eq!(decode_step_event(&raw1, &mut pos).unwrap(), StepEvent::DeltaColumn { column_delta: 1 });
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    #[test]
    fn chunks_pledge_their_content_size() {
        // The Nim reader's `decodeSpecChunkRecordCount` and `chunkSlot` both
        // FAIL on ZSTD_CONTENTSIZE_UNKNOWN, so an unpledged frame is not a
        // cosmetic byte difference — it is an unreadable stream. Measured:
        // `zstd::encode_all` produces `None` here.
        let mut enc = ExecStreamEncoder::new(4096, EXEC_COMPRESSION_LEVEL);
        for i in 0..50u64 {
            enc.write_event(StepEvent::AbsoluteStep {
                global_position_index: i * 3,
            })
            .unwrap();
        }
        let out = enc.finish().unwrap();
        let pledged = zstd::zstd_safe::get_frame_content_size(&out.dat).expect("a well-formed zstd frame");
        assert!(pledged.is_some(), "the chunk frame must pledge its content size");

        let mut streaming = Vec::new();
        let mut raw = Vec::new();
        for i in 0..50u64 {
            encode_step_event(
                &StepEvent::AbsoluteStep {
                    global_position_index: i * 3,
                },
                &mut raw,
            );
        }
        streaming.extend_from_slice(&zstd::encode_all(std::io::Cursor::new(&raw[..]), EXEC_COMPRESSION_LEVEL).unwrap());
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&streaming).unwrap(),
            None,
            "control: the streaming API is the one that omits the pledge"
        );
        assert_ne!(out.dat, streaming, "control: the two APIs really do differ in bytes");
    }
}
