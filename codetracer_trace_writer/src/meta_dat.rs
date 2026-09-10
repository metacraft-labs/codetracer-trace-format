//! Minimal binary `meta.dat` writer + flag reader for the Rust CTFS writer.
//!
//! The canonical, full-featured `meta.dat` writer/reader lives in
//! `codetracer-trace-format-nim` (`meta_dat.nim`) and the db-backend
//! (`ctfs_trace_reader/meta_dat.rs`). This module is the small slice the Rust
//! `CtfsTraceWriter` needs for M17a: emit a spec-shaped `meta.dat` header so the
//! new `has_call_stream` capability flag (bit 8) can be carried in the canonical
//! place, and read that flag back in the Rust reader.
//!
//! Layout (version 4), per
//! `codetracer-trace-format-spec/internal-files.md` §"Metadata (meta.dat)":
//!
//! ```text
//!   [4] magic "CTMD"
//!   [2] version u16 LE (4)
//!   [2] flags   u16 LE
//!   varint-prefixed recording_id (UUIDv7, 36-char canonical form)
//!   varint-prefixed program
//!   varint args_count, then varint-prefixed arg strings
//!   varint-prefixed workdir
//!   varint-prefixed recorder_id
//!   varint paths_count, then varint-prefixed path strings
//! ```
//!
//! The optional extended blocks (MCR / replay-launch / layout / filter
//! provenance) are not emitted by the Rust writer — their flag bits stay clear.
//!
//! # Version history
//!
//! * **v3** — added the required `recording_id` UUIDv7 ahead of `program`, and
//!   the trace-filter provenance flag bit.
//! * **v4** — the line-only `global_position_index` encode became
//!   `prefix_sum[file_id] + (line - 1)`, the exact inverse of the decode the
//!   spec states, where it had been `prefix_sum[file_id] + line`. Spec:
//!   `codetracer-trace-format-spec/internal-files.md` §"Global Line Index".
//!   The version had to move because the two encodes are INDISTINGUISHABLE in
//!   the bytes: both address a line the trace's own space can hold, so a
//!   container written under the old one reads back under the new one as a
//!   `(path, line)` pair for every step, each exactly one line high, with
//!   nothing to refuse. (The retired `(path_id << 32) | line` packing is
//!   catchable only because it lands OUTSIDE the space — see
//!   [`crate::line_position`].) `recorder_id` names the producer, not its
//!   address packing, and the same recorders span the change, so the schema
//!   version is the only field that tells the two apart. Pre-1.0 there is no
//!   shim, and one is not merely unimplemented: subtracting one from every
//!   address would correct a trace the old writer produced, but the version is
//!   what would have said it did.

/// `meta.dat` magic bytes ("CTMD").
pub const META_DAT_MAGIC: [u8; 4] = [0x43, 0x54, 0x4D, 0x44];
/// Current `meta.dat` version.
///
/// Both sides of this module move together with it: [`encode_meta_dat`] stamps
/// it, and [`read_meta_dat_flags`] — the reader every `meta_dat_has_*` helper
/// and [`decode_meta_dat`] go through — accepts only it. A container from the
/// canonical Nim writer and one from [`crate::CtfsTraceWriter`] are the same
/// wire format, so the two implementations must carry the same number or each
/// refuses the other's traces; see `LastShiftedGlobalIndexVersion` in
/// `codetracer-trace-format-nim/src/codetracer_trace_writer/meta_dat.nim`.
pub const META_DAT_VERSION: u16 = 4;
/// The highest schema version whose writer packed a line-only
/// `global_position_index` as `prefix_sum[file_id] + line`.
///
/// A container at or below it is refused by [`read_meta_dat_flags`] with the
/// reason named, rather than by the generic version mismatch, because the
/// consequence of reading one anyway is not a parse failure — it is a plausible
/// wrong answer at every step. Named rather than written as a literal `3` at
/// the refusal so the bound and the refusal move together: a later version that
/// changed the packing again would raise it, and a reader comparing against a
/// stale literal would answer such a container instead of refusing it.
pub const LAST_SHIFTED_GLOBAL_INDEX_VERSION: u16 = 3;
/// Bit 4 — the trace is column-aware. Must match the canonical Nim writer's
/// `meta_dat.nim` `FlagHasColumnAwareSteps`.
///
/// **This bit is not backward-compatible, by design.** Spec §"Reader Behaviour
/// and Back-Compat" requires a reader that does not understand it to refuse the
/// trace via the reserved-bits rule rather than misdecode the step stream —
/// because when it is set, three things change at once:
///
/// * `paths.dat` records carry the Layout A per-line table instead of bare path
///   bytes (see [`crate::column_aware::encode_path_record_layout_a`]);
/// * step records' `global_position_index` addresses `(line, column)` pairs,
///   not lines;
/// * the execution stream may contain `DeltaColumn` (tag 0x07) records.
///
/// The flag is trace-global: a writer must not mix column-aware and line-only
/// step records in one trace.
pub const FLAG_HAS_COLUMN_AWARE_STEPS: u16 = 0x10;
/// Bit 6 — the recorder's columns are sharp enough to place a breakpoint at a
/// specific `(line, column)`. A *capability* bit, not a wire-format one: it
/// says nothing about what is on the wire, only what the GUI may offer. Setting
/// it presupposes [`FLAG_HAS_COLUMN_AWARE_STEPS`]. Matches the Nim writer's
/// `FlagSupportsColumnBreakpoints`.
pub const FLAG_SUPPORTS_COLUMN_BREAKPOINTS: u16 = 0x40;
/// Bit 7 — the recorder supports per-column step over / in / out (its step
/// predicate fires per statement start, not per line). Like
/// [`FLAG_SUPPORTS_COLUMN_BREAKPOINTS`], a capability bit that presupposes
/// [`FLAG_HAS_COLUMN_AWARE_STEPS`]. Matches the Nim writer's
/// `FlagSupportsColumnMotions`.
pub const FLAG_SUPPORTS_COLUMN_MOTIONS: u16 = 0x80;
/// Bit 8 — M17a: a dedicated `calls.dat` call stream is present.
pub const FLAG_HAS_CALL_STREAM: u16 = 0x100;
/// Bit 9 — M23a: a dedicated `steps.dat` compact execution stream (+ its
/// companion `steps.idx`) is present. Additive and backward-compatible exactly
/// like [`FLAG_HAS_CALL_STREAM`]: a reader that does not know the bit ignores
/// `steps.dat`/`steps.idx` and reads the unified `events.log` unchanged. Must
/// match the canonical Nim writer's `meta_dat.nim` bit 9 and the db-backend
/// `ctfs_trace_reader::meta_dat::FLAG_HAS_STEP_STREAM`.
pub const FLAG_HAS_STEP_STREAM: u16 = 0x200;
/// Bit 10 — M23b: a dedicated `values.dat` parallel value stream (+ its
/// companion `values.idx`) is present, parallel-indexed to `steps.dat` (value
/// record N ↔ step N). Additive and backward-compatible exactly like
/// [`FLAG_HAS_CALL_STREAM`] / [`FLAG_HAS_STEP_STREAM`]: a reader that does not
/// know the bit ignores `values.dat`/`values.idx` and reads the unified
/// `events.log` unchanged. Must match the canonical Nim writer's `meta_dat.nim`
/// bit 10 and the db-backend
/// `ctfs_trace_reader::meta_dat::FLAG_HAS_VALUE_STREAM`.
pub const FLAG_HAS_VALUE_STREAM: u16 = 0x400;
/// Bit 11 — M23c: a dedicated `events.dat` I/O event stream (+ its companion
/// `events.idx`) is present, holding the `EventLogKind`-tagged I/O / log events
/// (stdout/stderr/file/network/error/log) split out of the unified `events.log`.
/// Each record carries `kind` (u8) / `step_id` (varint cross-ref to the
/// execution stream) / `metadata` / `content`. Additive and backward-compatible
/// exactly like [`FLAG_HAS_CALL_STREAM`] / [`FLAG_HAS_STEP_STREAM`] /
/// [`FLAG_HAS_VALUE_STREAM`]: a reader that does not know the bit ignores
/// `events.dat`/`events.idx` and reads the unified `events.log` unchanged. NOTE
/// the file naming — the legacy combined stream is `events.log`; this new I/O
/// stream is the distinct `events.dat`. Must match the canonical Nim writer's
/// `meta_dat.nim` bit 11 and the db-backend
/// `ctfs_trace_reader::meta_dat::FLAG_HAS_IO_EVENT_STREAM`.
pub const FLAG_HAS_IO_EVENT_STREAM: u16 = 0x800;
/// Bit 12 — M23d: the binary varint interning tables (`paths.dat`+`paths.off`,
/// `funcs.dat`+`funcs.off`, `types.dat`+`types.off`, `varnames.dat`+`varnames.off`)
/// are present, emitted from the SAME Path/Function/Type/VariableName interning
/// the writer already does for `events.log` / `paths.json`. These use the
/// Variable-Size Record Table (`.dat` + `.off`) pattern — a `.dat` of serialized
/// records plus a `u64`-LE offset index — for O(1) random access by id. Additive
/// and backward-compatible exactly like [`FLAG_HAS_CALL_STREAM`] /
/// [`FLAG_HAS_STEP_STREAM`] / [`FLAG_HAS_VALUE_STREAM`] / [`FLAG_HAS_IO_EVENT_STREAM`]:
/// a reader that does not know the bit ignores the eight new files and reads
/// `events.log` / `paths.json` unchanged. Must match the canonical Nim writer's
/// `meta_dat.nim` bit 12 and the db-backend
/// `ctfs_trace_reader::meta_dat::FLAG_HAS_INTERNING_TABLES`.
pub const FLAG_HAS_INTERNING_TABLES: u16 = 0x1000;
/// Bit 13 — RS-M1: the request/interval span streams (`spans.dat` +
/// `spans.idx`, plus the `spantype.ns` span-type index) are present. A span is
/// a bounded, labeled interval of execution named by the coordinate
/// `(process_ord, thread_id, step range)` — an HTTP request, a process, a test
/// — replacing the `session_manifest.jsonl` / `codetracer_spans.jsonl`
/// sidecars. Spec:
/// `codetracer-specs/Trace-Files/CTFS-Request-Span-Streams.md`.
///
/// **Unlike the stream bits above, this one is NOT backward-compatible at the
/// reader.** The doc comments on bits 8–12 describe them as "a reader that does
/// not know the bit ignores the new files", which holds only for readers that
/// do not enforce a known-bits mask. The canonical Nim reader does: its
/// `KnownFlags` / `read_meta_dat` equivalent REJECTS any container carrying a
/// bit outside the mask, and it is the implementation that governs `.ct` files
/// in practice. A reader predating this constant therefore refuses a
/// span-bearing container outright. Rollout consequence: reader support must
/// ship everywhere before any writer sets this bit.
///
/// Bit 14 is `FLAG_HAS_LINE_COUNT_TABLE` and bit 15 is
/// `FLAG_HAS_CORRELATION_INDEX`, so the flag word is now fully allocated.
/// A further flag needs a `version` bump, not a spare bit.
///
/// Must match the canonical Nim writer's `meta_dat.nim` bit 13
/// (`FlagHasSpanStream`) and the db-backend
/// `ctfs_trace_reader::meta_dat::FLAG_HAS_SPAN_STREAM` (RS-M2).
pub const FLAG_HAS_SPAN_STREAM: u16 = 0x2000;
/// Bit 14 — every `paths.dat` record carries its file's line count after the
/// path bytes (`payload_len + payload + line_count`), and the line-only global
/// position space is laid out from those counts instead of from
/// [`crate::line_position::DEFAULT_LINES_PER_FILE`].
///
/// This is the container stating what a line-only reader previously had to
/// assume. Spec `trace-events.md` §"Per-File Contiguous Integer Ranges" sizes a
/// line-only file at `file_size = line_count`, but no line-only container
/// carried the counts, so a reader could only re-apply the writer's convention
/// — unrecorded, and wrong above its own ceiling: a file with more lines
/// addresses positions inside the *next* file's range, which is a well-formed
/// address of a location that was never recorded and which no reader can
/// detect. The writer that records the counts is therefore also the party that
/// must refuse such a step.
///
/// **Mutually exclusive with [`FLAG_HAS_COLUMN_AWARE_STEPS`]**: a Layout A
/// record already carries `line_count` as the length of its per-line table, and
/// that mode sizes a file in addressable columns rather than lines. A header
/// setting both declares the same field under two record layouts.
///
/// **Like bit 13, NOT backward-compatible at the reader**, and for a sharper
/// reason: the record layout itself changes, so a reader that ignored the bit
/// would return a path with its own length prefix glued to the front. Rollout
/// is "readers before writers".
///
/// Must match the canonical Nim writer's `meta_dat.nim` bit 14
/// (`FlagHasLineCountTable`) and the db-backend
/// `ctfs_trace_reader::meta_dat::FLAG_HAS_LINE_COUNT_TABLE`.
///
/// Bit 15 is [`FLAG_HAS_CORRELATION_INDEX`], which spends the last one.
/// Widening the flag word — an "extended flag word follows" escape, or a
/// `u16` widened by a version bump — is a format decision that needs its own
/// milestone, and the next flag cannot land without it.
pub const FLAG_HAS_LINE_COUNT_TABLE: u16 = 0x4000;

/// Flag bit 15 — the container ships `corrmark.ns`, the record-time index of
/// the distributed-trace spans and boundary crossings the recording covers,
/// plus the `markers.dat` / `markers.off` table its boundary labels resolve
/// through.
///
/// A HINT, not a gate: the root file-entry array is the authority, and it is
/// the entry's presence that distinguishes "never indexed" from "indexed and
/// covering nothing". Recognising the bit still matters, because a reader
/// refuses any container carrying a flag outside its known mask — so without
/// it a reader would reject every marker-bearing recording instead of ignoring
/// an index it has no use for.
///
/// This index was drafted against bit 14 while [`FLAG_HAS_LINE_COUNT_TABLE`]
/// was taking the same bit on `dev`. Both describe the container, so the two
/// meanings could not share a bit: the line-count table shipped first and kept
/// 14, and the index took 15. Nothing on disk carried either bit at the time.
///
/// Must match the canonical Nim writer's `meta_dat.nim` bit 15
/// (`FlagHasCorrelationIndex`) and the db-backend
/// `ctfs_trace_reader::meta_dat::FLAG_HAS_CORRELATION_INDEX`.
pub const FLAG_HAS_CORRELATION_INDEX: u16 = 0x8000;

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

fn encode_varint_str(s: &str, out: &mut Vec<u8>) {
    encode_varint(s.len() as u64, out);
    out.extend_from_slice(s.as_bytes());
}

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err("meta.dat: truncated varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Serialize a `meta.dat` byte buffer. `flags` carries the capability bitfield
/// (e.g. [`FLAG_HAS_CALL_STREAM`]).
#[allow(clippy::too_many_arguments)]
pub fn encode_meta_dat(
    recording_id: &str,
    program: &str,
    args: &[String],
    workdir: &str,
    recorder_id: &str,
    paths: &[String],
    flags: u16,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&META_DAT_MAGIC);
    out.extend_from_slice(&META_DAT_VERSION.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    encode_varint_str(recording_id, &mut out);
    encode_varint_str(program, &mut out);
    encode_varint(args.len() as u64, &mut out);
    for a in args {
        encode_varint_str(a, &mut out);
    }
    encode_varint_str(workdir, &mut out);
    encode_varint_str(recorder_id, &mut out);
    encode_varint(paths.len() as u64, &mut out);
    for p in paths {
        encode_varint_str(p, &mut out);
    }
    out
}

/// Read the `flags` field from a `meta.dat` buffer. Returns an error if the
/// magic/version are not the expected [`META_DAT_VERSION`] header.
pub fn read_meta_dat_flags(data: &[u8]) -> Result<u16, String> {
    if data.len() < 8 {
        return Err(format!("meta.dat too short: {} bytes", data.len()));
    }
    if data[0..4] != META_DAT_MAGIC {
        return Err("meta.dat: bad magic".to_string());
    }
    let version = u16::from_le_bytes([data[4], data[5]]);
    if version <= LAST_SHIFTED_GLOBAL_INDEX_VERSION {
        // Phrased about the WRITER rather than about this container's contents:
        // the gate is on the schema version, so it also refuses a container at
        // that version that holds no steps at all, and "its steps were packed
        // as" would be a claim about such a trace that is not true.
        return Err(format!(
            "meta.dat: schema version {version} predates the global line index correction, and this \
             trace cannot be read. Writers at that version packed a line-only step position as \
             prefix_sum[path_id] + line; version {META_DAT_VERSION} packs \
             prefix_sum[path_id] + (line - 1). Both land inside the trace's address space, so a \
             step read under the current decode would come back one line high rather than fail, \
             and the container records nothing else that tells the two apart. Re-record the trace \
             with a current recorder. Spec: codetracer-trace-format-spec/internal-files.md \
             \"Global Line Index\""
        ));
    }
    if version != META_DAT_VERSION {
        return Err(format!("meta.dat: unsupported version {version}"));
    }
    Ok(u16::from_le_bytes([data[6], data[7]]))
}

/// Convenience: returns whether the `has_call_stream` capability flag (bit 8)
/// is set in a `meta.dat` buffer. A missing/invalid `meta.dat` ⇒ `false`
/// (the legacy unified-stream path), never an error — callers treat absence of
/// the flag as "no dedicated call stream".
///
/// # Why swallowing the error is sound here, and where it would not be
///
/// Turning a parse failure into `false` makes an unreadable header
/// indistinguishable from a readable one that clears the bit, which is the
/// shape of a real defect elsewhere. It is sound in this family because none of
/// these helpers is a GATE: the trace-format spec makes stream-presence flags a
/// hint rather than a gate (a writer may stamp one only at close), so every
/// reader in this crate answers presence STRUCTURALLY — see
/// [`crate::call_stream::CallStreamRecord`]'s reader, whose `from_files` spells
/// the argument `_meta`. The version check that decides whether a container may
/// be read at all lives in the container constructors, upstream and separate:
/// `readMetaDat` in the Nim reader and `parse_meta_dat` in the db-backend, both
/// of which propagate the refusal.
///
/// The consequence to keep in view is that after a version bump these helpers
/// answer `false` for every field of a superseded container, because
/// [`read_meta_dat_flags`] refuses its header. That is only harmless while the
/// helper's answer selects nothing that changes a decode. It is not harmless
/// for [`meta_dat_has_interning_tables`] and [`meta_dat_has_column_aware_steps`],
/// which select a record LAYOUT: a caller that consults either of those without
/// having gated on the version first would decode the wrong shape rather than
/// less of it, so those two belong downstream of a container constructor and
/// not in front of one.
pub fn meta_dat_has_call_stream(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_HAS_CALL_STREAM != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `has_step_stream` capability flag (bit 9)
/// is set in a `meta.dat` buffer. A missing/invalid `meta.dat` ⇒ `false`
/// (the legacy unified-stream path), never an error.
pub fn meta_dat_has_step_stream(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_HAS_STEP_STREAM != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `has_value_stream` capability flag (bit 10)
/// is set in a `meta.dat` buffer. A missing/invalid `meta.dat` ⇒ `false`
/// (the legacy unified-stream path), never an error.
pub fn meta_dat_has_value_stream(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_HAS_VALUE_STREAM != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `has_io_event_stream` capability flag (bit
/// 11) is set in a `meta.dat` buffer. A missing/invalid `meta.dat` ⇒ `false`
/// (the legacy unified-stream path), never an error.
pub fn meta_dat_has_io_event_stream(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_HAS_IO_EVENT_STREAM != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `has_interning_tables` capability flag (bit
/// 12) is set in a `meta.dat` buffer. A missing/invalid `meta.dat` ⇒ `false`
/// (the legacy interning path — `events.log` / `paths.json` only), never an
/// error.
pub fn meta_dat_has_interning_tables(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_HAS_INTERNING_TABLES != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `has_span_stream` capability flag (bit 13)
/// is set in a `meta.dat` buffer. A missing/invalid `meta.dat` ⇒ `false` (no
/// span streams), never an error.
pub fn meta_dat_has_span_stream(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_HAS_SPAN_STREAM != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `has_column_aware_steps` flag (bit 4) is
/// set in a `meta.dat` buffer.
///
/// Unlike its siblings this one is **not** merely informative: when it is set,
/// `paths.dat` is Layout A and the step stream's positions address
/// `(line, column)` pairs, so a reader that ignores it decodes the wrong thing
/// rather than less. A missing/invalid `meta.dat` ⇒ `false`.
pub fn meta_dat_has_column_aware_steps(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_HAS_COLUMN_AWARE_STEPS != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `has_line_count_table` flag (bit 14) is
/// set. A missing/invalid `meta.dat` ⇒ `false`.
///
/// Belongs to the same family as [`meta_dat_has_column_aware_steps`] and the
/// same caution applies with more force: this one selects a `paths.dat` record
/// LAYOUT, so a caller that consults it without having gated on the version
/// first decodes the wrong shape rather than less of it. Use it downstream of a
/// container constructor, never in front of one.
pub fn meta_dat_has_line_count_table(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_HAS_LINE_COUNT_TABLE != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `supports_column_breakpoints` capability
/// flag (bit 6) is set. A missing/invalid `meta.dat` ⇒ `false`.
pub fn meta_dat_supports_column_breakpoints(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_SUPPORTS_COLUMN_BREAKPOINTS != 0,
        Err(_) => false,
    }
}

/// Convenience: returns whether the `supports_column_motions` capability flag
/// (bit 7) is set. A missing/invalid `meta.dat` ⇒ `false`.
pub fn meta_dat_supports_column_motions(data: &[u8]) -> bool {
    match read_meta_dat_flags(data) {
        Ok(flags) => flags & FLAG_SUPPORTS_COLUMN_MOTIONS != 0,
        Err(_) => false,
    }
}

/// Decode just the `program` string from a `meta.dat` buffer (used by tests
/// asserting on the header round-trip).
pub fn read_meta_dat_program(data: &[u8]) -> Result<String, String> {
    read_meta_dat_flags(data)?; // validates header
    let mut pos = 8usize;
    // recording_id
    let len = decode_varint(data, &mut pos)? as usize;
    pos += len;
    // program
    let plen = decode_varint(data, &mut pos)? as usize;
    if pos + plen > data.len() {
        return Err("meta.dat: program extends past end".to_string());
    }
    String::from_utf8(data[pos..pos + plen].to_vec()).map_err(|e| format!("meta.dat: program not UTF-8: {e}"))
}

/// The decoded core field block of a [`META_DAT_VERSION`] header.
///
/// Covers the fields every container carries, in the order
/// `internal-files.md` §"Metadata (meta.dat)" lays them out. The
/// flag-gated extension blocks (MCR, replay-launch, layout snapshot, filter
/// provenance) are not decoded into fields; `trailing` holds whatever bytes
/// follow the path list so a caller can still tell "same core, different
/// extensions" from "identical".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaDat {
    pub version: u16,
    pub flags: u16,
    pub recording_id: String,
    pub program: String,
    pub args: Vec<String>,
    pub workdir: String,
    pub recorder_id: String,
    pub paths: Vec<String>,
    pub trailing: Vec<u8>,
}

fn decode_varint_str(data: &[u8], pos: &mut usize) -> Result<String, String> {
    let len = decode_varint(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err("meta.dat: string extends past end".to_string());
    }
    let s = String::from_utf8(data[*pos..*pos + len].to_vec()).map_err(|e| format!("meta.dat: string not UTF-8: {e}"))?;
    *pos += len;
    Ok(s)
}

/// Decode a `meta.dat` buffer into its core fields.
///
/// The inverse of [`encode_meta_dat`]. Exists so a consumer can compare or
/// report the metadata field by field rather than treating the file as an
/// opaque blob — comparing whole `meta.dat` bytes conflates a differing
/// `recording_id`, which differs by construction on every run, with a
/// differing `workdir` or capability flag, which is a defect.
pub fn decode_meta_dat(data: &[u8]) -> Result<MetaDat, String> {
    let flags = read_meta_dat_flags(data)?; // validates magic + version
    let version = u16::from_le_bytes([data[4], data[5]]);
    let mut pos = 8usize;
    let recording_id = decode_varint_str(data, &mut pos)?;
    let program = decode_varint_str(data, &mut pos)?;
    let args_count = decode_varint(data, &mut pos)? as usize;
    let mut args = Vec::with_capacity(args_count);
    for _ in 0..args_count {
        args.push(decode_varint_str(data, &mut pos)?);
    }
    let workdir = decode_varint_str(data, &mut pos)?;
    let recorder_id = decode_varint_str(data, &mut pos)?;
    let path_count = decode_varint(data, &mut pos)? as usize;
    let mut paths = Vec::with_capacity(path_count);
    for _ in 0..path_count {
        paths.push(decode_varint_str(data, &mut pos)?);
    }
    Ok(MetaDat {
        version,
        flags,
        recording_id,
        program,
        args,
        workdir,
        recorder_id,
        paths,
        trailing: data[pos..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_dat_decodes_back_to_the_fields_that_were_encoded() {
        let buf = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &["a".to_string(), "b".to_string()],
            "/wd",
            "rec",
            &["/p".to_string(), "/q".to_string()],
            FLAG_HAS_CALL_STREAM | FLAG_HAS_INTERNING_TABLES,
        );
        let m = decode_meta_dat(&buf).expect("decodes");
        assert_eq!(m.version, META_DAT_VERSION);
        assert_eq!(m.flags, FLAG_HAS_CALL_STREAM | FLAG_HAS_INTERNING_TABLES);
        assert_eq!(m.recording_id, "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb");
        assert_eq!(m.program, "prog");
        assert_eq!(m.args, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(m.workdir, "/wd");
        assert_eq!(m.recorder_id, "rec");
        assert_eq!(m.paths, vec!["/p".to_string(), "/q".to_string()]);
        // Nothing follows the path list when no extension block is flagged;
        // asserted so a decoder that stopped short would be caught here rather
        // than by a caller silently seeing extra "trailing" bytes.
        assert!(m.trailing.is_empty(), "trailing = {:?}", m.trailing);
    }

    #[test]
    fn meta_dat_decode_rejects_a_truncated_field_block() {
        // A length prefix that overruns the buffer must be an error, not a
        // panic and not a silently short string.
        let buf = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "/wd", "rec", &[], 0);
        for cut in [9usize, 20, buf.len() - 1] {
            assert!(
                decode_meta_dat(&buf[..cut]).is_err(),
                "a meta.dat truncated to {cut} bytes must not decode"
            );
        }
    }

    /// A header at the superseded schema version is refused, and the refusal
    /// says what reading it anyway would do — "one line high" is the only fact
    /// that tells a caller why re-recording is the remedy rather than a reader
    /// upgrade.
    ///
    /// The fixture is the header this writer emits with the version field set
    /// back, because the writer can no longer produce one: that is the whole
    /// point of the bump. Everything else about it is what a v3 writer wrote.
    #[test]
    fn a_header_from_before_the_line_index_correction_is_refused_by_name() {
        let mut buf = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "/wd",
            "rec",
            &["/p".to_string()],
            FLAG_HAS_STEP_STREAM,
        );
        assert_eq!(
            read_meta_dat_flags(&buf),
            Ok(FLAG_HAS_STEP_STREAM),
            "the header this writer emits must be readable before it is aged"
        );

        buf[4..6].copy_from_slice(&LAST_SHIFTED_GLOBAL_INDEX_VERSION.to_le_bytes());
        let err = read_meta_dat_flags(&buf).expect_err("a pre-correction container must be refused");
        assert!(err.contains("one line high"), "must name the consequence: {err}");
        assert!(err.contains("prefix_sum[path_id] + line"), "must name the superseded encode: {err}");
        assert!(err.contains("Re-record"), "must name the remedy: {err}");

        // The refusal is what makes the flag helpers answer `false`, so a
        // caller that reads a capability bit off such a container gets the
        // absence of the capability rather than the bit the writer stamped.
        assert!(!meta_dat_has_step_stream(&buf));
    }

    #[test]
    fn meta_dat_flag_roundtrip() {
        let buf = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &["a".to_string()],
            "/wd",
            "rec",
            &["/p".to_string()],
            FLAG_HAS_CALL_STREAM,
        );
        assert!(meta_dat_has_call_stream(&buf));
        assert_eq!(read_meta_dat_flags(&buf).unwrap(), FLAG_HAS_CALL_STREAM);
        assert_eq!(read_meta_dat_program(&buf).unwrap(), "prog");

        let buf0 = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], 0);
        assert!(!meta_dat_has_call_stream(&buf0));
        assert!(!meta_dat_has_step_stream(&buf0));
    }

    #[test]
    fn meta_dat_step_stream_flag_roundtrip() {
        // Both stream flags can coexist in one meta.dat (M23a writes calls.dat
        // and steps.dat together).
        let buf = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "/wd",
            "rec",
            &[],
            FLAG_HAS_CALL_STREAM | FLAG_HAS_STEP_STREAM,
        );
        assert!(meta_dat_has_call_stream(&buf));
        assert!(meta_dat_has_step_stream(&buf));

        // Step stream alone.
        let buf_step = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], FLAG_HAS_STEP_STREAM);
        assert!(meta_dat_has_step_stream(&buf_step));
        assert!(!meta_dat_has_call_stream(&buf_step));
    }

    #[test]
    fn meta_dat_value_stream_flag_roundtrip() {
        // M23b: a real bundle sets call+step+value bits together.
        let buf = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "/wd",
            "rec",
            &[],
            FLAG_HAS_CALL_STREAM | FLAG_HAS_STEP_STREAM | FLAG_HAS_VALUE_STREAM,
        );
        assert!(meta_dat_has_call_stream(&buf));
        assert!(meta_dat_has_step_stream(&buf));
        assert!(meta_dat_has_value_stream(&buf));

        // Value stream alone.
        let buf_val = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], FLAG_HAS_VALUE_STREAM);
        assert!(meta_dat_has_value_stream(&buf_val));
        assert!(!meta_dat_has_step_stream(&buf_val));
        assert!(!meta_dat_has_call_stream(&buf_val));
    }

    #[test]
    fn meta_dat_io_event_stream_flag_roundtrip() {
        // M23c: a real bundle sets call+step+value+io-event bits together.
        let buf = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "/wd",
            "rec",
            &[],
            FLAG_HAS_CALL_STREAM | FLAG_HAS_STEP_STREAM | FLAG_HAS_VALUE_STREAM | FLAG_HAS_IO_EVENT_STREAM,
        );
        assert!(meta_dat_has_call_stream(&buf));
        assert!(meta_dat_has_step_stream(&buf));
        assert!(meta_dat_has_value_stream(&buf));
        assert!(meta_dat_has_io_event_stream(&buf));

        // I/O event stream alone.
        let buf_io = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], FLAG_HAS_IO_EVENT_STREAM);
        assert!(meta_dat_has_io_event_stream(&buf_io));
        assert!(!meta_dat_has_value_stream(&buf_io));
        assert!(!meta_dat_has_step_stream(&buf_io));
        assert!(!meta_dat_has_call_stream(&buf_io));
    }

    #[test]
    fn meta_dat_interning_tables_flag_roundtrip() {
        // M23d: a real bundle sets call+step+value+io-event+interning bits together.
        let buf = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "/wd",
            "rec",
            &[],
            FLAG_HAS_CALL_STREAM | FLAG_HAS_STEP_STREAM | FLAG_HAS_VALUE_STREAM | FLAG_HAS_IO_EVENT_STREAM | FLAG_HAS_INTERNING_TABLES,
        );
        assert!(meta_dat_has_call_stream(&buf));
        assert!(meta_dat_has_step_stream(&buf));
        assert!(meta_dat_has_value_stream(&buf));
        assert!(meta_dat_has_io_event_stream(&buf));
        assert!(meta_dat_has_interning_tables(&buf));

        // Interning tables alone.
        let buf_it = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "",
            "",
            &[],
            FLAG_HAS_INTERNING_TABLES,
        );
        assert!(meta_dat_has_interning_tables(&buf_it));
        assert!(!meta_dat_has_io_event_stream(&buf_it));
        assert!(!meta_dat_has_value_stream(&buf_it));
        assert!(!meta_dat_has_step_stream(&buf_it));
        assert!(!meta_dat_has_call_stream(&buf_it));
    }

    #[test]
    fn meta_dat_column_flags_roundtrip_and_are_independent() {
        // The three column bits must equal the canonical Nim writer's
        // `FlagHasColumnAwareSteps` / `FlagSupportsColumnBreakpoints` /
        // `FlagSupportsColumnMotions`. A divergence here does not fail loudly —
        // it produces containers the reference reader rejects — so the values
        // are pinned rather than inferred.
        assert_eq!(FLAG_HAS_COLUMN_AWARE_STEPS, 0x10);
        assert_eq!(FLAG_SUPPORTS_COLUMN_BREAKPOINTS, 0x40);
        assert_eq!(FLAG_SUPPORTS_COLUMN_MOTIONS, 0x80);

        let all = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "/wd",
            "rec",
            &[],
            FLAG_HAS_COLUMN_AWARE_STEPS | FLAG_SUPPORTS_COLUMN_BREAKPOINTS | FLAG_SUPPORTS_COLUMN_MOTIONS | FLAG_HAS_STEP_STREAM,
        );
        assert!(meta_dat_has_column_aware_steps(&all));
        assert!(meta_dat_supports_column_breakpoints(&all));
        assert!(meta_dat_supports_column_motions(&all));
        assert!(meta_dat_has_step_stream(&all));

        // The wire-format bit without either capability bit is the ordinary
        // case: columns are on the wire, the GUI offers no per-column
        // affordances. Each accessor must be able to answer `false` while its
        // neighbours answer `true`, or none of the three is really a reading.
        let wire_only = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "",
            "",
            &[],
            FLAG_HAS_COLUMN_AWARE_STEPS,
        );
        assert!(meta_dat_has_column_aware_steps(&wire_only));
        assert!(!meta_dat_supports_column_breakpoints(&wire_only));
        assert!(!meta_dat_supports_column_motions(&wire_only));

        // And a line-only bundle must report all three clear even though its
        // stream bits are set — the case every existing recorder produces.
        let line_only = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "",
            "",
            &[],
            FLAG_HAS_STEP_STREAM | FLAG_HAS_VALUE_STREAM | FLAG_HAS_INTERNING_TABLES,
        );
        assert!(!meta_dat_has_column_aware_steps(&line_only));
        assert!(!meta_dat_supports_column_breakpoints(&line_only));
        assert!(!meta_dat_supports_column_motions(&line_only));
    }

    #[test]
    fn meta_dat_span_stream_flag_roundtrip() {
        // RS-M1: bit 13. The value MUST match the canonical Nim writer's
        // `FlagHasSpanStream`; a divergence here silently splits the registry.
        assert_eq!(FLAG_HAS_SPAN_STREAM, 0x2000);

        let buf = encode_meta_dat(
            "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
            "prog",
            &[],
            "/wd",
            "rec",
            &[],
            FLAG_HAS_CALL_STREAM
                | FLAG_HAS_STEP_STREAM
                | FLAG_HAS_VALUE_STREAM
                | FLAG_HAS_IO_EVENT_STREAM
                | FLAG_HAS_INTERNING_TABLES
                | FLAG_HAS_SPAN_STREAM,
        );
        assert!(meta_dat_has_span_stream(&buf));
        assert!(meta_dat_has_interning_tables(&buf));
        assert!(meta_dat_has_io_event_stream(&buf));

        // Span stream alone.
        let buf_sp = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], FLAG_HAS_SPAN_STREAM);
        assert!(meta_dat_has_span_stream(&buf_sp));
        assert!(!meta_dat_has_interning_tables(&buf_sp));
        assert!(!meta_dat_has_io_event_stream(&buf_sp));
        assert!(!meta_dat_has_value_stream(&buf_sp));
        assert!(!meta_dat_has_step_stream(&buf_sp));
        assert!(!meta_dat_has_call_stream(&buf_sp));

        // A container without spans must leave the bit clear.
        let buf_none = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], 0);
        assert!(!meta_dat_has_span_stream(&buf_none));
    }
}
