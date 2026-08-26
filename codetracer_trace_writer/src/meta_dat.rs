//! Minimal binary `meta.dat` writer + flag reader for the Rust CTFS writer.
//!
//! The canonical, full-featured `meta.dat` writer/reader lives in
//! `codetracer-trace-format-nim` (`meta_dat.nim`) and the db-backend
//! (`ctfs_trace_reader/meta_dat.rs`). This module is the small slice the Rust
//! `CtfsTraceWriter` needs for M17a: emit a spec-shaped `meta.dat` header so the
//! new `has_call_stream` capability flag (bit 8) can be carried in the canonical
//! place, and read that flag back in the Rust reader.
//!
//! Layout (version 3), per
//! `codetracer-trace-format-spec/internal-files.md` §"Metadata (meta.dat)":
//!
//! ```text
//!   [4] magic "CTMD"
//!   [2] version u16 LE (3)
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

/// `meta.dat` magic bytes ("CTMD").
pub const META_DAT_MAGIC: [u8; 4] = [0x43, 0x54, 0x4D, 0x44];
/// Current `meta.dat` version.
pub const META_DAT_VERSION: u16 = 3;
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
/// Bits 14 and 15 are the last two free bits and are deliberately left
/// UNALLOCATED by RS-M1; whether the final bit becomes an "extended flag word
/// follows" escape (or the `u16` is widened by a version bump) is a format
/// decision that needs its own milestone.
///
/// Must match the canonical Nim writer's `meta_dat.nim` bit 13
/// (`FlagHasSpanStream`) and the db-backend
/// `ctfs_trace_reader::meta_dat::FLAG_HAS_SPAN_STREAM` (RS-M2).
pub const FLAG_HAS_SPAN_STREAM: u16 = 0x2000;

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
/// magic/version are not the expected `meta.dat` v3 header.
pub fn read_meta_dat_flags(data: &[u8]) -> Result<u16, String> {
    if data.len() < 8 {
        return Err(format!("meta.dat too short: {} bytes", data.len()));
    }
    if data[0..4] != META_DAT_MAGIC {
        return Err("meta.dat: bad magic".to_string());
    }
    let version = u16::from_le_bytes([data[4], data[5]]);
    if version != META_DAT_VERSION {
        return Err(format!("meta.dat: unsupported version {version}"));
    }
    Ok(u16::from_le_bytes([data[6], data[7]]))
}

/// Convenience: returns whether the `has_call_stream` capability flag (bit 8)
/// is set in a `meta.dat` buffer. A missing/invalid `meta.dat` ⇒ `false`
/// (the legacy unified-stream path), never an error — callers treat absence of
/// the flag as "no dedicated call stream".
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

#[cfg(test)]
mod tests {
    use super::*;

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
