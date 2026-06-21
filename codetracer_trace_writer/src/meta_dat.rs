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
pub fn encode_meta_dat(recording_id: &str, program: &str, args: &[String], workdir: &str, recorder_id: &str, paths: &[String], flags: u16) -> Vec<u8> {
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
        let buf_it = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], FLAG_HAS_INTERNING_TABLES);
        assert!(meta_dat_has_interning_tables(&buf_it));
        assert!(!meta_dat_has_io_event_stream(&buf_it));
        assert!(!meta_dat_has_value_stream(&buf_it));
        assert!(!meta_dat_has_step_stream(&buf_it));
        assert!(!meta_dat_has_call_stream(&buf_it));
    }
}
