//! Reader for the binary varint interning tables (M23d).
//!
//! Resolves interned records by id from a CTFS container's four interning
//! tables — `paths.dat`+`paths.off`, `funcs.dat`+`funcs.off`,
//! `types.dat`+`types.off`, `varnames.dat`+`varnames.off` — using the
//! Variable-Size Record Table (`.dat` + `.off`) pattern from
//! `codetracer-trace-format-spec/internal-files.md`. Each lookup is O(1) random
//! access: read the record's start/end byte offsets from the `.off` index
//! (two `u64`s), then slice the `.dat` between them — there is NO sequential
//! scan, so resolving a mid-table id costs the same as the first.
//!
//! # Detection is by PRESENCE, and there is one record shape
//!
//! EXISTENCE of the binary interning tables is decided by STRUCTURAL PRESENCE of
//! `paths.dat`, never by the `has_interning_tables` hint (bit 12) in `meta.dat`
//! — a stream-presence bit is a hint, not a gate (`internal-files.md` "Stream-
//! presence flags are a hint, not a gate").
//!
//! Nor does bit 12 select a record LAYOUT. The spec gives each table one shape:
//!
//! ```text
//!   paths.dat / varnames.dat record = raw bytes (paths.dat framed under bits 4/14)
//!   funcs.dat   record = global_line_index: varint, name_len: varint, name: bytes
//!   types.dat   record = kind: u8, lang_type_len: varint, lang_type: bytes,
//!                        specific_info: binary (CBOR of TypeSpecificInfo)
//! ```
//!
//! The Nim writer wrote bare names into `funcs.dat`/`types.dat` until b891a0f
//! (2026-09-15), with bit 12 clear. This reader used to take bit 12 as a switch
//! between those bare names and the structured records; it now decodes the
//! spec's shape only, and refuses a bare-name record with the same message the
//! Nim reader gives (see `InterningTablesReader::func`).

use std::borrow::Cow;

use codetracer_ctfs::{CtfsReader, MemberBytes};
use codetracer_trace_types::{TypeKind, TypeSpecificInfo};
use codetracer_trace_writer::column_aware::FileTable;
use codetracer_trace_writer::line_position::{LinePositionError, LinePositionSpace};
use codetracer_trace_writer::meta_dat::{meta_dat_has_column_aware_steps, meta_dat_has_interning_tables, meta_dat_has_line_count_table};
use num_traits::FromPrimitive;

/// A decoded `funcs.dat` record: the `global_line_index` and the function name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuncRecord {
    /// The address of the function's declaration site in the trace's global
    /// position space; use [`FuncRecord::path_id_and_line`] to recover
    /// `(path_id, line)`.
    pub global_line_index: u64,
    /// The function name (raw bytes; UTF-8 for the recorders that produce it).
    pub name: Vec<u8>,
}

impl FuncRecord {
    /// Recover the `(path_id, line)` the function was declared at.
    ///
    /// The caller supplies the trace's own address space — the one built from
    /// its `paths.dat` — because an address means nothing without it. A record
    /// whose address the space cannot hold is refused rather than answered
    /// with a location the trace never contained; see
    /// [`codetracer_trace_writer::line_position`].
    pub fn path_id_and_line(&self, space: &LinePositionSpace) -> Result<(usize, i64), LinePositionError> {
        space.resolve(self.global_line_index)
    }
}

/// A decoded `types.dat` record: kind, lang_type, and the (CBOR-decoded)
/// type-specific info.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedTypeRecord {
    /// The `TypeKind` ordinal byte as stored on disk.
    pub kind: u8,
    /// The language-specific type name (raw bytes; UTF-8 for the recorders).
    pub lang_type: Vec<u8>,
    /// The structured type-specific info (decoded from the CBOR tail).
    pub specific_info: TypeSpecificInfo,
}

impl DecodedTypeRecord {
    /// The `TypeKind` enum for [`Self::kind`], or `None` if the on-disk ordinal
    /// is not a recognised `TypeKind`.
    pub fn type_kind(&self) -> Option<TypeKind> {
        TypeKind::from_u8(self.kind)
    }
}

// --- varint helper (unsigned LEB128) ---

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err("interning table: truncated varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err("interning table: varint too long".to_string());
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Split a line-count-table `paths.dat` record into its payload and its line
/// count.
///
/// The record is `payload_len + payload + line_count` — the column-aware
/// Layout A framing without its trailing per-line table
/// (`codetracer-trace-format-spec/internal-files.md` §"`paths.dat` line-count
/// table").
///
/// Only ever called on a container that DECLARED this layout through
/// `meta.dat` bit 14, so a record that does not decode is corruption and is
/// reported as such. There is deliberately no probing counterpart: the record
/// spaces of the three layouts overlap, so a bare record whose first byte
/// happens to equal its own remaining length decodes cleanly here and would
/// yield a truncated path and a fabricated count with no error.
fn decode_framed_path_record(raw: &[u8], id: usize) -> Result<(&[u8], u64, usize), String> {
    let mut pos = 0usize;
    let payload_len = usize::try_from(decode_varint(raw, &mut pos)?).map_err(|_| format!("paths.dat: record {id} payload_len exceeds usize"))?;
    let start = pos;
    let end = pos
        .checked_add(payload_len)
        .ok_or_else(|| format!("paths.dat: record {id} payload_len overflows"))?;
    if end > raw.len() {
        return Err(format!(
            "paths.dat: record {id} payload extends past the record (payload_len {payload_len}, {} byte(s) left)",
            raw.len() - start
        ));
    }
    pos = end;
    let count = decode_varint(raw, &mut pos)?;
    Ok((&raw[start..end], count, pos))
}

fn decode_line_count_path_record(raw: &[u8], id: usize) -> Result<(&[u8], u64), String> {
    let (payload, count, pos) = decode_framed_path_record(raw, id)?;
    if count == 0 {
        return Err(format!(
            "paths.dat: record {id} states line_count 0. A container setting              FLAG_HAS_LINE_COUNT_TABLE states every file's size, and a file sized 0 shares its              base with the next one — the two would be indistinguishable at decode"
        ));
    }
    if pos != raw.len() {
        return Err(format!(
            "paths.dat: record {id} has {} trailing byte(s) after line_count",
            raw.len() - pos
        ));
    }
    Ok((payload, count))
}

/// Split a Layout A `paths.dat` record into its path payload, decoding it whole.
///
/// The record is `path_len + path + line_count + line_lengths × line_count`,
/// the lengths zigzag varints (`codetracer-trace-format-spec/internal-files.md`
/// §"`paths.dat` Layout A"). Only the path is returned, but the per-line table
/// is walked to the end and the record must end exactly there. Taking the
/// `path_len` prefix alone accepts any record whose first byte is no larger
/// than what follows it — a bare absolute path starts with `/`, which is 47 —
/// and answers with that record's bytes 1..48 as the path, silently.
fn decode_layout_a_path_record(raw: &[u8], id: usize) -> Result<&[u8], String> {
    let (payload, line_count, mut pos) = decode_framed_path_record(raw, id)?;
    // Each entry is at least one byte, so a count larger than what is left
    // cannot be satisfied — refused before the walk rather than after it.
    let left = (raw.len() - pos) as u64;
    if line_count > left {
        return Err(format!(
            "paths.dat: record {id} states line_count {line_count} with only {left} byte(s) left for its Layout A line_lengths"
        ));
    }
    let mut previous: i64 = 0;
    for i in 0..line_count {
        let zigzag = decode_varint(raw, &mut pos).map_err(|e| format!("paths.dat: record {id} line_lengths[{i}]: {e}"))?;
        let v = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
        let length = if i == 0 { v } else { previous.wrapping_add(v) };
        if length < 0 {
            return Err(format!("paths.dat: record {id} line_lengths[{i}] decodes negative ({length})"));
        }
        previous = length;
    }
    if pos != raw.len() {
        return Err(format!(
            "paths.dat: record {id} has {} trailing byte(s) after its Layout A line_lengths — it was not written in the layout meta.dat bit 4 declares",
            raw.len() - pos
        ));
    }
    Ok(payload)
}

/// Decode a `funcs.dat` record: `global_line_index: varint, name_len: varint,
/// name`. Messages match the Nim reader's `decodeFuncRecord`.
fn decode_func_record(raw: &[u8]) -> Result<FuncRecord, String> {
    let mut pos = 0usize;
    let global_line_index = decode_varint_nim(raw, &mut pos)?;
    let name_len = decode_varint_nim(raw, &mut pos)?;
    let left = (raw.len() - pos) as u64;
    if left < name_len {
        return Err(format!(
            "funcs.dat record is truncated: declares a {name_len}-byte name with only {left} bytes left"
        ));
    }
    let name = raw[pos..pos + name_len as usize].to_vec();
    Ok(FuncRecord { global_line_index, name })
}

/// Decode a `types.dat` record: `kind: u8, lang_type_len: varint, lang_type,
/// specific_info` (CBOR). Messages match the Nim reader's `decodeTypeRecord`.
fn decode_type_record(raw: &[u8]) -> Result<DecodedTypeRecord, String> {
    if raw.is_empty() {
        return Err("types.dat record is empty: it must carry at least the kind byte".to_string());
    }
    let kind = raw[0];
    let mut pos = 1usize;
    let lang_len = decode_varint_nim(raw, &mut pos)?;
    let left = (raw.len() - pos) as u64;
    if left < lang_len {
        return Err(format!(
            "types.dat record is truncated: declares a {lang_len}-byte lang_type with only {left} bytes left"
        ));
    }
    let lang_type = raw[pos..pos + lang_len as usize].to_vec();
    pos += lang_len as usize;
    let specific_info: TypeSpecificInfo =
        cbor4ii::serde::from_slice(&raw[pos..]).map_err(|e| format!("types.dat record specific_info CBOR decode failed: {e}"))?;
    Ok(DecodedTypeRecord {
        kind,
        lang_type,
        specific_info,
    })
}

/// Unsigned LEB128 with the Nim reader's error messages.
fn decode_varint_nim(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        if *pos >= data.len() {
            return Err("varint: unexpected end of input".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err("varint: too many bytes (>10)".to_string())
}

/// A single Variable-Size Record Table: the `.dat` data file plus its `.off`
/// offset index. Records are resolved by 0-based id with O(1) random access:
/// two offsets read out of `.off`, then a slice of `.dat`. Both members are
/// held as read, shared with the container when it is in memory.
struct VarSizeTable {
    /// The concatenated record bytes.
    dat: MemberBytes,
    /// The offset index: `record_count + 1` little-endian `u64` byte offsets
    /// (the trailing entry is the total data length, so record `i`'s length
    /// is `offsets[i + 1] - offsets[i]` for every record).
    off: MemberBytes,
}

impl VarSizeTable {
    /// A table over a `.dat` data member and a `.off` offset index.
    fn new(name: &str, dat: MemberBytes, off: MemberBytes) -> Result<VarSizeTable, String> {
        if !off.len().is_multiple_of(8) {
            return Err(format!("{name}.off: length {} is not a multiple of 8", off.len()));
        }
        // A valid offset index has at least the trailing sentinel. An empty
        // table is exactly one sentinel entry (== 0).
        if off.is_empty() {
            return Err(format!("{name}.off: empty (missing the trailing sentinel offset)"));
        }
        Ok(VarSizeTable { dat, off })
    }

    /// Number of records in the table.
    fn count(&self) -> usize {
        self.off.len() / 8 - 1
    }

    /// Resolve record `id` to its raw bytes via the offset index (random access,
    /// no scan).
    fn record(&self, id: usize) -> Result<Cow<'_, [u8]>, String> {
        if id >= self.count() {
            return Err(format!("interning table: id {id} out of range (count {})", self.count()));
        }
        // `id < count`, so both offsets lie inside `.off`, read as one range.
        // An offset this target cannot address is out of range, not truncated.
        let pair = self
            .off
            .get(id * 8, id * 8 + 16)
            .map_err(|e| format!("interning table: record {id}'s offsets: {e}"))?;
        let offset = |at: usize| {
            let word: [u8; 8] = pair[at..at + 8].try_into().expect("an eight-byte slice");
            usize::try_from(u64::from_le_bytes(word)).unwrap_or(usize::MAX)
        };
        let (start, end) = (offset(0), offset(8));
        self.dat.get(start, end).map_err(|e| {
            format!(
                "interning table: record {id} offsets [{start}, {end}) out of range (dat len {}): {e}",
                self.dat.len()
            )
        })
    }
}

/// One `paths.dat` entry's place among the entries that share its string —
/// see [`InterningTablesReader::path_versions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathVersion {
    /// 0-based: how many lower path ids carry the same string.
    pub ordinal: u64,
    /// How many path ids, this one included, carry the same string.
    pub count: u64,
}

/// A reader over a container's binary interning tables, resolving interned
/// records by id with O(1) random access.
pub struct InterningTablesReader {
    paths: VarSizeTable,
    funcs: VarSizeTable,
    types: VarSizeTable,
    varnames: VarSizeTable,
    /// Whether `meta.dat` declares the tables (bit 12). Decides only how a
    /// record that does not decode is described: under bit 12 clear it is
    /// most likely a pre-b891a0f bare name.
    interning_tables_declared: bool,
    /// Whether `paths.dat` records carry the column-aware Layout A framing
    /// (`meta.dat` bit 4), independently of the bit-14 line-count table.
    column_aware: bool,
    /// Per-file line counts decoded out of `paths.dat`, one per record, when
    /// the container declares `meta.dat` bit 14. Empty when it does not.
    ///
    /// Empty is not "the files have no lines" — it is "this container states no
    /// sizes", which is every trace without bit 14, and telling the two apart
    /// is the whole point of the table: a caller that cannot is back to the
    /// assumption the table exists to remove.
    line_counts: Vec<u64>,
}

impl InterningTablesReader {
    /// Open the interning tables from an already-open CTFS reader. Returns
    /// `Ok(None)` only when the container genuinely carries no binary interning
    /// tables — the caller then falls back to the legacy `events.log` /
    /// `paths.json` interning.
    ///
    /// EXISTENCE is decided by STRUCTURAL PRESENCE of `paths.dat`, not by
    /// `meta.dat` bit 12, which is only a hint (see the module docs).
    pub fn open(reader: &mut CtfsReader) -> Result<Option<InterningTablesReader>, String> {
        crate::retired_streams::refuse_retired_members(reader)?;
        // `meta.dat` is read best-effort: a still-recording trace has none yet,
        // and each flag below then reads as unset.
        let meta = reader.read_file("meta.dat").ok();
        let interning_tables_declared = meta.as_deref().is_some_and(meta_dat_has_interning_tables);
        // Bit 14 selects the `paths.dat` RECORD layout, independently of bit
        // 12's `funcs.dat`/`types.dat` selector. A still-recording trace with
        // no `meta.dat` yet reads as the bare layout, which is what it is.
        let has_line_counts = meta.as_deref().is_some_and(meta_dat_has_line_count_table);
        // Bit 4 frames `paths.dat` too. A column-aware trace writes Layout A —
        // `path_len, path, line_count, line_lengths…` — for EVERY path,
        // including one whose recorder surfaced no per-line counts, where the
        // record is still framed and simply states `line_count = 0`. Reading
        // that as a bare record hands the caller its own length prefix and a
        // trailing NUL as part of the file name.
        let column_aware = meta.as_deref().is_some_and(meta_dat_has_column_aware_steps);
        // `paths.dat` is written first and unconditionally by both writers, so
        // its presence is the container's own answer to "do I carry interning
        // tables". Absent ⇒ no binary tables (legacy path).
        if reader.file_size("paths.dat").is_none() {
            return Ok(None);
        }
        // Read each table; a missing data file once `paths.dat` exists is an
        // error (the writer always emits all four together).
        let paths = Self::load_table(reader, "paths")?;
        let funcs = Self::load_table(reader, "funcs")?;
        let types = Self::load_table(reader, "types")?;
        let varnames = Self::load_table(reader, "varnames")?;
        // Decoded eagerly, and a failure here fails the OPEN. The container
        // declared this layout, so a record that does not decode is a corrupt
        // trace; falling back to the bare layout would hand the caller a path
        // with its own length prefix inside it and put every file back on the
        // assumed stride.
        let line_counts = if has_line_counts {
            let mut counts = Vec::with_capacity(paths.count());
            for id in 0..paths.count() {
                counts.push(decode_line_count_path_record(&paths.record(id)?, id)?.1);
            }
            counts
        } else {
            Vec::new()
        };
        Ok(Some(InterningTablesReader {
            paths,
            funcs,
            types,
            varnames,
            interning_tables_declared,
            column_aware,
            line_counts,
        }))
    }

    fn load_table(reader: &mut CtfsReader, name: &str) -> Result<VarSizeTable, String> {
        let dat = reader
            .read_member(&format!("{name}.dat"))
            .map_err(|e| format!("{name}.dat missing despite paths.dat presence: {e}"))?;
        let off = reader
            .read_member(&format!("{name}.off"))
            .map_err(|e| format!("{name}.off missing despite paths.dat presence: {e}"))?;
        VarSizeTable::new(name, dat, off)
    }

    /// Number of interned source paths.
    pub fn path_count(&self) -> usize {
        self.paths.count()
    }

    /// Number of interned functions.
    pub fn func_count(&self) -> usize {
        self.funcs.count()
    }

    /// Number of interned types.
    pub fn type_count(&self) -> usize {
        self.types.count()
    }

    /// Number of interned variable names.
    pub fn varname_count(&self) -> usize {
        self.varnames.count()
    }

    /// Resolve a path id to its file path (raw bytes; UTF-8 for the recorders).
    pub fn path(&self, path_id: u64) -> Result<Vec<u8>, String> {
        let raw = self.paths.record(path_id as usize)?;
        if self.column_aware {
            // Layout A: the path is the framed payload, and the rest of the
            // record is its per-line table. Bit 4 permits a zero count.
            return Ok(decode_layout_a_path_record(&raw, path_id as usize)?.to_vec());
        }
        if self.line_counts.is_empty() {
            return Ok(raw.into_owned());
        }
        // The bit-14 line-count record: Layout A's framing without its per-line
        // table. `open` already decoded every one of these whole.
        Ok(decode_framed_path_record(&raw, path_id as usize)?.0.to_vec())
    }

    /// Whether `paths.dat` records are Layout A (`meta.dat` bit 4).
    pub fn is_column_aware(&self) -> bool {
        self.column_aware
    }

    /// A Layout A record's per-line addressable column counts, spelled out;
    /// empty for every record of a trace that is not column-aware.
    ///
    /// A `line_count = 0` record is the conventional table and is returned as
    /// its 100000 entries of 1024 (`internal-files.md` §"`paths.dat` Layout
    /// A"). A caller that lays out or resolves positions takes
    /// [`Self::path_file_table`] instead, which says which it is and keeps the
    /// conventional table as its rule.
    pub fn path_line_lengths(&self, path_id: u64) -> Result<Vec<u32>, String> {
        Ok(match self.path_file_table(path_id)? {
            None => Vec::new(),
            Some(FileTable::Conventional) => codetracer_trace_writer::column_aware::conventional_line_lengths(),
            Some(FileTable::Lines(lls)) => lls,
        })
    }

    /// A Layout A record's table: [`FileTable::Conventional`] for a record of
    /// `line_count = 0`, [`FileTable::Lines`] otherwise; `None` when the trace
    /// is not column-aware, whose records carry no table.
    pub fn path_file_table(&self, path_id: u64) -> Result<Option<FileTable>, String> {
        if !self.column_aware {
            return Ok(None);
        }
        let raw = self.paths.record(path_id as usize)?;
        decode_layout_a_path_record(&raw, path_id as usize)?;
        let (_, lls) = codetracer_trace_writer::column_aware::decode_path_record_layout_a(&raw)?;
        Ok(Some(FileTable::from_record(lls)))
    }

    /// Resolve a path id to its file path as a `String` (lossy UTF-8).
    pub fn path_str(&self, path_id: u64) -> Result<String, String> {
        Ok(String::from_utf8_lossy(&self.path(path_id)?).into_owned())
    }

    /// Every `paths.dat` entry's version, in path-id order.
    ///
    /// A source reload appends a SECOND record for the same path string
    /// (`internal-files.md` §"`paths.dat` path versions"), so a string can
    /// name several path ids. Entry `id`'s
    /// [`ordinal`](PathVersion::ordinal) is how many earlier ids carry its
    /// string — 0 for the first, 1 for the second — and
    /// [`count`](PathVersion::count) is how many ids carry it in all. Every
    /// entry of a trace without reloads is `{ ordinal: 0, count: 1 }`.
    ///
    /// The ordinal is not a reload count: a reload of a file that never runs
    /// again mints no entry, and a reload marker's `generation` starts at 2.
    /// Same answers as the Nim reader's `pathVersionOrdinal` /
    /// `pathVersionCount`. Linear in the number of paths; it reads every
    /// path, so it is computed on request rather than at open.
    pub fn path_versions(&self) -> Result<Vec<PathVersion>, String> {
        let total = self.path_count();
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(total);
        let mut seen: std::collections::HashMap<Vec<u8>, u64> = std::collections::HashMap::with_capacity(total);
        let mut out = Vec::with_capacity(total);
        for id in 0..total {
            let p = self.path(id as u64).map_err(|e| format!("paths.dat[{id}]: {e}"))?;
            let n = seen.entry(p.clone()).or_insert(0);
            out.push(PathVersion { ordinal: *n, count: 0 });
            *n += 1;
            payloads.push(p);
        }
        for (v, p) in out.iter_mut().zip(&payloads) {
            v.count = seen[p];
        }
        Ok(out)
    }

    /// Every path id whose `paths.dat` string is `path`, in path-id order —
    /// index 0 is the earliest version. Empty when no entry carries it.
    pub fn path_ids_for(&self, path: &[u8]) -> Result<Vec<u64>, String> {
        let mut ids = Vec::new();
        for id in 0..self.path_count() as u64 {
            if self.path(id)? == path {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    /// The line count this container RECORDS for `path_id`, or `None` when it
    /// records none.
    ///
    /// `None` means the container states no size for the file — every trace
    /// without `meta.dat` bit 14 — and never "the file has no lines": a zero
    /// count fails the open, because a file sized zero shares its base with the
    /// next file and the two are indistinguishable at decode.
    pub fn line_count(&self, path_id: u64) -> Option<u64> {
        self.line_counts.get(path_id as usize).copied()
    }

    /// The per-file line counts this container records, in path-id order.
    /// Empty when it records none — feed [`LinePositionSpace::from_line_counts`]
    /// with these and [`LinePositionSpace::uniform`] without them.
    pub fn line_counts(&self) -> &[u64] {
        &self.line_counts
    }

    /// Resolve a function id to its decoded record (`global_line_index` +
    /// name). A record that is not the spec's structured record is refused by
    /// name — see [`Self::bare_record_diagnosis`].
    pub fn func(&self, function_id: u64) -> Result<FuncRecord, String> {
        let raw = self.funcs.record(function_id as usize)?;
        decode_func_record(&raw).map_err(|e| self.bare_record_diagnosis("funcs.dat", function_id, e))
    }

    /// Resolve a type id to its decoded record (kind / lang_type /
    /// specific_info). Refused by name like [`Self::func`].
    pub fn type_record(&self, type_id: u64) -> Result<DecodedTypeRecord, String> {
        let raw = self.types.record(type_id as usize)?;
        decode_type_record(&raw).map_err(|e| self.bare_record_diagnosis("types.dat", type_id, e))
    }

    /// The refusal for a `funcs.dat` / `types.dat` record that does not decode
    /// as the spec's structured record — word for word the Nim reader's
    /// (`new_trace_reader.nim` `bareRecordDiagnosis`), so a caller sees one
    /// message whichever reader it uses.
    ///
    /// The spec's records have always been structured (`internal-files.md`
    /// "Interning Tables"); bit 12 only says the tables are present. The Nim
    /// writer wrote BARE NAMES into both tables until b891a0f (2026-09-15), with
    /// bit 12 clear, at schema versions 4 and 5 — so no version check refuses
    /// such a container, and the structured decoder's own message describes
    /// neither the record nor the remedy.
    fn bare_record_diagnosis(&self, table: &str, id: u64, decode_error: String) -> String {
        if self.interning_tables_declared {
            return decode_error;
        }
        format!(
            "{table} record {id} is not the spec's structured record (internal-files.md \"Interning Tables\"); \
             in a container with meta.dat bit 12 clear it is a bare name, the shape the Nim writer wrote before \
             b891a0f (2026-09-15). Such a container does not conform to the spec at any schema version and is \
             not read; re-record it with a current recorder. (Decoding it as a structured record reported: \
             {decode_error})"
        )
    }

    /// Resolve a variable-name id to its name (raw bytes; UTF-8 for recorders).
    pub fn varname(&self, name_id: u64) -> Result<Vec<u8>, String> {
        Ok(self.varnames.record(name_id as usize)?.into_owned())
    }

    /// Resolve a variable-name id to its name as a `String` (lossy UTF-8).
    pub fn varname_str(&self, name_id: u64) -> Result<String, String> {
        Ok(String::from_utf8_lossy(&self.varnames.record(name_id as usize)?).into_owned())
    }
}

/// Open the interning tables directly from a `.ct` file path. Returns `Ok(None)`
/// when the container carries no binary interning tables.
pub fn open_interning_tables(path: &std::path::Path) -> Result<Option<InterningTablesReader>, String> {
    let mut reader = CtfsReader::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    InterningTablesReader::open(&mut reader)
}
