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
//! # Detection is by PRESENCE; `meta.dat` bit 12 selects only the LAYOUT
//!
//! EXISTENCE of the binary interning tables is decided by STRUCTURAL PRESENCE of
//! `paths.dat` (written first and unconditionally by every writer that emits the
//! tables), never by the `has_interning_tables` capability flag (bit 12) in
//! `meta.dat`. Bit 12 is NOT a presence check: the production Nim
//! `MultiStreamTraceWriter` emits all four tables but leaves the bit clear,
//! because the bit means "these records are in the M23d STRUCTURED layout" and
//! its records are in the simpler PLAIN layout. A reader that took the flag as a
//! presence gate found nothing on every real trace — a blank Variables pane over
//! data sitting on disk (this is exactly the gate removed from the
//! call/step/value/io stream readers; it was mirrored in db-backend's
//! `ctfs_trace_reader::interning_tables`, and this brought the format crate into
//! line). `meta.dat` is read best-effort and its bit 12 chooses ONLY how to
//! decode ([`RecordLayout`]). A missing/absent `meta.dat` (a still-recording
//! trace) therefore reads as PLAIN rather than refusing the tables.
//!
//! The legacy `events.log` / `paths.json` interning remains the source of truth;
//! M23d does NOT migrate any consumer off it, and this reader is additive.
//!
//! # Record layouts (mirrors `codetracer_trace_writer::interning_tables` and
//! db-backend's `ctfs_trace_reader::interning_tables`)
//!
//! ```text
//!   [`RecordLayout::Plain`] — what the production Nim writer emits (bit 12 clear)
//!     paths.dat / funcs.dat / types.dat / varnames.dat = raw name bytes
//!
//!   [`RecordLayout::Structured`] — M23d (bit 12 set)
//!     paths.dat / varnames.dat record = raw bytes
//!     funcs.dat   record = global_line_index: varint, name_len: varint, name: bytes
//!     types.dat   record = kind: u8, lang_type_len: varint, lang_type: bytes,
//!                          specific_info: binary (CBOR of TypeSpecificInfo)
//! ```

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::{TypeKind, TypeSpecificInfo};
use codetracer_trace_writer::meta_dat::meta_dat_has_interning_tables;
use codetracer_trace_writer::step_stream::unpack_global_line_index;
use num_traits::FromPrimitive;

/// A decoded `funcs.dat` record: the `global_line_index` and the function name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuncRecord {
    /// Packed source location of the function (see
    /// `codetracer_trace_writer::step_stream::pack_global_line_index`); use
    /// [`FuncRecord::path_id_and_line`] to recover `(path_id, line)`.
    pub global_line_index: u64,
    /// The function name (raw bytes; UTF-8 for the recorders that produce it).
    pub name: Vec<u8>,
}

impl FuncRecord {
    /// Recover the `(path_id, line)` the function's `global_line_index` was
    /// packed from. Inverse of the writer's `pack_global_line_index`.
    pub fn path_id_and_line(&self) -> (usize, i64) {
        unpack_global_line_index(self.global_line_index)
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

/// A single Variable-Size Record Table: the `.dat` data file plus its parsed
/// `.off` offset index. Records are resolved by 0-based id with O(1) random
/// access.
struct VarSizeTable {
    /// The concatenated record bytes.
    dat: Vec<u8>,
    /// The offset index: `record_count + 1` `u64` byte offsets (the trailing
    /// entry is the total data length, so record `i`'s length is
    /// `offsets[i + 1] - offsets[i]` for every record).
    offsets: Vec<u64>,
}

impl VarSizeTable {
    /// Load a table from a `.dat` data file and a `.off` offset index.
    fn new(name: &str, dat: Vec<u8>, off: &[u8]) -> Result<VarSizeTable, String> {
        if !off.len().is_multiple_of(8) {
            return Err(format!("{name}.off: length {} is not a multiple of 8", off.len()));
        }
        let mut offsets = Vec::with_capacity(off.len() / 8);
        let mut pos = 0usize;
        while pos + 8 <= off.len() {
            offsets.push(u64::from_le_bytes(off[pos..pos + 8].try_into().unwrap()));
            pos += 8;
        }
        // A valid offset index has at least the trailing sentinel. An empty
        // table is exactly one sentinel entry (== 0).
        if offsets.is_empty() {
            return Err(format!("{name}.off: empty (missing the trailing sentinel offset)"));
        }
        Ok(VarSizeTable { dat, offsets })
    }

    /// Number of records in the table.
    fn count(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Resolve record `id` to its raw bytes via the offset index (random access,
    /// no scan).
    fn record(&self, id: usize) -> Result<&[u8], String> {
        if id >= self.count() {
            return Err(format!("interning table: id {id} out of range (count {})", self.count()));
        }
        let start = self.offsets[id] as usize;
        let end = self.offsets[id + 1] as usize;
        if start > end || end > self.dat.len() {
            return Err(format!(
                "interning table: record {id} offsets [{start}, {end}) out of range (dat len {})",
                self.dat.len()
            ));
        }
        Ok(&self.dat[start..end])
    }
}

/// Which on-disk record layout the four interning tables use. Selected by
/// `meta.dat` bit 12 (`has_interning_tables`) — the flag is a LAYOUT selector,
/// never a presence gate (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordLayout {
    /// M23d structured records: `funcs.dat` carries a `global_line_index`
    /// varint prefix and `types.dat` a kind byte + CBOR specific-info tail.
    Structured,
    /// What the production Nim writer emits (bit 12 clear): every table record
    /// is raw name bytes, with no varint prefix or kind byte.
    Plain,
}

/// A reader over a container's binary interning tables, resolving interned
/// records by id with O(1) random access.
pub struct InterningTablesReader {
    paths: VarSizeTable,
    funcs: VarSizeTable,
    types: VarSizeTable,
    varnames: VarSizeTable,
    /// How `funcs.dat` / `types.dat` records are decoded (bit 12 layout
    /// selector; `paths.dat` / `varnames.dat` are raw bytes in both layouts).
    layout: RecordLayout,
}

impl InterningTablesReader {
    /// Open the interning tables from an already-open CTFS reader. Returns
    /// `Ok(None)` only when the container genuinely carries no binary interning
    /// tables — the caller then falls back to the legacy `events.log` /
    /// `paths.json` interning.
    ///
    /// EXISTENCE is decided by STRUCTURAL PRESENCE of `paths.dat`, not by
    /// `meta.dat` bit 12; the flag selects only the decode [`RecordLayout`]. See
    /// the module docs for why the two are separate (the production Nim writer
    /// emits the tables with bit 12 clear).
    pub fn open(reader: &mut CtfsReader) -> Result<Option<InterningTablesReader>, String> {
        // `meta.dat` is read best-effort: a missing/absent one (still-recording
        // trace) simply reads as the PLAIN layout rather than refusing the
        // tables.
        let layout = match reader.read_file("meta.dat") {
            Ok(meta) if meta_dat_has_interning_tables(&meta) => RecordLayout::Structured,
            _ => RecordLayout::Plain,
        };
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
        Ok(Some(InterningTablesReader {
            paths,
            funcs,
            types,
            varnames,
            layout,
        }))
    }

    /// Which on-disk record layout this reader decodes (bit 12 selector).
    pub fn layout(&self) -> RecordLayout {
        self.layout
    }

    fn load_table(reader: &mut CtfsReader, name: &str) -> Result<VarSizeTable, String> {
        let dat = reader
            .read_file(&format!("{name}.dat"))
            .map_err(|e| format!("{name}.dat missing despite paths.dat presence: {e}"))?;
        let off = reader
            .read_file(&format!("{name}.off"))
            .map_err(|e| format!("{name}.off missing despite paths.dat presence: {e}"))?;
        VarSizeTable::new(name, dat, &off)
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
        Ok(self.paths.record(path_id as usize)?.to_vec())
    }

    /// Resolve a path id to its file path as a `String` (lossy UTF-8).
    pub fn path_str(&self, path_id: u64) -> Result<String, String> {
        Ok(String::from_utf8_lossy(self.paths.record(path_id as usize)?).into_owned())
    }

    /// Resolve a function id to its decoded record (`global_line_index` + name).
    ///
    /// In the PLAIN layout (the production Nim writer) `funcs.dat` records are
    /// raw name bytes with no `global_line_index` prefix, so it is stubbed to
    /// `0` — parity with db-backend's `RecordLayout::Plain` and the Nim FFI
    /// reader, which stub the same field rather than lose data that is not on
    /// disk in that layout.
    pub fn func(&self, function_id: u64) -> Result<FuncRecord, String> {
        let raw = self.funcs.record(function_id as usize)?;
        match self.layout {
            RecordLayout::Plain => Ok(FuncRecord {
                global_line_index: 0,
                name: raw.to_vec(),
            }),
            RecordLayout::Structured => {
                let mut pos = 0usize;
                let global_line_index = decode_varint(raw, &mut pos)?;
                let name_len = decode_varint(raw, &mut pos)? as usize;
                if pos + name_len > raw.len() {
                    return Err(format!("funcs.dat: record {function_id} name extends past record"));
                }
                let name = raw[pos..pos + name_len].to_vec();
                Ok(FuncRecord { global_line_index, name })
            }
        }
    }

    /// Resolve a type id to its decoded record (kind / lang_type / specific_info).
    ///
    /// In the PLAIN layout `types.dat` records are the raw type NAME only, so the
    /// kind degrades to [`TypeKind::Raw`] and specific-info to
    /// [`TypeSpecificInfo::None`] — again matching db-backend's
    /// `RecordLayout::Plain` and the Nim FFI reader.
    pub fn type_record(&self, type_id: u64) -> Result<DecodedTypeRecord, String> {
        let raw = self.types.record(type_id as usize)?;
        match self.layout {
            RecordLayout::Plain => Ok(DecodedTypeRecord {
                kind: TypeKind::Raw as u8,
                lang_type: raw.to_vec(),
                specific_info: TypeSpecificInfo::None,
            }),
            RecordLayout::Structured => {
                if raw.is_empty() {
                    return Err(format!("types.dat: record {type_id} is empty (missing kind byte)"));
                }
                let kind = raw[0];
                let mut pos = 1usize;
                let lang_type_len = decode_varint(raw, &mut pos)? as usize;
                if pos + lang_type_len > raw.len() {
                    return Err(format!("types.dat: record {type_id} lang_type extends past record"));
                }
                let lang_type = raw[pos..pos + lang_type_len].to_vec();
                pos += lang_type_len;
                // The remainder is the CBOR-encoded TypeSpecificInfo blob.
                let specific_info: TypeSpecificInfo = cbor4ii::serde::from_slice(&raw[pos..])
                    .map_err(|e| format!("types.dat: record {type_id} specific_info CBOR decode failed: {e}"))?;
                Ok(DecodedTypeRecord {
                    kind,
                    lang_type,
                    specific_info,
                })
            }
        }
    }

    /// Resolve a variable-name id to its name (raw bytes; UTF-8 for recorders).
    pub fn varname(&self, name_id: u64) -> Result<Vec<u8>, String> {
        Ok(self.varnames.record(name_id as usize)?.to_vec())
    }

    /// Resolve a variable-name id to its name as a `String` (lossy UTF-8).
    pub fn varname_str(&self, name_id: u64) -> Result<String, String> {
        Ok(String::from_utf8_lossy(self.varnames.record(name_id as usize)?).into_owned())
    }
}

/// Open the interning tables directly from a `.ct` file path. Returns `Ok(None)`
/// when the container carries no binary interning tables.
pub fn open_interning_tables(path: &std::path::Path) -> Result<Option<InterningTablesReader>, String> {
    let mut reader = CtfsReader::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    InterningTablesReader::open(&mut reader)
}
