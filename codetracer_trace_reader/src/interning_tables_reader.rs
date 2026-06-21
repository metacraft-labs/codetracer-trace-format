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
//! The tables are gated by the `has_interning_tables` capability flag (bit 12)
//! in `meta.dat`. A reader that does not see the flag, or a container without
//! the table files, simply has no binary interning tables — the legacy
//! `events.log` / `paths.json` interning remains the source of truth. M23d does
//! NOT migrate any consumer off that legacy interning; this reader is additive.
//!
//! # Record layouts (mirrors `codetracer_trace_writer::interning_tables`)
//!
//! ```text
//!   paths.dat / varnames.dat record = raw bytes
//!   funcs.dat   record = global_line_index: varint, name_len: varint, name: bytes
//!   types.dat   record = kind: u8, lang_type_len: varint, lang_type: bytes,
//!                        specific_info: binary (CBOR of TypeSpecificInfo)
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
            return Err(format!("interning table: record {id} offsets [{start}, {end}) out of range (dat len {})", self.dat.len()));
        }
        Ok(&self.dat[start..end])
    }
}

/// A reader over a container's binary interning tables, resolving interned
/// records by id with O(1) random access.
pub struct InterningTablesReader {
    paths: VarSizeTable,
    funcs: VarSizeTable,
    types: VarSizeTable,
    varnames: VarSizeTable,
}

impl InterningTablesReader {
    /// Open the interning tables from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no binary interning tables (no
    /// `meta.dat` flag, or the table files are absent) — the caller falls back
    /// to the legacy `events.log` / `paths.json` interning.
    pub fn open(reader: &mut CtfsReader) -> Result<Option<InterningTablesReader>, String> {
        let has_flag = match reader.read_file("meta.dat") {
            Ok(meta) => meta_dat_has_interning_tables(&meta),
            Err(_) => false,
        };
        if !has_flag {
            return Ok(None);
        }
        // Read each table; a missing data file with the flag set is an error
        // (the writer always emits all four together).
        let paths = Self::load_table(reader, "paths")?;
        let funcs = Self::load_table(reader, "funcs")?;
        let types = Self::load_table(reader, "types")?;
        let varnames = Self::load_table(reader, "varnames")?;
        Ok(Some(InterningTablesReader {
            paths,
            funcs,
            types,
            varnames,
        }))
    }

    fn load_table(reader: &mut CtfsReader, name: &str) -> Result<VarSizeTable, String> {
        let dat = reader
            .read_file(&format!("{name}.dat"))
            .map_err(|e| format!("{name}.dat missing despite has_interning_tables flag: {e}"))?;
        let off = reader
            .read_file(&format!("{name}.off"))
            .map_err(|e| format!("{name}.off missing despite has_interning_tables flag: {e}"))?;
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
    pub fn func(&self, function_id: u64) -> Result<FuncRecord, String> {
        let raw = self.funcs.record(function_id as usize)?;
        let mut pos = 0usize;
        let global_line_index = decode_varint(raw, &mut pos)?;
        let name_len = decode_varint(raw, &mut pos)? as usize;
        if pos + name_len > raw.len() {
            return Err(format!("funcs.dat: record {function_id} name extends past record"));
        }
        let name = raw[pos..pos + name_len].to_vec();
        Ok(FuncRecord { global_line_index, name })
    }

    /// Resolve a type id to its decoded record (kind / lang_type / specific_info).
    pub fn type_record(&self, type_id: u64) -> Result<DecodedTypeRecord, String> {
        let raw = self.types.record(type_id as usize)?;
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
        let specific_info: TypeSpecificInfo =
            cbor4ii::serde::from_slice(&raw[pos..]).map_err(|e| format!("types.dat: record {type_id} specific_info CBOR decode failed: {e}"))?;
        Ok(DecodedTypeRecord {
            kind,
            lang_type,
            specific_info,
        })
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
