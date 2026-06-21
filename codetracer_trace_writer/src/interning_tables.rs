//! Dedicated binary varint interning tables for materialized CTFS `.ct` traces.
//!
//! This is the M23d deliverable of the Trace-Based-Incremental-Testing
//! campaign (the fourth sub-milestone of M23 — "finish the trace-events.md
//! Event Stream Redesign"): an *additive*, backward-compatible emission of the
//! four interning tables — `paths.dat`/`paths.off`, `funcs.dat`/`funcs.off`,
//! `types.dat`/`types.off`, `varnames.dat`/`varnames.off` — out of the SAME
//! Path/Function/Type/VariableName interning the writer already does for
//! `events.log` and `paths.json`. It mirrors the M23a `steps.dat` split, the
//! M23b `values.dat` split, and the M23c `events.dat` split exactly: recorders
//! that opt in emit, in addition to the unchanged `events.log` / `paths.json`,
//! the binary interning tables plus their companion offset indices, gated by a
//! new `meta.dat` capability flag `has_interning_tables` (bit 12). Readers that
//! do not know the flag simply ignore the extra files, so old `.ct`s and old
//! readers keep working byte-for-byte.
//!
//! # Storage pattern: Variable-Size Record Table (`.dat` + `.off`)
//!
//! Unlike the chunked-Zstd streams (`steps.dat`/`values.dat`/`events.dat`), the
//! interning tables use the *Variable-Size Record Table* pattern from
//! `codetracer-trace-format-spec/internal-files.md` §"Variable-Size Record
//! Table (dat + off)":
//!
//! - **Data file** (`*.dat`): records appended sequentially, variable length,
//!   no inline length prefixes.
//! - **Offset file** (`*.off`): a fixed-size table of `u64` LE values; entry `i`
//!   is the byte offset of record `i` in the data file. There are `N + 1`
//!   entries for `N` records — the final entry is the total data length, so the
//!   length of record `i` is `off[i + 1] - off[i]` for every record (including
//!   the last) without a special case.
//!
//! To read record `i`: read `off[i]` and `off[i + 1]` (8 bytes each at
//! `i * 8` / `(i + 1) * 8`), then read `off[i + 1] - off[i]` bytes from the
//! data file at `off[i]`. This gives O(1) random access by id — no scan.
//!
//! # Record layouts (per `codetracer-trace-format-spec/internal-files.md`)
//!
//! ```text
//!   paths.dat   record = raw bytes (file path, UTF-8)
//!   varnames.dat record = raw bytes (variable name, UTF-8)
//!   funcs.dat   record = global_line_index: varint
//!                        name_len: varint, name: bytes
//!   types.dat   record = kind: u8
//!                        lang_type_len: varint, lang_type: bytes
//!                        specific_info: binary (CBOR of TypeSpecificInfo)
//! ```
//!
//! All ids are 0-based indices and are referenced as varints in the event
//! streams. The `paths.dat` / `varnames.dat` records have no inline length —
//! their length is recovered from the offset index, matching the spec's "raw
//! bytes" record format.
//!
//! # `funcs.dat` `global_line_index`
//!
//! Each function record stores a `global_line_index` derived from the
//! function's `(path_id, line)` via the same reversible packing the M23a
//! `steps.dat` execution stream uses ([`crate::step_stream::pack_global_line_index`]),
//! so a reader recovers the exact `(path_id, line)` the `Function` event
//! carried. When the canonical per-file `global_line_index` interning lands,
//! this packing is the single place to change.
//!
//! # Consistency with `events.log` / `paths.json`
//!
//! The records are built from the SAME Path/Function/Type/VariableName events
//! that feed `events.log` (and, for paths, the SAME `path_list` that feeds
//! `paths.json`), in the SAME order, so the i-th record in each `.dat` resolves
//! the id the event stream references. M23d does NOT migrate any consumer off
//! the existing interning — it only emits the binary tables additively.

use codetracer_trace_types::{FunctionRecord, TraceLowLevelEvent, TypeRecord};

use crate::step_stream::pack_global_line_index;

// --- varint helper (unsigned LEB128) ---

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

/// Encode one `funcs.dat` record (`global_line_index` + name) into `out`.
/// Mirrors the reader's `funcs.dat` decode and the spec record layout.
pub fn encode_func_record(global_line_index: u64, name: &[u8], out: &mut Vec<u8>) {
    encode_varint(global_line_index, out);
    encode_varint(name.len() as u64, out);
    out.extend_from_slice(name);
}

/// Encode one `types.dat` record (kind / lang_type / specific_info) into `out`.
/// `specific_info` is the already-serialized binary blob (CBOR of
/// `TypeSpecificInfo`).
pub fn encode_type_record(kind: u8, lang_type: &[u8], specific_info: &[u8], out: &mut Vec<u8>) {
    out.push(kind);
    encode_varint(lang_type.len() as u64, out);
    out.extend_from_slice(lang_type);
    out.extend_from_slice(specific_info);
}

/// Serialize a `TypeSpecificInfo` to the binary blob stored in a `types.dat`
/// record. CBOR keeps the structured `Struct`/`Pointer` variants round-trippable
/// while staying compact; `TypeSpecificInfo::None` is a tiny blob.
fn serialize_specific_info(info: &codetracer_trace_types::TypeSpecificInfo) -> Vec<u8> {
    cbor4ii::serde::to_vec(Vec::new(), info).expect("CBOR encode of TypeSpecificInfo failed")
}

/// The four encoded interning tables, each as a `.dat` data file plus its
/// companion `.off` offset index.
pub struct EncodedInterningTables {
    pub paths_dat: Vec<u8>,
    pub paths_off: Vec<u8>,
    pub funcs_dat: Vec<u8>,
    pub funcs_off: Vec<u8>,
    pub types_dat: Vec<u8>,
    pub types_off: Vec<u8>,
    pub varnames_dat: Vec<u8>,
    pub varnames_off: Vec<u8>,
}

/// Accumulates the four interning tables from the same event sequence that
/// feeds `events.log`, so the tables are guaranteed consistent with the ids the
/// event stream references.
///
/// The builder observes `Path` / `Function` / `Type` / `VariableName` events in
/// stream order and appends one record per event to the corresponding table.
/// The id an event stream references is exactly this append index (0-based), the
/// same id [`crate::abstract_trace_writer::AbstractTraceWriter`] assigns when it
/// interns the name.
#[derive(Default)]
pub struct InterningTablesBuilder {
    /// Source paths, in interning order. Record = raw UTF-8 path bytes.
    paths: Vec<Vec<u8>>,
    /// Functions, in interning order. Record = `(global_line_index, name)`.
    funcs: Vec<(u64, Vec<u8>)>,
    /// Types, in interning order. Record = `(kind, lang_type, specific_info)`.
    types: Vec<(u8, Vec<u8>, Vec<u8>)>,
    /// Variable names, in interning order. Record = raw UTF-8 name bytes.
    varnames: Vec<Vec<u8>>,
}

impl InterningTablesBuilder {
    pub fn new() -> Self {
        InterningTablesBuilder::default()
    }

    /// Feed one event in stream order. Only the four interning events contribute
    /// records; all others are ignored. The legacy `Variable` event (tag 3) is a
    /// backward-compat alias of `VariableName` and is interned the same way so
    /// the table stays aligned with the writer's variable interning.
    pub fn observe(&mut self, event: &TraceLowLevelEvent) {
        match event {
            TraceLowLevelEvent::Path(path) => {
                self.paths.push(path.to_string_lossy().into_owned().into_bytes());
            }
            TraceLowLevelEvent::Function(FunctionRecord { path_id, line, name }) => {
                let gli = pack_global_line_index(path_id.0, line.0);
                self.funcs.push((gli, name.clone().into_bytes()));
            }
            TraceLowLevelEvent::Type(TypeRecord { kind, lang_type, specific_info }) => {
                self.types.push((*kind as u8, lang_type.clone().into_bytes(), serialize_specific_info(specific_info)));
            }
            TraceLowLevelEvent::VariableName(name) | TraceLowLevelEvent::Variable(name) => {
                self.varnames.push(name.clone().into_bytes());
            }
            _ => {}
        }
    }

    /// Number of path records accumulated so far.
    pub fn path_count(&self) -> usize {
        self.paths.len()
    }

    /// Number of function records accumulated so far.
    pub fn func_count(&self) -> usize {
        self.funcs.len()
    }

    /// Number of type records accumulated so far.
    pub fn type_count(&self) -> usize {
        self.types.len()
    }

    /// Number of variable-name records accumulated so far.
    pub fn varname_count(&self) -> usize {
        self.varnames.len()
    }

    /// Finalize: encode all four `.dat` data files and their `.off` offset
    /// indices.
    pub fn finish(self) -> EncodedInterningTables {
        // paths.dat / varnames.dat are raw-byte tables.
        let (paths_dat, paths_off) = encode_raw_table(&self.paths);
        let (varnames_dat, varnames_off) = encode_raw_table(&self.varnames);

        // funcs.dat: each record is global_line_index + name.
        let mut func_records: Vec<Vec<u8>> = Vec::with_capacity(self.funcs.len());
        for (gli, name) in &self.funcs {
            let mut rec = Vec::new();
            encode_func_record(*gli, name, &mut rec);
            func_records.push(rec);
        }
        let (funcs_dat, funcs_off) = encode_raw_table(&func_records);

        // types.dat: each record is kind + lang_type + specific_info.
        let mut type_records: Vec<Vec<u8>> = Vec::with_capacity(self.types.len());
        for (kind, lang_type, specific_info) in &self.types {
            let mut rec = Vec::new();
            encode_type_record(*kind, lang_type, specific_info, &mut rec);
            type_records.push(rec);
        }
        let (types_dat, types_off) = encode_raw_table(&type_records);

        EncodedInterningTables {
            paths_dat,
            paths_off,
            funcs_dat,
            funcs_off,
            types_dat,
            types_off,
            varnames_dat,
            varnames_off,
        }
    }
}

/// Encode a sequence of already-serialized records into a Variable-Size Record
/// Table: the concatenated data file plus the `u64`-LE offset index. The offset
/// index has `records.len() + 1` entries — the trailing entry is the total data
/// length, so a reader resolves record `i`'s length as `off[i + 1] - off[i]`
/// uniformly (including the last record).
pub fn encode_raw_table(records: &[Vec<u8>]) -> (Vec<u8>, Vec<u8>) {
    let mut dat: Vec<u8> = Vec::new();
    let mut off: Vec<u8> = Vec::with_capacity((records.len() + 1) * 8);
    for rec in records {
        off.extend_from_slice(&(dat.len() as u64).to_le_bytes());
        dat.extend_from_slice(rec);
    }
    // Trailing sentinel offset = total data length.
    off.extend_from_slice(&(dat.len() as u64).to_le_bytes());
    (dat, off)
}

#[cfg(test)]
mod tests {
    use super::*;
    use codetracer_trace_types::{Line, PathId, TypeKind, TypeSpecificInfo};
    use std::path::PathBuf;

    fn read_off(off: &[u8], i: usize) -> u64 {
        let base = i * 8;
        u64::from_le_bytes(off[base..base + 8].try_into().unwrap())
    }

    /// Resolve record `i` from a `.dat`+`.off` pair the way the reader will:
    /// random access, no scan.
    fn read_record<'a>(dat: &'a [u8], off: &[u8], i: usize) -> &'a [u8] {
        let start = read_off(off, i) as usize;
        let end = read_off(off, i + 1) as usize;
        &dat[start..end]
    }

    #[test]
    fn raw_table_offsets_and_lengths() {
        let recs = vec![b"abc".to_vec(), Vec::new(), b"hello".to_vec()];
        let (dat, off) = encode_raw_table(&recs);
        // N + 1 offsets.
        assert_eq!(off.len(), (recs.len() + 1) * 8);
        assert_eq!(read_record(&dat, &off, 0), b"abc");
        // An empty record resolves correctly (start == end).
        assert_eq!(read_record(&dat, &off, 1), b"");
        assert_eq!(read_record(&dat, &off, 2), b"hello");
        // Trailing sentinel == total length.
        assert_eq!(read_off(&off, 3) as usize, dat.len());
    }

    #[test]
    fn builder_records_each_table_in_interning_order() {
        let mut b = InterningTablesBuilder::new();
        b.observe(&TraceLowLevelEvent::Path(PathBuf::from("/a.rs")));
        b.observe(&TraceLowLevelEvent::Path(PathBuf::from("/b.rs")));
        b.observe(&TraceLowLevelEvent::Function(FunctionRecord {
            path_id: PathId(1),
            line: Line(42),
            name: "main".to_string(),
        }));
        b.observe(&TraceLowLevelEvent::Type(TypeRecord {
            kind: TypeKind::Int,
            lang_type: "i64".to_string(),
            specific_info: TypeSpecificInfo::None,
        }));
        b.observe(&TraceLowLevelEvent::VariableName("x".to_string()));
        b.observe(&TraceLowLevelEvent::VariableName("y".to_string()));
        // The legacy `Variable` alias also interns a varname.
        b.observe(&TraceLowLevelEvent::Variable("z".to_string()));

        assert_eq!(b.path_count(), 2);
        assert_eq!(b.func_count(), 1);
        assert_eq!(b.type_count(), 1);
        assert_eq!(b.varname_count(), 3);

        let tables = b.finish();

        // paths by id.
        assert_eq!(read_record(&tables.paths_dat, &tables.paths_off, 0), b"/a.rs");
        assert_eq!(read_record(&tables.paths_dat, &tables.paths_off, 1), b"/b.rs");

        // varnames by id (including the legacy-alias one).
        assert_eq!(read_record(&tables.varnames_dat, &tables.varnames_off, 0), b"x");
        assert_eq!(read_record(&tables.varnames_dat, &tables.varnames_off, 2), b"z");

        // funcs: decode the single record back to (global_line_index, name).
        let func_rec = read_record(&tables.funcs_dat, &tables.funcs_off, 0);
        let expected_gli = pack_global_line_index(1, 42);
        let mut buf = Vec::new();
        encode_func_record(expected_gli, b"main", &mut buf);
        assert_eq!(func_rec, &buf[..]);

        // types: kind byte is the TypeKind ordinal.
        let type_rec = read_record(&tables.types_dat, &tables.types_off, 0);
        assert_eq!(type_rec[0], TypeKind::Int as u8);
    }
}
