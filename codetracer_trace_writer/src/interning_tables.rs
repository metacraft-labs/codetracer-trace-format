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
//! Each function record stores the address of its declaration site in the
//! trace's global position space — the same address the `steps.dat` execution
//! stream would carry for a step at that `(path_id, line)`, computed through
//! the same [`crate::line_position::LinePositionSpace`]. A reader rebuilds the
//! space from `paths.dat` and resolves the address back to `(path_id, line)`.
//!
//! ## The canonical Nim writer does not write this field
//!
//! `codetracer-trace-format-nim`'s `ensureFunctionId` appends the function name
//! and nothing else, so its `funcs.dat` record is bare UTF-8 bytes with no
//! address at all — the same record shape as its line-only `paths.dat`.
//! Neither writer matches the spec's `global_line_index (varint) + name (bytes)`
//! for both writers at once, and reconciling them is a change to the Nim writer
//! and to both readers in one step. What is settled here is the *address*: when
//! a `funcs.dat` record carries one, it is the prefix sum and not a bit-field
//! packing. The record *shape* is deliberately left to the follow-on that can
//! change both writers together.
//!
//! # Consistency with `events.log` / `paths.json`
//!
//! The records are built from the SAME Path/Function/Type/VariableName events
//! that feed `events.log` (and, for paths, the SAME `path_list` that feeds
//! `paths.json`), in the SAME order, so the i-th record in each `.dat` resolves
//! the id the event stream references. M23d does NOT migrate any consumer off
//! the existing interning — it only emits the binary tables additively.

use codetracer_trace_types::{FunctionRecord, TraceLowLevelEvent, TypeRecord};

use crate::line_position::LinePositionSpace;

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
    /// Source paths, in interning order. Record = raw UTF-8 path bytes in
    /// line-only mode; the self-describing Layout A record when
    /// [`InterningTablesBuilder::column_aware`] is set — see
    /// [`InterningTablesBuilder::set_column_aware`].
    paths: Vec<Vec<u8>>,
    /// Per-path addressable column counts, positionally parallel to `paths`.
    /// Only consulted in column-aware mode; a path whose recorder supplied no
    /// table gets an empty entry and a `line_count = 0` record.
    path_line_lengths: Vec<Vec<u32>>,
    /// Whether `paths.dat` records use spec Layout A. Trace-global, and must be
    /// decided before the first `Path` event is observed.
    column_aware: bool,
    /// Functions, in interning order. Record = `(global_line_index, name)`.
    funcs: Vec<(u64, Vec<u8>)>,
    /// The trace's address space, grown as `Path` events intern files, so a
    /// function's declaration site is addressed exactly as a step there would
    /// be.
    space: LinePositionSpace,
    /// Types, in interning order. Record = `(kind, lang_type, specific_info)`.
    types: Vec<(u8, Vec<u8>, Vec<u8>)>,
    /// Variable names, in interning order. Record = raw UTF-8 name bytes.
    varnames: Vec<Vec<u8>>,
    /// Whether every `paths.dat` record carries its file's line count
    /// (`meta.dat` bit 14): `path_len + path + line_count`.
    line_count_table: bool,
    /// Per-path line counts, parallel to `paths`, when `line_count_table`.
    path_line_counts: Vec<u64>,
    /// The line count the NEXT `Path` event's record carries and its file is
    /// sized with. Consumed by that event.
    next_path_line_count: Option<u64>,
    /// The bytes a name stands for when they are not UTF-8: a function,
    /// type or variable name interned under a key of this map is recorded as
    /// these bytes. See [`InterningTablesBuilder::register_raw_name`].
    raw_names: std::collections::HashMap<String, Vec<u8>>,
}

impl InterningTablesBuilder {
    pub fn new() -> Self {
        InterningTablesBuilder::default()
    }

    /// Switch `paths.dat` to spec Layout A (`path_len` + path bytes +
    /// `line_count` + zigzag-delta `line_lengths`).
    ///
    /// Trace-global and must be called before the first `Path` event, exactly
    /// as on the Nim side: the flag decides the record shape, and a trace whose
    /// first records are bare bytes and whose later ones are Layout A cannot be
    /// decoded either way. The corresponding `meta.dat` bit is
    /// [`crate::meta_dat::FLAG_HAS_COLUMN_AWARE_STEPS`], and a reader keys off
    /// that bit to choose the parse.
    pub fn set_column_aware(&mut self, column_aware: bool) {
        self.column_aware = column_aware;
    }

    /// Switch `paths.dat` to the line-count-table record
    /// (`internal-files.md` §"`paths.dat` line-count table"). Trace-global;
    /// must be set before the first `Path` event.
    pub fn set_line_count_table(&mut self, on: bool) {
        self.line_count_table = on;
    }

    /// The line count the next `Path` event is recorded and sized with.
    pub fn set_next_path_line_count(&mut self, line_count: u64) {
        self.next_path_line_count = Some(line_count);
    }

    /// Whether `paths.dat` records will use Layout A.
    pub fn is_column_aware(&self) -> bool {
        self.column_aware
    }

    /// Attach the per-line addressable column counts for the path with
    /// interning id `path_id`.
    ///
    /// Called by the writer when a recorder registers a path together with its
    /// line lengths. Ignored when the builder is not column-aware, so a
    /// line-only trace's `paths.dat` stays byte-for-byte what it was.
    pub fn set_path_line_lengths(&mut self, path_id: usize, line_lengths: &[u32]) {
        if !self.column_aware {
            return;
        }
        if self.path_line_lengths.len() <= path_id {
            self.path_line_lengths.resize(path_id + 1, Vec::new());
        }
        self.path_line_lengths[path_id] = line_lengths.to_vec();
    }

    /// Record the name interned under `key` as `bytes`. Interning tables
    /// store names as bytes, which need not be UTF-8; a writer whose events
    /// carry names as `String` interns such a name under a key no UTF-8 name
    /// equals and registers its bytes here.
    pub fn register_raw_name(&mut self, key: String, bytes: Vec<u8>) {
        self.raw_names.insert(key, bytes);
    }

    fn name_bytes(&self, name: &str) -> Vec<u8> {
        self.raw_names.get(name).cloned().unwrap_or_else(|| name.as_bytes().to_vec())
    }

    /// Feed one event in stream order. Only the four interning events contribute
    /// records; all others are ignored. The legacy `Variable` event (tag 3) is a
    /// backward-compat alias of `VariableName` and is interned the same way so
    /// the table stays aligned with the writer's variable interning.
    pub fn observe(&mut self, event: &TraceLowLevelEvent) {
        match event {
            TraceLowLevelEvent::Path(path) => {
                self.paths.push(path.as_os_str().as_encoded_bytes().to_vec());
                match self.next_path_line_count.take() {
                    Some(count) => {
                        self.space.push_file(count);
                        self.path_line_counts.push(count);
                    }
                    None => {
                        self.space.ensure_file(self.paths.len() - 1);
                        self.path_line_counts.push(0);
                    }
                }
            }
            TraceLowLevelEvent::Function(FunctionRecord { path_id, line, name }) => {
                let gli = self.space.global_index(path_id.0, line.0);
                let name = self.name_bytes(name);
                self.funcs.push((gli, name));
            }
            TraceLowLevelEvent::Type(TypeRecord {
                kind,
                lang_type,
                specific_info,
            }) => {
                let lang_type = self.name_bytes(lang_type);
                self.types.push((*kind as u8, lang_type, serialize_specific_info(specific_info)));
            }
            TraceLowLevelEvent::VariableName(name) | TraceLowLevelEvent::Variable(name) => {
                let name = self.name_bytes(name);
                self.varnames.push(name);
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

    /// Append record `id` of table `table` (0 `paths`, 1 `funcs`, 2 `types`,
    /// 3 `varnames`) to `out`.
    pub fn append_record(&self, table: usize, id: usize, out: &mut Vec<u8>) {
        match table {
            0 if !self.column_aware && !self.line_count_table => out.extend_from_slice(&self.paths[id]),
            0 => out.extend_from_slice(&self.path_record(id)),
            1 => {
                let (gli, name) = &self.funcs[id];
                encode_func_record(*gli, name, out);
            }
            2 => {
                let (kind, lang_type, specific_info) = &self.types[id];
                encode_type_record(*kind, lang_type, specific_info, out);
            }
            _ => out.extend_from_slice(&self.varnames[id]),
        }
    }

    /// The number of records table `table` holds (numbered as in
    /// [`Self::append_record`]).
    pub fn count(&self, table: usize) -> usize {
        match table {
            0 => self.paths.len(),
            1 => self.funcs.len(),
            2 => self.types.len(),
            _ => self.varnames.len(),
        }
    }

    /// The `paths.dat` record of path `id`, in the trace's layout.
    pub fn path_record(&self, id: usize) -> Vec<u8> {
        let raw_path = &self.paths[id];
        if self.column_aware {
            let empty: Vec<u32> = Vec::new();
            let lls = self.path_line_lengths.get(id).unwrap_or(&empty);
            crate::column_aware::encode_path_bytes_record_layout_a(raw_path, lls)
        } else if self.line_count_table {
            let mut rec = Vec::with_capacity(raw_path.len() + 12);
            encode_varint(raw_path.len() as u64, &mut rec);
            rec.extend_from_slice(raw_path);
            encode_varint(self.path_line_counts.get(id).copied().unwrap_or(0), &mut rec);
            rec
        } else {
            raw_path.clone()
        }
    }

    /// The `funcs.dat` record of function `id`: `global_line_index` + name.
    pub fn func_record(&self, id: usize) -> Vec<u8> {
        let (gli, name) = &self.funcs[id];
        let mut rec = Vec::new();
        encode_func_record(*gli, name, &mut rec);
        rec
    }

    /// The `types.dat` record of type `id`: kind + lang_type + specific_info.
    pub fn type_record(&self, id: usize) -> Vec<u8> {
        let (kind, lang_type, specific_info) = &self.types[id];
        let mut rec = Vec::new();
        encode_type_record(*kind, lang_type, specific_info, &mut rec);
        rec
    }

    /// The `varnames.dat` record of name `id`: its raw bytes.
    pub fn varname_record(&self, id: usize) -> Vec<u8> {
        self.varnames[id].clone()
    }

    /// Finalize: encode all four `.dat` data files and their `.off` offset
    /// indices.
    pub fn finish(self) -> EncodedInterningTables {
        let table = |n: usize, f: &dyn Fn(usize) -> Vec<u8>| encode_raw_table(&(0..n).map(f).collect::<Vec<_>>());
        let (paths_dat, paths_off) = table(self.paths.len(), &|i| self.path_record(i));
        let (funcs_dat, funcs_off) = table(self.funcs.len(), &|i| self.func_record(i));
        let (types_dat, types_off) = table(self.types.len(), &|i| self.type_record(i));
        let (varnames_dat, varnames_off) = table(self.varnames.len(), &|i| self.varname_record(i));
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

        // funcs: the record carries the declaration site's address in the
        // trace's own space, and that address resolves back to (path 1, line 42).
        let func_rec = read_record(&tables.funcs_dat, &tables.funcs_off, 0);
        let mut space = LinePositionSpace::uniform(2);
        let expected_gli = space.global_index(1, 42);
        assert_eq!(space.resolve(expected_gli), Ok((1, 42)));
        let mut buf = Vec::new();
        encode_func_record(expected_gli, b"main", &mut buf);
        assert_eq!(func_rec, &buf[..]);

        // types: kind byte is the TypeKind ordinal.
        let type_rec = read_record(&tables.types_dat, &tables.types_off, 0);
        assert_eq!(type_rec[0], TypeKind::Int as u8);
    }

    #[test]
    fn column_aware_paths_dat_is_layout_a_and_line_only_is_unchanged() {
        // Same two paths through both modes. The line-only side is the control:
        // if Layout A leaked into it, every existing container's paths.dat
        // would change shape, so the two are asserted against each other rather
        // than each on its own.
        let mut line_only = InterningTablesBuilder::new();
        line_only.observe(&TraceLowLevelEvent::Path(PathBuf::from("/a.rs")));
        line_only.observe(&TraceLowLevelEvent::Path(PathBuf::from("/bb.rs")));
        // Line lengths offered but ignored, exactly as the Nim writer ignores
        // them when the trace is not column-aware.
        line_only.set_path_line_lengths(0, &[10, 20]);
        let line_tables = line_only.finish();
        assert_eq!(read_record(&line_tables.paths_dat, &line_tables.paths_off, 0), b"/a.rs");
        assert_eq!(read_record(&line_tables.paths_dat, &line_tables.paths_off, 1), b"/bb.rs");

        let mut column = InterningTablesBuilder::new();
        column.set_column_aware(true);
        column.observe(&TraceLowLevelEvent::Path(PathBuf::from("/a.rs")));
        column.observe(&TraceLowLevelEvent::Path(PathBuf::from("/bb.rs")));
        column.set_path_line_lengths(0, &[10, 20]);
        // Path 1 deliberately gets no table — the partially-populated case.
        let col_tables = column.finish();

        let rec0 = read_record(&col_tables.paths_dat, &col_tables.paths_off, 0);
        let (p0, lls0) = crate::column_aware::decode_path_record_layout_a(rec0).expect("record 0 parses as Layout A");
        assert_eq!(p0, "/a.rs");
        assert_eq!(lls0, vec![10, 20]);

        let rec1 = read_record(&col_tables.paths_dat, &col_tables.paths_off, 1);
        let (p1, lls1) = crate::column_aware::decode_path_record_layout_a(rec1).expect("record 1 parses as Layout A");
        assert_eq!(p1, "/bb.rs");
        assert!(lls1.is_empty(), "a path with no per-line data still gets line_count = 0");

        // The two modes must actually differ — without this the test would pass
        // if `set_column_aware` did nothing, because a bare-bytes record
        // happens to parse as Layout A when its first byte equals its length.
        assert_ne!(
            col_tables.paths_dat, line_tables.paths_dat,
            "Layout A must not equal the bare-bytes table"
        );
    }
}
