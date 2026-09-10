//! M23d round-trip tests for the binary varint interning tables.
//!
//! A trace is written with the `has_interning_tables` flag on, interning a
//! variety of paths, functions, types, and variable names. The four binary
//! tables (`paths.dat`+`paths.off`, `funcs.dat`+`funcs.off`,
//! `types.dat`+`types.off`, `varnames.dat`+`varnames.off`) are then read back by
//! id via the `InterningTablesReader` and compared against (a) the SAME
//! interning events read out of the unchanged `events.log` (the
//! Path/Function/Type/VariableName events), and (b) the `paths.json` list (for
//! paths).
//! The resolved path / func (name + line) / type / varname MUST equal what the
//! writer interned and what `events.log` / `paths.json` reference for the same
//! ids. A random-access-by-id check (a mid-table id, resolved directly with no
//! preceding sequential read) proves the `.off` offset index gives true random
//! access rather than a scan. A flag-off (legacy) trace confirms the tables are
//! absent and `events.log` / `paths.json` are byte-identical — proving the split
//! is additive.

use std::path::Path;

use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::interning_tables::InterningTablesBuilder;
use codetracer_trace_writer::line_position::LinePositionSpace;
use codetracer_trace_writer::trace_writer::TraceWriter;

/// Number of distinct source files / functions / variables interned. Chosen
/// large enough that a "mid-table" id is genuinely in the interior of every
/// table, so the random-access check is meaningful.
const N: usize = 40;

/// Write a trace that interns N paths, N functions (each at a distinct
/// path/line), a handful of types, and N variable names. Returns the `.ct` path.
fn write_trace(dir: &tempfile::TempDir, with_interning_tables: bool) -> std::path::PathBuf {
    let path_buf = dir.path().join("trace");
    let mut writer = CtfsTraceWriter::new("test_program", &[]).with_interning_tables(with_interning_tables);
    TraceWriter::begin_writing_trace_events(&mut writer, &path_buf).unwrap();

    // `start` interns the toplevel path/function and the None type.
    let main_src = Path::new("/test/main.rs");
    TraceWriter::start(&mut writer, main_src, Line(1));

    // Intern N functions, each in its own source file at a distinct line, so the
    // funcs/paths tables are non-trivial and ids interleave with steps.
    for i in 0..N {
        let src = format!("/test/mod_{i}.rs");
        let src_path = Path::new(&src);
        let fname = format!("fn_{i}");
        let line = Line((i as i64) * 10 + 3);
        let fid = TraceWriter::ensure_function_id(&mut writer, &fname, src_path, line);
        TraceWriter::register_call(&mut writer, fid, vec![]);
        TraceWriter::register_step(&mut writer, src_path, line);
        // Intern a variable name per function.
        let vname = format!("var_{i}");
        TraceWriter::register_variable_with_full_value(
            &mut writer,
            &vname,
            ValueRecord::Int {
                i: i as i64,
                type_id: NONE_TYPE_ID,
            },
        );
        TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });
    }

    // Intern a few distinct types with various kinds.
    let _ = TraceWriter::ensure_type_id(&mut writer, TypeKind::Int, "i64");
    let _ = TraceWriter::ensure_type_id(&mut writer, TypeKind::Bool, "bool");
    let _ = TraceWriter::ensure_type_id(&mut writer, TypeKind::String, "String");

    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
    path_buf.with_extension("ct")
}

/// Re-derive the expected interning tables straight from `events.log` (read with
/// the unchanged unified-stream reader), by replaying its events through the
/// SAME `InterningTablesBuilder` the writer uses. This is the ground truth the
/// `*.dat`-resolved records must equal.
fn expected_tables_from_events(ct_path: &Path) -> codetracer_trace_writer::interning_tables::EncodedInterningTables {
    let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(ct_path).unwrap();
    let mut builder = InterningTablesBuilder::new();
    for ev in &events {
        builder.observe(ev);
    }
    builder.finish()
}

/// Resolve a record `i` from an in-memory `.dat`+`.off` pair (the expected
/// ground-truth tables), mirroring the reader's offset-index access.
fn expected_record(dat: &[u8], off: &[u8], i: usize) -> Vec<u8> {
    let start = u64::from_le_bytes(off[i * 8..i * 8 + 8].try_into().unwrap()) as usize;
    let end = u64::from_le_bytes(off[(i + 1) * 8..(i + 1) * 8 + 8].try_into().unwrap()) as usize;
    dat[start..end].to_vec()
}

#[test]
fn interning_tables_resolve_by_id_matching_events_and_recorded_paths() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true);

    let it = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_path)
        .expect("open_interning_tables ok")
        .expect("interning tables present when has_interning_tables flag is set");

    // Read the same events / recorded path list that reference these ids.
    //
    // The oracle used to be the legacy `paths.json` sidecar.  That is retired,
    // so the independent reference is now `meta.dat`'s recorded path list —
    // kept alongside the `Path` event comparison below so the interning table
    // is still checked against TWO references it did not produce, not one.
    let (events, recorded_paths) = {
        let mut r = codetracer_ctfs::CtfsReader::open(&ct_path).unwrap();
        let meta = codetracer_trace_writer::meta_dat::decode_meta_dat(&r.read_file("meta.dat").unwrap())
            .expect("meta.dat must decode");
        let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
        let events = reader.load_trace_events(&ct_path).unwrap();
        (events, meta.paths)
    };

    // --- Paths: resolved path equals paths.json[id] and the Path events. ---
    let path_events: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Path(p) => Some(p.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(it.path_count(), recorded_paths.len(), "path table count must equal meta.dat's path count");
    assert_eq!(it.path_count(), path_events.len(), "path table count must equal the Path event count");
    assert!(it.path_count() >= N, "expected at least N interned paths");
    for id in 0..it.path_count() {
        let resolved = it.path_str(id as u64).unwrap();
        assert_eq!(resolved, recorded_paths[id], "path id {id} must equal meta.dat paths[{id}]");
        assert_eq!(resolved, path_events[id], "path id {id} must equal the {id}-th Path event");
    }

    // --- Functions: resolved (name, global_line_index) equals the Function events. ---
    let func_events: Vec<(String, PathId, Line)> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Function(f) => Some((f.name.clone(), f.path_id, f.line)),
            _ => None,
        })
        .collect();
    assert_eq!(it.func_count(), func_events.len(), "func table count must equal the Function event count");
    for (id, (name, path_id, line)) in func_events.iter().enumerate() {
        let rec = it.func(id as u64).unwrap();
        assert_eq!(String::from_utf8(rec.name.clone()).unwrap(), *name, "func id {id} name");
        // The funcs.dat global_line_index addresses the declaration site in the
        // trace's own space; it must resolve back to exactly the Function
        // event's (path_id, line).
        let mut space = LinePositionSpace::uniform(it.path_count());
        let expected_gli = space.global_index(path_id.0, line.0);
        assert_eq!(rec.global_line_index, expected_gli, "func id {id} global_line_index");
        assert_eq!(
            rec.path_id_and_line(&space),
            Ok((path_id.0, line.0)),
            "func id {id} must resolve to the location its Function event carried"
        );
    }

    // --- Types: resolved (kind, lang_type, specific_info) equals the Type events. ---
    let type_events: Vec<TypeRecord> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Type(t) => Some(t.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(it.type_count(), type_events.len(), "type table count must equal the Type event count");
    assert!(it.type_count() >= 4, "expected None + i64 + bool + String types");
    for (id, ev) in type_events.iter().enumerate() {
        let rec = it.type_record(id as u64).unwrap();
        assert_eq!(rec.kind, ev.kind as u8, "type id {id} kind byte");
        assert_eq!(rec.type_kind(), Some(ev.kind), "type id {id} TypeKind");
        assert_eq!(String::from_utf8(rec.lang_type.clone()).unwrap(), ev.lang_type, "type id {id} lang_type");
        assert_eq!(rec.specific_info, ev.specific_info, "type id {id} specific_info");
    }

    // --- Varnames: resolved name equals the VariableName events. ---
    let varname_events: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::VariableName(n) | TraceLowLevelEvent::Variable(n) => Some(n.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        it.varname_count(),
        varname_events.len(),
        "varname table count must equal the VariableName event count"
    );
    assert!(it.varname_count() >= N, "expected at least N interned variable names");
    for (id, name) in varname_events.iter().enumerate() {
        assert_eq!(&it.varname_str(id as u64).unwrap(), name, "varname id {id}");
    }

    // The reader-decoded tables must also byte-equal the events.log-rebuilt
    // ground-truth tables (a second, independent consistency proof).
    let expected = expected_tables_from_events(&ct_path);
    for id in 0..it.path_count() {
        assert_eq!(it.path(id as u64).unwrap(), expected_record(&expected.paths_dat, &expected.paths_off, id));
    }
    for id in 0..it.varname_count() {
        assert_eq!(
            it.varname(id as u64).unwrap(),
            expected_record(&expected.varnames_dat, &expected.varnames_off, id)
        );
    }
}

#[test]
fn random_access_by_mid_table_id() {
    // Prove the `.off` offset index gives true random access: resolve a single
    // mid-table id directly, with NO preceding sequential read priming any
    // cache, and check it matches the events.log-derived ground truth.
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true);

    let it = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_path)
        .unwrap()
        .unwrap();
    let expected = expected_tables_from_events(&ct_path);

    // Mid-table ids (interior of each table).
    let mid_path = it.path_count() / 2;
    let mid_func = it.func_count() / 2;
    let mid_var = it.varname_count() / 2;
    assert!(mid_path > 0 && mid_func > 0 && mid_var > 0, "tables must have an interior");

    // Path: random access by the middle id.
    assert_eq!(
        it.path(mid_path as u64).unwrap(),
        expected_record(&expected.paths_dat, &expected.paths_off, mid_path)
    );

    // Func: random access by the middle id, decoding the record.
    let func_rec = it.func(mid_func as u64).unwrap();
    let mut rebuilt = Vec::new();
    codetracer_trace_writer::interning_tables::encode_func_record(func_rec.global_line_index, &func_rec.name, &mut rebuilt);
    assert_eq!(rebuilt, expected_record(&expected.funcs_dat, &expected.funcs_off, mid_func));

    // Varname: random access by the middle id.
    assert_eq!(
        it.varname(mid_var as u64).unwrap(),
        expected_record(&expected.varnames_dat, &expected.varnames_off, mid_var)
    );

    // Reading the LAST id directly (also no preceding scan) resolves correctly —
    // exercises the trailing-sentinel-offset length recovery.
    let last_var = it.varname_count() - 1;
    assert_eq!(
        it.varname(last_var as u64).unwrap(),
        expected_record(&expected.varnames_dat, &expected.varnames_off, last_var)
    );

    // Out-of-range ids error, never panic.
    assert!(it.path(it.path_count() as u64).is_err());
    assert!(it.func(it.func_count() as u64 + 100).is_err());
}

#[test]
fn legacy_trace_has_no_interning_tables_and_files_byte_identical() {
    // The interning-tables emission is ADDITIVE: enabling it must not perturb
    // events.log a single byte, and a flag-off trace must carry no binary
    // tables.  (This used to also pin `paths.json` byte-identity; that sidecar
    // is retired.  `meta.dat` is not a substitute for that half of the check —
    // it carries the stream-capability flags, so it differs BY DESIGN between
    // flag-on and flag-off.)
    let dir_off = tempfile::tempdir().unwrap();
    let dir_on = tempfile::tempdir().unwrap();
    let ct_off = write_trace(&dir_off, false);
    let ct_on = write_trace(&dir_on, true);

    let mut r_off = codetracer_ctfs::CtfsReader::open(&ct_off).unwrap();
    let mut r_on = codetracer_ctfs::CtfsReader::open(&ct_on).unwrap();

    // events.log byte-identical regardless of the flag.
    assert_eq!(
        r_off.read_file("events.log").unwrap(),
        r_on.read_file("events.log").unwrap(),
        "events.log must be byte-identical regardless of the interning-tables flag"
    );

    // Both bundles carry a metadata document, and it records the same paths in
    // the same order either way — the part of the old paths.json check that was
    // about the recording rather than about byte layout.
    let meta_off = codetracer_trace_writer::meta_dat::decode_meta_dat(&r_off.read_file("meta.dat").unwrap())
        .expect("flag-off meta.dat must decode");
    let meta_on = codetracer_trace_writer::meta_dat::decode_meta_dat(&r_on.read_file("meta.dat").unwrap())
        .expect("flag-on meta.dat must decode");
    assert_eq!(
        meta_off.paths, meta_on.paths,
        "meta.dat must record the same paths regardless of the interning-tables flag"
    );

    // The legacy JSON sidecars are retired in both bundles.
    assert!(r_off.read_file("meta.json").is_err(), "meta.json was written");
    assert!(r_off.read_file("paths.json").is_err(), "paths.json was written");

    // The flag-off container carries no binary interning tables, and opening the
    // reader returns None (legacy path).
    assert!(r_off.read_file("paths.dat").is_err());
    assert!(r_off.read_file("funcs.dat").is_err());
    assert!(r_off.read_file("types.off").is_err());
    assert!(r_off.read_file("varnames.off").is_err());
    assert!(
        codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_off)
            .unwrap()
            .is_none(),
        "a flag-off trace exposes no interning tables"
    );

    // The flag-on container carries all eight table files.
    for f in [
        "paths.dat",
        "paths.off",
        "funcs.dat",
        "funcs.off",
        "types.dat",
        "types.off",
        "varnames.dat",
        "varnames.off",
    ] {
        assert!(r_on.read_file(f).is_ok(), "{f} must be present when the flag is set");
    }
}

/// Encode a PLAIN Variable-Size Record Table: raw record bytes concatenated into
/// `.dat`, and `record_count + 1` little-endian `u64` byte offsets into `.off`
/// (the trailing entry is the total data length). This is what the production
/// Nim writer emits for all four interning tables.
fn encode_plain_table(records: &[&[u8]]) -> (Vec<u8>, Vec<u8>) {
    let mut dat = Vec::new();
    let mut off = Vec::new();
    off.extend_from_slice(&0u64.to_le_bytes());
    for r in records {
        dat.extend_from_slice(r);
        off.extend_from_slice(&(dat.len() as u64).to_le_bytes());
    }
    (dat, off)
}

/// The real-trace case F3 fixes: the production Nim `MultiStreamTraceWriter`
/// emits all four interning tables in the PLAIN layout and leaves `meta.dat`
/// bit 12 (`has_interning_tables`) CLEAR. Existence must be resolved by
/// `paths.dat` STRUCTURAL PRESENCE, never by the flag — otherwise EVERY real
/// trace resolves to no interning tables (a blank Variables pane over data that
/// is on disk). Before the fix, `open` returned `Ok(None)` here.
#[test]
fn plain_layout_bit12_clear_resolves_interned_names() {
    use codetracer_ctfs::CtfsWriter;
    use codetracer_trace_reader::interning_tables_reader::{InterningTablesReader, RecordLayout};
    use codetracer_trace_writer::meta_dat::{encode_meta_dat, meta_dat_has_interning_tables};

    let paths = [b"/test/main.rs".as_slice(), b"/test/mod.rs".as_slice()];
    let funcs = [b"main".as_slice(), b"helper".as_slice()];
    let types = [b"i64".as_slice(), b"bool".as_slice()];
    let varnames = [b"x".as_slice(), b"y".as_slice(), b"z".as_slice()];

    let dir = tempfile::tempdir().unwrap();
    let ct_path = dir.path().join("plain.ct");
    {
        let mut w = CtfsWriter::create(&ct_path, 4096, 31).unwrap();
        // meta.dat WITHOUT the interning-tables flag — the real-trace case.
        let meta = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], 0);
        assert!(!meta_dat_has_interning_tables(&meta), "fixture must have bit 12 CLEAR");
        let h = w.add_file("meta.dat").unwrap();
        w.write(h, &meta).unwrap();
        for (name, records) in [
            ("paths", &paths[..]),
            ("funcs", &funcs[..]),
            ("types", &types[..]),
            ("varnames", &varnames[..]),
        ] {
            let (dat, off) = encode_plain_table(records);
            let h = w.add_file(&format!("{name}.dat")).unwrap();
            w.write(h, &dat).unwrap();
            let h = w.add_file(&format!("{name}.off")).unwrap();
            w.write(h, &off).unwrap();
        }
        w.close().unwrap();
    }

    let mut reader = codetracer_ctfs::CtfsReader::open(&ct_path).unwrap();
    let it = InterningTablesReader::open(&mut reader)
        .expect("open ok")
        .expect("paths.dat present ⇒ interning tables resolve even with bit 12 CLEAR");

    // bit 12 clear ⇒ PLAIN decode; paths.dat presence answered existence.
    assert_eq!(it.layout(), RecordLayout::Plain);

    // Names (raw bytes in both layouts) resolve — the case the gate broke.
    assert_eq!(it.path_count(), 2);
    assert_eq!(it.path_str(0).unwrap(), "/test/main.rs");
    assert_eq!(it.path_str(1).unwrap(), "/test/mod.rs");
    assert_eq!(it.varname_count(), 3);
    assert_eq!(it.varname_str(0).unwrap(), "x");
    assert_eq!(it.varname_str(2).unwrap(), "z");

    // Plain func/type records are raw names: global_line_index stubbed to 0,
    // type kind degrades to Raw (parity with db-backend / the Nim FFI reader).
    assert_eq!(it.func_count(), 2);
    let f = it.func(1).unwrap();
    assert_eq!(String::from_utf8(f.name).unwrap(), "helper");
    assert_eq!(f.global_line_index, 0);
    assert_eq!(it.type_count(), 2);
    let t = it.type_record(0).unwrap();
    assert_eq!(t.type_kind(), Some(TypeKind::Raw));
    assert_eq!(String::from_utf8(t.lang_type).unwrap(), "i64");
}

/// A container with NO `meta.dat` at all (a still-recording trace whose meta is
/// written only at close) but with the interning tables present must still
/// resolve names — `meta.dat` is read best-effort and its absence reads as the
/// PLAIN layout rather than refusing the tables.
#[test]
fn missing_meta_dat_still_resolves_interned_names() {
    use codetracer_ctfs::CtfsWriter;
    use codetracer_trace_reader::interning_tables_reader::{InterningTablesReader, RecordLayout};

    let paths = [b"/a.rs".as_slice(), b"/b.rs".as_slice()];
    let funcs = [b"f".as_slice()];
    let types = [b"T".as_slice()];
    let varnames = [b"v".as_slice()];

    let dir = tempfile::tempdir().unwrap();
    let ct_path = dir.path().join("nometa.ct");
    {
        let mut w = CtfsWriter::create(&ct_path, 4096, 31).unwrap();
        // Deliberately NO meta.dat.
        for (name, records) in [
            ("paths", &paths[..]),
            ("funcs", &funcs[..]),
            ("types", &types[..]),
            ("varnames", &varnames[..]),
        ] {
            let (dat, off) = encode_plain_table(records);
            let h = w.add_file(&format!("{name}.dat")).unwrap();
            w.write(h, &dat).unwrap();
            let h = w.add_file(&format!("{name}.off")).unwrap();
            w.write(h, &off).unwrap();
        }
        w.close().unwrap();
    }

    let mut reader = codetracer_ctfs::CtfsReader::open(&ct_path).unwrap();
    let it = InterningTablesReader::open(&mut reader)
        .expect("open ok")
        .expect("paths.dat present ⇒ resolves even with meta.dat absent");
    assert_eq!(it.layout(), RecordLayout::Plain);
    assert_eq!(it.path_str(1).unwrap(), "/b.rs");
    assert_eq!(it.varname_str(0).unwrap(), "v");
}

// ---------------------------------------------------------------------------
// The per-file line-count table (`meta.dat` bit 14)
// ---------------------------------------------------------------------------
//
// A line-only container used to state nothing about how its address space was
// apportioned between files, so a reader could only re-apply the writer's
// convention of `DEFAULT_LINES_PER_FILE` addresses each. Bit 14 makes every
// `paths.dat` record carry its file's line count, and the space is laid out
// from those. The fixtures below are hand-encoded rather than produced by a
// writer, so the test pins the WIRE FORMAT the canonical Nim writer emits and
// would catch this reader drifting from it.

/// Encode a `paths.dat` line-count-table record: `payload_len + payload +
/// line_count`.
fn encode_line_count_record(payload: &[u8], line_count: u64) -> Vec<u8> {
    fn varint(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let mut b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                b |= 0x80;
            }
            out.push(b);
            if v == 0 {
                break;
            }
        }
    }
    let mut out = Vec::new();
    varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    varint(line_count, &mut out);
    out
}

/// Build a container whose `paths.dat` holds the given `(path, line_count)`
/// records, with bit 14 set unless `declare` is false.
fn write_line_count_container(dir: &tempfile::TempDir, name: &str, records: &[Vec<u8>], declare: bool) -> std::path::PathBuf {
    use codetracer_ctfs::CtfsWriter;
    use codetracer_trace_writer::meta_dat::{FLAG_HAS_LINE_COUNT_TABLE, encode_meta_dat};

    let ct_path = dir.path().join(name);
    let mut w = CtfsWriter::create(&ct_path, 4096, 31).unwrap();
    let flags = if declare { FLAG_HAS_LINE_COUNT_TABLE } else { 0 };
    let meta = encode_meta_dat("01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb", "prog", &[], "", "", &[], flags);
    let h = w.add_file("meta.dat").unwrap();
    w.write(h, &meta).unwrap();

    let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
    let (dat, off) = encode_plain_table(&refs);
    let h = w.add_file("paths.dat").unwrap();
    w.write(h, &dat).unwrap();
    let h = w.add_file("paths.off").unwrap();
    w.write(h, &off).unwrap();
    // The other three tables must exist once `paths.dat` does.
    for other in ["funcs", "types", "varnames"] {
        let (dat, off) = encode_plain_table(&[b"x".as_slice()]);
        let h = w.add_file(&format!("{other}.dat")).unwrap();
        w.write(h, &dat).unwrap();
        let h = w.add_file(&format!("{other}.off")).unwrap();
        w.write(h, &off).unwrap();
    }
    w.close().unwrap();
    ct_path
}

/// A container that declares bit 14 resolves both the path and the recorded
/// size, and the space it defines is the sum of the counts rather than
/// `paths × DEFAULT_LINES_PER_FILE`.
#[test]
fn line_count_table_states_each_files_size() {
    use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;
    use codetracer_trace_writer::line_position::{DEFAULT_LINES_PER_FILE, LinePositionSpace};

    let dir = tempfile::tempdir().unwrap();
    let ct = write_line_count_container(
        &dir,
        "counted.ct",
        &[
            encode_line_count_record(b"/src/alpha.rb", 10),
            encode_line_count_record(b"/src/beta.rb", 7),
        ],
        true,
    );

    let mut reader = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let it = InterningTablesReader::open(&mut reader).expect("open ok").expect("paths.dat present");

    assert_eq!(it.path_count(), 2);
    assert_eq!(
        it.path_str(0).unwrap(),
        "/src/alpha.rb",
        "the record's framing must not leak into the path string"
    );
    assert_eq!(it.path_str(1).unwrap(), "/src/beta.rb");
    assert_eq!(it.line_count(0), Some(10));
    assert_eq!(it.line_count(1), Some(7));

    let space = LinePositionSpace::from_line_counts(it.line_counts());
    assert_eq!(
        space.total_lines(),
        17,
        "the space must be the sum of the recorded counts, not {}",
        2 * DEFAULT_LINES_PER_FILE
    );
    // The boundary the sizing exists to get right: file 0's last line is the
    // last address of file 0's slot, and file 1's first line is file 1's base.
    assert_eq!(space.resolve(9), Ok((0, 10)));
    assert_eq!(space.resolve(10), Ok((1, 1)));
    assert_eq!(space.resolve(16), Ok((1, 7)));
    assert!(space.resolve(17).is_err(), "17 is one past a 17-address space");
}

/// The mutation control for the flag. The same records with bit 14 CLEAR are
/// read as bare path bytes — framing and all — and the reader reports no
/// recorded size. If clearing the bit changed nothing, the bit would be
/// decorative and the test above would prove nothing about it.
#[test]
fn without_bit_14_the_same_records_read_as_bare_paths() {
    use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;

    let dir = tempfile::tempdir().unwrap();
    let records = [
        encode_line_count_record(b"/src/alpha.rb", 10),
        encode_line_count_record(b"/src/beta.rb", 7),
    ];
    let ct = write_line_count_container(&dir, "undeclared.ct", &records, false);

    let mut reader = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let it = InterningTablesReader::open(&mut reader).expect("open ok").expect("paths.dat present");

    assert_eq!(it.line_count(0), None, "a container without bit 14 records no size");
    assert!(it.line_counts().is_empty());
    assert_ne!(
        it.path_str(0).unwrap(),
        "/src/alpha.rb",
        "without the bit the record is read as bare bytes, so the path must come back with its \
         framing attached"
    );
}

/// A record that states a count of zero fails the OPEN. Under bit 14 every
/// file's size is a number the container states, and a file sized zero shares
/// its base with the next one — substituting a default there is the assumption
/// the table was added to remove, reintroduced by the reader.
#[test]
fn a_recorded_count_of_zero_fails_the_open() {
    use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;

    let dir = tempfile::tempdir().unwrap();
    let ct = write_line_count_container(
        &dir,
        "zeroed.ct",
        &[
            encode_line_count_record(b"/src/alpha.rb", 10),
            encode_line_count_record(b"/src/beta.rb", 0),
        ],
        true,
    );

    let mut reader = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let err = match InterningTablesReader::open(&mut reader) {
        Err(e) => e,
        Ok(_) => panic!("line_count 0 must fail the open"),
    };
    assert!(err.contains("line_count 0"), "the refusal must name the field; got: {err}");
    assert!(err.contains("record 1"), "the refusal must name the record; got: {err}");
}

/// A record whose declared payload length runs past the record fails the open
/// rather than yielding a truncated path. The container DECLARED this layout,
/// so a record that does not decode is corruption, and falling back to the bare
/// layout would answer with a path that has its own length prefix inside it.
#[test]
fn a_truncated_line_count_record_fails_the_open() {
    use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;

    let dir = tempfile::tempdir().unwrap();
    // payload_len = 200 over a 6-byte record.
    let bogus = vec![200u8, b'a', b'b', b'c', b'd', 4u8];
    let ct = write_line_count_container(&dir, "truncated.ct", &[bogus], true);

    let mut reader = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let err = match InterningTablesReader::open(&mut reader) {
        Err(e) => e,
        Ok(_) => panic!("a truncated record must fail the open"),
    };
    assert!(err.contains("paths.dat: record 0"), "the refusal must name the record; got: {err}");
}
