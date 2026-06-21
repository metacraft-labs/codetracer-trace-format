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
use codetracer_trace_writer::step_stream::pack_global_line_index;
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
        TraceWriter::register_variable_with_full_value(&mut writer, &vname, ValueRecord::Int { i: i as i64, type_id: NONE_TYPE_ID });
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
fn interning_tables_resolve_by_id_matching_events_and_paths_json() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true);

    let it = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_path)
        .expect("open_interning_tables ok")
        .expect("interning tables present when has_interning_tables flag is set");

    // Read the same events / paths.json that reference these ids.
    let (events, paths_json) = {
        let mut r = codetracer_ctfs::CtfsReader::open(&ct_path).unwrap();
        let paths_json: Vec<String> = serde_json::from_slice(&r.read_file("paths.json").unwrap()).unwrap();
        let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
        let events = reader.load_trace_events(&ct_path).unwrap();
        (events, paths_json)
    };

    // --- Paths: resolved path equals paths.json[id] and the Path events. ---
    let path_events: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Path(p) => Some(p.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(it.path_count(), paths_json.len(), "path table count must equal paths.json length");
    assert_eq!(it.path_count(), path_events.len(), "path table count must equal the Path event count");
    assert!(it.path_count() >= N, "expected at least N interned paths");
    for id in 0..it.path_count() {
        let resolved = it.path_str(id as u64).unwrap();
        assert_eq!(resolved, paths_json[id], "path id {id} must equal paths.json[{id}]");
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
        // The funcs.dat global_line_index packs (path_id, line); it must decode
        // back to exactly the Function event's (path_id, line).
        let expected_gli = pack_global_line_index(path_id.0, line.0);
        assert_eq!(rec.global_line_index, expected_gli, "func id {id} global_line_index");
        let (decoded_path_id, decoded_line) = rec.path_id_and_line();
        assert_eq!(decoded_path_id, path_id.0, "func id {id} decoded path_id");
        assert_eq!(decoded_line, line.0, "func id {id} decoded line");
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
    assert_eq!(it.varname_count(), varname_events.len(), "varname table count must equal the VariableName event count");
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
        assert_eq!(it.varname(id as u64).unwrap(), expected_record(&expected.varnames_dat, &expected.varnames_off, id));
    }
}

#[test]
fn random_access_by_mid_table_id() {
    // Prove the `.off` offset index gives true random access: resolve a single
    // mid-table id directly, with NO preceding sequential read priming any
    // cache, and check it matches the events.log-derived ground truth.
    let dir = tempfile::tempdir().unwrap();
    let ct_path = write_trace(&dir, true);

    let it = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_path).unwrap().unwrap();
    let expected = expected_tables_from_events(&ct_path);

    // Mid-table ids (interior of each table).
    let mid_path = it.path_count() / 2;
    let mid_func = it.func_count() / 2;
    let mid_var = it.varname_count() / 2;
    assert!(mid_path > 0 && mid_func > 0 && mid_var > 0, "tables must have an interior");

    // Path: random access by the middle id.
    assert_eq!(it.path(mid_path as u64).unwrap(), expected_record(&expected.paths_dat, &expected.paths_off, mid_path));

    // Func: random access by the middle id, decoding the record.
    let func_rec = it.func(mid_func as u64).unwrap();
    let mut rebuilt = Vec::new();
    codetracer_trace_writer::interning_tables::encode_func_record(func_rec.global_line_index, &func_rec.name, &mut rebuilt);
    assert_eq!(rebuilt, expected_record(&expected.funcs_dat, &expected.funcs_off, mid_func));

    // Varname: random access by the middle id.
    assert_eq!(it.varname(mid_var as u64).unwrap(), expected_record(&expected.varnames_dat, &expected.varnames_off, mid_var));

    // Reading the LAST id directly (also no preceding scan) resolves correctly —
    // exercises the trailing-sentinel-offset length recovery.
    let last_var = it.varname_count() - 1;
    assert_eq!(it.varname(last_var as u64).unwrap(), expected_record(&expected.varnames_dat, &expected.varnames_off, last_var));

    // Out-of-range ids error, never panic.
    assert!(it.path(it.path_count() as u64).is_err());
    assert!(it.func(it.func_count() as u64 + 100).is_err());
}

#[test]
fn legacy_trace_has_no_interning_tables_and_files_byte_identical() {
    // The interning-tables emission is ADDITIVE: enabling it must not perturb
    // events.log or paths.json a single byte, and a flag-off trace must carry no
    // binary tables.
    let dir_off = tempfile::tempdir().unwrap();
    let dir_on = tempfile::tempdir().unwrap();
    let ct_off = write_trace(&dir_off, false);
    let ct_on = write_trace(&dir_on, true);

    let mut r_off = codetracer_ctfs::CtfsReader::open(&ct_off).unwrap();
    let mut r_on = codetracer_ctfs::CtfsReader::open(&ct_on).unwrap();

    // events.log + paths.json byte-identical regardless of the flag.
    assert_eq!(
        r_off.read_file("events.log").unwrap(),
        r_on.read_file("events.log").unwrap(),
        "events.log must be byte-identical regardless of the interning-tables flag"
    );
    assert_eq!(
        r_off.read_file("paths.json").unwrap(),
        r_on.read_file("paths.json").unwrap(),
        "paths.json must be byte-identical regardless of the interning-tables flag"
    );

    // The flag-off container carries no binary interning tables, and opening the
    // reader returns None (legacy path).
    assert!(r_off.read_file("paths.dat").is_err());
    assert!(r_off.read_file("funcs.dat").is_err());
    assert!(r_off.read_file("types.off").is_err());
    assert!(r_off.read_file("varnames.off").is_err());
    assert!(
        codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_off).unwrap().is_none(),
        "a flag-off trace exposes no interning tables"
    );

    // The flag-on container carries all eight table files.
    for f in ["paths.dat", "paths.off", "funcs.dat", "funcs.off", "types.dat", "types.off", "varnames.dat", "varnames.off"] {
        assert!(r_on.read_file(f).is_ok(), "{f} must be present when the flag is set");
    }
}
