//! The Rust writer's line-count table, path versions and source reload markers
//! refuse what the spec forbids, and a reload marker is written only in a trace
//! that declared it before it opened.
//!
//! Spec: `codetracer-trace-format-spec/internal-files.md` §"`paths.dat`
//! line-count table", §"`paths.dat` path versions", §"Extended flags
//! (`flags_ext`)"; `trace-events.md` §"Source Reload Marker (Tag
//! 0x08)". The agreement of these containers with the Nim writer's is asserted
//! in `codetracer_trace_writer_nim/tests/source_reload_cross_writer.rs`; this
//! file covers the refusals, which that differential cannot reach.
//!
//! No mocks: every container is written to memory by the real writer and read
//! back with the real CTFS reader.

use std::path::Path;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::Line;
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::{CtfsOutput, CtfsTraceWriter};
use codetracer_trace_writer::meta_dat::{FLAG_EXT_HAS_SOURCE_RELOAD, META_DAT_VERSION, decode_meta_dat};
use codetracer_trace_writer::step_stream::SourceReloadChange;
use codetracer_trace_writer::trace_writer::TraceWriter;

fn open(program: &str) -> CtfsTraceWriter {
    let mut w = CtfsTraceWriter::new(program, &[]).with_output(CtfsOutput::Memory);
    TraceWriter::begin_writing_trace_events(&mut w, Path::new(program)).expect("begin");
    w
}

/// A writer whose recorder declared, before opening, that reloads may occur.
fn open_reloading(program: &str) -> CtfsTraceWriter {
    let mut w = CtfsTraceWriter::new(program, &[]).with_output(CtfsOutput::Memory);
    w.declare_source_reload().expect("declared before the trace opens");
    TraceWriter::begin_writing_trace_events(&mut w, Path::new(program)).expect("begin");
    w
}

fn finish(mut w: CtfsTraceWriter) -> Vec<u8> {
    TraceWriter::finish_writing_trace_events(&mut w).expect("finish");
    w.take_container_bytes().expect("in-memory container")
}

fn meta_of(bytes: &[u8]) -> codetracer_trace_writer::meta_dat::MetaDat {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("c.ct");
    std::fs::write(&p, bytes).unwrap();
    let meta = CtfsReader::open(&p).unwrap().read_file("meta.dat").unwrap();
    decode_meta_dat(&meta).expect("meta.dat decodes")
}

#[test]
fn a_trace_that_did_not_declare_reloads_has_no_extended_flag_even_with_path_versions() {
    let mut w = open("no_reload");
    w.enable_line_count_table().expect("table");
    w.register_path_with_line_count(Path::new("/a"), 5).unwrap();
    w.register_path_version(Path::new("/a"), 7).unwrap();
    AbstractTraceWriter::register_step(&mut w, Path::new("/a"), Line(6));
    let m = meta_of(&finish(w));
    assert_eq!(m.version, META_DAT_VERSION);
    assert_eq!(m.ext_flags, 0);
}

/// `internal-files.md` §"Extended flags": bit 0 is a capability declared at
/// open, and a writer refuses a reload in a trace that did not declare it,
/// failing the call.
#[test]
fn a_reload_in_a_trace_that_did_not_declare_it_is_refused() {
    let mut w = open("undeclared");
    w.enable_line_count_table().expect("table");
    w.register_path_with_line_count(Path::new("/a"), 5).unwrap();
    let v = w.register_path_version(Path::new("/a"), 7).unwrap().0 as u64;
    let err = w
        .register_source_reload(
            &[SourceReloadChange {
                old_path_id: 0,
                new_path_id: v,
                generation: 2,
            }],
            0,
        )
        .expect_err("an undeclared reload must be refused");
    assert!(err.contains("declare"), "the refusal names the missing declaration: {err}");
    assert_eq!(w.source_reload_count(), 0);
    let mut late = open("late_declare");
    assert!(
        late.declare_source_reload().is_err(),
        "the declaration is part of meta.dat, which is fixed when the trace opens"
    );
}

/// A trace that declared reloads and recorded none is well-formed.
#[test]
fn a_declared_trace_without_a_reload_keeps_the_flag() {
    let mut w = open_reloading("declared_quiet");
    AbstractTraceWriter::register_step(&mut w, Path::new("/a"), Line(1));
    let m = meta_of(&finish(w));
    assert_eq!(m.ext_flags, FLAG_EXT_HAS_SOURCE_RELOAD);
}

#[test]
fn a_declared_trace_records_its_reloads() {
    let mut w = open_reloading("reload");
    w.enable_line_count_table().expect("table");
    w.register_path_with_line_count(Path::new("/a"), 5).unwrap();
    AbstractTraceWriter::register_step(&mut w, Path::new("/a"), Line(1));
    let v = w.register_path_version(Path::new("/a"), 7).unwrap();
    assert_eq!(w.current_path_id(Path::new("/a")), Some(v), "a bare path resolves to its newest version");
    let change = SourceReloadChange {
        old_path_id: 0,
        new_path_id: v.0 as u64,
        generation: 2,
    };
    assert_eq!(w.register_source_reload(&[change], 0).unwrap(), 1);
    assert_eq!(w.register_source_reload(&[change], 0).unwrap(), 2, "ordinals increase by one");
    let m = meta_of(&finish(w));
    assert_eq!(m.version, META_DAT_VERSION);
    assert_eq!(m.ext_flags, FLAG_EXT_HAS_SOURCE_RELOAD);
}

#[test]
fn a_malformed_reload_is_refused_by_name() {
    let mut w = open_reloading("bad_reload");
    w.enable_line_count_table().expect("table");
    w.register_path_with_line_count(Path::new("/a"), 5).unwrap();
    let v = w.register_path_version(Path::new("/a"), 7).unwrap().0 as u64;
    let ch = |old, new, generation| SourceReloadChange {
        old_path_id: old,
        new_path_id: new,
        generation,
    };
    for (changed, needle) in [
        (vec![], "no changed files"),
        (vec![ch(0, 0, 2)], "old_path_id == new_path_id"),
        (vec![ch(0, v, 1)], "generation"),
        (vec![ch(0, 9, 2)], "not a registered path"),
    ] {
        let err = w.register_source_reload(&changed, 0).expect_err(needle);
        assert!(err.contains(needle), "expected a refusal naming {needle:?}; got: {err}");
    }
    assert_eq!(w.source_reload_count(), 0, "a refused marker is not written");
    let m = meta_of(&finish(w));
    assert_eq!(m.version, META_DAT_VERSION);
}

#[test]
fn the_table_refuses_what_it_cannot_represent() {
    // Enabling after a path, and on a column-aware writer.
    let mut late = open("late");
    AbstractTraceWriter::ensure_path_id(&mut late, Path::new("/a"));
    assert!(late.enable_line_count_table().is_err(), "the table cannot be enabled after a bare path");
    let mut col = CtfsTraceWriter::new("col", &[]).with_output(CtfsOutput::Memory);
    col.enable_column_aware_steps();
    assert!(col.enable_line_count_table().is_err(), "bits 4 and 14 are mutually exclusive");

    // A version without the table.
    let mut bare = open("bare");
    assert!(bare.register_path_version(Path::new("/a"), 3).is_err(), "versions require the table");

    // A zero count; a step past the count; a step on an uncounted path.
    let mut w = open("refusals");
    w.enable_line_count_table().expect("table");
    assert!(w.register_path_with_line_count(Path::new("/a"), 0).is_err(), "a count of 0 is refused");
    w.register_path_with_line_count(Path::new("/a"), 5).unwrap();
    AbstractTraceWriter::register_step(&mut w, Path::new("/a"), Line(5));
    AbstractTraceWriter::register_step(&mut w, Path::new("/a"), Line(6));
    AbstractTraceWriter::register_step(&mut w, Path::new("/uncounted"), Line(1));
    assert_eq!(w.refusals().len(), 2, "two refused steps; refusals: {:?}", w.refusals());
    assert!(w.refusals()[0].contains("line 6"), "{:?}", w.refusals());
    assert!(w.refusals()[1].contains("/uncounted"), "{:?}", w.refusals());
    let err = TraceWriter::finish_writing_trace_events(&mut w).expect_err("a recording with refused steps must not finish as a success");
    assert!(err.to_string().contains("2 operation(s) were refused"), "got: {err}");
    let bytes = w.take_container_bytes().expect("the container is still finalized");
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("r.ct");
    std::fs::write(&p, &bytes).unwrap();
    let mut r = CtfsReader::open(&p).unwrap();
    let steps = step_record_count(&mut r);
    assert_eq!(steps, 1, "only the step inside the file is written");
}

/// A column-aware writer may write a reload marker between distinct paths, as
/// the Nim writer may; the marker goes through the column-aware exec encoder.
#[test]
fn a_column_aware_writer_writes_a_reload_marker() {
    let mut w = CtfsTraceWriter::new("col_reload", &[]).with_output(CtfsOutput::Memory);
    w.enable_column_aware_steps();
    w.declare_source_reload().expect("declare");
    TraceWriter::begin_writing_trace_events(&mut w, Path::new("col_reload")).unwrap();
    w.register_path_with_line_lengths(Path::new("/a"), &[4, 4]);
    w.register_path_with_line_lengths(Path::new("/b"), &[4, 4]);
    AbstractTraceWriter::register_step(&mut w, Path::new("/a"), Line(1));
    w.register_source_reload(
        &[SourceReloadChange {
            old_path_id: 0,
            new_path_id: 1,
            generation: 2,
        }],
        0,
    )
    .unwrap();
    AbstractTraceWriter::register_step(&mut w, Path::new("/b"), Line(2));
    let bytes = finish(w);
    assert_eq!(meta_of(&bytes).ext_flags, FLAG_EXT_HAS_SOURCE_RELOAD);
}

/// The number of `steps.dat` records.
fn step_record_count(r: &mut CtfsReader) -> u64 {
    codetracer_trace_reader::step_stream_reader::StepStreamReader::open(r)
        .expect("steps.dat decodes")
        .expect("steps.dat present")
        .count()
}
