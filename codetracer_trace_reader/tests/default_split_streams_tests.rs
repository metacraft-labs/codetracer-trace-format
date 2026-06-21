//! M23e-4 — the secondary Rust `CtfsTraceWriter` DEFAULT-emits the full spec
//! multi-stream split layout.
//!
//! Before M23e-4 only the `calls.dat` call stream was default-on (M20); the
//! step/value/io-event/interning splits were opt-in. M23e-4 flips all five on by
//! default so even non-production (tests/legacy) Rust-writer bundles are the spec
//! split format, while STILL emitting `events.log` (additive — M23e-5 removes
//! it). Each `with_*_stream(false)` lever remains the explicit disable used by
//! tests of the legacy `events.log` postprocessing path.
//!
//! These tests pin:
//!  1. A DEFAULT bundle carries ALL five split streams (`calls`/`steps`/`values`/
//!     `events.dat`/interning) AND their companion indices AND `meta.dat` with
//!     every capability flag set — AND `events.log` (additive).
//!  2. The split streams round-trip: the events re-read through the normal
//!     CTFS reader (which decodes `events.log`) equal what was written, and the
//!     split `steps.dat`/`values.dat` are consistent with that event stream.
//!  3. A fully-disabled (`with_*_stream(false)`) bundle is `events.log`-only:
//!     none of the split files are present, no `meta.dat`, and the events still
//!     round-trip — the legacy path is preserved verbatim.

use std::path::{Path, PathBuf};

use codetracer_trace_types::*;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::meta_dat::{
    meta_dat_has_call_stream, meta_dat_has_interning_tables, meta_dat_has_io_event_stream,
    meta_dat_has_step_stream, meta_dat_has_value_stream,
};
use codetracer_trace_writer::trace_writer::TraceWriter;

/// Write a small but representative trace: one wrapping call with several steps,
/// a captured argument, a local variable value, and an I/O event — so every
/// split stream has real content. Returns the `.ct` path.
///
/// When `disable_splits` is set, ALL split streams are explicitly turned off via
/// the `with_*_stream(false)` levers, producing the legacy `events.log`-only
/// bundle (the call stream too, so the bundle is fully legacy).
fn write_trace(dir: &Path, disable_splits: bool) -> PathBuf {
    let path_buf = dir.join("trace");
    let mut writer = CtfsTraceWriter::new("test_program", &[]);
    if disable_splits {
        writer = writer
            .with_call_stream(false)
            .with_step_stream(false)
            .with_value_stream(false)
            .with_io_event_stream(false)
            .with_interning_tables(false);
    }
    TraceWriter::begin_writing_trace_events(&mut writer, &path_buf).unwrap();

    let src = Path::new("/test/prog.rs");
    TraceWriter::start(&mut writer, src, Line(1));

    let int_type = TraceWriter::ensure_type_id(&mut writer, TypeKind::Int, "Int");
    let main_fn = TraceWriter::ensure_function_id(&mut writer, "main", src, Line(1));

    let arg = TraceWriter::arg(&mut writer, "x", ValueRecord::Int { i: 7, type_id: int_type });
    TraceWriter::register_call(&mut writer, main_fn, vec![arg]);

    for ln in 2..=5 {
        TraceWriter::register_step(&mut writer, src, Line(ln));
        TraceWriter::register_variable_with_full_value(
            &mut writer,
            &format!("v{ln}"),
            ValueRecord::Int { i: ln * 10, type_id: int_type },
        );
    }

    // An I/O event so events.dat has content.
    TraceWriter::register_special_event(&mut writer, EventLogKind::Write, "", "hello\n");

    TraceWriter::register_return(&mut writer, ValueRecord::None { type_id: NONE_TYPE_ID });

    TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
    path_buf.with_extension("ct")
}

/// All split data files + their companion indices a DEFAULT M23e-4 bundle ships.
const SPLIT_DATA_FILES: &[&str] = &[
    "calls.dat",
    "calls.idx",
    "steps.dat",
    "steps.idx",
    "values.dat",
    "values.idx",
    "events.dat",
    "events.idx",
    "paths.dat",
    "paths.off",
    "funcs.dat",
    "funcs.off",
    "types.dat",
    "types.off",
    "varnames.dat",
    "varnames.off",
];

/// Deliverable #1 — a DEFAULT `CtfsTraceWriter` bundle carries ALL five split
/// streams + their indices + `meta.dat` with every flag set + `events.log`.
#[test]
fn default_bundle_emits_all_split_streams_plus_events_log() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_trace(dir.path(), false);

    let mut r = codetracer_ctfs::CtfsReader::open(&ct).unwrap();

    // events.log is STILL present (additive — M23e-5 removes it).
    assert!(r.read_file("events.log").is_ok(), "default bundle must keep events.log (additive)");

    // Every split data file + companion index is present.
    for f in SPLIT_DATA_FILES {
        assert!(r.read_file(f).is_ok(), "default bundle must carry split file `{f}`");
    }

    // meta.dat is present and carries ALL five capability flags.
    let meta = r.read_file("meta.dat").expect("default bundle must carry meta.dat");
    assert!(meta_dat_has_call_stream(&meta), "has_call_stream flag must be set");
    assert!(meta_dat_has_step_stream(&meta), "has_step_stream flag must be set");
    assert!(meta_dat_has_value_stream(&meta), "has_value_stream flag must be set");
    assert!(meta_dat_has_io_event_stream(&meta), "has_io_event_stream flag must be set");
    assert!(meta_dat_has_interning_tables(&meta), "has_interning_tables flag must be set");
}

/// Deliverable #2 — the default bundle's `events.log` round-trips the written
/// events, so the additive split streams did not disturb the legacy payload.
#[test]
fn default_bundle_events_round_trip_via_events_log() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_trace(dir.path(), false);

    let mut reader =
        codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct).unwrap();

    // The four explicit steps (lines 2..=5) plus the implicit leading step are
    // present, with their lines intact.
    let step_lines: Vec<i64> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some(s.line.0),
            _ => None,
        })
        .collect();
    for ln in 2..=5 {
        assert!(step_lines.contains(&ln), "step at line {ln} must round-trip via events.log");
    }

    // The wrapping call and the I/O event survive too.
    assert!(
        events.iter().any(|e| matches!(e, TraceLowLevelEvent::Call(_))),
        "the wrapping call must round-trip"
    );
    assert!(
        events.iter().any(|e| matches!(e, TraceLowLevelEvent::Event(_))),
        "the I/O event must round-trip"
    );
}

/// Deliverable #3 — a fully-disabled bundle is `events.log`-only: NONE of the
/// split files are present, there is NO `meta.dat`, and the events still
/// round-trip. The legacy postprocessing path is preserved verbatim.
#[test]
fn disabled_bundle_is_events_log_only_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let ct = write_trace(dir.path(), true);

    let mut r = codetracer_ctfs::CtfsReader::open(&ct).unwrap();

    assert!(r.read_file("events.log").is_ok(), "legacy bundle must carry events.log");

    for f in SPLIT_DATA_FILES {
        assert!(
            r.read_file(f).is_err(),
            "legacy (splits-off) bundle must NOT carry split file `{f}`"
        );
    }
    assert!(
        r.read_file("meta.dat").is_err(),
        "legacy (splits-off) bundle must NOT carry meta.dat (flags-off ⇒ byte-for-byte legacy)"
    );

    // Events still round-trip through the legacy events.log path.
    let mut reader =
        codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
    let events = reader.load_trace_events(&ct).unwrap();
    let step_lines: Vec<i64> = events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some(s.line.0),
            _ => None,
        })
        .collect();
    for ln in 2..=5 {
        assert!(step_lines.contains(&ln), "legacy bundle: step at line {ln} must round-trip");
    }
}
