//! `DropVariables` records must reach the trace, not be counted as discards.
//!
//! # The defect this pins
//!
//! `NimTraceWriter::drop_variables` used to call
//! `discard_unsupported("drop_variables")`.  Every recorder that reports scope
//! exits therefore produced a trace with none of them in it.  The BEAM
//! recorder worked around the gap by re-opening its own finished `.ct` and
//! hand-appending the records to a legacy combined `events.log` stream — which
//! a v4 container does not have, and whose mere presence flips
//! `codetracer_trace_reader`'s format discriminator onto the legacy branch, so
//! the container's real split streams were ignored entirely.
//!
//! As with `assign` before it, the loss was plumbing rather than a missing
//! format:
//!
//! * the CTFS wire format has carried the slot since it was specified —
//!   `codetracer-trace-format-spec/trace-events.md` §"Value Stream Events"
//!   tag 3, `DropVariables {count: varint, ids: [varint]}`, *"Drop multiple
//!   variables (end of scope)"*;
//! * the canonical Rust encoder/decoder for it
//!   (`codetracer_trace_writer::value_stream::ValueStreamEvent::DropVariables`)
//!   was already implemented.
//!
//! What did not exist was a Nim encoder, an FFI export, a header declaration,
//! and a Rust binding.  Those now exist
//! (`trace_writer_register_drop_variables`).
//!
//! # What this test asserts
//!
//! 1. `discarded_record_counts()` carries NO `drop_variables` entry after a
//!    writer that emitted scope exits is closed — and is completely empty, so
//!    the fix cannot have traded one discard kind for another.
//! 2. The drops are IN the produced container: `values.dat` decodes, through
//!    the canonical Rust `ValueStreamReader`, to exactly the emitted tag-3
//!    events, with each id resolved back to its name through `varnames.dat`.
//! 3. GROUPING is preserved.  Which variables left TOGETHER is what makes a
//!    drop a scope boundary, so two scopes closing must decode as two events,
//!    not as one flattened list — a distinction no count-only assertion can
//!    make.
//! 4. The count is proven COMPLETE, not merely `> 0`.
//! 5. Negative control: a writer that emits no drop produces a container with
//!    zero tag-3 events, so (2) cannot pass vacuously.
//! 6. The `add_event(TraceLowLevelEvent::DropVariables(..))` dispatch path —
//!    the one recorders that build up `Vec<TraceLowLevelEvent>` use — is
//!    covered, not just the direct `drop_variables()` call.
//!
//! # Mocking policy justification (workspace CLAUDE.md)
//!
//! **Nothing is mocked.**  Every assertion runs against a REAL
//! `NimTraceWriter` driving the REAL Nim static library through the REAL C
//! FFI, writing a REAL `.ct` container to a REAL temporary directory, which is
//! then re-opened and decoded by the REAL canonical Rust reader.  The entire
//! question is whether a record survives the Rust→C→Nim→container→Rust round
//! trip, so substituting anything on that path would test the substitute
//! instead of the boundary.

use std::path::Path;
use std::sync::Mutex;

use codetracer_trace_reader::value_stream_reader::open_value_stream;
use codetracer_trace_types::{Line, TraceLowLevelEvent, VariableId};
use codetracer_trace_writer::value_stream::ValueStreamEvent;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is not thread-safe — its global state lives behind a
/// single lock.  Serialize this binary's writers through it, as the other
/// Nim-backed suites in this crate do.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Drive a writer, close it, and return `(discards, decoded drop events)`.
///
/// Each decoded element is ONE tag-3 event's variable names, in wire order.
/// Events are kept separate rather than concatenated so a test can tell two
/// scopes closing from one scope closing twice as many variables.
fn record_and_decode(
    program: &str,
    emit: impl FnOnce(&mut NimTraceWriter, &Path),
) -> (std::collections::BTreeMap<&'static str, u64>, Vec<Vec<String>>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let events_path = dir.path().join("trace.json");
    let metadata_path = dir.path().join("trace_metadata.json");
    let paths_path = dir.path().join("trace_paths.json");

    let mut writer = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    writer.begin_writing_trace_events(&events_path).expect("begin_events");
    writer.begin_writing_trace_metadata(&metadata_path).expect("begin_metadata");
    writer.begin_writing_trace_paths(&paths_path).expect("begin_paths");

    let source_path = dir.path().join(format!("{program}.ex"));
    writer
        .register_path_with_line_count(&source_path, 16)
        .expect("register_path_with_line_count");
    writer.register_step(&source_path, Line(1));

    emit(&mut writer, &source_path);

    writer.finish_writing_trace_events().expect("finish_events");
    writer.finish_writing_trace_metadata().expect("finish_metadata");
    writer.finish_writing_trace_paths().expect("finish_paths");
    writer.close().expect("close");

    let discards = writer.discarded_record_counts().clone();
    drop(writer);

    let ct_path = dir.path().join(format!("{program}.ct"));
    assert!(ct_path.exists(), ".ct trace file was not created at {}", ct_path.display());

    let tables = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_path)
        .expect("interning tables open")
        .expect("interning tables present");

    let mut reader = open_value_stream(&ct_path).expect("values.dat opens").expect("values.dat present");
    let records = reader.read_all().expect("values.dat decodes");

    let mut decoded = Vec::new();
    for record in &records {
        for event in &record.events {
            if let ValueStreamEvent::DropVariables { variable_ids } = event {
                let names = variable_ids
                    .iter()
                    .map(|id| tables.varname_str(*id).expect("dropped id resolves in varnames.dat"))
                    .collect::<Vec<String>>();
                decoded.push(names);
            }
        }
    }

    // Keep the container alive only as long as it was needed.
    drop(dir);

    (discards, decoded)
}

/// As [`record_and_decode`], but collecting the tag-2 `DropVariable` events —
/// one name each — instead of the tag-3 scope exits.
fn record_and_decode_singular(
    program: &str,
    emit: impl FnOnce(&mut NimTraceWriter, &Path),
) -> (std::collections::BTreeMap<&'static str, u64>, Vec<String>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let events_path = dir.path().join("trace.json");
    let metadata_path = dir.path().join("trace_metadata.json");
    let paths_path = dir.path().join("trace_paths.json");

    let mut writer = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    writer.begin_writing_trace_events(&events_path).expect("begin_events");
    writer.begin_writing_trace_metadata(&metadata_path).expect("begin_metadata");
    writer.begin_writing_trace_paths(&paths_path).expect("begin_paths");

    let source_path = dir.path().join(format!("{program}.ex"));
    writer
        .register_path_with_line_count(&source_path, 16)
        .expect("register_path_with_line_count");
    writer.register_step(&source_path, Line(1));

    emit(&mut writer, &source_path);

    writer.finish_writing_trace_events().expect("finish_events");
    writer.finish_writing_trace_metadata().expect("finish_metadata");
    writer.finish_writing_trace_paths().expect("finish_paths");
    writer.close().expect("close");

    let discards = writer.discarded_record_counts().clone();
    drop(writer);

    let ct_path = dir.path().join(format!("{program}.ct"));
    let tables = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_path)
        .expect("interning tables open")
        .expect("interning tables present");
    let mut reader = open_value_stream(&ct_path).expect("values.dat opens").expect("values.dat present");
    let records = reader.read_all().expect("values.dat decodes");

    let mut decoded = Vec::new();
    for record in &records {
        for event in &record.events {
            if let ValueStreamEvent::DropVariable { variable_id } = event {
                decoded.push(tables.varname_str(*variable_id).expect("dropped id resolves in varnames.dat"));
            }
        }
    }

    drop(dir);

    (discards, decoded)
}

/// Tag 2 and tag 3 mean different things, so one must never be recorded as the
/// other.
///
/// This is the failure that would not crash anything: a lone variable ending
/// its life, written as a one-element scope exit, reads back as a scope
/// boundary the program never had — and a test that only counted drop events
/// would pass either way. Both accessors run against the SAME container, and
/// each must see only its own tag.
#[test]
fn a_single_drop_and_a_scope_exit_stay_distinct_in_the_container() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let emit = |writer: &mut NimTraceWriter, _source: &Path| {
        writer.drop_variable("solo");
        writer.drop_variables(&["scoped_a".to_string(), "scoped_b".to_string()]);
        writer.drop_variable("solo_again");
    };

    let (discards, plural) = record_and_decode("nim_writer_drop_kinds_plural", emit);
    assert!(discards.is_empty(), "neither drop form may be discarded; got {discards:?}");
    assert_eq!(
        plural,
        vec![vec!["scoped_a".to_string(), "scoped_b".to_string()]],
        "the tag-3 accessor must see ONLY the scope exit, not the two singular drops"
    );

    let (discards, singular) = record_and_decode_singular("nim_writer_drop_kinds_singular", emit);
    assert!(discards.is_empty(), "neither drop form may be discarded; got {discards:?}");
    assert_eq!(
        singular,
        vec!["solo".to_string(), "solo_again".to_string()],
        "the tag-2 accessor must see ONLY the singular drops, in wire order, \
         not the scope exit between them"
    );
}

/// Negative control for the tag-2 half: a writer that only closes a scope must
/// produce no singular drops, so the assertion above cannot pass by finding
/// tag-2 events that the plural call emitted.
#[test]
fn a_scope_exit_does_not_emit_singular_drop_events() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let (discards, singular) = record_and_decode_singular("nim_writer_drop_singular_control", |writer, _s| {
        writer.drop_variables(&["a".to_string(), "b".to_string()]);
    });

    assert!(discards.is_empty(), "the control writer must not discard anything; got {discards:?}");
    assert!(
        singular.is_empty(),
        "a scope exit must not decompose into singular drops; got {singular:?}"
    );
}

#[test]
fn drop_variables_is_persisted_and_no_longer_counted_as_a_discard() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // Two scopes closing, with different arities — the second is a single
    // variable, which is the case a flattening bug would merge into the first.
    let (discards, decoded) = record_and_decode("nim_writer_drops_persisted", |writer, _source| {
        writer.drop_variables(&["acc".to_string(), "idx".to_string(), "tmp".to_string()]);
        writer.drop_variables(&["result".to_string()]);
    });

    assert!(
        discards.is_empty(),
        "a writer that only emits supported records must report NO discards; got {discards:?}"
    );

    let expected: Vec<Vec<String>> = vec![vec!["acc".to_string(), "idx".to_string(), "tmp".to_string()], vec!["result".to_string()]];
    assert_eq!(
        decoded.len(),
        expected.len(),
        "the container must hold EVERY emitted scope exit as its own event \
         (complete, not merely non-empty, and not flattened); emitted {}, decoded {}: {decoded:?}",
        expected.len(),
        decoded.len()
    );
    assert_eq!(
        decoded, expected,
        "each scope exit must round-trip its variable names, in order, grouped as emitted"
    );
}

#[test]
fn add_event_drop_variables_dispatch_reaches_the_container() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // The shape a recorder building `Vec<TraceLowLevelEvent>` produces: the
    // names are interned first, then referred to by id.
    let (discards, decoded) = record_and_decode("nim_writer_drops_add_event", |writer, _source| {
        writer.add_event(TraceLowLevelEvent::VariableName("counter".to_string()));
        writer.add_event(TraceLowLevelEvent::VariableName("result".to_string()));
        writer.add_event(TraceLowLevelEvent::DropVariables(vec![VariableId(0), VariableId(1)]));
    });

    assert!(
        discards.is_empty(),
        "add_event(DropVariables) must not discard anything; got {discards:?}"
    );
    assert_eq!(
        decoded.len(),
        1,
        "the dispatched DropVariables event must be in the container, got {decoded:?}"
    );
    assert_eq!(
        decoded[0],
        vec!["counter".to_string(), "result".to_string()],
        "the dispatch must resolve each VariableId through the writer's variable table \
         and drop both names as ONE scope exit"
    );
}

/// An empty scope exit is recorded rather than dropped.
///
/// A scope that bound nothing still ended, and the C ABI accepts a zero count
/// deliberately.  If the writer silently swallowed the empty case, a recorder
/// could not distinguish "this scope bound nothing" from "this scope was never
/// reported" — which is the same silent-incompleteness class this suite exists
/// to prevent, in miniature.
#[test]
fn an_empty_scope_exit_is_still_recorded() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let (discards, decoded) = record_and_decode("nim_writer_drops_empty", |writer, _source| {
        writer.drop_variables(&[]);
    });

    assert!(discards.is_empty(), "an empty drop must not be counted as a discard; got {discards:?}");
    assert_eq!(
        decoded,
        vec![Vec::<String>::new()],
        "an empty scope exit must reach the container as one tag-3 event carrying zero ids"
    );
}

/// Negative control for the tests above.
///
/// Without this, "the container holds N DropVariables events" could pass
/// because something else on the write path emits tag-3 events regardless of
/// what the caller asked for. A writer driven identically but with no
/// `drop_variables` call must produce none.
#[test]
fn a_writer_that_emits_no_drop_produces_no_drop_events() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let (discards, decoded) = record_and_decode("nim_writer_drops_negative_control", |writer, source| {
        // Real work, just no scope exit.
        writer.register_step(source, Line(2));
    });

    assert!(
        discards.is_empty(),
        "the control writer must not discard anything either; got {discards:?}"
    );
    assert!(
        decoded.is_empty(),
        "a writer that emitted no scope exit must produce no tag-3 events; got {decoded:?}"
    );
}
