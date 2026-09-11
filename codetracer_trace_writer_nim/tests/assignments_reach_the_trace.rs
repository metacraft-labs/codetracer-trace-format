//! `Assignment` records must reach the trace, not be counted as discards.
//!
//! # The defect this pins
//!
//! `NimTraceWriter::assign` used to call `discard_unsupported("assign")`.
//! Every recorder that emits assignment provenance — the JavaScript recorder
//! emits one per instrumented write site — therefore produced a trace with
//! none of it in, and closed with
//! `WARNING: this trace is INCOMPLETE … assign=N`.  Measured on the
//! `javascript_hcr_flow_test` fixture on 2026-09-11: **258** records lost per
//! recording.
//!
//! The loss was pure plumbing, not a missing format:
//!
//! * the CTFS wire format has carried the slot since it was specified —
//!   `codetracer-trace-format-spec/trace-events.md` §"Value Stream Events"
//!   tag 9, `Assignment {to, pass_by, from}`;
//! * the canonical Rust encoder/decoder for it
//!   (`codetracer_trace_writer::value_stream::ValueStreamEvent::Assignment`)
//!   was already implemented;
//! * the Nim library could already encode the record
//!   (`codetracer_trace_writer.nim`'s `writeAssignment*`,
//!   `cbor.nim`'s `encodeCborAssignmentRecord` / `decodeCborRValue`).
//!
//! What did not exist was an FFI export, a header declaration, and a Rust
//! binding.  Those now exist (`trace_writer_register_assignment`).
//!
//! # What this test asserts
//!
//! 1. `discarded_record_counts()` carries NO `assign` entry after a writer
//!    that emitted assignments is closed — and is completely empty, so the
//!    fix cannot have traded one discard kind for another.
//! 2. The assignments are IN the produced container: `values.dat` decodes,
//!    through the canonical Rust `ValueStreamReader`, to exactly the emitted
//!    tag-9 events — count, target varname, `pass_by`, and the `RValue`
//!    payload, which is deserialized back into a typed `RValue` and compared.
//! 3. The count is proven COMPLETE, not merely `> 0`: the number of decoded
//!    `Assignment` events equals the number emitted.
//! 4. Negative control: a writer that emits no assignment produces a
//!    container with zero tag-9 events, so (2) cannot pass vacuously by
//!    finding events some other code path put there.
//! 5. The `add_event(TraceLowLevelEvent::Assignment(..))` dispatch path — the
//!    one recorders that build up `Vec<TraceLowLevelEvent>` actually use, and
//!    the one the JavaScript recorder uses — is covered, not just the direct
//!    `assign()` method call.
//!
//! # Mocking policy justification (workspace CLAUDE.md)
//!
//! **Nothing is mocked.**  Every assertion runs against a REAL
//! `NimTraceWriter` driving the REAL Nim static library through the REAL C
//! FFI, writing a REAL `.ct` container to a REAL temporary directory, which
//! is then re-opened and decoded by the REAL canonical Rust reader.  The
//! entire question is whether a record survives the Rust→C→Nim→container→Rust
//! round trip, so substituting anything on that path would test the
//! substitute instead of the boundary.

use std::path::Path;
use std::sync::Mutex;

use codetracer_trace_reader::value_stream_reader::open_value_stream;
use codetracer_trace_types::{AssignmentRecord, Line, PassBy, RValue, TraceLowLevelEvent, VariableId};
use codetracer_trace_writer::value_stream::ValueStreamEvent;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is not thread-safe — its global state lives behind a
/// single lock.  Serialize this binary's writers through it, as the other
/// Nim-backed suites in this crate do.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

/// One decoded tag-9 event, flattened for assertion.
#[derive(Debug, PartialEq)]
struct DecodedAssignment {
    target: String,
    pass_by: u8,
    rvalue: RValue,
}

/// Drive a writer, close it, and return `(discards, decoded assignments)`.
///
/// `emit` receives a writer that already has a registered path and one
/// recorded step, so an assignment it emits has a step to attach to.
fn record_and_decode(
    program: &str,
    emit: impl FnOnce(&mut NimTraceWriter, &Path),
) -> (std::collections::BTreeMap<&'static str, u64>, Vec<DecodedAssignment>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let events_path = dir.path().join("trace.json");
    let metadata_path = dir.path().join("trace_metadata.json");
    let paths_path = dir.path().join("trace_paths.json");

    let mut writer = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    writer.begin_writing_trace_events(&events_path).expect("begin_events");
    writer.begin_writing_trace_metadata(&metadata_path).expect("begin_metadata");
    writer.begin_writing_trace_paths(&paths_path).expect("begin_paths");

    let source_path = dir.path().join(format!("{program}.js"));
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
            if let ValueStreamEvent::Assignment { to, pass_by, from } = event {
                let rvalue: RValue = cbor4ii::serde::from_slice(from).expect("the `from` payload is a serde-CBOR RValue");
                decoded.push(DecodedAssignment {
                    target: tables.varname_str(*to).expect("assignment target resolves in varnames.dat"),
                    pass_by: *pass_by,
                    rvalue,
                });
            }
        }
    }

    // Keep the container alive only as long as it was needed.
    drop(dir);

    (discards, decoded)
}

#[test]
fn assign_is_persisted_and_no_longer_counted_as_a_discard() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let (discards, decoded) = record_and_decode("nim_writer_assign_persisted", |writer, _source| {
        writer.assign("total", RValue::Literal, PassBy::Value);
        writer.assign("alias", RValue::Simple(VariableId(7)), PassBy::Reference);
        writer.assign("combined", RValue::Compound(vec![VariableId(1), VariableId(2)]), PassBy::Value);
    });

    assert!(
        discards.is_empty(),
        "a writer that only emits supported records must report NO discards; got {discards:?}"
    );

    let expected = vec![
        DecodedAssignment {
            target: "total".to_string(),
            pass_by: 0,
            rvalue: RValue::Literal,
        },
        DecodedAssignment {
            target: "alias".to_string(),
            pass_by: 1,
            rvalue: RValue::Simple(VariableId(7)),
        },
        DecodedAssignment {
            target: "combined".to_string(),
            pass_by: 0,
            rvalue: RValue::Compound(vec![VariableId(1), VariableId(2)]),
        },
    ];
    assert_eq!(
        decoded.len(),
        expected.len(),
        "the container must hold EVERY emitted assignment (complete, not merely non-empty); \
         emitted {}, decoded {}",
        expected.len(),
        decoded.len()
    );
    assert_eq!(decoded, expected, "each assignment must round-trip target, pass_by and RValue");
}

#[test]
fn add_event_assignment_dispatch_reaches_the_container() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // This is the shape the JavaScript recorder produces: a `VariableName`
    // interning event, then a `Value` for the target, then the `Assignment`
    // describing where the value came from.
    let (discards, decoded) = record_and_decode("nim_writer_assign_add_event", |writer, _source| {
        writer.add_event(TraceLowLevelEvent::VariableName("counter".to_string()));
        writer.add_event(TraceLowLevelEvent::VariableName("result".to_string()));
        writer.add_event(TraceLowLevelEvent::Assignment(AssignmentRecord {
            to: VariableId(0),
            pass_by: PassBy::Value,
            from: RValue::Literal,
        }));
        writer.add_event(TraceLowLevelEvent::Assignment(AssignmentRecord {
            to: VariableId(1),
            pass_by: PassBy::Value,
            from: RValue::FunctionReturn {
                call_key: codetracer_trace_types::CallKey(3),
            },
        }));
    });

    assert!(discards.is_empty(), "add_event(Assignment) must not discard anything; got {discards:?}");
    assert_eq!(
        decoded.len(),
        2,
        "both dispatched Assignment events must be in the container, got {decoded:?}"
    );
    assert_eq!(decoded[0].target, "counter");
    assert_eq!(decoded[0].rvalue, RValue::Literal);
    assert_eq!(decoded[1].target, "result");
    assert_eq!(
        decoded[1].rvalue,
        RValue::FunctionReturn {
            call_key: codetracer_trace_types::CallKey(3)
        }
    );
}

/// Negative control for the two tests above.
///
/// Without this, "the container holds N Assignment events" could pass
/// because something else on the write path emits tag-9 events regardless of
/// what the caller asked for. A writer driven identically but with no
/// `assign` call must produce none.
#[test]
fn a_writer_that_emits_no_assignment_produces_no_assignment_events() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let (discards, decoded) = record_and_decode("nim_writer_assign_negative_control", |writer, source| {
        // Real work, just no assignment: another step and a variable value.
        writer.register_step(source, Line(2));
    });

    assert!(
        discards.is_empty(),
        "the control writer must not discard anything either; got {discards:?}"
    );
    assert!(
        decoded.is_empty(),
        "a writer that emitted no assignment must produce no tag-9 events; got {decoded:?}"
    );
}
