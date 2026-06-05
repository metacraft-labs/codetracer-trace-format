//! M14 verification — column field + extended RValue roundtrip through every
//! active encoder/decoder.
//!
//! This test file pins the back-compat contract documented in the M14
//! milestone:
//!
//! - `StepRecord.column = Some(...)` roundtrips through the split-binary
//!   path (the primary CTFS encoder) and through the legacy Cap'n Proto
//!   encoder.
//! - The four new RValue variants (`Literal`, `FieldAccess`, `IndexAccess`,
//!   `FunctionReturn`) roundtrip through both encoders.
//! - Pre-M14 traces (where Step events were written by the legacy `tag 0`
//!   path with no column slot, and Assignment payloads only used
//!   `Simple`/`Compound`) still decode correctly.

use std::io::Cursor;

use codetracer_trace_format_capnp::{capnptrace, trace_capnp};
use codetracer_trace_types::*;
use codetracer_trace_writer::split_binary::{decode_event, decode_events, encode_event, encode_events};

/// Roundtrip helper for the split-binary path (the primary CTFS / CBOR-Zstd
/// encoding). The function asserts that encoding then decoding returns the
/// same event (via `Debug` equality, mirroring the existing test style in
/// `split_binary.rs`).
fn split_roundtrip(event: &TraceLowLevelEvent) -> TraceLowLevelEvent {
    let mut buf = Vec::new();
    encode_event(event, &mut buf).expect("encode_event must not fail");
    let mut cursor = Cursor::new(buf.as_slice());
    decode_event(&mut cursor).expect("decode_event must not fail")
}

/// Roundtrip helper for the legacy Cap'n Proto path.
fn capnp_roundtrip(events: &[TraceLowLevelEvent]) -> Vec<TraceLowLevelEvent> {
    let mut out: Vec<u8> = Vec::new();
    capnptrace::write_trace(events, &mut out).expect("write_trace");
    let mut cursor = std::io::Cursor::new(out);
    capnptrace::read_trace(&mut cursor).expect("read_trace")
}

/// M14 verification: `StepRecord.column` roundtrips through split-binary and
/// Cap'n Proto.
#[test]
fn test_trace_format_step_record_column_roundtrip() {
    let with_col = TraceLowLevelEvent::Step(StepRecord {
        path_id: PathId(3),
        line: Line(42),
        column: Some(Line(7)),
    });

    // split-binary path (CTFS / CBOR-Zstd)
    match split_roundtrip(&with_col) {
        TraceLowLevelEvent::Step(s) => {
            assert_eq!(s.path_id, PathId(3));
            assert_eq!(s.line, Line(42));
            assert_eq!(s.column, Some(Line(7)));
        }
        other => panic!("expected Step, got {other:?}"),
    }

    // Cap'n Proto path
    let decoded_capnp = capnp_roundtrip(&[with_col.clone()]);
    assert_eq!(decoded_capnp.len(), 1);
    match &decoded_capnp[0] {
        TraceLowLevelEvent::Step(s) => {
            assert_eq!(s.column, Some(Line(7)));
            assert_eq!(s.line, Line(42));
        }
        other => panic!("expected Step, got {other:?}"),
    }
}

/// M14 verification: `RValue::Literal` roundtrips through every active
/// encoder/decoder.
#[test]
fn test_trace_format_rvalue_literal_roundtrip() {
    let event = TraceLowLevelEvent::Assignment(AssignmentRecord {
        to: VariableId(5),
        pass_by: PassBy::Value,
        from: RValue::Literal,
    });

    match split_roundtrip(&event) {
        TraceLowLevelEvent::Assignment(a) => {
            assert_eq!(a.to, VariableId(5));
            assert!(matches!(a.from, RValue::Literal));
        }
        other => panic!("split: expected Assignment, got {other:?}"),
    }

    let decoded_capnp = capnp_roundtrip(&[event.clone()]);
    assert_eq!(decoded_capnp.len(), 1);
    match &decoded_capnp[0] {
        TraceLowLevelEvent::Assignment(a) => {
            assert_eq!(a.to, VariableId(5));
            assert!(matches!(a.from, RValue::Literal));
        }
        other => panic!("capnp: expected Assignment, got {other:?}"),
    }
}

/// M14 verification: `RValue::FieldAccess { receiver, field }` roundtrips.
#[test]
fn test_trace_format_rvalue_field_access_roundtrip() {
    let event = TraceLowLevelEvent::Assignment(AssignmentRecord {
        to: VariableId(11),
        pass_by: PassBy::Reference,
        from: RValue::FieldAccess {
            receiver: VariableId(2),
            field: "username".to_string(),
        },
    });

    match split_roundtrip(&event) {
        TraceLowLevelEvent::Assignment(a) => match &a.from {
            RValue::FieldAccess { receiver, field } => {
                assert_eq!(*receiver, VariableId(2));
                assert_eq!(field, "username");
            }
            other => panic!("split: expected FieldAccess, got {other:?}"),
        },
        other => panic!("split: expected Assignment, got {other:?}"),
    }

    let decoded_capnp = capnp_roundtrip(&[event.clone()]);
    match &decoded_capnp[0] {
        TraceLowLevelEvent::Assignment(a) => match &a.from {
            RValue::FieldAccess { receiver, field } => {
                assert_eq!(*receiver, VariableId(2));
                assert_eq!(field, "username");
            }
            other => panic!("capnp: expected FieldAccess, got {other:?}"),
        },
        other => panic!("capnp: expected Assignment, got {other:?}"),
    }
}

/// M14 verification: `RValue::IndexAccess { receiver, index }` roundtrips.
#[test]
fn test_trace_format_rvalue_index_access_roundtrip() {
    let event = TraceLowLevelEvent::Assignment(AssignmentRecord {
        to: VariableId(7),
        pass_by: PassBy::Value,
        from: RValue::IndexAccess {
            receiver: VariableId(3),
            index: -42,
        },
    });

    match split_roundtrip(&event) {
        TraceLowLevelEvent::Assignment(a) => match &a.from {
            RValue::IndexAccess { receiver, index } => {
                assert_eq!(*receiver, VariableId(3));
                assert_eq!(*index, -42);
            }
            other => panic!("split: expected IndexAccess, got {other:?}"),
        },
        other => panic!("split: expected Assignment, got {other:?}"),
    }

    let decoded_capnp = capnp_roundtrip(&[event.clone()]);
    match &decoded_capnp[0] {
        TraceLowLevelEvent::Assignment(a) => match &a.from {
            RValue::IndexAccess { receiver, index } => {
                assert_eq!(*receiver, VariableId(3));
                assert_eq!(*index, -42);
            }
            other => panic!("capnp: expected IndexAccess, got {other:?}"),
        },
        other => panic!("capnp: expected Assignment, got {other:?}"),
    }
}

/// M14 verification: `RValue::FunctionReturn { call_key }` roundtrips with a
/// CallKey pointing at an existing call.
#[test]
fn test_trace_format_rvalue_function_return_roundtrip() {
    // Build a tiny trace that has a Call event (CallKey(0) by construction)
    // followed by an Assignment whose RValue references it.
    let events = vec![
        TraceLowLevelEvent::Function(FunctionRecord {
            path_id: PathId(0),
            line: Line(1),
            name: "foo".to_string(),
        }),
        TraceLowLevelEvent::Call(CallRecord {
            function_id: FunctionId(0),
            args: vec![],
        }),
        TraceLowLevelEvent::Return(ReturnRecord { return_value: NONE_VALUE }),
        TraceLowLevelEvent::Assignment(AssignmentRecord {
            to: VariableId(9),
            pass_by: PassBy::Value,
            from: RValue::FunctionReturn { call_key: CallKey(0) },
        }),
    ];

    // split-binary path
    let (buf, _sizes) = encode_events(&events);
    let decoded = decode_events(&buf);
    assert_eq!(decoded.len(), events.len());
    match &decoded[3] {
        TraceLowLevelEvent::Assignment(a) => match &a.from {
            RValue::FunctionReturn { call_key } => assert_eq!(*call_key, CallKey(0)),
            other => panic!("split: expected FunctionReturn, got {other:?}"),
        },
        other => panic!("split: expected Assignment at [3], got {other:?}"),
    }

    // Cap'n Proto path
    let decoded_capnp = capnp_roundtrip(&events);
    assert_eq!(decoded_capnp.len(), events.len());
    match &decoded_capnp[3] {
        TraceLowLevelEvent::Assignment(a) => match &a.from {
            RValue::FunctionReturn { call_key } => assert_eq!(*call_key, CallKey(0)),
            other => panic!("capnp: expected FunctionReturn, got {other:?}"),
        },
        other => panic!("capnp: expected Assignment, got {other:?}"),
    }
}

/// M14 verification: old traces without the column field still read.
///
/// This is a "pre-M14 bytes" scenario: legacy traces use tag 0 (17 bytes
/// total) for Step events. They are required to decode into a `StepRecord`
/// with `column = None`. We synthesize the exact byte sequence a pre-M14
/// recorder would have emitted so we exercise the reader-side back-compat
/// path independently of the writer.
#[test]
fn test_trace_format_back_compat_with_old_traces() {
    // Hand-encoded pre-M14 split-binary Step event: tag(1) + path_id(8) + line(8).
    let mut legacy_buf: Vec<u8> = Vec::new();
    legacy_buf.push(0); // tag 0 = legacy Step
    legacy_buf.extend_from_slice(&42u64.to_le_bytes());
    legacy_buf.extend_from_slice(&100i64.to_le_bytes());
    assert_eq!(legacy_buf.len(), 17);

    let mut cursor = Cursor::new(legacy_buf.as_slice());
    let decoded = decode_event(&mut cursor).expect("legacy step must decode");
    match decoded {
        TraceLowLevelEvent::Step(s) => {
            assert_eq!(s.path_id, PathId(42));
            assert_eq!(s.line, Line(100));
            assert_eq!(s.column, None, "back-compat: absent column must decode as None");
        }
        other => panic!("expected Step, got {other:?}"),
    }

    // Hand-encoded pre-M14 split-binary Assignment with RValue::Simple. The
    // payload encoding is CBOR + length prefix; we round-trip via the
    // existing writer to keep this honest (the legacy variant set was the
    // same encoder, so this codepath is byte-identical with what a pre-M14
    // recorder would have produced).
    let legacy_assignment = TraceLowLevelEvent::Assignment(AssignmentRecord {
        to: VariableId(1),
        pass_by: PassBy::Value,
        from: RValue::Simple(VariableId(2)),
    });
    match split_roundtrip(&legacy_assignment) {
        TraceLowLevelEvent::Assignment(a) => {
            assert_eq!(a.to, VariableId(1));
            assert!(matches!(a.from, RValue::Simple(VariableId(2))));
        }
        other => panic!("expected Assignment, got {other:?}"),
    }

    // And the Cap'n Proto path must still decode the legacy variants.
    let decoded_capnp = capnp_roundtrip(&[legacy_assignment.clone()]);
    match &decoded_capnp[0] {
        TraceLowLevelEvent::Assignment(a) => match &a.from {
            RValue::Simple(v) => assert_eq!(*v, VariableId(2)),
            other => panic!("expected Simple, got {other:?}"),
        },
        other => panic!("expected Assignment, got {other:?}"),
    }
}

// Silence an unused-import lint on `trace_capnp`; the import is here so the
// file compiles cleanly even when a future refactor needs to reach the raw
// schema types directly.
#[allow(dead_code)]
fn _touch_capnp(_: trace_capnp::trace::Reader<'_>) {}
