//! A `BigInt` value must survive the Cap'n Proto round trip.
//!
//! # The defect this pins
//!
//! `conv_valuerecord`'s `BigInt` arm walked its magnitude with an INCLUSIVE
//! range:
//!
//! ```ignore
//! let mut bigint_b = qbigint.reborrow().init_b(b.len().try_into().unwrap());
//! for i in 0..=b.len() {
//!     bigint_b.set(i.try_into().unwrap(), b[i]);
//! }
//! ```
//!
//! The final iteration indexes `b[b.len()]`, so `write_trace` panicked on
//! **every** trace containing a `BigInt` — and had it not panicked, it would
//! have written one element past the end of a list sized `b.len()`.
//!
//! It survived because this crate had no tests at all, and because `BigInt`
//! only appears for integers too wide for `i128` — so the ordinary corpus
//! never produced one. That is the shape worth naming: not a rare branch that
//! was wrong in a subtle way, but an unconditionally broken branch that
//! nothing ever executed.
//!
//! # What this asserts
//!
//! 1. Writing a `BigInt` does not panic (the regression itself).
//! 2. The magnitude bytes come back byte for byte, INCLUDING the last one —
//!    an off-by-one in the other direction would silently truncate it, and a
//!    length-only check would not notice.
//! 3. `negative` and `type_id` survive alongside it, so a fix that got the
//!    bytes right by rebuilding the record wrongly still fails.
//! 4. An empty magnitude round-trips, because `0..=0` iterating once is what
//!    the old loop did on an empty vector — the boundary the bug lived on.

use codetracer_trace_types::{FullValueRecord, TraceLowLevelEvent, TypeId, ValueRecord, VariableId};

/// Round-trip one event list through `write_trace` / `read_trace`.
fn round_trip(events: &[TraceLowLevelEvent]) -> Vec<TraceLowLevelEvent> {
    let mut buf: Vec<u8> = Vec::new();
    codetracer_trace_format_capnp::capnptrace::write_trace(events, &mut buf).expect("write_trace");
    let mut cursor = std::io::BufReader::new(std::io::Cursor::new(buf));
    codetracer_trace_format_capnp::capnptrace::read_trace(&mut cursor).expect("read_trace")
}

fn bigint_event(b: Vec<u8>, negative: bool) -> TraceLowLevelEvent {
    TraceLowLevelEvent::Value(FullValueRecord {
        variable_id: VariableId(0),
        value: ValueRecord::BigInt {
            b,
            negative,
            type_id: TypeId(7),
        },
    })
}

fn expect_bigint(events: &[TraceLowLevelEvent]) -> (&Vec<u8>, bool, TypeId) {
    let mut found = None;
    for ev in events {
        if let TraceLowLevelEvent::Value(FullValueRecord {
            value: ValueRecord::BigInt { b, negative, type_id },
            ..
        }) = ev
        {
            assert!(found.is_none(), "expected exactly one BigInt in the decoded trace");
            found = Some((b, *negative, *type_id));
        }
    }
    found.expect("the decoded trace carries no BigInt at all")
}

#[test]
fn a_bigint_magnitude_round_trips_including_its_last_byte() {
    // A magnitude whose bytes are all distinct, so a shift or truncation
    // cannot coincidentally still compare equal. The last byte is the one the
    // inclusive-range bug ran past.
    let magnitude: Vec<u8> = (1u8..=16).collect();

    let decoded = round_trip(&[bigint_event(magnitude.clone(), false)]);
    let (b, negative, type_id) = expect_bigint(&decoded);

    assert_eq!(
        b, &magnitude,
        "the magnitude must come back byte for byte; a truncated last byte here is the \
         off-by-one this test exists for"
    );
    assert_eq!(b.len(), magnitude.len(), "the magnitude length must be preserved");
    assert!(!negative, "sign must survive alongside the magnitude");
    assert_eq!(type_id, TypeId(7), "type id must survive alongside the magnitude");
}

#[test]
fn a_negative_bigint_keeps_its_sign() {
    let magnitude: Vec<u8> = vec![0xFF, 0x00, 0x7F];

    let decoded = round_trip(&[bigint_event(magnitude.clone(), true)]);
    let (b, negative, _) = expect_bigint(&decoded);

    assert_eq!(b, &magnitude);
    assert!(negative, "a negative BigInt must not come back positive");
}

#[test]
fn an_empty_bigint_magnitude_round_trips() {
    // The boundary the bug lived on: with an empty vector the old `0..=b.len()`
    // still iterated once and indexed `b[0]`.
    let decoded = round_trip(&[bigint_event(Vec::new(), false)]);
    let (b, _, _) = expect_bigint(&decoded);

    assert!(b.is_empty(), "an empty magnitude must come back empty, got {b:?}");
}
