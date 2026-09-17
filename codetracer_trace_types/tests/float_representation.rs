//! A `Float` is a native number in CBOR and a decimal string in JSON.
//!
//! # Why the two differ
//!
//! `ValueRecord::Float` used to carry `#[serde_as(as = "DisplayFromStr")]`, so
//! it went out as a STRING in every format. That is required in JSON, which
//! cannot express an infinity or a NaN — `codetracer_trace_util`'s `trace.json`
//! fixture pins `"inf"`, `"+inf"`, `"-inf"` and `"nan"` — but it is wrong in
//! CBOR, which encodes IEEE-754 directly.
//!
//! The consequence was not cosmetic. The Nim writer writes `values.dat` floats
//! natively (`streaming_value_encoder.nim`'s `writeFloat`), so the Rust reader
//! refused every Nim-written container carrying one:
//!
//!     invalid type: floating point `2.5`, expected a string
//!
//! It also made `Float` the only variant not written natively — `Int` and
//! `Bool` were already a CBOR integer and a CBOR bool — which is the tell that
//! a text format's limitation had leaked into a binary one.
//!
//! # What this asserts
//!
//! 1. CBOR carries a float as a CBOR float, and it round-trips.
//! 2. **The three non-finites round-trip through CBOR**: `inf`, `-inf`, `NaN`.
//!    They are the entire reason the string form existed, so they are what
//!    proves replacing it is not a regression. `NaN` is compared with
//!    `is_nan()`, because `NaN != NaN` and an equality assertion on it passes
//!    or fails for reasons unrelated to what is being tested.
//! 3. JSON still emits the decimal-string form, spelled out literally, so the
//!    `trace.json` fixtures keep parsing.
//! 4. JSON still round-trips the non-finites, which is the property that
//!    forced the string form in the first place.
//! 5. The two formats disagree ON PURPOSE, and the CBOR form is not a string —
//!    asserted directly on the bytes, so this cannot pass if both formats
//!    silently converged back on text.

use codetracer_trace_types::{TypeId, ValueRecord};

fn float(f: f64) -> ValueRecord {
    ValueRecord::Float { f, type_id: TypeId(3) }
}

fn cbor_bytes(v: &ValueRecord) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(v, &mut out).expect("CBOR encode");
    out
}

fn cbor_round_trip(v: &ValueRecord) -> ValueRecord {
    ciborium::from_reader(&cbor_bytes(v)[..]).expect("CBOR decode")
}

fn json_round_trip(v: &ValueRecord) -> ValueRecord {
    let text = serde_json::to_string(v).expect("JSON encode");
    serde_json::from_str(&text).expect("JSON decode")
}

fn f_of(v: &ValueRecord) -> f64 {
    match v {
        ValueRecord::Float { f, .. } => *f,
        other => panic!("expected a Float, got {other:?}"),
    }
}

#[test]
fn a_finite_float_round_trips_through_cbor() {
    let decoded = cbor_round_trip(&float(2.5));
    assert_eq!(decoded, float(2.5));
}

#[test]
fn the_non_finites_round_trip_through_cbor() {
    // The whole reason the string form existed. CBOR encodes all three
    // natively, so none of them needs it.
    let inf = f_of(&cbor_round_trip(&float(f64::INFINITY)));
    assert!(inf.is_infinite() && inf.is_sign_positive(), "+inf did not survive CBOR: {inf}");

    let neg_inf = f_of(&cbor_round_trip(&float(f64::NEG_INFINITY)));
    assert!(
        neg_inf.is_infinite() && neg_inf.is_sign_negative(),
        "-inf did not survive CBOR: {neg_inf}"
    );

    // `NaN != NaN`, so equality here would be testing IEEE-754 rather than the
    // encoder.
    let nan = f_of(&cbor_round_trip(&float(f64::NAN)));
    assert!(nan.is_nan(), "NaN did not survive CBOR: {nan}");
}

#[test]
fn cbor_writes_a_float_as_a_number_not_a_string() {
    // The bytes themselves, so this test cannot pass if both formats quietly
    // converged back on text. A CBOR float is major type 7 with additional
    // info 25/26/27 — 0xF9/0xFA/0xFB for half/single/double. The WIDTH is the
    // encoder's business (ciborium emits the shortest form that round-trips,
    // cbor4ii always emits float64), so this pins the major type, not a width.
    let bytes = cbor_bytes(&float(2.5));
    assert!(
        bytes.iter().any(|&b| matches!(b, 0xF9..=0xFB)),
        "values.dat must carry a CBOR float (0xF9/0xFA/0xFB), got bytes {bytes:02X?}"
    );
    assert!(
        !String::from_utf8_lossy(&bytes).contains("2.5"),
        "the float must not appear as the TEXT \"2.5\" anywhere in the CBOR: {bytes:02X?}"
    );
}

#[test]
fn json_still_writes_a_float_as_a_decimal_string() {
    // Spelled out literally, because `trace.json` fixtures parse this exact
    // shape and a change here would break them silently.
    let text = serde_json::to_string(&float(2.5)).expect("JSON encode");
    assert!(text.contains("\"f\":\"2.5\""), "JSON must keep the decimal-string form: {text}");
    assert_eq!(json_round_trip(&float(2.5)), float(2.5));
}

#[test]
fn json_still_round_trips_the_non_finites() {
    // The property that forced the string form; it must not have been lost
    // while fixing the binary side.
    let text = serde_json::to_string(&float(f64::INFINITY)).expect("JSON encode");
    assert!(text.contains("\"f\":\"inf\""), "JSON must spell +inf: {text}");

    let inf = f_of(&json_round_trip(&float(f64::INFINITY)));
    assert!(inf.is_infinite() && inf.is_sign_positive(), "+inf did not survive JSON");

    let neg_inf = f_of(&json_round_trip(&float(f64::NEG_INFINITY)));
    assert!(neg_inf.is_infinite() && neg_inf.is_sign_negative(), "-inf did not survive JSON");

    let nan = f_of(&json_round_trip(&float(f64::NAN)));
    assert!(nan.is_nan(), "NaN did not survive JSON");
}

#[test]
fn json_accepts_the_spellings_the_fixtures_use() {
    // `trace.json` carries all four; `f64::from_str` takes them and this pins
    // that it keeps doing so.
    for (text, check) in [
        ("inf", f64::is_infinite as fn(f64) -> bool),
        ("+inf", f64::is_infinite),
        ("-inf", f64::is_infinite),
        ("nan", f64::is_nan),
    ] {
        let json = format!(r#"{{"kind":"Float","f":"{text}","type_id":3}}"#);
        let decoded: ValueRecord = serde_json::from_str(&json).unwrap_or_else(|e| panic!("{text:?} must parse: {e}"));
        assert!(check(f_of(&decoded)), "{text:?} decoded wrongly: {decoded:?}");
    }
}
