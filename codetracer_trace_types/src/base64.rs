//! Byte-vector serialisation for `ValueRecord::BigInt`'s magnitude.
//!
//! # Why this is format-dependent
//!
//! This module used to encode unconditionally as base64 in a serde `String`. In JSON that is
//! the right answer. In CBOR it is a defect, and a total one: `String::serialize` emits a CBOR
//! **text** string (major type 3), while the reference Nim reader — the one `ct-print` is built
//! from — reads a **byte** string (major type 2). The result was
//!
//! ```text
//! Error reading events: failed to decode events: cbor: expected byte string (major 2), got major 3
//! ```
//!
//! and it is raised while decoding the EVENT STREAM, not while decoding one value, so a single
//! wide integer anywhere in a recording made the ENTIRE trace unreadable. `BigInt` is the only
//! full-precision variant in `ValueRecord`, which meant the one variant that exists to carry a
//! value too large for `i64` could not be read back at all. Recorders worked around it by
//! truncating or by rendering to text instead — see `noir/tooling/tracer/src/tracer_glue.rs`,
//! where a `Field` is rendered as fixed-width hex partly for this reason.
//!
//! # The fix
//!
//! `Serializer::is_human_readable()` is serde's own mechanism for exactly this split, and
//! `ciborium` reports `false` from both its serialiser and its deserialiser
//! (`ciborium-0.2.2/src/ser/mod.rs:320`, `src/de/mod.rs:599`), while `serde_json` reports
//! `true`. So:
//!
//! * **human-readable formats (JSON)** keep the base64 text they have always had — every
//!   existing JSON consumer is unaffected, byte for byte;
//! * **binary formats (CBOR)** get `serialize_bytes`, i.e. major type 2, which is what the Nim
//!   reader has always expected.
//!
//! # Reading is deliberately more permissive than writing
//!
//! Containers written before this change carry base64 TEXT inside CBOR. Those files are not
//! hypothetical, so reading dispatches on the shape of the data — `deserialize_any` with a
//! visitor that accepts a byte string OR a text string, decoding the latter as base64 — rather
//! than on the format. Writing is unambiguous; reading is tolerant. The change is therefore
//! backwards-compatible for every reader in this crate, and the only thing it alters is what
//! NEW containers contain. See `deserialize` below for why reading must not branch on
//! `is_human_readable()` even though writing can.

use base64::Engine;
use serde::de::{Error as DeError, SeqAccess, Visitor};
use serde::{Deserializer, Serialize, Serializer};
use std::fmt;

const ENGINE: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
    if s.is_human_readable() {
        let base64 = ENGINE.encode(v);
        String::serialize(&base64, s)
    } else {
        s.serialize_bytes(v)
    }
}

/// Accepts every shape a byte vector legitimately arrives in.
///
/// `visit_str` is the backwards-compatibility arm and is NOT dead code: it is what reads a
/// container written by the pre-fix encoder. `visit_seq` covers binary formats that model a
/// byte string as a sequence of integers rather than as a native byte string.
struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a byte string, or a base64 text string written by a pre-2026-09 writer")
    }

    fn visit_bytes<E: DeError>(self, v: &[u8]) -> Result<Self::Value, E> {
        Ok(v.to_vec())
    }

    fn visit_byte_buf<E: DeError>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        Ok(v)
    }

    fn visit_str<E: DeError>(self, v: &str) -> Result<Self::Value, E> {
        ENGINE.decode(v.as_bytes()).map_err(E::custom)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(byte) = seq.next_element::<u8>()? {
            out.push(byte);
        }
        Ok(out)
    }
}

/// Reading does NOT branch on `is_human_readable()`, and that asymmetry with `serialize` is
/// deliberate — it is a serde pitfall, measured here rather than reasoned about.
///
/// `ValueRecord` is `#[serde(tag = "kind")]`. To deserialise an internally-tagged enum serde
/// must read the tag before it knows the variant, so it buffers the whole map into
/// `serde::__private::de::Content` and replays it through `ContentDeserializer`. **That replay
/// deserialiser reports `is_human_readable() == true` whatever the original format was.** So a
/// CBOR byte string arrived here with `is_human_readable()` claiming JSON, the base64 branch
/// was taken, and the raw magnitude `01 00 00 …` was fed to a base64 decoder, which failed
/// with `Invalid symbol 1, offset 0`.
///
/// Branching on the shape of the DATA instead is correct for both formats and immune to the
/// buffering: JSON yields a text string and CBOR yields a byte string, and `BytesVisitor`
/// accepts either. Writing can still branch on the format, because serialising an internally
/// tagged enum delegates to the real serialiser rather than buffering.
pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    d.deserialize_any(BytesVisitor)
}

#[cfg(test)]
mod tests {
    use crate::{TypeId, ValueRecord};

    fn wide_bigint() -> ValueRecord {
        // 2^200 as a big-endian magnitude: 0x01 followed by 25 zero bytes. Wider than i128,
        // which is the case `BigInt` exists for and the case that used to poison a trace.
        let mut b = vec![0u8; 26];
        b[0] = 1;
        ValueRecord::BigInt { b, negative: false, type_id: TypeId(0) }
    }

    /// THE REGRESSION THIS MODULE EXISTS TO PREVENT. The magnitude must land in the CBOR
    /// stream as a byte string (major type 2). The reference Nim reader rejects major type 3
    /// outright, and it does so while decoding the event stream, so getting this wrong makes a
    /// whole recording unreadable rather than one value.
    #[test]
    fn a_bigint_magnitude_is_a_cbor_byte_string_and_not_text() {
        // Asserted through the decoded CBOR item rather than by scanning for a header byte.
        // A 26-byte string does not use the compact `0x40 | len` form — lengths above 23 take
        // the one-byte-length form `0x58 0x1a` — so a raw byte scan is easy to write wrongly
        // and would still "pass" against the old encoder for some lengths.
        let value = ciborium::value::Value::serialized(&wide_bigint()).expect("to Value");
        let ciborium::value::Value::Map(entries) = &value else {
            panic!("a ValueRecord serialises as a CBOR map, got {value:?}");
        };
        let b = entries
            .iter()
            .find(|(k, _)| matches!(k, ciborium::value::Value::Text(t) if t == "b"))
            .map(|(_, v)| v)
            .expect("the record must contain a `b` field");

        assert!(
            matches!(b, ciborium::value::Value::Bytes(_)),
            "`b` must be a CBOR byte string (major 2); the reference Nim reader rejects a text \
             string (major 3) and does so while decoding the EVENT STREAM, so getting this \
             wrong makes a whole recording unreadable rather than one value. Got {b:?}"
        );

        // And the header really is major 2 on the wire, for a length that needs the extended
        // form: 0x58 = major 2 with a following one-byte length, 0x1a = 26.
        let mut buf = Vec::new();
        ciborium::into_writer(&wide_bigint(), &mut buf).expect("serialise");
        assert!(
            buf.windows(2).any(|w| w == [0x58, 0x1a]),
            "expected a 26-byte byte-string header (58 1a) in {buf:02x?}"
        );
        assert!(
            !buf.windows(2).any(|w| w == [0x78, 0x1a]),
            "78 1a is the 26-byte TEXT-string header — the defect this test pins: {buf:02x?}"
        );
    }

    #[test]
    fn a_bigint_round_trips_through_cbor_unchanged() {
        let mut buf = Vec::new();
        ciborium::into_writer(&wide_bigint(), &mut buf).expect("serialise");
        let back: ValueRecord = ciborium::from_reader(&buf[..]).expect("deserialise");
        assert_eq!(back, wide_bigint());
    }

    /// JSON consumers must be unaffected: the encoding there is still base64 text. This is
    /// asserted rather than assumed, because the whole point of the change is that the two
    /// formats now differ, and a mistake in the branch would silently move JSON too.
    #[test]
    fn json_still_carries_base64_text() {
        let json = serde_json::to_string(&wide_bigint()).expect("serialise");
        // 2^200 base64-encodes to "AQAAAA..." — assert the field is a quoted string, not an
        // array of numbers, which is what serialize_bytes would produce here.
        assert!(json.contains("\"AQ"), "expected base64 text in {json}");
        let back: ValueRecord = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(back, wide_bigint());
    }

    /// A container written by the PREVIOUS encoder — base64 text inside CBOR — must still
    /// read. Those files exist, and a fix that made them unreadable would trade one outage
    /// for another.
    ///
    /// The old container is built by serialising a real record and then rewriting ONLY the
    /// `b` field from a byte string to the base64 text string the old encoder would have
    /// written. Everything else — the tag, the field order, the `type_id` encoding — is
    /// therefore exactly what this crate produces, so the test cannot drift from the real
    /// shape the way a hand-written byte literal would.
    #[test]
    fn a_pre_fix_container_with_base64_text_still_reads() {
        use base64::Engine;

        let value = ciborium::value::Value::serialized(&wide_bigint()).expect("to Value");
        let ciborium::value::Value::Map(entries) = value else {
            panic!("a ValueRecord serialises as a CBOR map, got {value:?}");
        };

        let mut rewritten = Vec::new();
        let mut saw_b = false;
        for (k, v) in entries {
            let is_b = matches!(&k, ciborium::value::Value::Text(t) if t == "b");
            if is_b {
                let ciborium::value::Value::Bytes(raw) = &v else {
                    panic!("`b` must already be a byte string after the fix, got {v:?}");
                };
                saw_b = true;
                let text = super::ENGINE.encode(raw);
                rewritten.push((k, ciborium::value::Value::Text(text)));
            } else {
                rewritten.push((k, v));
            }
        }
        assert!(saw_b, "the record must contain a `b` field");

        let mut buf = Vec::new();
        ciborium::into_writer(&ciborium::value::Value::Map(rewritten), &mut buf)
            .expect("serialise the old shape");

        let back: ValueRecord =
            ciborium::from_reader(&buf[..]).expect("a pre-fix container must still read");
        assert_eq!(back, wide_bigint());
    }
}
