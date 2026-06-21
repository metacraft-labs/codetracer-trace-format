//! M24a-2 cross-read proof: a NEW Nim-written production bundle's `values.dat`
//! is read by the canonical Rust `ValueStreamReader`, and the decoded per-step
//! value records equal the values the Nim writer recorded.
//!
//! This is the load-bearing byte-compatibility test for the M24a-2 deliverable:
//! the Nim multi-stream writer now emits the SPEC-canonical
//! `values.dat`/`values.idx` chunked layout (chunked Zstd records +
//! a `[chunk_size: u32][offset: u64]...` index, with the `has_value_stream`
//! meta.dat flag set). If that layout were not byte-compatible with the Rust
//! reader, this decode would diverge or error.
//!
//! It also verifies the PARALLEL-INDEX invariant: record N ↔ step N, with an
//! empty record for value-less steps (those sidecar lines are empty).
//!
//! Driven by the Nim test `tests/test_nim_value_stream_crossread.nim`, which
//! builds the Nim writer, produces the bundle + a sidecar of decoded per-step
//! values, and runs this test with two env vars set:
//!
//! - `CT_NIM_VALUE_FIXTURE` — path to the Nim-written `<bundle>.ct`.
//! - `CT_NIM_VALUE_FIXTURE_VALUES` — path to the `<bundle>.ct.values.txt`
//!   sidecar (one line per step: `name_id=hex(cbor);name_id=hex(cbor);...`,
//!   empty line for a value-less step).
//!
//! When the env vars are absent (e.g. the Rust suite run on its own) the test
//! is a no-op: there is no Nim fixture to cross-read.

use codetracer_trace_reader::value_stream_reader::open_value_stream;
use codetracer_trace_writer::value_stream::ValueStreamEvent;

/// Parse one sidecar line into the expected `(name_id, cbor_bytes)` pairs.
/// An empty line denotes a value-less step (empty record).
fn parse_line(line: &str) -> Vec<(u64, Vec<u8>)> {
    let line = line.trim();
    if line.is_empty() {
        return Vec::new();
    }
    line.split(';')
        .map(|pair| {
            let (name, hex) = pair
                .split_once('=')
                .unwrap_or_else(|| panic!("malformed sidecar pair: {pair:?}"));
            let name_id = name.parse::<u64>().expect("parse name_id");
            let bytes = decode_hex(hex);
            (name_id, bytes)
        })
        .collect()
}

fn decode_hex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex in sidecar: {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("parse hex byte"))
        .collect()
}

/// Extract the flat `(name_id, cbor_bytes)` pairs from a decoded value record.
/// The Nim production writer only emits the tag-0 `StepValues` event, so the
/// record is either empty or a single `StepValues`.
fn record_pairs(events: &[ValueStreamEvent]) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    for ev in events {
        match ev {
            ValueStreamEvent::StepValues { values } => out.extend(values.iter().cloned()),
            other => panic!("unexpected non-StepValues event in Nim bundle: {other:?}"),
        }
    }
    out
}

#[test]
fn nim_values_dat_read_by_rust_reader() {
    let fixture = match std::env::var("CT_NIM_VALUE_FIXTURE") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            eprintln!(
                "nim_values_dat_read_by_rust_reader: CT_NIM_VALUE_FIXTURE unset — \
                 skipping (run via the Nim driver test for the cross-read proof)"
            );
            return;
        }
    };
    let values_path = std::env::var("CT_NIM_VALUE_FIXTURE_VALUES")
        .expect("CT_NIM_VALUE_FIXTURE set but CT_NIM_VALUE_FIXTURE_VALUES missing");

    // Expected per-step value records the Nim FFI reader decoded out of the
    // same bundle (one line per step).
    let expected: Vec<Vec<(u64, Vec<u8>)>> = std::fs::read_to_string(&values_path)
        .expect("read sidecar values")
        .lines()
        .map(parse_line)
        .collect();
    assert!(!expected.is_empty(), "sidecar must list at least one step");

    // Open the Nim-written values.dat with the canonical Rust reader. The
    // has_value_stream flag (set by the Nim writer) must be honored, so this
    // returns Some(reader) rather than None.
    let mut vs = open_value_stream(std::path::Path::new(&fixture))
        .expect("open_value_stream on Nim bundle must succeed")
        .expect("Nim bundle must expose a value stream (has_value_stream flag set)");

    assert_eq!(
        vs.count() as usize,
        expected.len(),
        "Rust reader's record count must equal the Nim-recorded step count"
    );

    // Read every record and compare the decoded StepValues pairs to the sidecar
    // (byte-for-byte CBOR equality on each value, plus name_id equality).
    let mut value_less_steps = 0usize;
    for (step, exp) in expected.iter().enumerate() {
        let rec = vs.read(step as u64).expect("Rust reader read value record");
        let got = record_pairs(&rec.events);
        assert_eq!(
            got, *exp,
            "step {step}: Rust ValueStreamReader must decode the Nim-written \
             values.dat to the exact (name_id, CBOR) pairs the Nim reader \
             decoded — byte-compatible"
        );
        if exp.is_empty() {
            // Parallel-index invariant: a value-less step is an empty record.
            assert!(
                rec.events.is_empty(),
                "step {step}: value-less step must decode to an empty record"
            );
            value_less_steps += 1;
        }
    }

    // The fixture deliberately interleaves value-less steps; make sure at least
    // one was exercised (so the empty-record path is actually proven).
    assert!(
        value_less_steps > 0,
        "fixture must contain at least one value-less step (empty record)"
    );

    // Spot-check seeking into a later chunk decodes correctly (independent
    // per-chunk decode over the Nim-produced chunk boundaries).
    let last = expected.len() as u64 - 1;
    let last_rec = vs.read(last).expect("seek to last Nim value record");
    assert_eq!(record_pairs(&last_rec.events), expected[expected.len() - 1]);
}
