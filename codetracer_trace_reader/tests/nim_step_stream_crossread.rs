//! M24a-1 cross-read proof: a NEW Nim-written production bundle's `steps.dat`
//! is read by the canonical Rust `StepStreamReader`, and the decoded absolute
//! `global_line_index` sequence equals the steps the Nim writer recorded.
//!
//! This is the load-bearing byte-compatibility test for the M24a-1
//! deliverable: the Nim multi-stream writer now emits the SPEC-canonical
//! `steps.dat`/`steps.idx` layout (header-less Zstd chunks + a
//! `[chunk_size: u32][offset: u64]...` index, with the `has_step_stream`
//! meta.dat flag set). If that layout were not byte-compatible with the Rust
//! reader, this decode would diverge or error.
//!
//! Driven by the Nim test `tests/test_nim_step_stream_crossread.nim`, which
//! builds the Nim writer, produces the bundle + a sidecar of decoded GLIs, and
//! runs this test with two env vars set:
//!
//! - `CT_NIM_STEP_FIXTURE` — path to the Nim-written `<bundle>.ct`.
//! - `CT_NIM_STEP_FIXTURE_GLIS` — path to the `<bundle>.ct.steps-glis.txt`
//!   sidecar (one decoded u64 GLI per line).
//!
//! When the env vars are absent (e.g. the Rust suite run on its own) the test
//! is a no-op: there is no Nim fixture to cross-read. The Nim driver is the one
//! that makes this assertion load-bearing in CI.

use codetracer_trace_reader::step_stream_reader::open_step_stream;
use codetracer_trace_writer::step_stream::StepStreamRecord;

#[test]
fn nim_steps_dat_read_by_rust_reader() {
    let fixture = match std::env::var("CT_NIM_STEP_FIXTURE") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            eprintln!(
                "nim_steps_dat_read_by_rust_reader: CT_NIM_STEP_FIXTURE unset — \
                 skipping (run via the Nim driver test for the cross-read proof)"
            );
            return;
        }
    };
    let glis_path = std::env::var("CT_NIM_STEP_FIXTURE_GLIS")
        .expect("CT_NIM_STEP_FIXTURE set but CT_NIM_STEP_FIXTURE_GLIS missing");

    // Expected absolute global_line_index sequence the Nim FFI reader decoded
    // out of the same bundle (one u64 per line).
    let expected: Vec<u64> = std::fs::read_to_string(&glis_path)
        .expect("read sidecar GLIs")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim().parse::<u64>().expect("parse sidecar GLI"))
        .collect();
    assert!(!expected.is_empty(), "sidecar must list at least one step GLI");

    // Open the Nim-written steps.dat with the canonical Rust reader. The
    // has_step_stream flag (set by the Nim writer) must be honored, so this
    // returns Some(reader) rather than None.
    let mut ss = open_step_stream(std::path::Path::new(&fixture))
        .expect("open_step_stream on Nim bundle must succeed")
        .expect("Nim bundle must expose a step stream (has_step_stream flag set)");

    assert_eq!(
        ss.count() as usize,
        expected.len(),
        "Rust reader's record count must equal the Nim-recorded step count"
    );

    let decoded = ss.read_all().expect("Rust reader read_all on Nim steps.dat");
    let decoded_glis: Vec<u64> = decoded
        .iter()
        .map(|r| match r {
            StepStreamRecord::Step { global_line_index } => *global_line_index,
            other => panic!("unexpected non-Step record in Nim bundle: {other:?}"),
        })
        .collect();

    assert_eq!(
        decoded_glis, expected,
        "Rust StepStreamReader must decode the Nim-written steps.dat to the \
         exact global_line_index sequence the Nim reader decoded — byte-compatible"
    );

    // Spot-check seeking into a later chunk decodes correctly (independent
    // per-chunk decode over the Nim-produced chunk boundaries).
    let last = expected.len() as u64 - 1;
    match ss.read(last).expect("seek to last Nim step") {
        StepStreamRecord::Step { global_line_index } => {
            assert_eq!(global_line_index, expected[expected.len() - 1]);
        }
        other => panic!("expected Step at last index, got {other:?}"),
    }
}
