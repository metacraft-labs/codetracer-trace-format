//! The two implementations answer a `step-map.ns` lookup the same way,
//! including a lookup of line 0 (`internal-files.md` §"`step-map.ns`",
//! "Reading": a lookup of line 0 is a lookup of line 1, the key a step
//! registered at line 0 is filed under).
//!
//! Driven by the Nim test `tests/test_nim_step_map_crossread.nim` in
//! `codetracer-trace-format-nim`. It writes a container with the Nim writer,
//! with steps registered at line 0 and line 1 of one file, at line 0 only in
//! another and away from line 1 in a third, answers a fixed list of lookups
//! with the Nim reader, and runs this test with:
//!
//! - `CT_NIM_STEP_MAP_FIXTURE` — the Nim-written `.ct`;
//! - `CT_NIM_STEP_MAP_ANSWERS` — one lookup per line: `path line ids`, the ids
//!   comma-separated and `-` for none.
//!
//! This reads the same container with the Rust `StepMapReader` and requires
//! the same answer to every lookup. Without the variables (the Rust suite run
//! on its own) there is no fixture and the test does nothing.

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::step_map_reader::StepMapReader;

#[test]
fn nim_and_rust_answer_every_step_map_lookup_alike() {
    let fixture = match std::env::var("CT_NIM_STEP_MAP_FIXTURE") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            eprintln!("CT_NIM_STEP_MAP_FIXTURE unset: run through the Nim driver for the cross-read");
            return;
        }
    };
    let answers = std::fs::read_to_string(std::env::var("CT_NIM_STEP_MAP_ANSWERS").expect("CT_NIM_STEP_MAP_ANSWERS with the fixture"))
        .expect("read the Nim answers");

    let mut ctfs = CtfsReader::open(std::path::Path::new(&fixture)).expect("open the Nim container");
    let mut map = StepMapReader::open(&mut ctfs)
        .expect("read step-map.ns")
        .expect("the Nim container carries step-map.ns");

    let mut line0_lookups = 0;
    let mut compared = 0;
    for row in answers.lines().filter(|l| !l.trim().is_empty()) {
        let f: Vec<&str> = row.split_whitespace().collect();
        assert_eq!(f.len(), 3, "malformed answer row {row:?}");
        let path: u64 = f[0].parse().unwrap();
        let line: u32 = f[1].parse().unwrap();
        let nim: Vec<u64> = if f[2] == "-" {
            Vec::new()
        } else {
            f[2].split(',').map(|s| s.parse().unwrap()).collect()
        };
        let rust = map.lookup(path, line).expect("lookup").unwrap_or_default();
        assert_eq!(rust, nim, "lookup ({path}, {line}): Rust answers {rust:?}, Nim {nim:?}");
        compared += 1;
        if line == 0 && !nim.is_empty() {
            line0_lookups += 1;
        }
    }
    assert!(compared >= 6, "the answer list must not be empty ({compared} rows)");
    assert!(line0_lookups >= 2, "the fixture must exercise line-0 lookups that find steps");
}
