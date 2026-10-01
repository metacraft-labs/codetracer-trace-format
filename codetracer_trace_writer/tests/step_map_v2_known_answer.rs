//! `step-map.ns` version 2 (`internal-files.md` §"`step-map.ns`"), byte for
//! byte against the reference encoder.
//!
//! The vectors below were produced by `encode_packed_mode(&map, PACKED_TARGET,
//! PackedMode::RleZstd)` of `codetracer-trace-format-spec/tools/ctfs-measure`
//! (spec `latest` `37263ab`, the encoder the specification names as version 2
//! "byte for byte"), with libzstd 1.5.7 — the same library this workspace
//! links. The producer is the `kav` program reproduced in the doc comment of
//! [`lcg_hits`]; the multi-chunk map is pinned by its length, its header and
//! chunk table, and an FNV-1a-64 digest of the whole member rather than by
//! 130 KB of literal bytes.

use codetracer_trace_writer::step_map::StepMapBuilder;

/// Map A: the four steps of the line-only step-map test — `(path, line,
/// step id)`.
const HITS_A: [(u64, i64, u64); 4] = [(1, 7, 0), (0, 3, 1), (1, 7, 3), (0, 1, 4)];

const MAP_A: [u8; 72] = [
    0x50, 0x4d, 0x54, 0x53, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x28, 0xb5,
    0x2f, 0xfd, 0x20, 0x11, 0x89, 0x00, 0x00, 0x00, 0x01, 0x01, 0x05, 0x01, 0x00, 0x02, 0x01, 0x02, 0x01, 0x01, 0x07, 0x02, 0x01, 0x01, 0x03, 0x01,
];

/// A trace with no steps: the 26-byte header with every count 0.
const MAP_EMPTY: [u8; 26] = [
    0x50, 0x4d, 0x54, 0x53, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00,
];

/// Map B's header and three-entry chunk table.
const MAP_B_PREFIX: [u8; 86] = [
    0x50, 0x4d, 0x54, 0x53, 0x02, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0xb0, 0x04, 0x00, 0x00, 0x60, 0xea, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xee, 0xb9,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0xa8, 0x73, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x01, 0x00, 0x00,
];
const MAP_B_LEN: usize = 130_894;
const MAP_B_FNV1A64: u64 = 0xf569_ec56_7112_6b4f;

/// Map B: 60,000 steps over 4 paths and 300 lines with irregular gaps, enough
/// to fill three chunks. The reference program built the same hits:
///
/// ```text
/// let mut r: u64 = 0x2545F4914F6CDD1D;
/// let mut id = 0i64;
/// for _ in 0..60000 {
///     r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
///     let p = (r >> 33) % 4;
///     let l = ((r >> 40) % 300) as u32 + 1;
///     id += 1 + ((r >> 20) % 3) as i64;
///     hits.push((p, l, id));
/// }
/// ```
fn lcg_hits() -> Vec<(u64, i64, u64)> {
    let mut r: u64 = 0x2545_F491_4F6C_DD1D;
    let mut id = 0u64;
    (0..60_000)
        .map(|_| {
            r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let p = (r >> 33) % 4;
            let l = ((r >> 40) % 300) as i64 + 1;
            id += 1 + (r >> 20) % 3;
            (p, l, id)
        })
        .collect()
}

fn build(hits: &[(u64, i64, u64)]) -> Vec<u8> {
    let mut b = StepMapBuilder::new();
    for &(p, l, s) in hits {
        b.record_step(p, l, s);
    }
    b.serialize().expect("serialize")
}

fn fnv1a64(b: &[u8]) -> u64 {
    b.iter()
        .fold(0xcbf2_9ce4_8422_2325u64, |h, x| (h ^ *x as u64).wrapping_mul(0x100_0000_01b3))
}

#[test]
fn a_small_map_is_the_reference_bytes() {
    assert_eq!(build(&HITS_A), MAP_A);
}

#[test]
fn an_empty_map_is_the_bare_header() {
    assert_eq!(build(&[]), MAP_EMPTY);
}

#[test]
fn a_map_of_several_chunks_is_the_reference_bytes() {
    let got = build(&lcg_hits());
    assert_eq!(&got[..MAP_B_PREFIX.len()], &MAP_B_PREFIX, "header and chunk table");
    assert_eq!(got.len(), MAP_B_LEN, "member length");
    assert_eq!(fnv1a64(&got), MAP_B_FNV1A64, "member digest");
}
