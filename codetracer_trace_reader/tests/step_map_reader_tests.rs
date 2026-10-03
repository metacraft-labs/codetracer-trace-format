//! The `step-map.ns` version 2 reader (`internal-files.md` §"`step-map.ns`",
//! "Reading"): a full load, a single-line lookup through the chunk table, and
//! every refusal the specification lists, each named.
//!
//! Well-formed maps come from the production writer (`StepMapBuilder`, which
//! `codetracer_trace_writer/tests/step_map_v2_known_answer.rs` holds to the
//! reference encoder byte for byte). Malformed maps are assembled by hand from
//! raw chunk contents, because no writer produces them; their frames are
//! compressed exactly as a writer's are.

use std::collections::BTreeMap;

use codetracer_trace_reader::step_map_reader::StepMapReader;
use codetracer_trace_writer::step_map::StepMapBuilder;

fn writer_map(hits: &[(u64, i64, u64)]) -> Vec<u8> {
    let mut b = StepMapBuilder::new();
    for &(p, l, s) in hits {
        b.record_step(p, l, s);
    }
    b.serialize().unwrap()
}

/// Irregular hits over several paths and lines, enough for several chunks.
fn many_hits() -> Vec<(u64, i64, u64)> {
    let mut r: u64 = 7;
    let mut id = 0u64;
    (0..80_000)
        .map(|_| {
            r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            id += 1 + (r >> 20) % 5;
            ((r >> 33) % 5, ((r >> 40) % 400) as i64 + 1, id)
        })
        .collect()
}

fn expected(hits: &[(u64, i64, u64)]) -> BTreeMap<(u64, u32), Vec<u64>> {
    let mut m: BTreeMap<(u64, u32), Vec<u64>> = BTreeMap::new();
    for &(p, l, s) in hits {
        m.entry((p, l as u32)).or_default().push(s);
    }
    m
}

fn put(v: u64, out: &mut Vec<u8>) {
    let mut v = v;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// A member from header counts and raw chunk contents, each with its table key.
fn member(counts: (u32, u32, u64), chunks: &[((u64, u32), Vec<u8>)]) -> Vec<u8> {
    let frames: Vec<Vec<u8>> = chunks
        .iter()
        .map(|(_, c)| codetracer_ctfs::compress_pledged(c, 3, "step-map.ns").unwrap())
        .collect();
    let mut out = Vec::new();
    out.extend_from_slice(&0x5354_4D50u32.to_le_bytes());
    out.extend_from_slice(&2u16.to_le_bytes());
    out.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    out.extend_from_slice(&counts.0.to_le_bytes());
    out.extend_from_slice(&counts.1.to_le_bytes());
    out.extend_from_slice(&counts.2.to_le_bytes());
    let mut off = 0u64;
    for (((p, l), _), f) in chunks.iter().zip(&frames) {
        out.extend_from_slice(&off.to_le_bytes());
        out.extend_from_slice(&p.to_le_bytes());
        out.extend_from_slice(&l.to_le_bytes());
        off += f.len() as u64;
    }
    for f in frames {
        out.extend_from_slice(&f);
    }
    out
}

/// One raw line record: path delta, line field, count, runs.
fn rec(path_delta: u64, line: u64, count: u64, runs: &[(u64, u64)]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in [path_delta, line, count] {
        put(v, &mut out);
    }
    for &(g, r) in runs {
        put(g, &mut out);
        put(r, &mut out);
    }
    out
}

fn refuse(bytes: Vec<u8>) -> String {
    match StepMapReader::from_bytes(bytes) {
        Err(e) => e,
        Ok(r) => r.load_all().expect_err("the map must be refused"),
    }
}

#[test]
fn a_full_load_returns_every_list() {
    let hits = many_hits();
    let r = StepMapReader::from_bytes(writer_map(&hits)).unwrap();
    assert!(r.chunk_count() > 1, "the fixture must span several chunks, or the table is not exercised");
    assert_eq!(r.load_all().unwrap(), expected(&hits));
}

#[test]
fn a_lookup_inflates_one_chunk_and_finds_the_line_or_nothing() {
    let hits = many_hits();
    let mut r = StepMapReader::from_bytes(writer_map(&hits)).unwrap();
    for (key, ids) in expected(&hits).iter().step_by(37) {
        assert_eq!(r.lookup(key.0, key.1).unwrap().as_ref(), Some(ids), "{key:?}");
    }
    assert_eq!(r.lookup(2, 401).unwrap(), None, "a line that never ran");
    assert_eq!(r.lookup(99, 1).unwrap(), None, "a key past the last chunk");
}

/// A step registered at line 0 is filed under line 1, so a lookup of line 0
/// is a lookup of line 1: the same ids, including on a path whose only steps
/// were registered at line 0.
#[test]
fn a_lookup_of_line_0_is_a_lookup_of_line_1() {
    let hits = [(0, 0, 3), (0, 1, 5), (0, 2, 6), (1, 0, 9), (2, 7, 11)];
    let mut r = StepMapReader::from_bytes(writer_map(&hits)).unwrap();
    assert_eq!(r.lookup(0, 1).unwrap(), Some(vec![3, 5]));
    assert_eq!(r.lookup(0, 0).unwrap(), Some(vec![3, 5]));
    assert_eq!(r.lookup(1, 0).unwrap(), Some(vec![9]));
    assert_eq!(r.lookup(1, 1).unwrap(), Some(vec![9]));
    assert_eq!(r.lookup(2, 0).unwrap(), None, "line 1 of path 2 never ran");
}

/// The reader keeps the chunk its last lookup inflated. Lookups that jump
/// between chunks, return to one, and repeat a key must answer as the full
/// load does, and so must a lookup for a line that never ran inside a chunk
/// that is cached.
#[test]
fn lookups_in_any_order_agree_with_the_full_load() {
    let hits = many_hits();
    let mut r = StepMapReader::from_bytes(writer_map(&hits)).unwrap();
    let all = r.load_all().unwrap();
    let keys: Vec<(u64, u32)> = all.keys().copied().collect();
    let mut s: u64 = 11;
    for _ in 0..3_000 {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let key = keys[(s >> 33) as usize % keys.len()];
        assert_eq!(r.lookup(key.0, key.1).unwrap().as_ref(), all.get(&key), "{key:?}");
        assert_eq!(r.lookup(key.0, 401).unwrap(), None, "({}, 401) never ran", key.0);
    }
}

#[test]
fn an_empty_map_loads_empty() {
    let mut r = StepMapReader::from_bytes(writer_map(&[])).unwrap();
    assert!(r.load_all().unwrap().is_empty());
    assert_eq!(r.lookup(0, 1).unwrap(), None);
}

#[test]
fn version_1_and_every_other_version_are_refused_by_name() {
    for v in [1u16, 3, 0xffff] {
        let mut b = writer_map(&[(0, 1, 0)]);
        b[4..6].copy_from_slice(&v.to_le_bytes());
        let err = StepMapReader::from_bytes(b).expect_err("refused");
        assert!(err.contains(&format!("version {v}")) && err.contains('2'), "{err}");
    }
}

#[test]
fn counts_that_disagree_with_the_header_are_refused() {
    let one = vec![((0u64, 5u32), rec(0, 5, 2, &[(3, 2)]))];
    for (counts, what) in [((2, 1, 2), "path"), ((1, 2, 2), "line"), ((1, 1, 3), "step")] {
        let err = refuse(member(counts, &one));
        assert!(err.contains("header") && err.contains(what), "{what}: {err}");
    }
    assert!(StepMapReader::from_bytes(member((1, 1, 2), &one)).unwrap().load_all().is_ok(), "control");
}

#[test]
fn a_chunk_whose_first_key_is_not_its_table_key_is_refused() {
    let err = refuse(member((1, 1, 1), &[((0, 6), rec(0, 5, 1, &[(1, 1)]))]));
    assert!(err.contains("table key"), "{err}");
}

#[test]
fn keys_that_do_not_ascend_strictly_are_refused() {
    // Within a chunk: a line delta of 0 repeats the key.
    let c = [rec(0, 5, 1, &[(1, 1)]), rec(0, 0, 1, &[(2, 1)])].concat();
    let err = refuse(member((1, 2, 2), &[((0, 5), c)]));
    assert!(err.contains("ascend"), "{err}");
    // Across chunks.
    let err = refuse(member((1, 2, 2), &[((0, 5), rec(0, 5, 1, &[(1, 1)])), ((0, 5), rec(0, 5, 1, &[(2, 1)]))]));
    assert!(err.contains("ascend"), "{err}");
}

#[test]
fn a_zero_count_gap_or_repeat_is_refused() {
    for (record, what) in [
        (rec(0, 5, 0, &[]), "count"),
        (rec(0, 5, 1, &[(0, 1)]), "gap"),
        (rec(0, 5, 1, &[(1, 0)]), "repeat"),
    ] {
        let err = refuse(member((1, 1, 1), &[((0, 5), record)]));
        assert!(err.contains(what) && err.contains('0'), "{what}: {err}");
    }
}

#[test]
fn runs_that_overshoot_the_count_are_refused() {
    let err = refuse(member((1, 1, 2), &[((0, 5), rec(0, 5, 2, &[(1, 3)]))]));
    assert!(err.contains("overshoot"), "{err}");
}

#[test]
fn a_frame_that_does_not_decode_to_its_declared_size_is_refused() {
    let content = rec(0, 5, 1, &[(1, 1)]);
    let mut b = member((1, 1, 1), &[((0, 5), content.clone())]);
    // A second frame appended to the only chunk's span: the span no longer
    // decodes to the size its frame declares.
    b.extend_from_slice(&codetracer_ctfs::compress_pledged(&content, 3, "x").unwrap());
    let err = refuse(b);
    assert!(err.contains("declare"), "{err}");

    // A frame that declares no size at all.
    let mut b = member((1, 1, 1), &[]);
    b[6..10].copy_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&0u64.to_le_bytes());
    b.extend_from_slice(&0u64.to_le_bytes());
    b.extend_from_slice(&5u32.to_le_bytes());
    b.extend_from_slice(&zstd::encode_all(&content[..], 3).unwrap());
    let err = refuse(b);
    assert!(err.contains("declare"), "{err}");
}

/// The first lookup into a chunk checks every record of it, so a defect
/// anywhere in the chunk refuses the lookup, also of a line before the
/// defect; a later lookup into the same chunk meets the same refusal.
#[test]
fn a_lookup_checks_its_whole_chunk() {
    let c = [rec(0, 5, 1, &[(1, 1)]), rec(0, 2, 1, &[(0, 1)])].concat();
    let mut r = StepMapReader::from_bytes(member((1, 2, 2), &[((0, 5), c)])).unwrap();
    for _ in 0..2 {
        let err = r.lookup(0, 5).expect_err("the chunk holds a gap of 0");
        assert!(err.contains("gap") && err.contains("(0, 7)"), "{err}");
    }
    let good = [rec(0, 5, 1, &[(1, 1)]), rec(0, 2, 1, &[(4, 1)])].concat();
    let mut r = StepMapReader::from_bytes(member((1, 2, 2), &[((0, 5), good)])).unwrap();
    assert_eq!(r.lookup(0, 5).unwrap(), Some(vec![0]), "control");
    assert_eq!(r.lookup(0, 7).unwrap(), Some(vec![3]), "control");
}
