//! Container version 5: the three forms of `FileEntry.MapBlock`
//! (`ctfs-container.md` §1, §2 "`MapBlock` has three forms", §4 "Block
//! Resolution", §5 "Appending Data", §6).
//!
//! * An empty member is `(Size, MapBlock) = (0, 0)` and owns no block.
//! * A member of at most one block owns one data block `b` and no mapping
//!   block: `MapBlock = CTFS_DIRECT | b`.
//! * The append that takes a member past one block claims the level-1 mapping
//!   block first, puts the old data block in slot 0, then claims the new data
//!   blocks; `MapBlock` becomes the untagged mapping block.
//!
//! Both writers are measured on the bytes they produce, and both readers on
//! the refusals the spec requires of them. No mocks: every container here is
//! written by a real writer to a real file (or hand-edited from one, where the
//! case is a damaged or foreign container no writer produces).

use std::path::{Path, PathBuf};

use codetracer_ctfs::{ConcurrentCtfsReader, ConcurrentCtfsWriter, CtfsReader, CtfsWriter};

const BS: usize = 4096;
const DIRECT: u64 = 1 << 63;

fn u64_le(bytes: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap())
}

/// `(size, map_block)` of root entry `i`.
fn entry(container: &[u8], i: usize) -> (u64, u64) {
    let off = 16 + 24 * i;
    (u64_le(container, off), u64_le(container, off + 8))
}

fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

fn tmp(dir: &Path, name: &str) -> PathBuf {
    dir.join(name)
}

#[test]
fn writers_write_version_5() {
    let dir = tempfile::tempdir().unwrap();
    let p = tmp(dir.path(), "a.ct");
    CtfsWriter::create(&p, 4096, 31).unwrap().close().unwrap();
    assert_eq!(std::fs::read(&p).unwrap()[5], 5, "CtfsWriter header version");

    let q = tmp(dir.path(), "b.ct");
    let w = ConcurrentCtfsWriter::create(&q, 4096, 31).unwrap();
    close_shared(w);
    assert_eq!(std::fs::read(&q).unwrap()[5], 5, "ConcurrentCtfsWriter header version");
}

fn close_shared(w: std::sync::Arc<ConcurrentCtfsWriter>) {
    std::sync::Arc::try_unwrap(w).expect("sole owner").close().unwrap();
}

#[test]
fn an_empty_member_claims_no_block() {
    let dir = tempfile::tempdir().unwrap();
    let p = tmp(dir.path(), "e.ct");
    let mut w = CtfsWriter::create(&p, 4096, 31).unwrap();
    w.add_file("empty.dat").unwrap();
    w.close().unwrap();
    let c = std::fs::read(&p).unwrap();
    assert_eq!(entry(&c, 0), (0, 0), "a never-written member is (0, 0)");
    assert_eq!(c.len(), BS, "the container is block 0 alone");
    let mut r = CtfsReader::open(&p).unwrap();
    assert_eq!(r.list_files(), vec!["empty.dat"], "an empty member is still present");
    assert_eq!(r.read_file("empty.dat").unwrap(), Vec::<u8>::new());
    let cr = ConcurrentCtfsReader::open(&p).unwrap();
    assert_eq!(cr.read_file("empty.dat").unwrap(), Vec::<u8>::new());
}

#[test]
fn a_small_member_is_one_tagged_data_block() {
    for size in [1usize, 100, BS] {
        let dir = tempfile::tempdir().unwrap();
        let p = tmp(dir.path(), "s.ct");
        let data = bytes(7, size);
        let mut w = CtfsWriter::create(&p, 4096, 31).unwrap();
        let h = w.add_file("small.dat").unwrap();
        w.write(h, &data).unwrap();
        w.close().unwrap();
        let c = std::fs::read(&p).unwrap();
        assert_eq!(entry(&c, 0), (size as u64, DIRECT | 1), "size {size}: direct data block 1");
        assert_eq!(c.len(), 2 * BS, "size {size}: block 0 plus one data block, no mapping block");
        assert_eq!(&c[BS..BS + size], &data[..]);
        assert_eq!(CtfsReader::open(&p).unwrap().read_file("small.dat").unwrap(), data);
        assert_eq!(ConcurrentCtfsReader::open(&p).unwrap().read_file("small.dat").unwrap(), data);
        let mut buf = vec![0u8; 10.min(size)];
        let off = (size - buf.len()) as u64;
        assert_eq!(CtfsReader::open(&p).unwrap().read_at("small.dat", off, &mut buf).unwrap(), buf.len());
        assert_eq!(&buf[..], &data[off as usize..]);
    }
}

#[test]
fn a_first_write_past_one_block_claims_the_mapping_block_first() {
    let dir = tempfile::tempdir().unwrap();
    let p = tmp(dir.path(), "m.ct");
    let data = bytes(3, BS + 1);
    let mut w = CtfsWriter::create(&p, 4096, 31).unwrap();
    let h = w.add_file("big.dat").unwrap();
    w.write(h, &data).unwrap();
    w.close().unwrap();
    let c = std::fs::read(&p).unwrap();
    assert_eq!(entry(&c, 0), ((BS + 1) as u64, 1), "untagged mapping block 1");
    assert_eq!(u64_le(&c, BS), 2, "slot 0: the first data block, claimed after the mapping block");
    assert_eq!(u64_le(&c, BS + 8), 3, "slot 1: the second data block");
    assert_eq!(c.len(), 4 * BS);
    assert_eq!(CtfsReader::open(&p).unwrap().read_file("big.dat").unwrap(), data);
}

#[test]
fn growing_past_one_block_moves_the_data_block_into_slot_0() {
    let dir = tempfile::tempdir().unwrap();
    let p = tmp(dir.path(), "g.ct");
    let (a, b) = (bytes(1, 100), bytes(2, 4000));
    let mut w = CtfsWriter::create(&p, 4096, 31).unwrap();
    let h = w.add_file("grow.dat").unwrap();
    let other = w.add_file("other.dat").unwrap();
    w.write(h, &a).unwrap();
    w.write(other, &bytes(9, 10)).unwrap();
    w.write(h, &b).unwrap();
    w.close().unwrap();
    let c = std::fs::read(&p).unwrap();
    // grow.dat's first write claimed block 1, other.dat's block 2; the
    // transition claims the mapping block (3), then the new data block (4).
    assert_eq!(entry(&c, 1), (10, DIRECT | 2));
    assert_eq!(entry(&c, 0), (4100, 3), "grow.dat is mapped through block 3");
    assert_eq!(u64_le(&c, 3 * BS), 1, "slot 0 is the member's old data block");
    assert_eq!(u64_le(&c, 3 * BS + 8), 4, "slot 1 is the data block claimed after the mapping block");
    let mut want = a.clone();
    want.extend_from_slice(&b);
    assert_eq!(CtfsReader::open(&p).unwrap().read_file("grow.dat").unwrap(), want);
    assert_eq!(ConcurrentCtfsReader::open(&p).unwrap().read_file("grow.dat").unwrap(), want);
}

#[test]
fn the_concurrent_writer_uses_the_same_three_forms_and_a_live_reader_follows_them() {
    let dir = tempfile::tempdir().unwrap();
    let p = tmp(dir.path(), "c.ct");
    let w = ConcurrentCtfsWriter::create(&p, 4096, 31).unwrap();
    let mut empty = w.add_file("empty.dat").unwrap();
    let mut fw = w.add_file("grow.dat").unwrap();
    empty.flush(&w).unwrap();

    let a = bytes(4, 100);
    fw.write(&w, &a).unwrap();
    fw.flush(&w).unwrap();
    let c = std::fs::read(&p).unwrap();
    assert_eq!(entry(&c, 0), (0, 0), "an empty member claims nothing, even when flushed");
    assert_eq!(entry(&c, 1), (100, DIRECT | 1), "first write: one tagged data block");
    let mut live = ConcurrentCtfsReader::open(&p).unwrap();
    assert_eq!(live.read_file("grow.dat").unwrap(), a);

    let b = bytes(5, 5000);
    fw.write(&w, &b).unwrap();
    fw.flush(&w).unwrap();
    let c = std::fs::read(&p).unwrap();
    assert_eq!(entry(&c, 1), (5100, 2), "the transition stores the untagged mapping block");
    assert_eq!(u64_le(&c, 2 * BS), 1, "slot 0 is the old data block");
    assert_eq!(u64_le(&c, 2 * BS + 8), 3, "slot 1 was claimed after the mapping block");
    live.refresh().unwrap();
    let mut want = a.clone();
    want.extend_from_slice(&b);
    assert_eq!(live.read_file("grow.dat").unwrap(), want);

    drop(fw);
    drop(empty);
    close_shared(w);
    let c = std::fs::read(&p).unwrap();
    assert_eq!(entry(&c, 0), (0, 0));
    assert_eq!(CtfsReader::open(&p).unwrap().read_file("grow.dat").unwrap(), want);
}

#[test]
fn open_append_continues_a_direct_member_and_crosses_into_the_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let p = tmp(dir.path(), "a.ct");
    let a = bytes(6, 300);
    {
        let mut w = CtfsWriter::create(&p, 4096, 31).unwrap();
        let h = w.add_file("log.dat").unwrap();
        w.write(h, &a).unwrap();
        w.add_file("none.dat").unwrap();
        w.close().unwrap();
    }
    let b = bytes(8, 6000);
    {
        let mut w = CtfsWriter::open_append(&p).unwrap();
        let h = w.find_file("log.dat").unwrap();
        w.append(h, &b).unwrap();
        let n = w.find_file("none.dat").unwrap();
        w.append(n, b"x").unwrap();
        w.close().unwrap();
    }
    let c = std::fs::read(&p).unwrap();
    assert_eq!(entry(&c, 0), (6300, 2), "mapping block 2 claimed after the existing data block 1");
    assert_eq!(u64_le(&c, 2 * BS), 1, "the direct block keeps its data and becomes slot 0");
    assert_eq!(entry(&c, 1), (1, DIRECT | 4), "the empty member's first append: a tagged block");
    let mut want = a.clone();
    want.extend_from_slice(&b);
    let mut r = CtfsReader::open(&p).unwrap();
    assert_eq!(r.read_file("log.dat").unwrap(), want);
    assert_eq!(r.read_file("none.dat").unwrap(), b"x");
}

// --- the readers --------------------------------------------------------

/// A one-member container written by the real writer, plus its bytes.
fn one_small_member(dir: &Path) -> (PathBuf, Vec<u8>) {
    let p = tmp(dir, "r.ct");
    let mut w = CtfsWriter::create(&p, 4096, 31).unwrap();
    let h = w.add_file("small.dat").unwrap();
    w.write(h, &bytes(1, 100)).unwrap();
    w.close().unwrap();
    let c = std::fs::read(&p).unwrap();
    (p, c)
}

fn set_entry(path: &Path, c: &[u8], size: u64, map_block: u64) {
    let mut c = c.to_vec();
    c[16..24].copy_from_slice(&size.to_le_bytes());
    c[24..32].copy_from_slice(&map_block.to_le_bytes());
    std::fs::write(path, c).unwrap();
}

/// Both readers' `read_file` error for `small.dat`, as text.
fn both_refuse(path: &Path) -> [String; 2] {
    let a = CtfsReader::open(path)
        .unwrap()
        .read_file("small.dat")
        .expect_err("CtfsReader must refuse");
    let b = ConcurrentCtfsReader::open(path)
        .unwrap()
        .read_file("small.dat")
        .expect_err("ConcurrentCtfsReader must refuse");
    [a.to_string(), b.to_string()]
}

/// Version 6 is read by `CtfsReader` (`compact_profile.rs`), and not by the
/// concurrent reader or the appending writer, which work on containers being
/// written, and every writer writes version 5.
#[test]
fn readers_refuse_every_version_but_5_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let (p, c) = one_small_member(dir.path());
    let mut relabelled = c.clone();
    relabelled[5] = 6;
    std::fs::write(&p, &relabelled).unwrap();
    for msg in [
        ConcurrentCtfsReader::open(&p)
            .err()
            .expect("ConcurrentCtfsReader must refuse")
            .to_string(),
        CtfsWriter::open_append(&p).err().expect("open_append must refuse").to_string(),
    ] {
        assert!(msg.contains("version 6") && msg.contains('5'), "{msg}");
    }
    // A version-5 body under a version-6 stamp is not a version-6 container:
    // its byte 16 is read as the profile, and refused by value.
    let msg = CtfsReader::open(&p).err().expect("CtfsReader must refuse").to_string();
    assert!(msg.contains("profile"), "{msg}");
    for v in [1u8, 2, 3, 4, 7, 255] {
        let mut old = c.clone();
        old[5] = v;
        std::fs::write(&p, &old).unwrap();
        for msg in [
            CtfsReader::open(&p).err().expect("CtfsReader must refuse").to_string(),
            ConcurrentCtfsReader::open(&p)
                .err()
                .expect("ConcurrentCtfsReader must refuse")
                .to_string(),
            CtfsWriter::open_append(&p).err().expect("open_append must refuse").to_string(),
        ] {
            assert!(
                msg.contains(&format!("version {v}")) && msg.contains('5'),
                "the refusal names the version found ({v}) and the one read (5): {msg}"
            );
        }
    }
}

#[test]
fn readers_refuse_a_tagged_member_larger_than_one_block() {
    let dir = tempfile::tempdir().unwrap();
    let (p, c) = one_small_member(dir.path());
    set_entry(&p, &c, BS as u64 + 1, DIRECT | 1);
    for msg in both_refuse(&p) {
        assert!(msg.contains("small.dat") && msg.contains("4097"), "{msg}");
    }
}

#[test]
fn readers_refuse_a_null_tagged_data_block() {
    let dir = tempfile::tempdir().unwrap();
    let (p, c) = one_small_member(dir.path());
    set_entry(&p, &c, 100, DIRECT);
    for msg in both_refuse(&p) {
        assert!(msg.contains("small.dat") && msg.contains("null"), "{msg}");
        assert!(!msg.contains("truncated"), "a null is not a truncation: {msg}");
    }
}

#[test]
fn readers_refuse_a_tagged_data_block_past_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let (p, c) = one_small_member(dir.path());
    set_entry(&p, &c, 100, DIRECT | 9);
    for msg in both_refuse(&p) {
        assert!(msg.contains("small.dat") && msg.contains("out of bounds"), "{msg}");
    }
}

#[test]
fn readers_refuse_a_null_map_block_with_a_size() {
    let dir = tempfile::tempdir().unwrap();
    let (p, c) = one_small_member(dir.path());
    set_entry(&p, &c, 100, 0);
    for msg in both_refuse(&p) {
        assert!(msg.contains("small.dat") && msg.contains("null"), "{msg}");
    }
}

/// A live reader can see an untagged mapping with a small `Size` between the
/// writer's two stores (§2 "Readers", third bullet); it reads through the
/// mapping, whose slot 0 is the old data block.
#[test]
fn an_untagged_mapping_with_a_small_size_reads_through_the_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let p = tmp(dir.path(), "t.ct");
    let a = bytes(2, 100);
    let mut data = a.clone();
    data.extend_from_slice(&bytes(3, 5000));
    let mut w = CtfsWriter::create(&p, 4096, 31).unwrap();
    let h = w.add_file("small.dat").unwrap();
    w.write(h, &data).unwrap();
    w.close().unwrap();
    let c = std::fs::read(&p).unwrap();
    let (_, m) = entry(&c, 0);
    assert_eq!(m & DIRECT, 0);
    // The old `Size` with the new mapping: the first 100 bytes, read through slot 0.
    set_entry(&p, &c, 100, m);
    assert_eq!(CtfsReader::open(&p).unwrap().read_file("small.dat").unwrap(), a);
    assert_eq!(ConcurrentCtfsReader::open(&p).unwrap().read_file("small.dat").unwrap(), a);
}
