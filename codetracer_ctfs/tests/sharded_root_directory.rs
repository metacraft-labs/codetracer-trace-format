//! Independent wire fixtures isolate the canonical directory offset and bounds.
//! They deliberately do not use the writer, whose public API is unsharded.
//!
//! The directory offset asserted here is the one `ctfs-container.md` §1,
//! "The free list root area is removed (2026-10-08)", fixes: the `FileEntry`
//! array begins right after the 16-byte header whatever `MaxShards` is, and a
//! reader MUST NOT skip any bytes between them. These fixtures were first
//! written against the earlier text, which placed `42 * MaxShards` bytes of
//! free list roots before the entries; the bait/actual pair below now checks
//! the opposite placement, so a reader that still skips that area fails here.
use codetracer_ctfs::{base40_encode, CtfsReader};
use std::fs;

fn image(shards: u8, count: u32, len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len];
    bytes[..8].copy_from_slice(&[0xc0, 0xde, 0x72, 0xac, 0xe2, 5, 0, shards]);
    bytes[8..12].copy_from_slice(&4096u32.to_le_bytes());
    bytes[12..16].copy_from_slice(&count.to_le_bytes());
    bytes
}

fn entry(bytes: &mut [u8], at: usize, name: &str, size: u64, block: u64) {
    bytes[at..at + 8].copy_from_slice(&size.to_le_bytes());
    bytes[at + 8..at + 16].copy_from_slice(&block.to_le_bytes());
    bytes[at + 16..at + 24].copy_from_slice(&base40_encode(name).unwrap().to_le_bytes());
}

fn both(bytes: Vec<u8>, count: u32, names: &[&str]) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("image.ct");
    fs::write(&path, &bytes).unwrap();
    let file = CtfsReader::open(&path).unwrap();
    let owned = CtfsReader::from_owned_bytes(bytes).unwrap();
    assert!(owned.is_owned_image());
    for reader in [file, owned] {
        assert_eq!(reader.max_entries(), count);
        assert_eq!(reader.list_files(), names);
    }
    fs::remove_file(path).unwrap();
}

#[test]
fn canonical_shard_offset_autofill_and_explicit_overflow() {
    // One shard: the entry at 16 is the directory; the bytes where the
    // removed free list root area used to end (58) are not an entry.
    let mut sharded = image(1, 1, 4096);
    entry(&mut sharded, 16, "actual", 0, 0);
    entry(&mut sharded, 58, "bait", 0, 0);
    both(sharded, 1, &["actual"]);
    // Auto-fill is the remainder of block 0 after the header, whatever the
    // shard count.
    for shards in [0, 1, 16] {
        let first = 16;
        let count = ((4096 - first) / 24) as u32;
        let mut bytes = image(shards, 0, 4096);
        entry(&mut bytes, first, "actual", 0, 0);
        both(bytes, count, &["actual"]);
    }
    let mut overflow = image(1, 200, 8192);
    entry(&mut overflow, 16 + 199 * 24, "last", 0, 0);
    both(overflow, 200, &["last"]);
}

fn refuses_both(bytes: Vec<u8>) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("image.ct");
    fs::write(&path, &bytes).unwrap();
    assert!(CtfsReader::open(&path).is_err());
    assert!(CtfsReader::from_owned_bytes(bytes).is_err());
}

#[test]
fn incomplete_directory_and_unsupported_versions_refuse() {
    // A header cut short, a one-entry directory one byte short (16 + 24 = 40),
    // an auto-filled directory one byte short of its block, and an explicit
    // overflow directory (16 + 200 * 24 = 4816) cut at the first block.
    for bytes in [image(1, 1, 16)[..15].to_vec(), image(1, 1, 39), image(0, 0, 4095), image(1, 200, 4096)] {
        refuses_both(bytes);
    }
    // Version 6 is read now (`ctfs-container.md` §1a), so the unsupported
    // version past it is 7.
    for version in [2, 3, 4, 7] {
        let mut bytes = image(0, 1, 4096);
        bytes[5] = version;
        refuses_both(bytes);
    }
}

#[test]
fn unused_partial_tail_is_not_a_referenced_block() {
    let mut bytes = image(0, 1, 4100);
    entry(&mut bytes, 16, "actual", 1, (1u64 << 63) | 1);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("image.ct");
    fs::write(&path, &bytes).unwrap();
    for mut reader in [CtfsReader::open(&path).unwrap(), CtfsReader::from_owned_bytes(bytes).unwrap()] {
        assert_eq!(reader.list_files(), ["actual"]);
        let error = reader.read_file("actual").unwrap_err().to_string();
        assert!(error.contains("actual"), "{error}");
    }
    both(image(0, 1, 4100), 1, &[]);
}
