//! A header declaring `MaxRootEntries = 0` is read with the auto-filled root
//! directory (`ctfs-container.md` §1, "Auto-fill"; §1a for version 6): every
//! reader of this crate scans `(BlockSize - header) / 24` slots -- 170 at
//! version 5 and 169 at version 6 for a 4096-byte block -- and never reads the
//! `0` as "no entries".
//!
//! # The null prediction
//!
//! A reader that takes the declared `0` literally scans no slot. It lists no
//! member, refuses every read with `FileNotFound`, and reports a zero-entry
//! directory; `open_append` recovers no member and refuses the first
//! `add_file` with `TooManyFiles`. Every assertion below fails under that
//! reading. The explicit-170 control reads the same members through the same
//! code with the declaration spelled out, so a failure in the zero arm cannot
//! be a defect of the fixture.
//!
//! # The fixtures
//!
//! The production `CtfsWriter` writes a container whose declared root fills
//! block 0 exactly (170 entries: `16 + 170 * 24 = 4096`), with a member in
//! every slot, the last one included. Only header bytes 12..15 are then
//! rewritten to `0`. That is the same container: auto-fill names the same 170
//! slots, and no other byte changes. A member in slot 169 is what tells "170"
//! from any smaller count a reader might substitute (31, the Nim library's
//! default, among them). The version-6 fixture moves the same entry array 8
//! bytes later behind a 24-byte header, at 169 entries, which is the version-6
//! auto-fill count; every data block keeps its number.

use std::collections::BTreeSet;

use codetracer_ctfs::{CompressionMethod, ConcurrentCtfsReader, CtfsError, CtfsReader, CtfsWriter};

const BS: u32 = 4096;
const V5_AUTO: usize = (BS as usize - 16) / 24;
const V6_AUTO: usize = (BS as usize - 24) / 24;

fn bytes(seed: u64, n: usize) -> Vec<u8> {
    let mut s = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..n)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 33) as u8
        })
        .collect()
}

/// `count` members of every layout: empty, a partial direct block, a whole
/// block, and mapped members of a few blocks.
fn members(count: usize) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let len = match i % 5 {
                0 => 0,
                1 => 1 + i * 7,
                2 => BS as usize,
                3 => BS as usize + 1 + i,
                _ => 3 * BS as usize + i * 13,
            };
            (format!("m{i}"), bytes(i as u64 + 1, len))
        })
        .collect()
}

/// A version-5 container written by `CtfsWriter` declaring `declared` root
/// entries, holding `files` in slot order.
fn written(declared: u32, files: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut w = CtfsWriter::create_in_memory(BS, declared, CompressionMethod::None).unwrap();
    for (name, content) in files {
        let h = w.add_file(name).unwrap();
        w.write(h, content).unwrap();
    }
    let image = w.finish_to_bytes().unwrap();
    assert_eq!(image[5], 5, "the fixture writer writes version 5");
    assert_eq!(u32::from_le_bytes(image[12..16].try_into().unwrap()), declared);
    image
}

fn declare(image: &mut [u8], count: u32) {
    image[12..16].copy_from_slice(&count.to_le_bytes());
}

/// The version-5 image `v5`, whose root holds `V6_AUTO` entries, as a
/// version-6 full-profile, uncompressed container declaring `0`.
fn as_v6_auto_fill(v5: &[u8]) -> Vec<u8> {
    let bs = BS as usize;
    let mut out = vec![0u8; v5.len()];
    out[..16].copy_from_slice(&v5[..16]);
    out[5] = 6;
    out[12..16].copy_from_slice(&0u32.to_le_bytes());
    // Bytes 16..24 stay zero: profile 0 (full), compression 0 (none), reserved.
    let entries = &v5[16..16 + V6_AUTO * 24];
    out[24..24 + entries.len()].copy_from_slice(entries);
    out[bs..].copy_from_slice(&v5[bs..]);
    out
}

fn names(files: &[(String, Vec<u8>)]) -> BTreeSet<String> {
    files.iter().map(|(n, _)| n.clone()).collect()
}

/// Every member of `files`, and no other, reads back byte-exact from `r`.
fn assert_reads(r: &mut CtfsReader, files: &[(String, Vec<u8>)], slots: usize, what: &str) {
    assert_eq!(r.max_entries() as usize, slots, "{what}: root directory size");
    let listed: BTreeSet<String> = r.list_files().into_iter().collect();
    assert_eq!(listed, names(files), "{what}: listed members");
    for (name, content) in files {
        let got = r.read_file(name).unwrap_or_else(|e| panic!("{what}: {name}: {e:?}"));
        assert!(
            got == *content,
            "{what}: {name} differs ({} bytes, expected {})",
            got.len(),
            content.len()
        );
    }
}

/// The version-5 fixture: `(members, the explicit-170 image, the same image
/// declaring 0)`.
fn v5_fixture() -> (Vec<(String, Vec<u8>)>, Vec<u8>, Vec<u8>) {
    let files = members(V5_AUTO);
    let explicit = written(V5_AUTO as u32, &files);
    let mut zero = explicit.clone();
    declare(&mut zero, 0);
    assert_eq!(explicit[16..], zero[16..], "only the declaration differs");
    (files, explicit, zero)
}

#[test]
fn version_5_explicit_declaration_control() {
    let (files, explicit, _) = v5_fixture();
    assert_reads(
        &mut CtfsReader::from_bytes(explicit).unwrap(),
        &files,
        V5_AUTO,
        "explicit 170, from_bytes",
    );
}

#[test]
fn version_5_zero_declaration_from_bytes() {
    let (files, _, zero) = v5_fixture();
    assert_reads(&mut CtfsReader::from_bytes(zero).unwrap(), &files, V5_AUTO, "declared 0, from_bytes");
}

#[test]
fn version_5_zero_declaration_open_and_refresh() {
    let (files, _, zero) = v5_fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero.ct");
    std::fs::write(&path, &zero).unwrap();
    let mut r = CtfsReader::open(&path).unwrap();
    assert_reads(&mut r, &files, V5_AUTO, "declared 0, open");
    r.refresh().unwrap();
    assert_reads(&mut r, &files, V5_AUTO, "declared 0, open, after refresh");
}

#[test]
fn version_5_zero_declaration_concurrent_reader() {
    let (files, _, zero) = v5_fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero.ct");
    std::fs::write(&path, &zero).unwrap();
    let mut c = ConcurrentCtfsReader::open(&path).unwrap();
    for pass in ["open", "refresh"] {
        if pass == "refresh" {
            c.refresh().unwrap();
        }
        let listed: BTreeSet<String> = c.list_files().into_iter().collect();
        assert_eq!(listed, names(&files), "concurrent reader, {pass}: listed members");
        for (name, content) in &files {
            let got = c.read_file(name).unwrap_or_else(|e| panic!("concurrent reader, {pass}: {name}: {e:?}"));
            assert!(got == *content, "concurrent reader, {pass}: {name} differs");
        }
    }
}

#[test]
fn version_6_explicit_declaration_control() {
    let files = members(V6_AUTO);
    let mut v6 = as_v6_auto_fill(&written(V6_AUTO as u32, &files));
    declare(&mut v6, V6_AUTO as u32);
    assert_reads(&mut CtfsReader::from_bytes(v6).unwrap(), &files, V6_AUTO, "v6 explicit 169, from_bytes");
}

#[test]
fn version_6_zero_declaration_reads_the_auto_filled_root() {
    let files = members(V6_AUTO);
    let v6 = as_v6_auto_fill(&written(V6_AUTO as u32, &files));
    assert_eq!((v6[5], &v6[12..16]), (6, &[0u8; 4][..]));

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero_v6.ct");
    std::fs::write(&path, &v6).unwrap();

    assert_reads(&mut CtfsReader::from_bytes(v6).unwrap(), &files, V6_AUTO, "v6 declared 0, from_bytes");
    assert_reads(&mut CtfsReader::open(&path).unwrap(), &files, V6_AUTO, "v6 declared 0, open");
}

#[test]
fn open_append_on_a_zero_declaration_keeps_its_members_and_adds_to_the_auto_filled_root() {
    let files = members(3);
    let mut zero = written(V5_AUTO as u32, &files);
    declare(&mut zero, 0);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("append.ct");
    std::fs::write(&path, &zero).unwrap();

    let added = ("added".to_string(), bytes(99, 2 * BS as usize + 5));
    let mut w = CtfsWriter::open_append(&path).unwrap();
    let h = w.add_file(&added.0).unwrap();
    w.write(h, &added.1).unwrap();
    w.close().unwrap();

    let after = std::fs::read(&path).unwrap();
    assert_eq!(&after[..16], &zero[..16], "the header is written once and never changes (§1)");
    let mut all = files.clone();
    all.push(added);
    assert_reads(&mut CtfsReader::open(&path).unwrap(), &all, V5_AUTO, "declared 0, after open_append");

    // A full auto-filled directory refuses the next member, as a declared 170 would.
    let full = members(V5_AUTO);
    let mut zero_full = written(V5_AUTO as u32, &full);
    declare(&mut zero_full, 0);
    let full_path = dir.path().join("full.ct");
    std::fs::write(&full_path, &zero_full).unwrap();
    let mut w = CtfsWriter::open_append(&full_path).unwrap();
    assert!(
        w.find_file("m169").is_some(),
        "open_append recovers the member in the last auto-filled slot"
    );
    match w.add_file("onemore") {
        Err(CtfsError::TooManyFiles) => {}
        other => panic!("a full auto-filled root must refuse the next member, got {other:?}"),
    }
}
