//! `ConcurrentCtfsWriter`'s `flush` on a partial block, and the stream that
//! comes back afterwards.
//!
//! `FileWriter::write` buffers bytes and hands a block to
//! `flush_data_block` only when `buffer.len() >= block_size`, so every data
//! block it publishes is a *full* block and `data_block_count` is the number
//! of full blocks. `flush` breaks that invariant: it drains whatever is in the
//! buffer — a partial block — through the same `flush_data_block`, which pads
//! it with zeros, publishes it at the next logical block index and bumps
//! `data_block_count`. The bytes that arrive after the flush therefore begin
//! at the start of the *following* block, while the entry's `size` keeps
//! counting them contiguously. Every reader resolves logical byte `p` to
//! logical block `p / block_size`, so from the flush point on, the reader and
//! the writer disagree about which block a byte lives in: the reader serves
//! the flushed block's zero padding as content and loses the same number of
//! real bytes off the end.
//!
//! `CtfsWriter` — the single-threaded writer in the same crate — does not have
//! this defect. Its `sync_entry` keeps the buffer, allocates a *pending* block,
//! and `flush_data_block` reuses that pending block when the buffer later fills
//! (`writer.rs`, `pending_block`). The concurrent writer had no equivalent.
//!
//! # Why the assertions are cross-implementation
//!
//! This is a *writer* defect, so a round trip through one reader could only
//! ever prove the two halves of one crate agree with each other. §5d of
//! `CTFS-Binary-Format.md` makes agreement between implementations the
//! acceptance criterion. The container written here is therefore read back by
//! both of this crate's readers *and* handed to the adjudicating Nim reader
//! (`codetracer-trace-format-nim/tests/check_ctfs_container.nim`), the same
//! helper `ctfs_crossread.rs` and the wasm recorder's Go tests already drive.
//!
//! # NO MOCKS
//!
//! Real containers written by the production `ConcurrentCtfsWriter` onto a real
//! filesystem, read by the real production readers of both implementations.
//!
//! # Skip discipline
//!
//! The Rust half always runs. The Nim half runs whenever the sibling checkout
//! is present; when it is absent it says so out loud. A checkout that is
//! present but whose checker cannot be built is a failure, not a skip.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use codetracer_ctfs::{ConcurrentCtfsReader, ConcurrentCtfsWriter, CtfsReader};
use tempfile::TempDir;

const BS: usize = 4096;
const MAX_ROOT_ENTRIES: u32 = 16;

fn deterministic_bytes(seed: u64, n: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state >> 33) as u8);
    }
    out
}

/// Write `chunks` to one stream, calling `flush` after each chunk, and seal the
/// container. Returns the concatenation the container is supposed to hold.
fn write_with_flush_after_each_chunk(path: &Path, name: &str, chunks: &[&[u8]]) -> Vec<u8> {
    let writer = ConcurrentCtfsWriter::create(path, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    let mut fw = writer.add_file(name).unwrap();
    let mut expected = Vec::new();
    for chunk in chunks {
        fw.write(&writer, chunk).unwrap();
        fw.flush(&writer).unwrap();
        expected.extend_from_slice(chunk);
    }
    drop(fw);
    Arc::try_unwrap(writer).expect("no other Arc refs").close().unwrap();
    expected
}

/// Read the stream back with both of this crate's readers and return the two
/// answers, so the caller can compare each against what was written rather
/// than against the other.
fn read_with_both_rust_readers(path: &Path, name: &str) -> (Vec<u8>, Vec<u8>) {
    let mut seek = CtfsReader::open(path).unwrap();
    let from_seek = seek.read_file(name).unwrap();

    let mut concurrent = ConcurrentCtfsReader::open(path).unwrap();
    concurrent.refresh().unwrap();
    let from_concurrent = concurrent.read_file(name).unwrap();

    (from_seek, from_concurrent)
}

fn first_difference(expected: &[u8], got: &[u8]) -> String {
    if expected.len() != got.len() {
        return format!("length {} vs expected {}", got.len(), expected.len());
    }
    match expected.iter().zip(got.iter()).position(|(a, b)| a != b) {
        Some(i) => format!("first differing byte at offset {i}: got {:#04x}, expected {:#04x}", got[i], expected[i]),
        None => "identical".to_string(),
    }
}

/// The stream must come back exactly as written, whatever offsets the producer
/// happened to flush at.
#[test]
fn a_flush_on_a_partial_block_does_not_corrupt_the_bytes_written_after_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("flushed.ct");

    // 100 bytes is deliberately not a block multiple, and the second chunk is
    // long enough that the stream spans several blocks after the flush point.
    let head = deterministic_bytes(11, 100);
    let tail = deterministic_bytes(12, BS * 3 + 55);
    let expected = write_with_flush_after_each_chunk(&path, "stream.dat", &[&head, &tail]);
    assert_eq!(expected.len(), 100 + BS * 3 + 55);

    let (from_seek, from_concurrent) = read_with_both_rust_readers(&path, "stream.dat");

    assert_eq!(
        from_seek,
        expected,
        "CtfsReader disagrees with what was written: {}",
        first_difference(&expected, &from_seek)
    );
    assert_eq!(
        from_concurrent,
        expected,
        "ConcurrentCtfsReader disagrees with what was written: {}",
        first_difference(&expected, &from_concurrent)
    );
}

/// Several flushes at unaligned offsets, which is what a producer that flushes
/// on a timer rather than on a block boundary actually does.
#[test]
fn repeated_flushes_at_unaligned_offsets_do_not_corrupt_the_stream() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("many-flushes.ct");

    let chunks: Vec<Vec<u8>> = [1usize, 4095, 2, 4097, 300, 8191, 7]
        .iter()
        .enumerate()
        .map(|(i, n)| deterministic_bytes(200 + i as u64, *n))
        .collect();
    let refs: Vec<&[u8]> = chunks.iter().map(|c| c.as_slice()).collect();
    let expected = write_with_flush_after_each_chunk(&path, "events.dat", &refs);

    let (from_seek, from_concurrent) = read_with_both_rust_readers(&path, "events.dat");

    assert_eq!(
        from_seek,
        expected,
        "CtfsReader disagrees with what was written: {}",
        first_difference(&expected, &from_seek)
    );
    assert_eq!(
        from_concurrent,
        expected,
        "ConcurrentCtfsReader disagrees with what was written: {}",
        first_difference(&expected, &from_concurrent)
    );
}

/// A flush that lands exactly on a block multiple is the case the existing
/// tests already cover; it must keep working, and it is the control that says
/// the fix did not simply move the corruption.
#[test]
fn a_flush_on_a_block_multiple_still_round_trips() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("aligned.ct");

    let head = deterministic_bytes(21, BS * 2);
    let tail = deterministic_bytes(22, BS);
    let expected = write_with_flush_after_each_chunk(&path, "aligned.dat", &[&head, &tail]);

    let (from_seek, from_concurrent) = read_with_both_rust_readers(&path, "aligned.dat");
    assert_eq!(from_seek, expected, "{}", first_difference(&expected, &from_seek));
    assert_eq!(from_concurrent, expected, "{}", first_difference(&expected, &from_concurrent));
}

/// The multi-level walk of §4: a stream long enough to need a level-2 mapping,
/// flushed at an unaligned offset partway through. `usable` is 511 for a
/// 4096-byte block, so more than 511 data blocks forces the chain.
#[test]
fn a_partial_flush_inside_a_multi_level_stream_does_not_corrupt_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("multilevel.ct");

    let head = deterministic_bytes(31, BS * 300 + 17);
    let tail = deterministic_bytes(32, BS * 300);
    let expected = write_with_flush_after_each_chunk(&path, "snapshot.mem", &[&head, &tail]);

    let (from_seek, from_concurrent) = read_with_both_rust_readers(&path, "snapshot.mem");
    assert_eq!(from_seek, expected, "{}", first_difference(&expected, &from_seek));
    assert_eq!(from_concurrent, expected, "{}", first_difference(&expected, &from_concurrent));
}

/// A live reader must see the flushed prefix, byte-exact, *while* the producer
/// keeps writing. This is the reason `flush` exists at all, so the fix is only
/// a fix if it survives being observed mid-stream.
#[test]
fn a_reader_sees_the_flushed_prefix_byte_exact_while_the_writer_continues() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.ct");

    let writer = ConcurrentCtfsWriter::create(&path, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    let mut fw = writer.add_file("live.dat").unwrap();

    let all = deterministic_bytes(41, BS * 5);
    let cuts = [100usize, 4096, 4097, 9000, BS * 5];
    let mut written = 0usize;
    for cut in cuts {
        fw.write(&writer, &all[written..cut]).unwrap();
        fw.flush(&writer).unwrap();
        written = cut;

        let mut reader = ConcurrentCtfsReader::open(&path).unwrap();
        reader.refresh().unwrap();
        let size = reader.file_size("live.dat").unwrap() as usize;
        assert_eq!(size, cut, "the entry's size does not match what was flushed");
        let seen = reader.read_file("live.dat").unwrap();
        assert_eq!(
            seen,
            &all[..cut],
            "a reader saw the wrong bytes after a flush at offset {cut}: {}",
            first_difference(&all[..cut], &seen)
        );
    }

    drop(fw);
    Arc::try_unwrap(writer).unwrap().close().unwrap();
}

// ---------------------------------------------------------------------------
// The cross-implementation half.
// ---------------------------------------------------------------------------

/// The sibling Nim checkout and the direnv that supplies its toolchain.
/// `None` means the cross-implementation half cannot run here.
fn nim_checker() -> Option<(PathBuf, PathBuf, PathBuf)> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../codetracer-trace-format-nim");
    if !repo.join("tests/check_ctfs_container.nim").exists() {
        eprintln!(
            "SKIP: the sibling codetracer-trace-format-nim checkout has no \
             tests/check_ctfs_container.nim (looked in {}), so the cross-implementation half did \
             not run. This crate's own readers were still exercised.",
            repo.display()
        );
        return None;
    }
    let home = PathBuf::from(std::env::var("HOME").ok()?);
    let direnv = home.join(".nix-profile/bin/direnv");
    let direnv = if direnv.exists() {
        direnv
    } else {
        match which_direnv() {
            Some(p) => p,
            None => {
                eprintln!(
                    "SKIP: direnv is not available, and the sibling repo's Nim toolchain comes from \
                     its own nix dev shell rather than this one, so the production Nim reader cannot be built."
                );
                return None;
            }
        }
    };
    Some((fs::canonicalize(&repo).ok()?, direnv, home))
}

fn which_direnv() -> Option<PathBuf> {
    let out = Command::new("sh").arg("-c").arg("command -v direnv").output().ok()?;
    if !out.status.success() {
        return None;
    }
    let p = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
    if p.exists() {
        Some(p)
    } else {
        None
    }
}

/// Drive the production Nim reader over `container` with a manifest of
/// name -> exact expected bytes. Returns the combined output and whether the
/// checker exited 0.
///
/// `env -i` is deliberate: this repo's dev shell is on the environment and the
/// sibling repo's is not, and letting the two mix is how a cross-read ends up
/// proving something about the wrong toolchain.
fn run_nim_checker(work: &Path, container: &Path, present: &[(&str, &[u8])], absent: &[&str]) -> (String, bool) {
    let (repo, direnv, home) = nim_checker().expect("caller checked");

    let mut manifest = String::new();
    for (name, body) in present {
        let expect = work.join(format!("expect-{}", name.replace('/', "_")));
        fs::write(&expect, body).unwrap();
        manifest.push_str(&format!("{} {}\n", name, expect.display()));
    }
    // The checker requires a negative control: a manifest of nothing but
    // positives would pass for a reader that finds everything.
    for a in absent {
        manifest.push_str(&format!("!{a}\n"));
    }
    let manifest_path = work.join("manifest.txt");
    fs::write(&manifest_path, &manifest).unwrap();

    let out = Command::new("env")
        .args([
            "-i".into(),
            format!("HOME={}", home.display()),
            "PATH=/run/current-system/sw/bin:/usr/bin:/bin".into(),
            direnv.display().to_string(),
            "exec".into(),
            repo.display().to_string(),
            "nim".into(),
            "c".into(),
            "-r".into(),
            "-d:release".into(),
            "-p:src".into(),
            "--hints:off".into(),
            format!("--nimcache:{}", work.join("nimcache").display()),
            format!("-o:{}", work.join("check_ctfs_container").display()),
            "tests/check_ctfs_container.nim".into(),
            container.display().to_string(),
            manifest_path.display().to_string(),
        ])
        .current_dir(&repo)
        .output()
        .expect("failed to spawn env/direnv");

    let combined = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    eprintln!(
        "--- Nim checker on {} (exit {:?}) ---\n{combined}",
        container.display(),
        out.status.code()
    );
    assert!(
        combined.contains("check_ctfs_container:"),
        "the sibling repo's Nim toolchain could not run the checker, so the cross-implementation half \
         did not adjudicate anything (exit {:?}):\n{combined}",
        out.status.code()
    );
    (combined, out.status.success())
}

/// The independent Nim reader must read back exactly what the concurrent
/// writer was told to write, across a flush at an unaligned offset.
#[test]
fn the_nim_reader_agrees_with_what_the_concurrent_writer_was_told_to_write() {
    if nim_checker().is_none() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("crossread-flush.ct");

    let head = deterministic_bytes(51, 100);
    let tail = deterministic_bytes(52, BS * 3 + 55);
    let expected = write_with_flush_after_each_chunk(&path, "stream.dat", &[&head, &tail]);

    let (out, ok) = run_nim_checker(dir.path(), &path, &[("stream.dat", &expected)], &["absent.dat"]);
    assert!(
        ok,
        "the independent Nim reader does not agree with what the concurrent writer was told to \
         write, so the container this crate produced is wrong rather than merely read back \
         consistently:\n{out}"
    );
}
