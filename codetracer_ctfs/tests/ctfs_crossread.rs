//! Cross-implementation agreement about a damaged container.
//!
//! `CTFS-Binary-Format.md` §5d makes agreement between implementations the
//! acceptance criterion, not a round trip: a round trip inside one crate proves
//! nothing about whether two readers answer the same question about the same
//! bytes the same way. Until M59 the five conformant readers — the wasm
//! recorder's Go `internal/ctfs`, `codetracer-trace-format-nim`'s
//! `readInternalFile`, the Nim `check_ctfs_container` adjudicator, the
//! db-backend's `CtfsReader` and the native recorder's disk reader — agreed
//! with each other and **not** with this crate, which read the truncated
//! container below as 12 388 bytes of success.
//!
//! This test hands the *same file* to the two Rust readers in this crate and to
//! the adjudicating Nim reader, exactly the way
//! `codetracer-wasm-recorder/internal/ctfs/partial_tail_test.go` already does
//! for the Go reader, and compares the answers:
//!
//! - on a container with a 777-byte partial tail, all of them return every
//!   stream byte-exact;
//! - on a truncated container, all of them lose exactly the stream whose data
//!   block fell outside the whole blocks, and keep the other byte-exact.
//!
//! # NO MOCKS
//!
//! Real containers written by the production `CtfsWriter`, damaged on a real
//! filesystem, read by the real production readers of both implementations.
//! The Nim side is the same helper binary the Go cross-read tests drive, built
//! from the sibling checkout with its own toolchain.
//!
//! # Skip discipline
//!
//! A cross-read can only run where both checkouts and both toolchains are
//! present. When the sibling `codetracer-trace-format-nim` checkout is absent
//! the Rust half still runs and the Nim half says out loud that it did not.
//! When the checkout **is** present but the checker cannot be built or run,
//! that is a failure, not a skip — never make a red cross-read go away by
//! arranging for the skip.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use codetracer_ctfs::{ConcurrentCtfsReader, CtfsReader, CtfsWriter};
use tempfile::TempDir;

const BS: usize = 4096;
const MAX_ROOT_ENTRIES: u32 = 64;

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

/// The sibling Nim checkout and the direnv that supplies its toolchain.
/// `None` means the cross-implementation half cannot run here.
fn nim_checker() -> Option<(PathBuf, PathBuf, PathBuf)> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../codetracer-trace-format-nim");
    if !repo.join("tests/check_ctfs_container.nim").exists() {
        eprintln!(
            "SKIP: the sibling codetracer-trace-format-nim checkout has no \
             tests/check_ctfs_container.nim (looked in {}), so the cross-implementation half of the \
             §5d agreement did not run. This crate's own readers were still exercised.",
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

/// Drive the production Nim reader over `container` with a manifest built from
/// `present` (name -> exact expected bytes) and `absent`. Returns the combined
/// output and whether the checker exited 0.
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
    // Always log the adjudicator's verdict (visible under `--nocapture`), so a
    // reader of the test output can see what the other implementation said
    // rather than only learning it when an assertion fails.
    eprintln!(
        "--- Nim checker on {} (exit {:?}) ---\n{combined}",
        container.display(),
        out.status.code()
    );
    // A checkout that exists but cannot be built is a failure, not a skip.
    assert!(
        combined.contains("check_ctfs_container:"),
        "the sibling repo's Nim toolchain could not run the checker, so the cross-implementation half \
         did not adjudicate anything (exit {:?}):\n{combined}",
        out.status.code()
    );
    (combined, out.status.success())
}

/// Both fixtures, both Rust readers and the adjudicating Nim reader, in one
/// test so the Nim checker is compiled once.
#[test]
fn the_rust_readers_agree_with_the_nim_reader_about_a_damaged_container() {
    let dir = TempDir::new().unwrap();

    // ---------------------------------------------------------------- fixture 1
    // A crash inside an append's tail write: a sealed container plus a whole
    // block and a 777-byte fragment that nothing references. `snapshot.mem` is
    // 600 data blocks, past the 511-block level-1 threshold, so the agreement
    // covers §4's multi-level walk and not just a flat file.
    let torn = dir.path().join("torn.ct");
    let meta = deterministic_bytes(77, 9000);
    let snapshot = deterministic_bytes(78, BS * 600);
    let mut w = CtfsWriter::create(&torn, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    let h = w.add_file("meta.dat").unwrap();
    w.write(h, &meta).unwrap();
    let h = w.add_file("snapshot.mem").unwrap();
    w.write(h, &snapshot).unwrap();
    w.close().unwrap();
    let sealed = fs::read(&torn).unwrap();
    assert_eq!(
        sealed.len() % BS,
        0,
        "the sealed container is {} bytes, not a block multiple",
        sealed.len()
    );
    let mut torn_bytes = sealed.clone();
    torn_bytes.extend_from_slice(&deterministic_bytes(79, BS + 777));
    fs::write(&torn, &torn_bytes).unwrap();

    // ---------------------------------------------------------------- fixture 2
    // A truncated container: the same shape, and indistinguishable from the
    // first from the bytes alone. `z.dat`'s last data block carries only 100
    // bytes, so the clamped read is short enough to be satisfied out of the
    // partial region — which is what makes a missing bound produce wrong
    // content rather than an error.
    let cut = dir.path().join("cut.ct");
    let survivor = deterministic_bytes(11, 9000);
    let lost = deterministic_bytes(12, 3 * BS + 100);
    let mut w = CtfsWriter::create(&cut, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    let h = w.add_file("meta.dat").unwrap();
    w.write(h, &survivor).unwrap();
    let h = w.add_file("z.dat").unwrap();
    w.write(h, &lost).unwrap();
    w.close().unwrap();
    let full = fs::read(&cut).unwrap();
    let cut_at = full.len() - BS + 100;
    fs::write(&cut, &full[..cut_at]).unwrap();
    assert_eq!(cut_at % BS, 100, "the truncated fixture must leave a 100-byte partial region");

    // ------------------------------------------------------- this crate's answers
    let mut r = CtfsReader::open(&torn).unwrap();
    assert_eq!(r.read_file("meta.dat").unwrap(), meta);
    assert_eq!(r.read_file("snapshot.mem").unwrap(), snapshot);
    let c = ConcurrentCtfsReader::open(&torn).unwrap();
    assert_eq!(c.read_file("meta.dat").unwrap(), meta);
    assert_eq!(c.read_file("snapshot.mem").unwrap(), snapshot);

    let mut r = CtfsReader::open(&cut).unwrap();
    assert_eq!(r.read_file("meta.dat").unwrap(), survivor, "the surviving stream came back changed");
    assert!(r.read_file("z.dat").is_err(), "CtfsReader served z.dat out of the partial region");
    let c = ConcurrentCtfsReader::open(&cut).unwrap();
    assert_eq!(c.read_file("meta.dat").unwrap(), survivor, "the surviving stream came back changed");
    assert!(
        c.read_file("z.dat").is_err(),
        "ConcurrentCtfsReader served z.dat out of the partial region"
    );

    if nim_checker().is_none() {
        return;
    }

    // ---------------------------------------------- the adjudicating Nim reader
    // 1. The partial tail: every stream byte-exact, same as above.
    let (out, ok) = run_nim_checker(
        dir.path(),
        &torn,
        &[("meta.dat", &meta), ("snapshot.mem", &snapshot)],
        &["nothere.dat", "absent.ns"],
    );
    assert!(
        ok,
        "the production Nim reader refused a partial-tail container this crate reads byte-exact — the two \
         implementations disagree about the same bytes:\n{out}"
    );
    assert!(
        out.contains("check_ctfs_container: OK"),
        "the Nim checker exited 0 without reporting a pass:\n{out}"
    );
    assert!(
        out.contains("777-byte partial tail"),
        "the Nim checker did not report the 777-byte partial tail, so it may not have adjudicated the damaged file at all:\n{out}"
    );

    // 2. The truncated container: the same stream must be lost by both.
    let (out, ok) = run_nim_checker(dir.path(), &cut, &[("meta.dat", &survivor), ("z.dat", &lost)], &["nothere.dat"]);
    assert!(
        !ok,
        "the Nim checker read z.dat back byte-exact from a container whose last data block for it is gone, \
         while this crate refuses it; the two implementations disagree:\n{out}"
    );
    assert!(
        out.contains("out of bounds") && out.contains("truncated"),
        "the Nim reader's refusal does not name the truncation:\n{out}"
    );
    assert!(out.contains("z.dat"), "the Nim reader did not name z.dat as the lost stream:\n{out}");

    // 3. …and no other stream. Asked only about the survivor, it must pass —
    //    the half that keeps "refuse the truncated stream" from becoming
    //    "refuse the container".
    let (out, ok) = run_nim_checker(dir.path(), &cut, &[("meta.dat", &survivor)], &["nothere.dat"]);
    assert!(
        ok,
        "a truncation that cost both implementations z.dat also cost the Nim reader meta.dat, which this crate \
         still reads byte-exact:\n{out}"
    );
}
