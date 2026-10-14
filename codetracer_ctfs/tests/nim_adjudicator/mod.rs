//! Driving the sibling Nim implementation's `check_ctfs_container` over a
//! container this crate produced.
//!
//! §5d of `CTFS-Binary-Format.md` makes agreement *between implementations* the
//! acceptance criterion for a writer, because a round trip inside one crate can
//! only ever prove that its two halves agree with each other. This module is
//! the shared plumbing for that: it locates the sibling
//! `codetracer-trace-format-nim` checkout, builds and runs its production
//! reader through the repo's own dev shell, and reports what that reader made
//! of the bytes.
//!
//! It lives in `tests/nim_adjudicator/` rather than being copied into each test
//! file on purpose. A second transcription of the same cross-read plumbing is
//! how the two copies drift, and this campaign's standing rule is to keep one
//! implementation of a thing rather than bound two.
//!
//! # NO MOCKS
//!
//! There is nothing to mock here: the whole point is that the adjudicating
//! reader is the real production reader of a genuinely separate implementation,
//! built from the sibling checkout's own toolchain.
//!
//! # Skip discipline
//!
//! `nim_checker()` returns `None`, loudly, only when the sibling checkout or
//! `direnv` is absent — i.e. when the cross-implementation half *cannot* run.
//! A checkout that is present but whose checker fails to build is a **failure**,
//! not a skip: `run_nim_checker` asserts that the checker actually produced its
//! own output before any verdict is read from it.

#![allow(dead_code)] // Each test file uses a subset of this module.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Reuse only the complete runtime already qualified by the owning entry caller.
pub fn declared_runtime_nim() -> Option<PathBuf> {
    use sha2::{Digest, Sha256};
    let receipt = std::env::var_os("ENTRY_NIM_RUNTIME_AUTHORITY")?;
    let authority: serde_json::Value =
        serde_json::from_slice(&fs::read(receipt).expect("declared runtime authority absent")).expect("malformed declared runtime authority");
    assert_eq!(authority["schema"], "codetracer-trace-format.unix-runtime-authority.v1");
    for field in ["finished", "success", "finalGuards"] {
        assert_eq!(authority[field], true, "incomplete declared runtime authority");
    }
    let roles = authority["expectedSelectedImages"].as_object().expect("declared runtime roles absent");
    assert_eq!(roles.len(), 5, "declared runtime role inventory differs");
    for role in ["nim", "nimble", "git", "gcc", "bash"] {
        let image = roles.get(role).expect("mandatory declared runtime role absent");
        let lexical = PathBuf::from(image["lexical"].as_str().expect("declared runtime image path absent"));
        let search = std::env::var_os("PATH").expect("declared runtime PATH absent");
        let actual = std::env::split_paths(&search)
            .map(|directory| directory.join(role))
            .find(|candidate| {
                let Ok(metadata) = fs::metadata(candidate) else {
                    return false;
                };
                if !metadata.is_file() {
                    return false;
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    metadata.permissions().mode() & 0o111 != 0
                }
                #[cfg(not(unix))]
                {
                    true
                }
            })
            .expect("mandatory runtime PATH role absent");
        assert_eq!(actual, lexical, "runtime PATH selected another image");
        let resolved = fs::canonicalize(&lexical).expect("declared runtime image absent");
        assert!(resolved.starts_with("/nix/store"), "declared runtime image is mutable");
        assert_eq!(
            resolved,
            PathBuf::from(image["resolved"].as_str().expect("declared resolved image absent"))
        );
        let metadata = fs::metadata(&resolved).expect("declared runtime metadata absent");
        assert!(metadata.is_file(), "declared runtime image is not regular");
        assert_eq!(
            format!("{:x}", Sha256::digest(fs::read(&resolved).expect("declared runtime image unreadable"))),
            image["sha256"].as_str().expect("declared runtime image hash absent")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(Some(metadata.dev()), image["dev"].as_u64());
            assert_eq!(Some(metadata.ino()), image["ino"].as_u64());
            assert_eq!(Some((metadata.mode() & 0o7777) as u64), image["mode"].as_u64());
        }
    }
    Some(PathBuf::from(roles["nim"]["lexical"].as_str().expect("declared Nim image absent")))
}

/// The Nim checkout to adjudicate with: the `CODETRACER_TRACE_FORMAT_NIM_DIR`
/// override `codetracer_trace_writer_nim/build.rs` also honours, or the
/// sibling repo.
pub fn nim_repo() -> PathBuf {
    std::env::var("CODETRACER_TRACE_FORMAT_NIM_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../codetracer-trace-format-nim"))
}

/// The sibling Nim checkout, the `direnv` that supplies its toolchain, and
/// `HOME`. `None` means the cross-implementation half cannot run here.
pub fn nim_checker() -> Option<(PathBuf, PathBuf, PathBuf)> {
    let repo = nim_repo();
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
    if let Some(nim) = declared_runtime_nim() {
        return Some((fs::canonicalize(&repo).ok()?, nim, home));
    }
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
/// name -> exact expected bytes, plus names that must be absent. Returns the
/// combined output and whether the checker exited 0.
///
/// `env -i` is deliberate: this repo's dev shell is on the environment and the
/// sibling repo's is not, and letting the two mix is how a cross-read ends up
/// proving something about the wrong toolchain.
pub fn run_nim_checker(work: &Path, container: &Path, present: &[(&str, &[u8])], absent: &[&str]) -> (String, bool) {
    let (repo, direnv, home) = nim_checker().expect("caller checked");
    // Keep the empty-environment toolchain boundary. Only the declared matching
    // hook constructor is forwarded; the sibling still admits its own compiler.
    let constructor = PathBuf::from(std::env::var_os("REPROBUILD_REPRO").expect("declared matching hook constructor is required"));
    let constructor = fs::canonicalize(constructor).expect("declared matching hook constructor is absent");
    assert!(constructor.is_file(), "declared matching hook constructor is not a regular file");

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

    let mut command = if let Some(nim) = declared_runtime_nim() {
        // The entry caller forwards the complete, independently verified peer environment.
        Command::new(nim)
    } else {
        let mut command = Command::new("env");
        command.args([
            "-i".to_string(),
            format!("HOME={}", home.display()),
            format!("REPROBUILD_REPRO={}", constructor.display()),
            "PATH=/run/current-system/sw/bin:/usr/bin:/bin".to_string(),
            direnv.display().to_string(),
            "exec".into(),
            repo.display().to_string(),
            "nim".into(),
        ]);
        command
    };
    let out = command
        .args([
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
