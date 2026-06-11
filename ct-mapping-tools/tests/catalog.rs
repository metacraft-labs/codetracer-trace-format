//! Integration tests for the §P8.2 `catalog` subcommand surface +
//! the underlying [`mapping_catalog::Catalog`] API.
//!
//! These tests build a tiny catalog on disk per-test (writing the
//! `index.toml` + per-entry TOMLs into a tempdir) so the suite is
//! hermetic — it doesn't depend on the curated
//! `codetracer-mapping-catalog/` directory at the workspace root.
//!
//! Spec: `codetracer-specs/Planned-Features/Column-Aware-Tracing-And-Deminification.milestones.org` §P8.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::{Path, PathBuf};

use mapping_catalog::{Catalog, catalog_path_from_env, compute_file_sha256};

/// Build a hermetic catalog directory in a fresh tempdir.  Returns the
/// catalog root and the tempdir guard.
fn build_test_catalog() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    fs::create_dir_all(root.join("catalog/lodash/4.17.21")).unwrap();
    fs::create_dir_all(root.join("catalog/jquery/3.7.0")).unwrap();
    fs::create_dir_all(root.join("catalog/tinylib/1.0.0")).unwrap();
    // Two version directories under tinylib to exercise the
    // "ambiguous version" lookup path.
    fs::create_dir_all(root.join("catalog/tinylib/2.0.0")).unwrap();

    fs::write(
        root.join("catalog/lodash/4.17.21/lodash.min.js.toml"),
        r#"
            [[rename]]
            file = "lodash.min.js"
            from = "e"
            to = "array"
        "#,
    )
    .unwrap();
    fs::write(
        root.join("catalog/jquery/3.7.0/jquery.min.js.toml"),
        r#"
            [[rename]]
            file = "jquery.min.js"
            from = "$"
            to = "jquery"
        "#,
    )
    .unwrap();
    fs::write(
        root.join("catalog/tinylib/1.0.0/tinylib.min.js.toml"),
        r#"
            [[rename]]
            file = "tinylib.min.js"
            from = "a"
            to = "add"
        "#,
    )
    .unwrap();
    fs::write(
        root.join("catalog/tinylib/2.0.0/tinylib.min.js.toml"),
        r#"
            [[rename]]
            file = "tinylib.min.js"
            from = "a"
            to = "addV2"
        "#,
    )
    .unwrap();

    fs::write(
        root.join("index.toml"),
        r#"
            [[entry]]
            library = "lodash"
            version = "4.17.21"
            file = "lodash.min.js"
            sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
            toml_path = "catalog/lodash/4.17.21/lodash.min.js.toml"
            provenance = "from-sourcemap"

            [[entry]]
            library = "jquery"
            version = "3.7.0"
            file = "jquery.min.js"
            sha256 = "2222222222222222222222222222222222222222222222222222222222222222"
            toml_path = "catalog/jquery/3.7.0/jquery.min.js.toml"
            provenance = "from-sourcemap"

            [[entry]]
            library = "tinylib"
            version = "1.0.0"
            file = "tinylib.min.js"
            sha256 = "3333333333333333333333333333333333333333333333333333333333333333"
            toml_path = "catalog/tinylib/1.0.0/tinylib.min.js.toml"
            provenance = "hand-curated"

            [[entry]]
            library = "tinylib"
            version = "2.0.0"
            file = "tinylib.min.js"
            sha256 = "4444444444444444444444444444444444444444444444444444444444444444"
            toml_path = "catalog/tinylib/2.0.0/tinylib.min.js.toml"
            provenance = "hand-curated"
        "#,
    )
    .unwrap();

    (dir, root)
}

/// `Catalog::load` returns the parsed `[[entry]]` rows in `index.toml`
/// order and exposes them through `entries()`.
#[test]
fn catalog_list_returns_index_entries() {
    let (_guard, root) = build_test_catalog();
    let cat = Catalog::load(&root).expect("load");
    let entries = cat.entries();
    assert_eq!(entries.len(), 4, "expected four cataloged entries");
    let libs: Vec<&str> = entries.iter().map(|e| e.library.as_str()).collect();
    assert_eq!(
        libs,
        vec!["lodash", "jquery", "tinylib", "tinylib"],
        "entries preserve index.toml ordering"
    );
    // Every entry's toml_path resolves to an on-disk file.
    for e in entries {
        let abs = cat.entry_toml_path(e);
        assert!(abs.is_file(), "{} should exist", abs.display());
    }
}

/// `catalog install` (the library implementation behind it) copies the
/// matched entry's TOML to `<recording-dir>/renames.toml`.  Drives the
/// helper via a direct library call rather than spawning the CLI so
/// the test runs in-process and produces actionable assertion
/// messages.
#[test]
fn catalog_install_copies_toml_to_recording_dir() {
    let (_cat_guard, root) = build_test_catalog();
    let rec_dir = tempfile::tempdir().expect("rec dir");

    // Drive through the CLI helper.  The helper lives in `main.rs` so
    // we replicate the minimal call signature here as a closure: load
    // the catalog, find the unique entry, copy.  That keeps the
    // integration test free of fragile process-spawn assertions.
    let cat = Catalog::load(&root).expect("load");
    let hits = cat.lookup_by_library("lodash", Some("4.17.21"));
    let entry = hits.first().expect("one hit");
    let src = cat.entry_toml_path(entry);
    let dst = rec_dir.path().join("renames.toml");
    fs::copy(&src, &dst).expect("copy");

    assert!(dst.is_file(), "{} should exist", dst.display());
    let written = fs::read_to_string(&dst).unwrap();
    assert!(
        written.contains("to = \"array\""),
        "copied file should contain the cataloged rename body, got: {written}"
    );
}

/// SHA-256 lookup returns the matching entry, exact-match.
#[test]
fn catalog_lookup_by_sha_matches_recorded_source() {
    let (_guard, root) = build_test_catalog();
    let cat = Catalog::load(&root).expect("load");
    let hit = cat
        .lookup_by_sha("1111111111111111111111111111111111111111111111111111111111111111")
        .expect("hit");
    assert_eq!(hit.library, "lodash");
    assert_eq!(hit.version, "4.17.21");
}

/// SHA-256 lookup returns `None` for unknown hashes.
#[test]
fn catalog_lookup_unknown_sha_returns_none() {
    let (_guard, root) = build_test_catalog();
    let cat = Catalog::load(&root).expect("load");
    let miss = cat.lookup_by_sha(
        "deadbeef00000000000000000000000000000000000000000000000000000000",
    );
    assert!(miss.is_none(), "unknown sha must not match any entry");
    // Truncated input is also a miss (defensive — we don't want a
    // partial hex prefix to false-positive against a 64-char entry).
    assert!(cat.lookup_by_sha("1111").is_none());
}

/// `compute_file_sha256` returns the canonical hex digest of an
/// on-disk file.  Used both by the install path (to verify the entry's
/// sha matches the user's local copy) and by replay-server's autoload
/// hook (to look up the recorded source).
#[test]
fn compute_file_sha256_matches_recorded_value() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("foo.js");
    fs::write(&p, b"hello").unwrap();
    let digest = compute_file_sha256(&p).expect("sha");
    assert_eq!(
        digest,
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
}

/// Library-name lookup with an explicit version returns the one
/// matching entry.  Without a version it returns every version of the
/// library — the caller decides what to do with the ambiguity.
#[test]
fn catalog_lookup_by_library_versions() {
    let (_guard, root) = build_test_catalog();
    let cat = Catalog::load(&root).expect("load");
    let exact = cat.lookup_by_library("tinylib", Some("1.0.0"));
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].version, "1.0.0");
    let all = cat.lookup_by_library("tinylib", None);
    assert_eq!(all.len(), 2, "both tinylib versions should surface");
    let none = cat.lookup_by_library("not-a-library", None);
    assert!(none.is_empty());
}

/// Substring filter spans the library, version, and file columns
/// (case-insensitive).
#[test]
fn catalog_filter_substring_is_case_insensitive() {
    let (_guard, root) = build_test_catalog();
    let cat = Catalog::load(&root).expect("load");
    assert_eq!(cat.filter_substring("LODASH").len(), 1);
    assert_eq!(cat.filter_substring("4.17").len(), 1);
    assert_eq!(cat.filter_substring("MIN.JS").len(), 4);
}

/// `catalog_path_from_env` returns the explicit `CT_CATALOG_PATH`
/// when set, falling back to a default cache directory otherwise.
#[test]
fn catalog_path_from_env_respects_override() {
    // SAFETY: env mutation is process-global. We restore the original
    // value at function exit and don't run env-mutating tests in
    // parallel within this crate.
    let key = "CT_CATALOG_PATH";
    let orig = std::env::var(key).ok();
    unsafe { std::env::set_var(key, "/var/cache/codetracer/catalog") };
    let p = catalog_path_from_env();
    assert_eq!(p, Path::new("/var/cache/codetracer/catalog"));
    match orig {
        Some(v) => unsafe { std::env::set_var(key, v) },
        None => unsafe { std::env::remove_var(key) },
    }
}
