//! M14 verification tests for the discovery library.
//!
//! Tests covered here:
//!
//! - `test_recorder_origin_pattern_discovery_finds_library_files`
//! - `test_recorder_origin_pattern_explicit_include_flag`

use std::fs;
use std::path::PathBuf;

use codetracer_origin_pattern_discovery::{
    DiscoveryConfig,
    codetracer_origin_patterns::{EmbeddedPatternsIndex, INDEX_FILE, META_DAT_SUBDIR, parse_index},
    discover_and_embed, parse_cli_flags,
};

fn make_pattern_file(dir: &std::path::Path, library_dir_name: &str) -> PathBuf {
    let lib = dir.join(library_dir_name);
    fs::create_dir_all(lib.join(".codetracer")).unwrap();
    let path = lib.join(".codetracer").join("origin-patterns.toml");
    fs::write(
        &path,
        r#"
[[forwarder]]
match = "$x.clone()"
continuation = "$x"
description = "test pattern"
"#,
    )
    .unwrap();
    path
}

#[test]
fn test_recorder_origin_pattern_discovery_finds_library_files() {
    let workspace = tempfile::tempdir().unwrap();
    // Recorded program's source tree
    let source_root = workspace.path().join("program");
    fs::create_dir_all(&source_root).unwrap();
    // Program-local pattern
    make_pattern_file(&source_root, "subpkg");

    // Dependency closure with two libraries containing pattern files
    let dep_root = workspace.path().join("vendor");
    fs::create_dir_all(&dep_root).unwrap();
    make_pattern_file(&dep_root, "lib_alpha");
    make_pattern_file(&dep_root, "lib_beta");

    // Library without a pattern file — should be silently ignored
    fs::create_dir_all(dep_root.join("lib_no_patterns")).unwrap();

    let trace_root = workspace.path().join("trace");
    fs::create_dir_all(&trace_root).unwrap();

    let config = DiscoveryConfig::new()
        .with_source_root(source_root.clone())
        .with_dependency_root(dep_root.clone());

    let report = discover_and_embed(&config, &trace_root).unwrap();
    assert_eq!(report.entries.len(), 3, "expected 3 pattern files");
    assert!(report.skipped.is_empty(), "no parse failures expected");

    // Each library lives under meta_dat/origin-patterns/<library_id>/.
    let embed_root = trace_root.join("meta_dat").join(META_DAT_SUBDIR);
    let subpkg_file = embed_root.join("subpkg").join("origin-patterns.toml");
    let alpha_file = embed_root.join("lib_alpha").join("origin-patterns.toml");
    let beta_file = embed_root.join("lib_beta").join("origin-patterns.toml");
    assert!(subpkg_file.exists(), "program-local pattern not embedded: {subpkg_file:?}");
    assert!(alpha_file.exists(), "lib_alpha pattern not embedded: {alpha_file:?}");
    assert!(beta_file.exists(), "lib_beta pattern not embedded: {beta_file:?}");

    // Manifest records discovery order: program source first, then
    // dependency roots, sorted within each root for cross-platform
    // determinism.
    let index_path = embed_root.join(INDEX_FILE);
    let index_text = fs::read_to_string(&index_path).unwrap();
    let index: EmbeddedPatternsIndex = parse_index(&index_text).unwrap();
    assert_eq!(index.libraries.len(), 3);
    assert_eq!(index.libraries[0].library_id, "subpkg");
    assert_eq!(index.libraries[1].library_id, "lib_alpha");
    assert_eq!(index.libraries[2].library_id, "lib_beta");

    // M14 spec: source_path is recorded for diagnostic display.
    for entry in &index.libraries {
        assert!(
            entry.source_path.is_some(),
            "expected source_path for diagnostic display, got None for {}",
            entry.library_id
        );
    }
}

#[test]
fn test_recorder_origin_pattern_explicit_include_flag() {
    let workspace = tempfile::tempdir().unwrap();
    let source_root = workspace.path().join("program");
    fs::create_dir_all(&source_root).unwrap();

    // The extra-include root contains a pattern file the program's
    // dependency closure does NOT cover.
    let extra_root = workspace.path().join("extra");
    fs::create_dir_all(&extra_root).unwrap();
    make_pattern_file(&extra_root, "extra_lib");

    // First pass: extra include is honoured.
    let trace_root_a = workspace.path().join("trace_a");
    fs::create_dir_all(&trace_root_a).unwrap();
    let cli_parse = parse_cli_flags(["--origin-patterns-include", extra_root.to_str().unwrap()]);
    assert!(!cli_parse.disabled);
    assert_eq!(cli_parse.includes.len(), 1);

    let mut config = DiscoveryConfig::new().with_source_root(source_root.clone());
    for inc in &cli_parse.includes {
        config = config.with_extra_include(inc.clone());
    }
    let report = discover_and_embed(&config, &trace_root_a).unwrap();
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].library_id, "extra_lib");

    // Second pass: --no-origin-patterns disables discovery entirely.
    let trace_root_b = workspace.path().join("trace_b");
    fs::create_dir_all(&trace_root_b).unwrap();
    let cli_parse_disabled = parse_cli_flags(["--origin-patterns-include", extra_root.to_str().unwrap(), "--no-origin-patterns"]);
    assert!(cli_parse_disabled.disabled);

    let mut config = DiscoveryConfig::new().with_source_root(source_root);
    for inc in &cli_parse_disabled.includes {
        config = config.with_extra_include(inc.clone());
    }
    if cli_parse_disabled.disabled {
        config.enabled = false;
    }
    let report = discover_and_embed(&config, &trace_root_b).unwrap();
    assert!(report.entries.is_empty(), "--no-origin-patterns must skip walk");
    assert!(
        !trace_root_b.join("meta_dat").exists(),
        "--no-origin-patterns must not even create the meta_dat subtree"
    );
}
