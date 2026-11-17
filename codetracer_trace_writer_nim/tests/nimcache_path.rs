//! Tests the production path selector without invoking Nim: the failure is the
//! chosen scratch path, before compilation, rather than native compiler output.
#[path = "../nimcache_path.rs"]
mod nimcache_path;

use std::path::Path;

fn root() -> &'static Path {
    Path::new(if cfg!(windows) { "C:/" } else { "/" })
}

#[test]
fn short_operator_temp_directory_is_honoured() {
    let temp = root().join("disk/scratch");
    let (path, fallback) = nimcache_path::select(&temp, &root().join("tmp"), 42);
    assert_eq!(path, temp.join("ctnw/000000000000002a"));
    assert!(!fallback);
}

#[test]
fn overlong_operator_temp_directory_falls_back_explicitly() {
    let temp = root().join("disk").join("long-scratch/".repeat(20));
    let (path, fallback) = nimcache_path::select(&temp, &root().join("tmp"), 42);
    assert_eq!(path, root().join("tmp/ctnw/000000000000002a"));
    assert!(fallback, "caller must warn that the operator's scratch directory was rejected");
}

#[test]
fn entire_cache_path_including_hash_counts_towards_budget() {
    // Root + component + `/ctnw/` + 16 hex digits = 100 native units.
    let component = "x".repeat(100 - root().as_os_str().len() - 22);
    let boundary = root().join(&component);
    let (path, fallback) = nimcache_path::select(&boundary, &root().join("tmp"), 1);
    assert_eq!(path, boundary.join("ctnw/0000000000000001"));
    assert!(!fallback);
    let over = root().join(format!("{component}x"));
    let (path, fallback) = nimcache_path::select(&over, &root().join("tmp"), 1);
    assert_eq!(path, root().join("tmp/ctnw/0000000000000001"));
    assert!(fallback);
}

#[test]
#[should_panic(expected = "exceeds the 100-unit path budget")]
fn an_overlong_fallback_is_refused() {
    let long = root().join("x".repeat(120));
    nimcache_path::select(&long, &long, 1);
}

#[test]
fn cache_key_remains_separate_for_each_build_unit() {
    let temp = root().join("tmp");
    let first = nimcache_path::select(&temp, &temp, 1).0;
    let second = nimcache_path::select(&temp, &temp, 2).0;
    assert_ne!(first, second);
    assert_eq!(first, nimcache_path::select(&temp, &temp, 1).0);
}

#[cfg(unix)]
#[test]
fn unix_budget_counts_bytes_instead_of_unicode_characters() {
    let temp = format!("/{}", "é".repeat(39));
    let (_, fallback) = nimcache_path::select(Path::new(&temp), Path::new("/tmp"), 1);
    assert!(fallback);
}
