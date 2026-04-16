/// Cross-repo filemap compatibility test.
///
/// This integration test verifies that:
/// 1. Rust can serialize the canonical compat filemap and produce known bytes.
/// 2. If FILEMAP_COMPAT_NIM_FILE is set, Rust can deserialize a Nim-generated
///    filemap and verify all fields match.
/// 3. The test writes its own serialized output to FILEMAP_COMPAT_RUST_FILE
///    (if set) so the Nim side can read and verify it.

use codetracer_ctfs::base40_encode;
use codetracer_ctfs::filemap::*;

/// Build the canonical compat filemap. Must match the Nim
/// `buildCompatFilemap` function exactly.
fn build_compat_filemap() -> Filemap {
    let mut entries = Vec::new();

    // Binary entry
    entries.push(FilemapEntry {
        ctfs_name: base40_encode("myapp").unwrap(),
        entry_type: FilemapEntryType::Binary,
        flags: 0x01,
        build_id: (1u8..=20).collect(),
        real_path: "/usr/bin/myapp".to_string(),
        ..Default::default()
    });

    // Debug entry
    entries.push(FilemapEntry {
        ctfs_name: base40_encode("myapp.dbg").unwrap(),
        entry_type: FilemapEntryType::DebugSymbol,
        flags: 0,
        build_id: (1u8..=20).collect(),
        real_path: "/usr/lib/debug/myapp.debug".to_string(),
        binary_ref: base40_encode("myapp").unwrap(),
        ..Default::default()
    });

    // Source entry
    entries.push(FilemapEntry {
        ctfs_name: base40_encode("main.c").unwrap(),
        entry_type: FilemapEntryType::SourceFile,
        flags: 0,
        build_id: vec![],
        real_path: "/home/user/src/main.c".to_string(),
        compilation_dir: "/home/user/build".to_string(),
        ..Default::default()
    });

    Filemap {
        version: 1,
        entries,
    }
}

fn verify_compat_filemap(fm: &Filemap) {
    assert_eq!(fm.version, 1);
    assert_eq!(fm.entries.len(), 3);

    // Binary entry
    let e = &fm.entries[0];
    assert_eq!(e.ctfs_name, base40_encode("myapp").unwrap());
    assert_eq!(e.entry_type, FilemapEntryType::Binary);
    assert_eq!(e.flags, 0x01);
    assert_eq!(e.build_id, (1u8..=20).collect::<Vec<u8>>());
    assert_eq!(e.real_path, "/usr/bin/myapp");

    // Debug entry
    let e = &fm.entries[1];
    assert_eq!(e.ctfs_name, base40_encode("myapp.dbg").unwrap());
    assert_eq!(e.entry_type, FilemapEntryType::DebugSymbol);
    assert_eq!(e.flags, 0);
    assert_eq!(e.build_id, (1u8..=20).collect::<Vec<u8>>());
    assert_eq!(e.real_path, "/usr/lib/debug/myapp.debug");
    assert_eq!(e.binary_ref, base40_encode("myapp").unwrap());

    // Source entry
    let e = &fm.entries[2];
    assert_eq!(e.ctfs_name, base40_encode("main.c").unwrap());
    assert_eq!(e.entry_type, FilemapEntryType::SourceFile);
    assert_eq!(e.flags, 0);
    assert_eq!(e.build_id.len(), 0);
    assert_eq!(e.real_path, "/home/user/src/main.c");
    assert_eq!(e.compilation_dir, "/home/user/build");
}

#[test]
fn test_filemap_compat_rust_roundtrip() {
    let fm = build_compat_filemap();
    let data = fm.serialize();
    let fm2 = Filemap::deserialize(&data).unwrap();
    verify_compat_filemap(&fm2);

    // Write Rust output if requested
    if let Ok(path) = std::env::var("FILEMAP_COMPAT_RUST_FILE") {
        std::fs::write(&path, &data).unwrap();
        eprintln!("Wrote Rust filemap ({} bytes) to {}", data.len(), path);
    }
}

#[test]
fn test_filemap_compat_read_nim() {
    let nim_path = match std::env::var("FILEMAP_COMPAT_NIM_FILE") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("FILEMAP_COMPAT_NIM_FILE not set, skipping Nim compat check");
            return;
        }
    };

    let nim_data = std::fs::read(&nim_path).unwrap_or_else(|e| {
        panic!("failed to read Nim compat file {}: {}", nim_path, e);
    });

    // Deserialize the Nim-generated filemap
    let fm = Filemap::deserialize(&nim_data).unwrap_or_else(|e| {
        panic!("failed to deserialize Nim filemap: {}", e);
    });

    verify_compat_filemap(&fm);

    // Also verify byte-level compatibility
    let rust_data = build_compat_filemap().serialize();
    assert_eq!(
        rust_data, nim_data,
        "Rust and Nim serialization produce different bytes ({} vs {} bytes)",
        rust_data.len(),
        nim_data.len()
    );

    eprintln!("Nim compat check passed: {} bytes match exactly", nim_data.len());
}
