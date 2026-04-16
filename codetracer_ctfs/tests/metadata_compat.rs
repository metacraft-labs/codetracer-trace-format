/// Cross-repo metadata compatibility test.
///
/// This integration test verifies that:
/// 1. Rust can serialize/deserialize the canonical compat platform and mmap data.
/// 2. If METADATA_COMPAT_NIM_CT is set, Rust opens the Nim-generated .ct file
///    and reads filemap.bin, platform.bin, and mmap.bin, verifying all fields.
/// 3. The test writes its own serialized outputs to METADATA_COMPAT_RUST_PLATFORM
///    and METADATA_COMPAT_RUST_MMAP (if set) so the Nim side can verify.

use codetracer_ctfs::base40_encode;
use codetracer_ctfs::filemap::*;
use codetracer_ctfs::mmap_info::*;
use codetracer_ctfs::platform_info::*;

// ---------------------------------------------------------------------------
// Canonical compat data builders
// ---------------------------------------------------------------------------

fn build_compat_platform() -> PlatformInfo {
    PlatformInfo {
        os: PlatformOs::Linux,
        arch: PlatformArch::X86_64,
        pointer_size: 8,
        endianness: 0,
        page_size: 4096,
        kernel_major: 6,
        kernel_minor: 12,
        kernel_patch: 63,
        libc_name: "glibc-2.40".to_string(),
        kernel_version: "6.12.63-generic".to_string(),
    }
}

fn build_compat_mmap() -> MmapTable {
    MmapTable {
        entries: vec![
            MmapEntry {
                address: 0x400000,
                size: 0x10000,
                binary_ref: base40_encode("myapp").unwrap(),
                file_offset: 0,
                permissions: 0x05,
            },
            MmapEntry {
                address: 0x600000,
                size: 0x2000,
                binary_ref: base40_encode("myapp").unwrap(),
                file_offset: 0x10000,
                permissions: 0x03,
            },
            MmapEntry {
                address: 0x7fff00000000,
                size: 0x21000,
                binary_ref: 0,
                file_offset: 0,
                permissions: 0x03,
            },
        ],
    }
}

fn build_compat_filemap() -> Filemap {
    let mut entries = Vec::new();

    entries.push(FilemapEntry {
        ctfs_name: base40_encode("myapp").unwrap(),
        entry_type: FilemapEntryType::Binary,
        flags: 0x01,
        build_id: (1u8..=20).collect(),
        real_path: "/usr/bin/myapp".to_string(),
        ..Default::default()
    });

    entries.push(FilemapEntry {
        ctfs_name: base40_encode("myapp.dbg").unwrap(),
        entry_type: FilemapEntryType::DebugSymbol,
        flags: 0,
        build_id: (1u8..=20).collect(),
        real_path: "/usr/lib/debug/myapp.debug".to_string(),
        binary_ref: base40_encode("myapp").unwrap(),
        ..Default::default()
    });

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

// ---------------------------------------------------------------------------
// Verification helpers
// ---------------------------------------------------------------------------

fn verify_platform(p: &PlatformInfo) {
    assert_eq!(p.os, PlatformOs::Linux);
    assert_eq!(p.arch, PlatformArch::X86_64);
    assert_eq!(p.pointer_size, 8);
    assert_eq!(p.endianness, 0);
    assert_eq!(p.page_size, 4096);
    assert_eq!(p.kernel_major, 6);
    assert_eq!(p.kernel_minor, 12);
    assert_eq!(p.kernel_patch, 63);
    assert_eq!(p.libc_name, "glibc-2.40");
    assert_eq!(p.kernel_version, "6.12.63-generic");
}

fn verify_mmap(t: &MmapTable) {
    assert_eq!(t.entries.len(), 3);

    assert_eq!(t.entries[0].address, 0x400000);
    assert_eq!(t.entries[0].size, 0x10000);
    assert_eq!(t.entries[0].binary_ref, base40_encode("myapp").unwrap());
    assert_eq!(t.entries[0].file_offset, 0);
    assert_eq!(t.entries[0].permissions, 0x05);

    assert_eq!(t.entries[1].address, 0x600000);
    assert_eq!(t.entries[1].size, 0x2000);
    assert_eq!(t.entries[1].binary_ref, base40_encode("myapp").unwrap());
    assert_eq!(t.entries[1].file_offset, 0x10000);
    assert_eq!(t.entries[1].permissions, 0x03);

    assert_eq!(t.entries[2].address, 0x7fff00000000);
    assert_eq!(t.entries[2].size, 0x21000);
    assert_eq!(t.entries[2].binary_ref, 0);
    assert_eq!(t.entries[2].file_offset, 0);
    assert_eq!(t.entries[2].permissions, 0x03);
}

fn verify_filemap(fm: &Filemap) {
    assert_eq!(fm.version, 1);
    assert_eq!(fm.entries.len(), 3);

    let e = &fm.entries[0];
    assert_eq!(e.ctfs_name, base40_encode("myapp").unwrap());
    assert_eq!(e.entry_type, FilemapEntryType::Binary);
    assert_eq!(e.flags, 0x01);
    assert_eq!(e.build_id, (1u8..=20).collect::<Vec<u8>>());
    assert_eq!(e.real_path, "/usr/bin/myapp");

    let e = &fm.entries[1];
    assert_eq!(e.ctfs_name, base40_encode("myapp.dbg").unwrap());
    assert_eq!(e.entry_type, FilemapEntryType::DebugSymbol);
    assert_eq!(e.binary_ref, base40_encode("myapp").unwrap());

    let e = &fm.entries[2];
    assert_eq!(e.ctfs_name, base40_encode("main.c").unwrap());
    assert_eq!(e.entry_type, FilemapEntryType::SourceFile);
    assert_eq!(e.compilation_dir, "/home/user/build");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_platform_compat_rust_roundtrip() {
    let p = build_compat_platform();
    let data = p.serialize();
    let p2 = PlatformInfo::deserialize(&data).unwrap();
    verify_platform(&p2);

    // Write Rust output if requested
    if let Ok(path) = std::env::var("METADATA_COMPAT_RUST_PLATFORM") {
        std::fs::write(&path, &data).unwrap();
        eprintln!("Wrote Rust platform ({} bytes) to {}", data.len(), path);
    }
}

#[test]
fn test_mmap_compat_rust_roundtrip() {
    let t = build_compat_mmap();
    let data = t.serialize();
    let t2 = MmapTable::deserialize(&data).unwrap();
    verify_mmap(&t2);

    // Write Rust output if requested
    if let Ok(path) = std::env::var("METADATA_COMPAT_RUST_MMAP") {
        std::fs::write(&path, &data).unwrap();
        eprintln!("Wrote Rust mmap ({} bytes) to {}", data.len(), path);
    }
}

#[test]
fn test_metadata_compat_read_nim_ct() {
    // Read a .ct file generated by Nim containing filemap.bin + platform.bin + mmap.bin
    let ct_path = match std::env::var("METADATA_COMPAT_NIM_CT") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("METADATA_COMPAT_NIM_CT not set, skipping Nim .ct compat check");
            return;
        }
    };

    eprintln!("Reading Nim .ct file: {}", ct_path);

    let mut reader = codetracer_ctfs::CtfsReader::open(std::path::Path::new(&ct_path))
        .unwrap_or_else(|e| panic!("failed to open Nim .ct file: {}", e));

    let files = reader.list_files();
    eprintln!("Files in .ct: {:?}", files);

    // Read and verify filemap.bin
    {
        let data = reader.read_file("filemap.bin")
            .unwrap_or_else(|e| panic!("failed to read filemap.bin: {}", e));
        eprintln!("filemap.bin: {} bytes", data.len());
        let fm = Filemap::deserialize(&data)
            .unwrap_or_else(|e| panic!("failed to deserialize filemap.bin: {}", e));
        verify_filemap(&fm);

        // Byte-level comparison with Rust serialization
        let rust_data = build_compat_filemap().serialize();
        assert_eq!(
            rust_data, data,
            "filemap.bin: Rust and Nim serialization differ ({} vs {} bytes)",
            rust_data.len(), data.len()
        );
        eprintln!("filemap.bin: byte-level match ({} bytes)", data.len());
    }

    // Read and verify platform.bin
    {
        let data = reader.read_file("platform.bin")
            .unwrap_or_else(|e| panic!("failed to read platform.bin: {}", e));
        eprintln!("platform.bin: {} bytes", data.len());
        let p = PlatformInfo::deserialize(&data)
            .unwrap_or_else(|e| panic!("failed to deserialize platform.bin: {}", e));
        verify_platform(&p);

        // Byte-level comparison
        let rust_data = build_compat_platform().serialize();
        assert_eq!(
            rust_data, data,
            "platform.bin: Rust and Nim serialization differ ({} vs {} bytes)",
            rust_data.len(), data.len()
        );
        eprintln!("platform.bin: byte-level match ({} bytes)", data.len());
    }

    // Read and verify mmap.bin
    {
        let data = reader.read_file("mmap.bin")
            .unwrap_or_else(|e| panic!("failed to read mmap.bin: {}", e));
        eprintln!("mmap.bin: {} bytes", data.len());
        let t = MmapTable::deserialize(&data)
            .unwrap_or_else(|e| panic!("failed to deserialize mmap.bin: {}", e));
        verify_mmap(&t);

        // Byte-level comparison
        let rust_data = build_compat_mmap().serialize();
        assert_eq!(
            rust_data, data,
            "mmap.bin: Rust and Nim serialization differ ({} vs {} bytes)",
            rust_data.len(), data.len()
        );
        eprintln!("mmap.bin: byte-level match ({} bytes)", data.len());
    }

    eprintln!("All metadata compat checks passed for Nim .ct file");
}

#[test]
fn test_metadata_compat_read_nim_raw_files() {
    // Alternative: read raw .bin files generated by Nim (not inside .ct)
    if let Ok(path) = std::env::var("METADATA_COMPAT_NIM_PLATFORM") {
        let nim_data = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path, e));
        let p = PlatformInfo::deserialize(&nim_data)
            .unwrap_or_else(|e| panic!("failed to deserialize Nim platform: {}", e));
        verify_platform(&p);

        let rust_data = build_compat_platform().serialize();
        assert_eq!(
            rust_data, nim_data,
            "platform: Rust and Nim differ ({} vs {} bytes)",
            rust_data.len(), nim_data.len()
        );
        eprintln!("Nim platform raw file: byte-level match ({} bytes)", nim_data.len());
    } else {
        eprintln!("METADATA_COMPAT_NIM_PLATFORM not set, skipping");
    }

    if let Ok(path) = std::env::var("METADATA_COMPAT_NIM_MMAP") {
        let nim_data = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path, e));
        let t = MmapTable::deserialize(&nim_data)
            .unwrap_or_else(|e| panic!("failed to deserialize Nim mmap: {}", e));
        verify_mmap(&t);

        let rust_data = build_compat_mmap().serialize();
        assert_eq!(
            rust_data, nim_data,
            "mmap: Rust and Nim differ ({} vs {} bytes)",
            rust_data.len(), nim_data.len()
        );
        eprintln!("Nim mmap raw file: byte-level match ({} bytes)", nim_data.len());
    } else {
        eprintln!("METADATA_COMPAT_NIM_MMAP not set, skipping");
    }
}
