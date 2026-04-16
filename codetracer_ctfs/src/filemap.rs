/// Filemap binary format for portable CTFS traces.
/// Maps short CTFS names to real filesystem paths.
///
/// Wire format uses LEB128 varints for string lengths and little-endian
/// fixed-width integers, consistent with the rest of the CTFS format.

/// LEB128 varint encoding (same as DWARF/protobuf).
/// Each byte uses 7 data bits + 1 continuation bit (high bit set means more
/// bytes follow).
pub fn encode_leb128(value: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut v = value;
    loop {
        let mut b = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if v == 0 {
            break;
        }
    }
    buf
}

/// Decode LEB128 varint starting at `offset` in `data`.
/// Returns `(value, bytes_consumed)` on success.
pub fn decode_leb128(data: &[u8], offset: usize) -> Result<(u64, usize), String> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut pos = offset;
    while pos < data.len() {
        let b = data[pos];
        value |= ((b & 0x7F) as u64) << shift;
        pos += 1;
        if (b & 0x80) == 0 {
            return Ok((value, pos - offset));
        }
        shift += 7;
        if shift >= 64 {
            return Err("LEB128 overflow".to_string());
        }
    }
    Err("truncated LEB128".to_string())
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FilemapEntryType {
    Binary = 0,
    DebugSymbol = 1,
    SourceFile = 2,
}

impl FilemapEntryType {
    fn from_u8(v: u8) -> Result<Self, String> {
        match v {
            0 => Ok(FilemapEntryType::Binary),
            1 => Ok(FilemapEntryType::DebugSymbol),
            2 => Ok(FilemapEntryType::SourceFile),
            _ => Err(format!("unknown filemap entry type: {}", v)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilemapEntry {
    /// Base40-encoded CTFS filename
    pub ctfs_name: u64,
    pub entry_type: FilemapEntryType,
    /// bit 0: is_main_executable, bit 1: is_dynamic_linker
    pub flags: u8,
    /// ELF build-id (typically 20 bytes, or empty)
    pub build_id: Vec<u8>,
    /// UTF-8 filesystem path
    pub real_path: String,
    /// For DebugSymbol: CTFS name of the parent binary
    pub binary_ref: u64,
    /// For SourceFile: compilation directory
    pub compilation_dir: String,
}

impl Default for FilemapEntry {
    fn default() -> Self {
        FilemapEntry {
            ctfs_name: 0,
            entry_type: FilemapEntryType::Binary,
            flags: 0,
            build_id: Vec::new(),
            real_path: String::new(),
            binary_ref: 0,
            compilation_dir: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filemap {
    pub version: u16,
    pub entries: Vec<FilemapEntry>,
}

const FILEMAP_MAGIC: [u8; 4] = [0x46, 0x4D, 0x41, 0x50]; // "FMAP"

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

impl Filemap {
    /// Serialize a Filemap to binary.
    ///
    /// Header: "FMAP" (4) + version (u16 LE) + entry_count (u16 LE)
    /// Entries: ctfs_name (u64 LE) + entry_type (u8) + flags (u8) +
    ///          build_id_len (u8) + build_id bytes +
    ///          path_len (varint) + path bytes +
    ///          [for debug: binary_ref (u64 LE)] +
    ///          [for source: comp_dir_len (varint) + comp_dir bytes]
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Magic
        buf.extend_from_slice(&FILEMAP_MAGIC);

        // Version (u16 LE)
        buf.extend_from_slice(&self.version.to_le_bytes());

        // Entry count (u16 LE)
        buf.extend_from_slice(&(self.entries.len() as u16).to_le_bytes());

        for entry in &self.entries {
            // ctfs_name (u64 LE)
            buf.extend_from_slice(&entry.ctfs_name.to_le_bytes());

            // entry_type (u8)
            buf.push(entry.entry_type as u8);

            // flags (u8)
            buf.push(entry.flags);

            // build_id_len (u8) + build_id bytes
            buf.push(entry.build_id.len() as u8);
            buf.extend_from_slice(&entry.build_id);

            // path_len (varint) + path bytes
            buf.extend_from_slice(&encode_leb128(entry.real_path.len() as u64));
            buf.extend_from_slice(entry.real_path.as_bytes());

            // Type-specific fields
            match entry.entry_type {
                FilemapEntryType::DebugSymbol => {
                    buf.extend_from_slice(&entry.binary_ref.to_le_bytes());
                }
                FilemapEntryType::SourceFile => {
                    buf.extend_from_slice(
                        &encode_leb128(entry.compilation_dir.len() as u64),
                    );
                    buf.extend_from_slice(entry.compilation_dir.as_bytes());
                }
                FilemapEntryType::Binary => {}
            }
        }

        buf
    }

    /// Deserialize a Filemap from binary. Validates magic and version.
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        if data.len() < 8 {
            return Err("filemap data too short for header".to_string());
        }

        // Validate magic
        if data[0..4] != FILEMAP_MAGIC {
            return Err("invalid filemap magic".to_string());
        }

        let version = u16::from_le_bytes([data[4], data[5]]);
        if version == 0 || version > 1 {
            return Err(format!("unsupported filemap version: {}", version));
        }

        let entry_count = u16::from_le_bytes([data[6], data[7]]) as usize;

        let mut entries = Vec::with_capacity(entry_count);
        let mut pos = 8;

        for _ in 0..entry_count {
            let mut entry = FilemapEntry::default();

            // ctfs_name (u64 LE)
            if pos + 8 > data.len() {
                return Err(format!(
                    "truncated filemap: expected ctfs_name at offset {}",
                    pos
                ));
            }
            entry.ctfs_name =
                u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            // entry_type (u8)
            if pos + 1 > data.len() {
                return Err("truncated filemap: expected entry_type".to_string());
            }
            entry.entry_type = FilemapEntryType::from_u8(data[pos])?;
            pos += 1;

            // flags (u8)
            if pos + 1 > data.len() {
                return Err("truncated filemap: expected flags".to_string());
            }
            entry.flags = data[pos];
            pos += 1;

            // build_id_len (u8) + build_id
            if pos + 1 > data.len() {
                return Err("truncated filemap: expected build_id_len".to_string());
            }
            let bid_len = data[pos] as usize;
            pos += 1;
            if pos + bid_len > data.len() {
                return Err("truncated filemap: expected build_id bytes".to_string());
            }
            entry.build_id = data[pos..pos + bid_len].to_vec();
            pos += bid_len;

            // path_len (varint) + path
            let (path_len, var_bytes) = decode_leb128(data, pos)
                .map_err(|e| format!("truncated filemap: invalid path_len varint: {}", e))?;
            pos += var_bytes;
            let path_len = path_len as usize;
            if pos + path_len > data.len() {
                return Err("truncated filemap: expected path bytes".to_string());
            }
            entry.real_path = String::from_utf8(data[pos..pos + path_len].to_vec())
                .map_err(|e| format!("invalid UTF-8 in path: {}", e))?;
            pos += path_len;

            // Type-specific fields
            match entry.entry_type {
                FilemapEntryType::DebugSymbol => {
                    if pos + 8 > data.len() {
                        return Err(
                            "truncated filemap: expected binary_ref".to_string(),
                        );
                    }
                    entry.binary_ref =
                        u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                }
                FilemapEntryType::SourceFile => {
                    let (cd_len, cd_var_bytes) = decode_leb128(data, pos)
                        .map_err(|e| {
                            format!(
                                "truncated filemap: invalid comp_dir_len varint: {}",
                                e
                            )
                        })?;
                    pos += cd_var_bytes;
                    let cd_len = cd_len as usize;
                    if pos + cd_len > data.len() {
                        return Err(
                            "truncated filemap: expected comp_dir bytes".to_string(),
                        );
                    }
                    entry.compilation_dir =
                        String::from_utf8(data[pos..pos + cd_len].to_vec())
                            .map_err(|e| {
                                format!("invalid UTF-8 in compilation_dir: {}", e)
                            })?;
                    pos += cd_len;
                }
                FilemapEntryType::Binary => {}
            }

            entries.push(entry);
        }

        Ok(Filemap { version, entries })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base40_encode;

    #[test]
    fn test_leb128_roundtrip() {
        let values: &[u64] = &[0, 1, 127, 128, 255, 16383, 16384];
        for &v in values {
            let encoded = encode_leb128(v);
            let (decoded, consumed) = decode_leb128(&encoded, 0).unwrap();
            assert_eq!(decoded, v, "LEB128 roundtrip failed for {}", v);
            assert!(consumed > 0);

            // Verify encoding size properties
            if v <= 127 {
                assert_eq!(consumed, 1, "values <= 127 should encode in 1 byte");
            } else if v <= 16383 {
                assert_eq!(consumed, 2, "values <= 16383 should encode in 2 bytes");
            }
        }

        // Also test large values
        let large_values: &[u64] = &[u32::MAX as u64, 1u64 << 63];
        for &v in large_values {
            let encoded = encode_leb128(v);
            let (decoded, _) = decode_leb128(&encoded, 0).unwrap();
            assert_eq!(decoded, v, "LEB128 roundtrip failed for {}", v);
        }
    }

    #[test]
    fn test_filemap_empty_roundtrip() {
        let fm = Filemap {
            version: 1,
            entries: vec![],
        };
        let data = fm.serialize();
        assert_eq!(data.len(), 8, "empty filemap should be 8 bytes (header only)");

        let fm2 = Filemap::deserialize(&data).unwrap();
        assert_eq!(fm2.version, 1);
        assert_eq!(fm2.entries.len(), 0);
    }

    #[test]
    fn test_filemap_binary_entry() {
        let build_id: Vec<u8> = (0..20).map(|i| ((i * 13 + 7) % 256) as u8).collect();

        let entry = FilemapEntry {
            ctfs_name: base40_encode("test.bin").unwrap(),
            entry_type: FilemapEntryType::Binary,
            flags: 0x01, // is_main_executable
            build_id: build_id.clone(),
            real_path: "/usr/bin/test".to_string(),
            ..Default::default()
        };

        let fm = Filemap {
            version: 1,
            entries: vec![entry],
        };
        let data = fm.serialize();

        let fm2 = Filemap::deserialize(&data).unwrap();
        assert_eq!(fm2.entries.len(), 1);
        let e = &fm2.entries[0];
        assert_eq!(e.ctfs_name, base40_encode("test.bin").unwrap());
        assert_eq!(e.entry_type, FilemapEntryType::Binary);
        assert_eq!(e.flags, 0x01);
        assert_eq!(e.build_id.len(), 20);
        assert_eq!(e.build_id, build_id);
        assert_eq!(e.real_path, "/usr/bin/test");
    }

    #[test]
    fn test_filemap_mixed_entries() {
        let mut entries = Vec::new();

        // 3 binary entries
        for i in 0..3u32 {
            let bid: Vec<u8> = (0..20).map(|j| ((i * 20 + j) % 256) as u8).collect();
            entries.push(FilemapEntry {
                ctfs_name: base40_encode(&format!("bin{}", i)).unwrap(),
                entry_type: FilemapEntryType::Binary,
                flags: if i == 0 { 0x01 } else { 0x00 },
                build_id: bid,
                real_path: format!("/usr/bin/program{}", i),
                ..Default::default()
            });
        }

        // 2 debug entries
        for i in 0..2u32 {
            entries.push(FilemapEntry {
                ctfs_name: base40_encode(&format!("dbg{}", i)).unwrap(),
                entry_type: FilemapEntryType::DebugSymbol,
                flags: 0,
                build_id: vec![0xDE, 0xAD],
                real_path: format!("/usr/lib/debug/prog{}.debug", i),
                binary_ref: base40_encode(&format!("bin{}", i)).unwrap(),
                ..Default::default()
            });
        }

        // 5 source entries
        for i in 0..5u32 {
            entries.push(FilemapEntry {
                ctfs_name: base40_encode(&format!("src{}.c", i)).unwrap(),
                entry_type: FilemapEntryType::SourceFile,
                flags: 0,
                build_id: vec![],
                real_path: format!("/home/user/src/file{}.c", i),
                compilation_dir: "/home/user/build".to_string(),
                ..Default::default()
            });
        }

        let fm = Filemap {
            version: 1,
            entries,
        };
        let data = fm.serialize();

        let fm2 = Filemap::deserialize(&data).unwrap();
        assert_eq!(fm2.entries.len(), 10);

        // Verify binary entries
        for i in 0..3 {
            assert_eq!(fm2.entries[i].entry_type, FilemapEntryType::Binary);
            assert_eq!(fm2.entries[i].real_path, format!("/usr/bin/program{}", i));
            assert_eq!(fm2.entries[i].build_id.len(), 20);
        }

        // Verify debug entries
        for i in 0..2 {
            let e = &fm2.entries[3 + i];
            assert_eq!(e.entry_type, FilemapEntryType::DebugSymbol);
            assert_eq!(
                e.binary_ref,
                base40_encode(&format!("bin{}", i)).unwrap()
            );
        }

        // Verify source entries
        for i in 0..5 {
            let e = &fm2.entries[5 + i];
            assert_eq!(e.entry_type, FilemapEntryType::SourceFile);
            assert_eq!(e.compilation_dir, "/home/user/build");
            assert_eq!(e.real_path, format!("/home/user/src/file{}.c", i));
        }
    }

    #[test]
    fn test_filemap_long_path() {
        let long_path: String = (0..4096)
            .map(|i| (b'a' + (i % 26) as u8) as char)
            .collect();

        let entry = FilemapEntry {
            ctfs_name: base40_encode("longfile").unwrap(),
            entry_type: FilemapEntryType::Binary,
            flags: 0,
            build_id: vec![],
            real_path: long_path.clone(),
            ..Default::default()
        };

        let fm = Filemap {
            version: 1,
            entries: vec![entry],
        };
        let data = fm.serialize();

        let fm2 = Filemap::deserialize(&data).unwrap();
        assert_eq!(fm2.entries.len(), 1);
        assert_eq!(fm2.entries[0].real_path.len(), 4096);
        assert_eq!(fm2.entries[0].real_path, long_path);
    }

    #[test]
    fn test_filemap_invalid_magic() {
        let data = vec![0x58, 0x59, 0x5A, 0x57, 0x01, 0x00, 0x00, 0x00];
        let res = Filemap::deserialize(&data);
        assert!(res.is_err(), "should fail with invalid magic");
        assert!(res.unwrap_err().contains("invalid filemap magic"));
    }

    #[test]
    fn test_filemap_truncated() {
        // Valid header claiming 1 entry, but no entry data
        let data = vec![0x46, 0x4D, 0x41, 0x50, 0x01, 0x00, 0x01, 0x00];
        let res = Filemap::deserialize(&data);
        assert!(res.is_err(), "should fail with truncated data");

        // Too short for header
        let res2 = Filemap::deserialize(&[0x46, 0x4D]);
        assert!(res2.is_err(), "should fail with data too short");
    }

    /// Test that Rust serialization produces identical bytes to what the Nim
    /// implementation would produce for the same logical filemap. This test
    /// uses a golden reference file if available (written by Nim), otherwise
    /// just verifies the Rust roundtrip.
    #[test]
    fn test_filemap_cross_compat_roundtrip() {
        // Build the canonical compat filemap (same entries used by the Nim
        // compat test)
        let fm = build_compat_filemap();
        let data = fm.serialize();

        // Verify Rust can round-trip its own output
        let fm2 = Filemap::deserialize(&data).unwrap();
        assert_eq!(fm, fm2, "Rust roundtrip mismatch");

        // If there is a Nim-generated file, verify byte-level compatibility
        let nim_path = std::env::var("FILEMAP_COMPAT_NIM_FILE").ok();
        if let Some(path) = nim_path {
            let nim_data = std::fs::read(&path).unwrap_or_else(|e| {
                panic!("failed to read Nim compat file {}: {}", path, e);
            });
            assert_eq!(
                data, nim_data,
                "Rust and Nim serialization produce different bytes"
            );
            // Also verify Rust can deserialize the Nim bytes
            let fm_from_nim = Filemap::deserialize(&nim_data).unwrap();
            assert_eq!(fm, fm_from_nim, "Nim bytes deserialized differently in Rust");
        }
    }

    /// Build the canonical filemap used by both Nim and Rust compat tests.
    /// This function must produce identical logical entries to the Nim
    /// `buildCompatFilemap` function so the serialized bytes match.
    pub(crate) fn build_compat_filemap() -> Filemap {
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
}
