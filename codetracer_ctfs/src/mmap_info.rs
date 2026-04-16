/// mmap.bin — binary memory mapping table format for CTFS portable traces.
///
/// Describes every memory-mapped region in the recorded process at a given
/// point in time. Each entry is fixed-size (33 bytes) for O(1) indexing.
///
/// Wire format uses little-endian fixed-width integers, consistent with
/// the rest of the CTFS format.

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct MmapEntry {
    pub address: u64,
    pub size: u64,
    pub binary_ref: u64,
    pub file_offset: u64,
    pub permissions: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MmapTable {
    pub entries: Vec<MmapEntry>,
}

const MMAP_MAGIC: [u8; 4] = [0x4D, 0x4D, 0x41, 0x50]; // "MMAP"

/// Each entry is exactly 33 bytes: 8+8+8+8+1
pub const MMAP_ENTRY_SIZE: usize = 33;

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

impl MmapTable {
    /// Serialize an MmapTable to binary.
    ///
    /// Format: "MMAP" (4) + entry_count(u32 LE) +
    ///         entries (33 bytes each): address(u64) + size(u64) +
    ///         binaryRef(u64) + fileOffset(u64) + permissions(u8)
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.entries.len() * MMAP_ENTRY_SIZE);

        // Magic
        buf.extend_from_slice(&MMAP_MAGIC);

        // Entry count (u32 LE)
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        // Entries
        for entry in &self.entries {
            buf.extend_from_slice(&entry.address.to_le_bytes());
            buf.extend_from_slice(&entry.size.to_le_bytes());
            buf.extend_from_slice(&entry.binary_ref.to_le_bytes());
            buf.extend_from_slice(&entry.file_offset.to_le_bytes());
            buf.push(entry.permissions);
        }

        buf
    }

    /// Deserialize an MmapTable from binary. Validates magic.
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        let min_size = 4 + 4; // magic + entry_count
        if data.len() < min_size {
            return Err("mmap data too short for header".to_string());
        }

        // Validate magic
        if data[0..4] != MMAP_MAGIC {
            return Err("invalid mmap magic".to_string());
        }

        let entry_count =
            u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;

        // Validate data length
        let expected_size = 8 + entry_count * MMAP_ENTRY_SIZE;
        if data.len() < expected_size {
            return Err(format!(
                "truncated mmap: expected {} bytes, got {}",
                expected_size,
                data.len()
            ));
        }

        let mut entries = Vec::with_capacity(entry_count);
        let mut pos = 8;

        for _ in 0..entry_count {
            let address = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let binary_ref = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let file_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let permissions = data[pos];
            pos += 1;

            entries.push(MmapEntry {
                address,
                size,
                binary_ref,
                file_offset,
                permissions,
            });
        }

        Ok(MmapTable { entries })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mmap_roundtrip_5_entries() {
        let mut entries = Vec::new();
        for i in 0..5u64 {
            entries.push(MmapEntry {
                address: 0x400000 + i * 0x1000,
                size: 0x1000,
                binary_ref: if i < 3 { 100 + i } else { 0 },
                file_offset: i * 0x1000,
                permissions: if i == 2 { 0x05 } else { 0x03 },
            });
        }

        let t = MmapTable { entries };
        let data = t.serialize();

        let t2 = MmapTable::deserialize(&data).unwrap();
        assert_eq!(t2.entries.len(), 5);

        for i in 0..5usize {
            assert_eq!(t2.entries[i].address, 0x400000 + i as u64 * 0x1000);
            assert_eq!(t2.entries[i].size, 0x1000);
            assert_eq!(
                t2.entries[i].binary_ref,
                if i < 3 { 100 + i as u64 } else { 0 }
            );
            assert_eq!(t2.entries[i].file_offset, i as u64 * 0x1000);
            assert_eq!(
                t2.entries[i].permissions,
                if i == 2 { 0x05 } else { 0x03 }
            );
        }
    }

    #[test]
    fn test_mmap_roundtrip_100_entries() {
        let mut entries = Vec::new();
        for i in 0..100u64 {
            entries.push(MmapEntry {
                address: 0x10000 + i * 0x1000,
                size: 0x1000,
                binary_ref: i % 10,
                file_offset: i * 0x1000,
                permissions: (i % 16) as u8,
            });
        }

        let t = MmapTable { entries };
        let data = t.serialize();
        assert_eq!(data.len(), 8 + 100 * MMAP_ENTRY_SIZE);

        let t2 = MmapTable::deserialize(&data).unwrap();
        assert_eq!(t2.entries.len(), 100);

        for i in 0..100usize {
            assert_eq!(t2.entries[i].address, 0x10000 + i as u64 * 0x1000);
            assert_eq!(t2.entries[i].size, 0x1000);
            assert_eq!(t2.entries[i].binary_ref, i as u64 % 10);
            assert_eq!(t2.entries[i].file_offset, i as u64 * 0x1000);
            assert_eq!(t2.entries[i].permissions, (i % 16) as u8);
        }
    }

    #[test]
    fn test_mmap_empty() {
        let t = MmapTable {
            entries: vec![],
        };
        let data = t.serialize();
        assert_eq!(data.len(), 8, "empty mmap should be 8 bytes (header only)");

        let t2 = MmapTable::deserialize(&data).unwrap();
        assert_eq!(t2.entries.len(), 0);
    }

    #[test]
    fn test_mmap_fixed_entry_size() {
        let one = MmapTable {
            entries: vec![MmapEntry {
                address: 0x1000,
                size: 0x2000,
                binary_ref: 42,
                file_offset: 0,
                permissions: 0x07,
            }],
        };
        let data = one.serialize();
        // Header: 4 (magic) + 4 (count) = 8 bytes
        // Entry: 33 bytes
        assert_eq!(data.len(), 8 + 33);
        assert_eq!(MMAP_ENTRY_SIZE, 33);

        let two = MmapTable {
            entries: vec![
                MmapEntry {
                    address: 0x1000,
                    size: 0x2000,
                    binary_ref: 42,
                    file_offset: 0,
                    permissions: 0x07,
                },
                MmapEntry {
                    address: 0x3000,
                    size: 0x1000,
                    binary_ref: 43,
                    file_offset: 0x2000,
                    permissions: 0x05,
                },
            ],
        };
        let data2 = two.serialize();
        assert_eq!(data2.len(), 8 + 2 * 33);
    }

    #[test]
    fn test_mmap_invalid_magic() {
        let mut data = MmapTable { entries: vec![] }.serialize();
        data[0] = b'X';
        data[2] = b'Z';
        let res = MmapTable::deserialize(&data);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("invalid mmap magic"));
    }

    #[test]
    fn test_mmap_truncated() {
        // Too short for header
        let res = MmapTable::deserialize(&[0x4D, 0x4D]);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("too short"));

        // Valid header claiming 1 entry but no entry data
        let mut data = vec![0x4D, 0x4D, 0x41, 0x50]; // MMAP magic
        data.extend_from_slice(&1u32.to_le_bytes()); // 1 entry
        let res = MmapTable::deserialize(&data);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("truncated"));
    }

    /// Build the canonical compat mmap table used by both Nim and Rust compat tests.
    pub(crate) fn build_compat_mmap() -> MmapTable {
        use crate::base40_encode;

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
}
