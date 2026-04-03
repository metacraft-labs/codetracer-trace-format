use std::io::{Read, Write};
use crate::CtfsError;

pub const FILE_ENTRY_SIZE: usize = 24;

/// A file entry in the root block (24 bytes on disk).
#[derive(Debug, Clone, Copy)]
pub struct FileEntry {
    /// File size in bytes.
    pub size: u64,
    /// Root allocation/mapping block number (0 = empty/unused entry).
    pub map_block: u64,
    /// Base40-encoded filename.
    pub name: u64,
}

impl FileEntry {
    pub fn empty() -> Self {
        FileEntry { size: 0, map_block: 0, name: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.name == 0 && self.map_block == 0 && self.size == 0
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<(), CtfsError> {
        w.write_all(&self.size.to_le_bytes())?;
        w.write_all(&self.map_block.to_le_bytes())?;
        w.write_all(&self.name.to_le_bytes())?;
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> Result<Self, CtfsError> {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;
        let size = u64::from_le_bytes(buf);
        r.read_exact(&mut buf)?;
        let map_block = u64::from_le_bytes(buf);
        r.read_exact(&mut buf)?;
        let name = u64::from_le_bytes(buf);
        Ok(FileEntry { size, map_block, name })
    }
}
