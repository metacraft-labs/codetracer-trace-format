use crate::CtfsError;
use std::io::{Read, Write};

pub const FILE_ENTRY_SIZE: usize = 24;

/// Bit 63 of `FileEntry.MapBlock`: the member is one data block, named by the
/// remaining bits, and owns no mapping block (`ctfs-container.md` §2,
/// "`MapBlock` has three forms"). No block number reaches this bit: block
/// `2^63` would begin past any offset a 64-bit file can address.
pub const CTFS_DIRECT: u64 = 1 << 63;

/// How a member's bytes are laid out, decided from `MapBlock` alone — never
/// from `Size` (`ctfs-container.md` §2, "Readers").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberLayout {
    /// `MapBlock = 0`: the member owns no block. Legitimate only with
    /// `Size = 0`; a reader refuses a non-zero size with it as a null pointer.
    Empty,
    /// `MapBlock = CTFS_DIRECT | b`: the member is the single data block `b`.
    Direct(u64),
    /// Any other `MapBlock`: the member's level-1 mapping block (§4).
    Mapped(u64),
}

impl MemberLayout {
    /// Decode a `MapBlock` field.
    pub fn from_map_block(map_block: u64) -> Self {
        if map_block == 0 {
            MemberLayout::Empty
        } else if map_block & CTFS_DIRECT != 0 {
            MemberLayout::Direct(map_block & !CTFS_DIRECT)
        } else {
            MemberLayout::Mapped(map_block)
        }
    }

    /// The `MapBlock` field that stores this layout.
    pub fn map_block(self) -> u64 {
        match self {
            MemberLayout::Empty => 0,
            MemberLayout::Direct(b) => CTFS_DIRECT | b,
            MemberLayout::Mapped(m) => m,
        }
    }
}

/// A file entry in the root block (24 bytes on disk).
#[derive(Debug, Clone, Copy)]
pub struct FileEntry {
    /// File size in bytes.
    pub size: u64,
    /// `0` for an empty member, `CTFS_DIRECT | b` for a member that is the
    /// single data block `b`, otherwise the member's level-1 mapping block.
    /// See [`MemberLayout`].
    pub map_block: u64,
    /// Base40-encoded filename.
    pub name: u64,
}

impl FileEntry {
    pub fn empty() -> Self {
        FileEntry {
            size: 0,
            map_block: 0,
            name: 0,
        }
    }

    /// The member's layout, read from `map_block`.
    pub fn layout(&self) -> MemberLayout {
        MemberLayout::from_map_block(self.map_block)
    }

    /// Whether this root-directory slot is unused (all 24 bytes zero). A
    /// created member that was never written is `(0, 0, name)` and is not an
    /// unused slot.
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
