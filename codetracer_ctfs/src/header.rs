use std::io::{Read, Write};
use crate::CtfsError;

pub const MAGIC: [u8; 5] = [0xC0, 0xDE, 0x72, 0xAC, 0xE2];
pub const VERSION: u8 = 2;
pub const HEADER_SIZE: usize = 8;
pub const EXTENDED_HEADER_SIZE: usize = 8;

/// The 8-byte root block header.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub id: [u8; 5],
    pub version: u8,
    pub reserved: [u8; 2],
}

impl Header {
    pub fn new() -> Self {
        Header {
            id: MAGIC,
            version: VERSION,
            reserved: [0; 2],
        }
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<(), CtfsError> {
        w.write_all(&self.id)?;
        w.write_all(&[self.version])?;
        w.write_all(&self.reserved)?;
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> Result<Self, CtfsError> {
        let mut id = [0u8; 5];
        r.read_exact(&mut id)?;
        if id != MAGIC {
            return Err(CtfsError::InvalidMagic);
        }
        let mut ver = [0u8; 1];
        r.read_exact(&mut ver)?;
        if ver[0] != VERSION {
            return Err(CtfsError::InvalidVersion(ver[0]));
        }
        let mut reserved = [0u8; 2];
        r.read_exact(&mut reserved)?;
        Ok(Header { id, version: ver[0], reserved })
    }
}

/// The 8-byte extended header.
#[derive(Debug, Clone, Copy)]
pub struct ExtendedHeader {
    pub block_size: u32,
    pub max_root_entries: u32,
}

impl ExtendedHeader {
    pub fn new(block_size: u32, max_root_entries: u32) -> Result<Self, CtfsError> {
        if block_size != 1024 && block_size != 2048 && block_size != 4096 {
            return Err(CtfsError::InvalidBlockSize(block_size));
        }
        Ok(ExtendedHeader { block_size, max_root_entries })
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<(), CtfsError> {
        w.write_all(&self.block_size.to_le_bytes())?;
        w.write_all(&self.max_root_entries.to_le_bytes())?;
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> Result<Self, CtfsError> {
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf)?;
        let block_size = u32::from_le_bytes(buf);
        if block_size != 1024 && block_size != 2048 && block_size != 4096 {
            return Err(CtfsError::InvalidBlockSize(block_size));
        }
        r.read_exact(&mut buf)?;
        let max_root_entries = u32::from_le_bytes(buf);
        Ok(ExtendedHeader { block_size, max_root_entries })
    }
}
