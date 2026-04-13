use std::io::{Read, Write};
use crate::CtfsError;

pub const MAGIC: [u8; 5] = [0xC0, 0xDE, 0x72, 0xAC, 0xE2];
pub const VERSION: u8 = 3;
pub const VERSION_V2: u8 = 2;
pub const HEADER_SIZE: usize = 8;
pub const EXTENDED_HEADER_SIZE: usize = 8;

/// Compression method stored in header byte 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionMethod {
    None = 0,
    Zstd = 1,
    Lz4 = 2,
}

impl CompressionMethod {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => CompressionMethod::None,
            1 => CompressionMethod::Zstd,
            2 => CompressionMethod::Lz4,
            _ => CompressionMethod::None, // Unknown, treat as none
        }
    }
}

/// Encryption method stored in header byte 7.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EncryptionMethod {
    None = 0,
    Aes256Gcm = 1,
}

impl EncryptionMethod {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => EncryptionMethod::None,
            1 => EncryptionMethod::Aes256Gcm,
            _ => EncryptionMethod::None,
        }
    }
}

/// Chunk index entry for chunked compressed streams.
/// Stored at the end of each CTFS internal file that uses chunked compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkIndexEntry {
    /// Byte offset of this chunk within the stream.
    pub compressed_offset: u64,
    /// Number of events in this chunk.
    pub event_count: u32,
    /// GEID of the first event in this chunk.
    pub first_geid: u64,
}

/// Size of a serialized ChunkIndexEntry: 8 + 4 + 8 = 20 bytes.
pub const CHUNK_INDEX_ENTRY_SIZE: usize = 20;

/// Default number of events per chunk.
pub const DEFAULT_CHUNK_SIZE: usize = 4096;

/// The 8-byte root block header.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub id: [u8; 5],
    pub version: u8,
    pub compression: CompressionMethod,
    pub encryption: EncryptionMethod,
}

impl Header {
    pub fn new() -> Self {
        Header {
            id: MAGIC,
            version: VERSION,
            compression: CompressionMethod::None,
            encryption: EncryptionMethod::None,
        }
    }

    /// Create a new header with the specified compression method.
    pub fn with_compression(compression: CompressionMethod) -> Self {
        Header {
            id: MAGIC,
            version: VERSION,
            compression,
            encryption: EncryptionMethod::None,
        }
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<(), CtfsError> {
        w.write_all(&self.id)?;
        w.write_all(&[self.version])?;
        w.write_all(&[self.compression as u8])?;
        w.write_all(&[self.encryption as u8])?;
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
        // Accept both v2 and v3
        if ver[0] != VERSION && ver[0] != VERSION_V2 {
            return Err(CtfsError::InvalidVersion(ver[0]));
        }
        let mut tag_bytes = [0u8; 2];
        r.read_exact(&mut tag_bytes)?;
        // For v2 files, bytes 6-7 were reserved (0x00), which maps to None/None
        let compression = CompressionMethod::from_byte(tag_bytes[0]);
        let encryption = EncryptionMethod::from_byte(tag_bytes[1]);
        Ok(Header {
            id,
            version: ver[0],
            compression,
            encryption,
        })
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
