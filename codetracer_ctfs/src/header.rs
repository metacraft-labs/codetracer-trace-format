use crate::CtfsError;
use std::io::{Read, Write};

pub const MAGIC: [u8; 5] = [0xC0, 0xDE, 0x72, 0xAC, 0xE2];

/// The one container version this implementation writes and reads
/// (`ctfs-container.md` §1 and §2, "Older versions are refused").
///
/// There is no compatibility path. Version 5 changed what `FileEntry.MapBlock`
/// means (`crate::file_entry::MemberLayout`), and nothing in the bytes of an
/// older container says which meaning it was written under, so a reader that
/// accepted one would either read a small member's mapping block as its
/// content or keep every writer of the old layout alive. A container of any
/// other version is refused by [`Header::read_from`], naming the version.
///
/// Bytes 6 and 7 are the encryption method and the maximum shard count, as
/// they were in version 4. The header carries no compression field:
/// compression is a property of each member's format, not of the container.
pub const VERSION: u8 = 5;
pub const HEADER_SIZE: usize = 8;
pub const EXTENDED_HEADER_SIZE: usize = 8;

/// A compression method a writer applies in its chunked helpers. Not stored in
/// the container header.
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

/// Encryption method stored in header byte 6.
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

/// Inline chunk header for chunked compressed streams.
/// Written before each compressed chunk in the stream:
///   [ChunkHeader: 16 bytes][compressed data: compressed_size bytes]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkIndexEntry {
    /// Size of the compressed data following this header.
    pub compressed_size: u32,
    /// Number of events in this chunk.
    pub event_count: u32,
    /// GEID of the first event in this chunk.
    pub first_geid: u64,
}

/// Size of a serialized ChunkIndexEntry (inline header): 4 + 4 + 8 = 16 bytes.
pub const CHUNK_INDEX_ENTRY_SIZE: usize = 16;

/// Default number of events per chunk.
pub const DEFAULT_CHUNK_SIZE: usize = 4096;

/// The 8-byte root block header.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub id: [u8; 5],
    pub version: u8,
    /// Not serialised: the header has no compression field (see [`VERSION`]).
    /// A writer keeps the method it was created with for its own chunked
    /// helpers; a header read from a container always reports `None`.
    pub compression: CompressionMethod,
    pub encryption: EncryptionMethod,
    /// Byte 7. `0` means the container is not sharded, which is what
    /// this implementation produces.
    pub max_shards: u8,
}

impl Default for Header {
    fn default() -> Self {
        Self::new()
    }
}

impl Header {
    pub fn new() -> Self {
        Header {
            id: MAGIC,
            version: VERSION,
            compression: CompressionMethod::None,
            encryption: EncryptionMethod::None,
            max_shards: 0,
        }
    }

    /// Create a new header with the specified compression method.
    pub fn with_compression(compression: CompressionMethod) -> Self {
        Header {
            id: MAGIC,
            version: VERSION,
            compression,
            encryption: EncryptionMethod::None,
            max_shards: 0,
        }
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<(), CtfsError> {
        w.write_all(&self.id)?;
        w.write_all(&[self.version])?;
        w.write_all(&[self.encryption as u8])?;
        w.write_all(&[self.max_shards])?;
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
        let mut tag_bytes = [0u8; 2];
        r.read_exact(&mut tag_bytes)?;
        let (compression, encryption, max_shards) = (CompressionMethod::None, EncryptionMethod::from_byte(tag_bytes[0]), tag_bytes[1]);
        Ok(Header {
            id,
            version: ver[0],
            compression,
            encryption,
            max_shards,
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
        Ok(ExtendedHeader {
            block_size,
            max_root_entries,
        })
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
        Ok(ExtendedHeader {
            block_size,
            max_root_entries,
        })
    }
}
