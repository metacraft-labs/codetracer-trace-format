use crate::CtfsError;
use std::io::{Read, Write};

pub const MAGIC: [u8; 5] = [0xC0, 0xDE, 0x72, 0xAC, 0xE2];

/// The version this implementation WRITES. `ctfs-container.md` §1 states that
/// header byte 5 is `4`.
///
/// Version 4 RE-DEFINES bytes 6 and 7, which is why bumping this alone would
/// not have been enough:
///
/// * v2/v3: byte 6 = compression method, byte 7 = encryption method.
/// * v4:    byte 6 = encryption method,  byte 7 = max shard count.
///
/// The v4 header carries NO compression field, and that is deliberate rather
/// than an omission: compression in this format is a per-stream property of the
/// chunked writer, not a property of the container. `write_to` therefore
/// serialises according to the version rather than to a fixed layout, so a
/// caller that asks for Zstd cannot end up with `1` sitting in the byte a v4
/// reader interprets as AES-256-GCM.
pub const VERSION: u8 = 4;
pub const VERSION_V2: u8 = 2;
pub const VERSION_V3: u8 = 3;
pub const VERSION_V4: u8 = 4;

/// The versions this implementation READS. Writing v4 does not retire the
/// ability to open what earlier versions produced, and the two are separate
/// decisions — dropping v3 from this list is a deliberate act, not a side
/// effect of moving the writer forward.
pub const SUPPORTED_VERSIONS: [u8; 3] = [VERSION_V2, VERSION_V3, VERSION_V4];
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
    /// Byte 6 under v2/v3 only. A v4 header has no compression field —
    /// compression is a per-stream property of the chunked writer — so this is
    /// carried for reading older containers and is not serialised under v4.
    pub compression: CompressionMethod,
    pub encryption: EncryptionMethod,
    /// Byte 7 under v4. `0` means the container is not sharded, which is what
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
        // BYTES 6 AND 7 MEAN DIFFERENT THINGS PER VERSION — see `VERSION`.
        // Reading already branched here; writing did not, which is the whole
        // of why the version could not simply be bumped.
        if self.version >= VERSION_V4 {
            w.write_all(&[self.encryption as u8])?;
            w.write_all(&[self.max_shards])?;
        } else {
            w.write_all(&[self.compression as u8])?;
            w.write_all(&[self.encryption as u8])?;
        }
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
        // Accept every version in `SUPPORTED_VERSIONS`, and note that this must
        // NOT be spelled in terms of `VERSION`: that names the version written,
        // and once it moved to 4 the old `!= VERSION && != VERSION_V2 &&
        // != VERSION_V4` quietly stopped accepting v3 — a reader losing the
        // ability to open existing containers as a side effect of the writer
        // moving forward, which is a decision nobody would have taken on
        // purpose in that line.
        if !SUPPORTED_VERSIONS.contains(&ver[0]) {
            return Err(CtfsError::InvalidVersion(ver[0]));
        }
        let mut tag_bytes = [0u8; 2];
        r.read_exact(&mut tag_bytes)?;
        // V4 changed the header layout:
        //   v2/v3: byte 6 = compression, byte 7 = encryption
        //   v4:    byte 6 = encryption,  byte 7 = max_shards
        // V4 files produced by the Nim writer currently use no compression,
        // so we default to None.
        let (compression, encryption, max_shards) = if ver[0] >= VERSION_V4 {
            (CompressionMethod::None, EncryptionMethod::from_byte(tag_bytes[0]), tag_bytes[1])
        } else {
            // For v2 files, bytes 6-7 were reserved (0x00), which maps to None/None
            (CompressionMethod::from_byte(tag_bytes[0]), EncryptionMethod::from_byte(tag_bytes[1]), 0)
        };
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
