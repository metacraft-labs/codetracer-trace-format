use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::base40::base40_decode;
use crate::file_entry::FileEntry;
use crate::header::{ExtendedHeader, Header};
use crate::CtfsError;

/// Reader for CTFS containers.
pub struct CtfsReader {
    file: File,
    block_size: u32,
    entries: Vec<FileEntry>,
}

/// Compute the capacity of a single level in the chain.
/// Level 1: usable data blocks (direct pointers)
/// Level 2: usable^2 data blocks (via usable level-1 sub-blocks)
/// Level k: usable^k
fn level_capacity(usable: u64, level: u32) -> u64 {
    usable.saturating_pow(level)
}

impl CtfsReader {
    /// Open an existing CTFS container.
    pub fn open(path: &Path) -> Result<Self, CtfsError> {
        let mut file = File::open(path)?;

        let _header = Header::read_from(&mut file)?;
        let ext_header = ExtendedHeader::read_from(&mut file)?;

        let mut entries = Vec::new();
        for _ in 0..ext_header.max_root_entries {
            let entry = FileEntry::read_from(&mut file)?;
            entries.push(entry);
        }

        Ok(CtfsReader {
            file,
            block_size: ext_header.block_size,
            entries,
        })
    }

    /// Get the block size of this container.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Get the maximum number of root entries (files) this container supports.
    pub fn max_entries(&self) -> u32 {
        self.entries.len() as u32
    }

    /// List all file names in the container.
    pub fn list_files(&self) -> Vec<String> {
        self.entries
            .iter()
            .filter(|e| !e.is_empty())
            .map(|e| base40_decode(e.name))
            .collect()
    }

    /// Get the size of a named file, or None if not found.
    pub fn file_size(&self, name: &str) -> Option<u64> {
        self.find_entry(name).map(|e| e.size)
    }

    /// Read an entire file's contents.
    pub fn read_file(&mut self, name: &str) -> Result<Vec<u8>, CtfsError> {
        let entry = *self.find_entry(name)
            .ok_or_else(|| CtfsError::FileNotFound(name.to_string()))?;

        if entry.size == 0 {
            return Ok(Vec::new());
        }

        let mut data = Vec::with_capacity(entry.size as usize);
        let bs = self.block_size as u64;
        let num_blocks = (entry.size + bs - 1) / bs;

        for block_idx in 0..num_blocks {
            let data_block = self.resolve_block(&entry, block_idx)?;
            let block_offset = data_block * bs;
            self.file.seek(SeekFrom::Start(block_offset))?;

            let remaining = entry.size as usize - data.len();
            let to_read = remaining.min(bs as usize);
            let mut buf = vec![0u8; to_read];
            self.file.read_exact(&mut buf)?;
            data.extend_from_slice(&buf);
        }

        Ok(data)
    }

    /// Read from an arbitrary position within a file.
    ///
    /// Returns the number of bytes actually read (may be less than buf.len()
    /// if the read extends past the end of the file).
    pub fn read_at(&mut self, name: &str, offset: u64, buf: &mut [u8]) -> Result<usize, CtfsError> {
        let entry = *self.find_entry(name)
            .ok_or_else(|| CtfsError::FileNotFound(name.to_string()))?;

        if offset >= entry.size {
            return Ok(0);
        }

        let bs = self.block_size as u64;
        let available = (entry.size - offset) as usize;
        let to_read = buf.len().min(available);
        let mut bytes_read = 0;

        while bytes_read < to_read {
            let current_offset = offset + bytes_read as u64;
            let block_idx = current_offset / bs;
            let offset_in_block = (current_offset % bs) as usize;

            let data_block = self.resolve_block(&entry, block_idx)?;
            let block_offset = data_block * bs + offset_in_block as u64;
            self.file.seek(SeekFrom::Start(block_offset))?;

            let chunk = (bs as usize - offset_in_block).min(to_read - bytes_read);
            self.file.read_exact(&mut buf[bytes_read..bytes_read + chunk])?;
            bytes_read += chunk;
        }

        Ok(bytes_read)
    }

    /// Resolve a data block index to its physical block number by navigating
    /// the bottom-up chain mapping structure.
    ///
    /// The chain model:
    /// - Start at the root mapping block (always level-1).
    /// - Level 1: entries[0..N-2] are direct data block pointers.
    ///   If block_index < N-1, return entries[block_index].
    /// - If block_index >= N-1, subtract N-1, follow entries[N-1] to level-2 block.
    /// - Level 2: entries[0..N-2] each point to level-1 sub-blocks.
    ///   Each sub-block holds N-1 data pointers, so level-2 capacity = (N-1)^2.
    /// - Continue up: level-k capacity = (N-1)^k.
    fn resolve_block(&mut self, entry: &FileEntry, block_index: u64) -> Result<u64, CtfsError> {
        let n = self.block_size as u64 / 8;
        let usable = n - 1;

        let mut idx = block_index;
        let mut current_level_block = entry.map_block;
        let mut level = 1u32;

        // Walk up through levels to find which level contains this index
        loop {
            let cap = level_capacity(usable, level);
            if idx < cap {
                break;
            }
            idx -= cap;
            level += 1;
            if level > 5 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "block index exceeds 5-level mapping capacity",
                )));
            }
            // Follow chain pointer at entries[N-1] to the next higher level
            let chain_ptr = self.read_block_ptr(current_level_block, usable as usize)?;
            if chain_ptr == 0 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("null chain pointer at block {} following to level {}", current_level_block, level),
                )));
            }
            current_level_block = chain_ptr;
        }

        // Navigate down within this level's block to find the data block
        self.navigate_to_data_block(current_level_block, level, idx, usable)
    }

    /// Navigate within a level-k block to find the data block pointer.
    /// For level 1: return entries[idx].
    /// For level k>1: compute which sub-entry, follow to child, recurse.
    fn navigate_to_data_block(
        &mut self,
        mapping_block: u64,
        level: u32,
        idx_within_level: u64,
        usable: u64,
    ) -> Result<u64, CtfsError> {
        if level == 1 {
            let ptr = self.read_block_ptr(mapping_block, idx_within_level as usize)?;
            if ptr == 0 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("null data block pointer at block {} index {}", mapping_block, idx_within_level),
                )));
            }
            return Ok(ptr);
        }

        let sub_cap = level_capacity(usable, level - 1);
        let entry_idx = idx_within_level / sub_cap;
        let sub_idx = idx_within_level % sub_cap;

        let child_block = self.read_block_ptr(mapping_block, entry_idx as usize)?;
        if child_block == 0 {
            return Err(CtfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("null mapping pointer at block {} index {}", mapping_block, entry_idx),
            )));
        }

        self.navigate_to_data_block(child_block, level - 1, sub_idx, usable)
    }

    /// Read a u64 pointer at a given entry index within a mapping block.
    fn read_block_ptr(&mut self, block_num: u64, index: usize) -> Result<u64, CtfsError> {
        let offset = block_num * self.block_size as u64 + (index * 8) as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; 8];
        self.file.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn find_entry(&self, name: &str) -> Option<&FileEntry> {
        let encoded = crate::base40::base40_encode(name).ok()?;
        self.entries.iter().find(|e| e.name == encoded && !e.is_empty())
    }
}
