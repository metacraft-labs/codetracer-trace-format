use std::fs::File;
use std::path::Path;

use crate::base40::base40_decode;
use crate::block_bounds::BlockBound;
use crate::file_entry::{FileEntry, FILE_ENTRY_SIZE};
use crate::header::{EXTENDED_HEADER_SIZE, HEADER_SIZE};
use crate::pread_compat::pread_exact;
use crate::CtfsError;

/// Thread-safe reader for CTFS containers.
///
/// Uses positional read for all I/O, so it is safe to share
/// across threads. The file entries are read at open time, but `refresh()`
/// can re-read them to pick up updates from concurrent writers.
pub struct ConcurrentCtfsReader {
    file: File,
    block_size: u32,
    max_root_entries: u32,
    entries: Vec<FileEntry>,
    entries_offset: u64,
}

// Safety: pread-based I/O is thread-safe (no shared file offset).
unsafe impl Send for ConcurrentCtfsReader {}
unsafe impl Sync for ConcurrentCtfsReader {}

/// Compute the capacity of a single level in the chain.
fn level_capacity(usable: u64, level: u32) -> u64 {
    usable.saturating_pow(level)
}

/// Read a u64 pointer at a given index within a block using positional read.
fn read_ptr_at(file: &File, block_num: u64, index: usize, block_size: u32) -> Result<u64, CtfsError> {
    let offset = block_num * block_size as u64 + (index * 8) as u64;
    let mut buf = [0u8; 8];
    pread_exact(file, &mut buf, offset)?;
    Ok(u64::from_le_bytes(buf))
}

impl ConcurrentCtfsReader {
    /// Open an existing CTFS container for concurrent reading.
    pub fn open(path: &Path) -> Result<Self, CtfsError> {
        let file = File::open(path)?;

        let entries_offset = (HEADER_SIZE + EXTENDED_HEADER_SIZE) as u64;

        // Read header
        let mut header_buf = [0u8; HEADER_SIZE];
        pread_exact(&file, &mut header_buf, 0)?;
        if header_buf[0..5] != crate::header::MAGIC {
            return Err(CtfsError::InvalidMagic);
        }
        if header_buf[5] != crate::header::VERSION && header_buf[5] != crate::header::VERSION_V2 && header_buf[5] != crate::header::VERSION_V4 {
            return Err(CtfsError::InvalidVersion(header_buf[5]));
        }

        // Read extended header
        let mut ext_buf = [0u8; EXTENDED_HEADER_SIZE];
        pread_exact(&file, &mut ext_buf, HEADER_SIZE as u64)?;
        let block_size = u32::from_le_bytes(ext_buf[0..4].try_into().unwrap());
        let max_root_entries = u32::from_le_bytes(ext_buf[4..8].try_into().unwrap());

        if block_size != 1024 && block_size != 2048 && block_size != 4096 {
            return Err(CtfsError::InvalidBlockSize(block_size));
        }

        // Read file entries
        let mut entries = Vec::with_capacity(max_root_entries as usize);
        for i in 0..max_root_entries {
            let offset = entries_offset + (i as u64) * FILE_ENTRY_SIZE as u64;
            let mut buf = [0u8; FILE_ENTRY_SIZE];
            pread_exact(&file, &mut buf, offset)?;
            let size = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let map_block = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            let name = u64::from_le_bytes(buf[16..24].try_into().unwrap());
            entries.push(FileEntry { size, map_block, name });
        }

        Ok(ConcurrentCtfsReader {
            file,
            block_size,
            max_root_entries,
            entries,
            entries_offset,
        })
    }

    /// Re-read file entries from disk to pick up updates from concurrent writers.
    pub fn refresh(&mut self) -> Result<(), CtfsError> {
        for i in 0..self.max_root_entries {
            let offset = self.entries_offset + (i as u64) * FILE_ENTRY_SIZE as u64;
            let mut buf = [0u8; FILE_ENTRY_SIZE];
            pread_exact(&self.file, &mut buf, offset)?;
            let size = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let map_block = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            let name = u64::from_le_bytes(buf[16..24].try_into().unwrap());
            self.entries[i as usize] = FileEntry { size, map_block, name };
        }
        Ok(())
    }

    /// List all file names in the container.
    pub fn list_files(&self) -> Vec<String> {
        self.entries.iter().filter(|e| !e.is_empty()).map(|e| base40_decode(e.name)).collect()
    }

    /// Get the size of a named file, or None if not found.
    pub fn file_size(&self, name: &str) -> Option<u64> {
        self.find_entry(name).map(|e| e.size)
    }

    /// Read an entire file's contents using positional read.
    ///
    /// Refuses — rather than serving out of the partial region, or out of a
    /// zero-filled buffer — a stream whose mapping root, mapping blocks or data
    /// blocks fall outside the container's whole blocks. See `block_bounds` and
    /// `CTFS-Binary-Format.md` §5d. The bound is re-derived from the file on
    /// every call, so it grows with a live producer.
    pub fn read_file(&self, name: &str) -> Result<Vec<u8>, CtfsError> {
        let entry = *self.find_entry(name).ok_or_else(|| CtfsError::FileNotFound(name.to_string()))?;

        if entry.size == 0 {
            return Ok(Vec::new());
        }

        let bound = BlockBound::of(&self.file, self.block_size)?;
        let mut data = Vec::with_capacity(entry.size as usize);
        let bs = self.block_size as u64;
        let num_blocks = entry.size.div_ceil(bs);

        for block_idx in 0..num_blocks {
            let data_block = self.resolve_block(&entry, block_idx, name, &bound)?;
            let block_offset = data_block * bs;

            let remaining = entry.size as usize - data.len();
            let to_read = remaining.min(bs as usize);
            let mut buf = vec![0u8; to_read];
            pread_exact(&self.file, &mut buf, block_offset)?;
            data.extend_from_slice(&buf);
        }

        Ok(data)
    }

    /// Read from an arbitrary position within a file using positional read.
    pub fn read_at(&self, name: &str, offset: u64, buf: &mut [u8]) -> Result<usize, CtfsError> {
        let entry = *self.find_entry(name).ok_or_else(|| CtfsError::FileNotFound(name.to_string()))?;

        if offset >= entry.size {
            return Ok(0);
        }

        let bound = BlockBound::of(&self.file, self.block_size)?;
        let bs = self.block_size as u64;
        let available = (entry.size - offset) as usize;
        let to_read = buf.len().min(available);
        let mut bytes_read = 0;

        while bytes_read < to_read {
            let current_offset = offset + bytes_read as u64;
            let block_idx = current_offset / bs;
            let offset_in_block = (current_offset % bs) as usize;

            let data_block = self.resolve_block(&entry, block_idx, name, &bound)?;
            let block_offset = data_block * bs + offset_in_block as u64;

            let chunk = (bs as usize - offset_in_block).min(to_read - bytes_read);
            pread_exact(&self.file, &mut buf[bytes_read..bytes_read + chunk], block_offset)?;
            bytes_read += chunk;
        }

        Ok(bytes_read)
    }

    /// Resolve a data block index to its physical block number.
    ///
    /// Every block number this walk produces is checked against `bound` before
    /// its bytes are read — the mapping root here, the chained and descended
    /// mapping blocks below, and the data block in `navigate_to_data_block`.
    /// That is §5d's "all three paths".
    fn resolve_block(&self, entry: &FileEntry, block_index: u64, name: &str, bound: &BlockBound) -> Result<u64, CtfsError> {
        let n = self.block_size as u64 / 8;
        let usable = n - 1;

        let mut idx = block_index;
        let mut current_level_block = entry.map_block;
        let mut level = 1u32;

        // Path 1 of 3: the entry's mapping root.
        bound.check_mapping_root(current_level_block, &format!("mapping root block of internal file {name}"))?;

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
            let chain_ptr = read_ptr_at(&self.file, current_level_block, usable as usize, self.block_size)?;
            if chain_ptr == 0 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("null chain pointer at block {} following to level {}", current_level_block, level),
                )));
            }
            // Path 2a of 3: a mapping block reached through the chain.
            bound.check(chain_ptr, &format!("chain pointer at level {level} of internal file {name}"))?;
            current_level_block = chain_ptr;
        }

        self.navigate_to_data_block(current_level_block, level, idx, usable, block_index, name, bound)
    }

    #[allow(clippy::too_many_arguments)]
    fn navigate_to_data_block(
        &self,
        mapping_block: u64,
        level: u32,
        idx_within_level: u64,
        usable: u64,
        block_index: u64,
        name: &str,
        bound: &BlockBound,
    ) -> Result<u64, CtfsError> {
        if level == 1 {
            let ptr = read_ptr_at(&self.file, mapping_block, idx_within_level as usize, self.block_size)?;
            if ptr == 0 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("null data block pointer at block {} index {}", mapping_block, idx_within_level),
                )));
            }
            // Path 3 of 3, and the one that was missing. `read_file`'s copy is
            // clamped to what is left of the entry, so a stream whose last data
            // block landed in the partial region was served successfully out of
            // bytes the container does not own — and before `pread_exact`, out
            // of a zero-filled buffer when those bytes were not there at all.
            // Check the block NUMBER, before any of its bytes are touched.
            bound.check(ptr, &format!("data block {block_index} of internal file {name}"))?;
            return Ok(ptr);
        }

        let sub_cap = level_capacity(usable, level - 1);
        let entry_idx = idx_within_level / sub_cap;
        let sub_idx = idx_within_level % sub_cap;

        let child_block = read_ptr_at(&self.file, mapping_block, entry_idx as usize, self.block_size)?;
        if child_block == 0 {
            return Err(CtfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("null mapping pointer at block {} index {}", mapping_block, entry_idx),
            )));
        }
        // Path 2b of 3: a mapping block reached by descending the hierarchy.
        bound.check(child_block, &format!("child block pointer at level {level} of internal file {name}"))?;

        self.navigate_to_data_block(child_block, level - 1, sub_idx, usable, block_index, name, bound)
    }

    fn find_entry(&self, name: &str) -> Option<&FileEntry> {
        let encoded = crate::base40::base40_encode(name).ok()?;
        self.entries.iter().find(|e| e.name == encoded && !e.is_empty())
    }
}
