#[cfg(unix)]
use std::os::unix::fs::FileExt;

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::base40::base40_encode;
use crate::block_alloc::AtomicBlockAllocator;
use crate::file_entry::FILE_ENTRY_SIZE;
use crate::header::{ExtendedHeader, Header, EXTENDED_HEADER_SIZE, HEADER_SIZE};
use crate::CtfsError;

/// State for a file entry tracked in the root table.
#[derive(Debug)]
struct FileEntryState {
    name_encoded: u64,
    map_block: u64,
    /// The committed size visible to readers (updated on flush).
    size: u64,
}

/// Concurrent writer for CTFS containers.
///
/// Shared across threads via `Arc`. Each thread gets its own `FileWriter`
/// handle for writing to a specific file within the container.
pub struct ConcurrentCtfsWriter {
    file: File,
    block_size: u32,
    max_root_entries: u32,
    allocator: AtomicBlockAllocator,
    file_entries: Mutex<Vec<FileEntryState>>,
    entries_offset: u64,
}

impl std::fmt::Debug for ConcurrentCtfsWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentCtfsWriter")
            .field("block_size", &self.block_size)
            .field("max_root_entries", &self.max_root_entries)
            .finish()
    }
}

// Safety: File descriptor I/O via pread/pwrite is thread-safe.
// The Mutex protects the file_entries vec. AtomicBlockAllocator is lock-free.
unsafe impl Send for ConcurrentCtfsWriter {}
unsafe impl Sync for ConcurrentCtfsWriter {}

/// Per-file writer handle. Owned by one thread, NOT shared.
pub struct FileWriter {
    file_index: usize,
    name_encoded: u64,
    root_block: u64,
    /// Total data blocks written (full blocks flushed to disk).
    data_block_count: u64,
    /// Total logical bytes written.
    size: u64,
    /// Buffered partial block data.
    buffer: Vec<u8>,
    block_size: u32,
}

/// Compute the capacity of a single level in the chain.
fn level_capacity(usable: u64, level: u32) -> u64 {
    usable.saturating_pow(level)
}

/// Read a u64 pointer at a given index within a block using pread.
#[cfg(unix)]
fn read_ptr_at(file: &File, block_num: u64, index: usize, block_size: u32) -> Result<u64, CtfsError> {
    let offset = block_num * block_size as u64 + (index * 8) as u64;
    let mut buf = [0u8; 8];
    file.read_at(&mut buf, offset)?;
    Ok(u64::from_le_bytes(buf))
}

/// Write a u64 pointer at a given index within a block using pwrite.
#[cfg(unix)]
fn write_ptr_at(file: &File, block_num: u64, index: usize, value: u64, block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64 + (index * 8) as u64;
    file.write_at(&value.to_le_bytes(), offset)?;
    Ok(())
}

/// Write a zero-filled block using pwrite.
#[cfg(unix)]
fn write_zero_block_at(file: &File, block_num: u64, block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64;
    let zeros = vec![0u8; block_size as usize];
    file.write_at(&zeros, offset)?;
    Ok(())
}

/// Write data to a block using pwrite.
#[cfg(unix)]
fn write_block_data_at(file: &File, block_num: u64, data: &[u8], block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64;
    let mut padded = data.to_vec();
    padded.resize(block_size as usize, 0);
    file.write_at(&padded, offset)?;
    Ok(())
}

impl ConcurrentCtfsWriter {
    /// Create a new CTFS container at the given path.
    /// Returns an `Arc<Self>` for sharing across threads.
    #[cfg(unix)]
    pub fn create(path: &Path, block_size: u32, max_root_entries: u32) -> Result<Arc<Self>, CtfsError> {
        let _ext_header = ExtendedHeader::new(block_size, max_root_entries)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let entries_offset = (HEADER_SIZE + EXTENDED_HEADER_SIZE) as u64;

        // Build the entire root block in memory and write with pwrite
        let mut root_block = vec![0u8; block_size as usize];

        // Header: magic + version + reserved
        let header = Header::new();
        root_block[0..5].copy_from_slice(&header.id);
        root_block[5] = header.version;
        // reserved bytes [6..8] already zero

        // Extended header: block_size + max_root_entries
        root_block[8..12].copy_from_slice(&block_size.to_le_bytes());
        root_block[12..16].copy_from_slice(&max_root_entries.to_le_bytes());

        // File entries are already zero (empty)
        // Write the entire root block at offset 0
        file.write_at(&root_block, 0)?;

        Ok(Arc::new(ConcurrentCtfsWriter {
            file,
            block_size,
            max_root_entries,
            allocator: AtomicBlockAllocator::new(1), // block 0 = root
            file_entries: Mutex::new(Vec::new()),
            entries_offset,
        }))
    }

    /// Add a new named file to the container. Returns a `FileWriter` handle.
    ///
    /// This briefly locks the file entries mutex.
    #[cfg(unix)]
    pub fn add_file(&self, name: &str) -> Result<FileWriter, CtfsError> {
        let name_encoded = base40_encode(name)?;

        let mut entries = self.file_entries.lock().unwrap();
        if entries.len() >= self.max_root_entries as usize {
            return Err(CtfsError::TooManyFiles);
        }

        let file_index = entries.len();

        // Allocate a level-1 mapping block for this file
        let map_block = self.allocator.allocate();
        write_zero_block_at(&self.file, map_block, self.block_size)?;

        entries.push(FileEntryState {
            name_encoded,
            map_block,
            size: 0,
        });

        Ok(FileWriter {
            file_index,
            name_encoded,
            root_block: map_block,
            data_block_count: 0,
            size: 0,
            buffer: Vec::new(),
            block_size: self.block_size,
        })
    }

    /// Close the container, writing all file entry metadata to disk.
    /// All `FileWriter` handles must have been flushed and dropped before calling this.
    #[cfg(unix)]
    pub fn close(self) -> Result<(), CtfsError> {
        let entries = self.file_entries.lock().unwrap();

        for (i, entry_state) in entries.iter().enumerate() {
            let entry_offset = self.entries_offset + (i as u64) * FILE_ENTRY_SIZE as u64;
            let mut buf = [0u8; FILE_ENTRY_SIZE];
            buf[0..8].copy_from_slice(&entry_state.size.to_le_bytes());
            buf[8..16].copy_from_slice(&entry_state.map_block.to_le_bytes());
            buf[16..24].copy_from_slice(&entry_state.name_encoded.to_le_bytes());
            self.file.write_at(&buf, entry_offset)?;
        }

        self.file.sync_all()?;
        Ok(())
    }
}

impl FileWriter {
    /// Write data to this file (appends to end).
    #[cfg(unix)]
    pub fn write(&mut self, parent: &ConcurrentCtfsWriter, data: &[u8]) -> Result<usize, CtfsError> {
        let bs = self.block_size as usize;
        self.buffer.extend_from_slice(data);
        self.size += data.len() as u64;

        // Flush complete blocks
        while self.buffer.len() >= bs {
            let block_data: Vec<u8> = self.buffer.drain(..bs).collect();
            self.flush_data_block(parent, &block_data)?;
        }

        Ok(data.len())
    }

    /// Flush any buffered data and update the file entry size in the parent.
    #[cfg(unix)]
    pub fn flush(&mut self, parent: &ConcurrentCtfsWriter) -> Result<(), CtfsError> {
        // Flush any remaining partial block
        if !self.buffer.is_empty() {
            let block_data = std::mem::take(&mut self.buffer);
            self.flush_data_block(parent, &block_data)?;
        }

        // Update file entry size in the parent (in-memory)
        {
            let mut entries = parent.file_entries.lock().unwrap();
            entries[self.file_index].size = self.size;
        }

        // Write the file entry to disk so readers can see the updated size
        let entry_offset = parent.entries_offset + (self.file_index as u64) * FILE_ENTRY_SIZE as u64;
        let mut buf = [0u8; FILE_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.size.to_le_bytes());
        buf[8..16].copy_from_slice(&self.root_block.to_le_bytes());
        buf[16..24].copy_from_slice(&self.name_encoded.to_le_bytes());
        parent.file.write_at(&buf, entry_offset)?;

        Ok(())
    }

    /// Flush a single data block into the mapping chain.
    #[cfg(unix)]
    fn flush_data_block(&mut self, parent: &ConcurrentCtfsWriter, block_data: &[u8]) -> Result<(), CtfsError> {
        let bs = self.block_size;
        let n = bs as u64 / 8;
        let usable = n - 1;

        // Allocate a data block and write data via pwrite
        let data_block = parent.allocator.allocate();
        write_block_data_at(&parent.file, data_block, block_data, bs)?;

        let block_index = self.data_block_count;

        // Navigate the bottom-up chain to insert the data block pointer
        self.insert_data_block_chain(parent, self.root_block, block_index, data_block, usable, bs)?;

        self.data_block_count += 1;

        Ok(())
    }

    /// Insert a data block pointer at the given block_index using the bottom-up chain model.
    #[cfg(unix)]
    fn insert_data_block_chain(
        &mut self,
        parent: &ConcurrentCtfsWriter,
        root_block: u64,
        block_index: u64,
        data_block: u64,
        usable: u64,
        bs: u32,
    ) -> Result<(), CtfsError> {
        let mut idx = block_index;
        let mut current_level_block = root_block;
        let mut level = 1u32;

        // Walk up through levels until we find which level contains this index
        loop {
            let cap = level_capacity(usable, level);
            if idx < cap {
                break;
            }
            idx -= cap;
            level += 1;

            if level > 5 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "file too large: exceeds 5-level mapping",
                )));
            }

            // Follow or create the chain pointer from current_level_block[N-1]
            let chain_ptr = read_ptr_at(&parent.file, current_level_block, usable as usize, bs)?;
            if chain_ptr == 0 {
                let new_block = parent.allocator.allocate();
                write_zero_block_at(&parent.file, new_block, bs)?;
                write_ptr_at(&parent.file, current_level_block, usable as usize, new_block, bs)?;
                current_level_block = new_block;
            } else {
                current_level_block = chain_ptr;
            }
        }

        self.navigate_and_insert(parent, current_level_block, level, idx, data_block, usable, bs)
    }

    /// Navigate within a level-k block to insert a data block pointer.
    #[cfg(unix)]
    fn navigate_and_insert(
        &self,
        parent: &ConcurrentCtfsWriter,
        mapping_block: u64,
        level: u32,
        idx_within_level: u64,
        data_block: u64,
        usable: u64,
        bs: u32,
    ) -> Result<(), CtfsError> {
        if level == 1 {
            debug_assert!(
                idx_within_level < usable,
                "idx {} >= usable {} at level 1",
                idx_within_level,
                usable
            );
            write_ptr_at(&parent.file, mapping_block, idx_within_level as usize, data_block, bs)?;
            return Ok(());
        }

        let sub_cap = level_capacity(usable, level - 1);
        let entry_idx = idx_within_level / sub_cap;
        let sub_idx = idx_within_level % sub_cap;

        debug_assert!(
            entry_idx < usable,
            "entry_idx {} >= usable {} at level {}",
            entry_idx,
            usable,
            level
        );

        let child_block = read_ptr_at(&parent.file, mapping_block, entry_idx as usize, bs)?;
        let target_block = if child_block == 0 {
            let new_block = parent.allocator.allocate();
            write_zero_block_at(&parent.file, new_block, bs)?;
            write_ptr_at(&parent.file, mapping_block, entry_idx as usize, new_block, bs)?;
            new_block
        } else {
            child_block
        };

        self.navigate_and_insert(parent, target_block, level - 1, sub_idx, data_block, usable, bs)
    }
}
