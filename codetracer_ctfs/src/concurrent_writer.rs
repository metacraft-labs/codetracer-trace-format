use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::base40::base40_encode;
use crate::block_alloc::AtomicBlockAllocator;
use crate::file_entry::FILE_ENTRY_SIZE;
use crate::header::{ExtendedHeader, Header, EXTENDED_HEADER_SIZE, HEADER_SIZE};
use crate::pread_compat::{pread_exact, pwrite_all};
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
    /// The data block a `flush` published the current partial `buffer` into,
    /// if there has been one since the last complete block.
    ///
    /// A flush has to make the partial block visible to readers, but it must
    /// **not** consume a logical block index: the bytes that arrive next belong
    /// to the same logical block, because every reader resolves logical byte
    /// `p` to logical block `p / block_size`. So the block is allocated once,
    /// its pointer is inserted into the mapping chain at the *current*
    /// `data_block_count`, and `data_block_count` stays put; the block is
    /// rewritten in place on each further flush and finally handed to
    /// `flush_data_block` when the buffer fills, which is the point at which
    /// the index is consumed.
    ///
    /// This mirrors `CtfsWriter::sync_entry` / `pending_block` in `writer.rs`;
    /// the two writers must lay out the same blocks for the same byte stream.
    pending_block: Option<u64>,
    block_size: u32,
}

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

/// Write a u64 pointer at a given index within a block using positional write.
fn write_ptr_at(file: &File, block_num: u64, index: usize, value: u64, block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64 + (index * 8) as u64;
    pwrite_all(file, &value.to_le_bytes(), offset)?;
    Ok(())
}

/// Write a zero-filled block using positional write.
fn write_zero_block_at(file: &File, block_num: u64, block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64;
    let zeros = vec![0u8; block_size as usize];
    pwrite_all(file, &zeros, offset)?;
    Ok(())
}

/// Write data to a block using positional write.
fn write_block_data_at(file: &File, block_num: u64, data: &[u8], block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64;
    let mut padded = data.to_vec();
    padded.resize(block_size as usize, 0);
    pwrite_all(file, &padded, offset)?;
    Ok(())
}

impl ConcurrentCtfsWriter {
    /// Create a new CTFS container at the given path.
    /// Returns an `Arc<Self>` for sharing across threads.
    pub fn create(path: &Path, block_size: u32, max_root_entries: u32) -> Result<Arc<Self>, CtfsError> {
        let _ext_header = ExtendedHeader::new(block_size, max_root_entries)?;

        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?;

        let entries_offset = (HEADER_SIZE + EXTENDED_HEADER_SIZE) as u64;

        // Build the entire root block in memory and write with pwrite
        let mut root_block = vec![0u8; block_size as usize];

        // Header: magic + version + compression + encryption
        let header = Header::new();
        root_block[0..5].copy_from_slice(&header.id);
        root_block[5] = header.version;
        root_block[6] = header.compression as u8;
        root_block[7] = header.encryption as u8;

        // Extended header: block_size + max_root_entries
        root_block[8..12].copy_from_slice(&block_size.to_le_bytes());
        root_block[12..16].copy_from_slice(&max_root_entries.to_le_bytes());

        // File entries are already zero (empty)
        // Write the entire root block at offset 0
        pwrite_all(&file, &root_block, 0)?;

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
            pending_block: None,
            block_size: self.block_size,
        })
    }

    /// Close the container, writing all file entry metadata to disk.
    /// All `FileWriter` handles must have been flushed and dropped before calling this.
    pub fn close(self) -> Result<(), CtfsError> {
        let entries = self.file_entries.lock().unwrap();

        for (i, entry_state) in entries.iter().enumerate() {
            let entry_offset = self.entries_offset + (i as u64) * FILE_ENTRY_SIZE as u64;
            let mut buf = [0u8; FILE_ENTRY_SIZE];
            buf[0..8].copy_from_slice(&entry_state.size.to_le_bytes());
            buf[8..16].copy_from_slice(&entry_state.map_block.to_le_bytes());
            buf[16..24].copy_from_slice(&entry_state.name_encoded.to_le_bytes());
            pwrite_all(&self.file, &buf, entry_offset)?;
        }

        self.file.sync_all()?;
        Ok(())
    }
}

impl FileWriter {
    /// Write data to this file (appends to end).
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
    ///
    /// A partial block is published through a *pending* block that keeps its
    /// logical index, so writing can continue afterwards. Draining the buffer
    /// into a fresh block instead — which is what this did before — advanced
    /// the logical block index by one while the entry's `size` kept counting
    /// bytes contiguously, so every reader placed the post-flush bytes one
    /// block too early and served the flushed block's zero padding as content.
    pub fn flush(&mut self, parent: &ConcurrentCtfsWriter) -> Result<(), CtfsError> {
        // Publish the partial block without consuming its logical index.
        if !self.buffer.is_empty() {
            let bs = self.block_size;
            let data_block = match self.pending_block {
                Some(block) => block,
                None => {
                    let n = bs as u64 / 8;
                    let usable = n - 1;
                    let block_index = self.data_block_count;
                    let root_block = self.root_block;
                    let data_block = parent.allocator.allocate();
                    self.insert_data_block_chain(parent, root_block, block_index, data_block, usable, bs)?;
                    self.pending_block = Some(data_block);
                    data_block
                }
            };
            // Rewritten whole each time, so stale padding from an earlier
            // flush of the same block cannot survive under later bytes.
            write_block_data_at(&parent.file, data_block, &self.buffer, bs)?;
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
        pwrite_all(&parent.file, &buf, entry_offset)?;

        Ok(())
    }

    /// Flush a single data block into the mapping chain.
    fn flush_data_block(&mut self, parent: &ConcurrentCtfsWriter, block_data: &[u8]) -> Result<(), CtfsError> {
        let bs = self.block_size;
        let n = bs as u64 / 8;
        let usable = n - 1;

        // If a `flush` already published this logical block as a pending
        // block, reuse it: its pointer is in the mapping chain at this very
        // index, and allocating a second block here is what shifted every
        // subsequent byte one block forward.
        let data_block = match self.pending_block.take() {
            Some(pending) => pending,
            None => {
                let data_block = parent.allocator.allocate();
                let block_index = self.data_block_count;
                // Navigate the bottom-up chain to insert the data block pointer
                self.insert_data_block_chain(parent, self.root_block, block_index, data_block, usable, bs)?;
                data_block
            }
        };
        write_block_data_at(&parent.file, data_block, block_data, bs)?;

        self.data_block_count += 1;

        Ok(())
    }

    /// Insert a data block pointer at the given block_index using the bottom-up chain model.
    ///
    /// # A null pointer here is not always "not allocated yet"
    ///
    /// The same rule `CtfsWriter::insert_data_block_chain` documents and
    /// `CTFS-Binary-Format.md` §4 states normatively: a mapping is filled in
    /// strictly increasing block-index order, so a null pointer is legitimate
    /// only for the **first index that pointer covers**, and a null anywhere
    /// else is damage that allocating over would orphan.
    ///
    /// **Not reachable today, and kept anyway.** `ConcurrentCtfsWriter` has no
    /// `open_append`: every container it writes it also created, so the only
    /// mapping it walks is one it built in the same session and no input can
    /// drive either branch to a corrupted zero. The guard is here because this
    /// is the same walk with the same rule, and the two writers must not answer
    /// a format question differently — the way they already did over
    /// `pending_block`, which is what made a timed flush corrupt a stream. Its
    /// correctness is demonstrated against `CtfsWriter`, which *is* reachable.
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
                return Err(CtfsError::Io(std::io::Error::other("file too large: exceeds 5-level mapping")));
            }

            // Follow or create the chain pointer from current_level_block[N-1]
            let chain_ptr = read_ptr_at(&parent.file, current_level_block, usable as usize, bs)?;
            if chain_ptr == 0 {
                if idx != 0 {
                    return Err(CtfsError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "null chain pointer at block {current_level_block} following to level {level}, \
                             but data block index {block_index} is not the first index that pointer covers \
                             (offset {idx} within level {level}); the mapping is damaged, and allocating a \
                             replacement here would orphan the existing level-{level} subtree"
                        ),
                    )));
                }
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
            debug_assert!(idx_within_level < usable, "idx {} >= usable {} at level 1", idx_within_level, usable);
            write_ptr_at(&parent.file, mapping_block, idx_within_level as usize, data_block, bs)?;
            return Ok(());
        }

        let sub_cap = level_capacity(usable, level - 1);
        let entry_idx = idx_within_level / sub_cap;
        let sub_idx = idx_within_level % sub_cap;

        debug_assert!(entry_idx < usable, "entry_idx {} >= usable {} at level {}", entry_idx, usable, level);

        let child_block = read_ptr_at(&parent.file, mapping_block, entry_idx as usize, bs)?;
        let target_block = if child_block == 0 {
            if sub_idx != 0 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "null mapping pointer at block {mapping_block} index {entry_idx} (level {level}), \
                         but the index being placed is not the first that pointer covers (offset {sub_idx} \
                         within it); the mapping is damaged, and allocating a replacement here would orphan \
                         the existing level-{} subtree",
                        level - 1
                    ),
                )));
            }
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
