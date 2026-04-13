use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read as IoRead, Seek, SeekFrom, Write};
use std::path::Path;

use crate::base40::base40_encode;
use crate::block_alloc::BlockAllocator;
use crate::file_entry::{FileEntry, FILE_ENTRY_SIZE};
use crate::header::{CompressionMethod, ExtendedHeader, Header, EXTENDED_HEADER_SIZE, HEADER_SIZE};
use crate::CtfsError;

/// Opaque handle to an open file within a CTFS container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHandle(pub(crate) usize);

/// Bottom-up chain mapping state for a single file.
///
/// The spec defines a bottom-up chain model:
/// - The root mapping block (from FileEntry.map_block) is ALWAYS a level-1 block.
/// - Level-1 block: entries[0..N-2] are direct data block pointers,
///   entries[N-1] points to a level-2 mapping block (0 if not needed).
/// - Level-2 block: entries[0..N-2] point to level-1 mapping blocks,
///   entries[N-1] points to a level-3 mapping block (0 if not needed).
/// - And so on up to level 5.
///
/// N = block_size / 8 (entries per block)
/// Usable entries per mapping block = N - 1 (last slot reserved for chain pointer)
#[derive(Debug)]
struct MappingChain {
    /// The root mapping block number (always level-1).
    root_block: u64,
    /// Total data blocks mapped so far.
    data_block_count: u64,
}

/// State for an open file being written.
#[derive(Debug)]
struct OpenFile {
    entry_index: usize,
    name_encoded: u64,
    mapping: MappingChain,
    /// Total bytes written.
    size: u64,
    /// Buffered partial block data.
    buffer: Vec<u8>,
    /// Block number used by `sync_entry` to write partial-block data.
    /// This block is pre-allocated and its pointer is inserted into the
    /// mapping chain. When the buffer fills a complete block, the pending
    /// block becomes a regular data block (the pointer is already set).
    pending_block: Option<u64>,
}

/// Writer for creating CTFS containers.
pub struct CtfsWriter {
    writer: BufWriter<File>,
    block_size: u32,
    max_root_entries: u32,
    allocator: BlockAllocator,
    files: Vec<OpenFile>,
    entries_offset: u64,
    compression: CompressionMethod,
}

/// Compute the capacity of a single level in the chain.
/// Level 1: N-1 data blocks (direct pointers)
/// Level 2: (N-1)^2 data blocks (via (N-1) level-1 sub-blocks)
/// Level k: (N-1)^k
fn level_capacity(usable: u64, level: u32) -> u64 {
    usable.saturating_pow(level)
}


/// Read a full block from the writer's underlying file.
fn read_block(writer: &mut BufWriter<File>, block_num: u64, block_size: u32) -> Result<Vec<u8>, CtfsError> {
    writer.flush()?;
    let offset = block_num * block_size as u64;
    writer.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; block_size as usize];
    writer.get_mut().read_exact(&mut buf)?;
    Ok(buf)
}

/// Read a u64 pointer at a given index within a block.
fn read_ptr(block_data: &[u8], index: usize) -> u64 {
    let off = index * 8;
    u64::from_le_bytes(block_data[off..off + 8].try_into().unwrap())
}

/// Write a u64 pointer at a given index within a block on disk.
fn write_ptr(writer: &mut BufWriter<File>, block_num: u64, index: usize, value: u64, block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64 + (index * 8) as u64;
    writer.seek(SeekFrom::Start(offset))?;
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

/// Write a zero-filled block.
fn write_zero_block(writer: &mut BufWriter<File>, block_num: u64, block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64;
    writer.seek(SeekFrom::Start(offset))?;
    let zeros = vec![0u8; block_size as usize];
    writer.write_all(&zeros)?;
    Ok(())
}

impl CtfsWriter {
    /// Create a new CTFS container at the given path.
    pub fn create(path: &Path, block_size: u32, max_root_entries: u32) -> Result<Self, CtfsError> {
        Self::create_with_compression(path, block_size, max_root_entries, CompressionMethod::None)
    }

    /// Create a new CTFS container at the given path with the specified compression method.
    pub fn create_with_compression(
        path: &Path,
        block_size: u32,
        max_root_entries: u32,
        compression: CompressionMethod,
    ) -> Result<Self, CtfsError> {
        let ext_header = ExtendedHeader::new(block_size, max_root_entries)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);

        // Write header (v3 with compression/encryption tags)
        let header = Header::with_compression(compression);
        header.write_to(&mut writer)?;
        ext_header.write_to(&mut writer)?;

        let entries_offset = (HEADER_SIZE + EXTENDED_HEADER_SIZE) as u64;

        // Write empty file entries
        let empty = FileEntry::empty();
        for _ in 0..max_root_entries {
            empty.write_to(&mut writer)?;
        }

        // Pad block 0 to block_size
        let root_used = HEADER_SIZE + EXTENDED_HEADER_SIZE + FILE_ENTRY_SIZE * (max_root_entries as usize);
        if root_used < block_size as usize {
            let padding = vec![0u8; block_size as usize - root_used];
            writer.write_all(&padding)?;
        }

        writer.flush()?;

        Ok(CtfsWriter {
            writer,
            block_size,
            max_root_entries,
            allocator: BlockAllocator::new(),
            files: Vec::new(),
            entries_offset,
            compression,
        })
    }

    /// Get the compression method for this container.
    pub fn compression(&self) -> CompressionMethod {
        self.compression
    }

    /// Open an existing CTFS container for appending.
    pub fn open_append(path: &Path) -> Result<Self, CtfsError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let header = Header::read_from(&mut file)?;
        let ext_header = ExtendedHeader::read_from(&mut file)?;

        let entries_offset = (HEADER_SIZE + EXTENDED_HEADER_SIZE) as u64;

        let mut entries = Vec::new();
        for _ in 0..ext_header.max_root_entries {
            let entry = FileEntry::read_from(&mut file)?;
            entries.push(entry);
        }

        // Determine the highest block in use by scanning the file size
        let file_len = file.seek(SeekFrom::End(0))?;
        let next_block = (file_len + ext_header.block_size as u64 - 1) / ext_header.block_size as u64;

        let mut allocator = BlockAllocator::new();
        // Advance allocator to the next free block
        while allocator.next() < next_block {
            allocator.alloc();
        }

        let bs = ext_header.block_size as u64;

        let mut files = Vec::new();
        for (i, entry) in entries.iter().enumerate() {
            if !entry.is_empty() {
                let total_blocks = if entry.size == 0 { 0 } else { (entry.size + bs - 1) / bs };
                let partial_bytes = entry.size % bs;
                let has_partial = partial_bytes != 0 && entry.size > 0;

                // If the last block is partial, we need to read it back into the buffer
                // and "undo" it so subsequent writes can re-fill it.
                let (data_block_count, buffer, logical_size) = if has_partial {
                    let full_blocks = total_blocks - 1;

                    // Read the partial block data from the file using bottom-up chain navigation
                    let partial_data = read_last_data_block_chain(
                        &mut file, entry.map_block, total_blocks - 1,
                        ext_header.block_size, partial_bytes as usize,
                    )?;

                    (full_blocks, partial_data, entry.size)
                } else {
                    (total_blocks, Vec::new(), entry.size)
                };

                files.push(OpenFile {
                    entry_index: i,
                    name_encoded: entry.name,
                    mapping: MappingChain {
                        root_block: entry.map_block,
                        data_block_count,
                    },
                    size: logical_size,
                    buffer,
                    pending_block: None,
                });
            }
        }

        let writer = BufWriter::new(file);

        Ok(CtfsWriter {
            writer,
            block_size: ext_header.block_size,
            max_root_entries: ext_header.max_root_entries,
            allocator,
            files,
            entries_offset,
            compression: header.compression,
        })
    }

    /// Add a new named file to the container. Returns a handle for writing.
    pub fn add_file(&mut self, name: &str) -> Result<FileHandle, CtfsError> {
        if self.files.len() >= self.max_root_entries as usize {
            return Err(CtfsError::TooManyFiles);
        }
        let name_encoded = base40_encode(name)?;
        let entry_index = self.files.len();

        // Allocate a level-1 mapping block for this file
        let map_block = self.allocator.alloc();
        write_zero_block(&mut self.writer, map_block, self.block_size)?;

        self.files.push(OpenFile {
            entry_index,
            name_encoded,
            mapping: MappingChain {
                root_block: map_block,
                data_block_count: 0,
            },
            size: 0,
            buffer: Vec::new(),
            pending_block: None,
        });

        Ok(FileHandle(entry_index))
    }

    /// Find a file handle by name (for appending to existing files).
    pub fn find_file(&self, name: &str) -> Option<FileHandle> {
        let encoded = base40_encode(name).ok()?;
        self.files.iter().position(|f| f.name_encoded == encoded).map(FileHandle)
    }

    /// Write data to an open file (appends to end).
    pub fn write(&mut self, handle: FileHandle, data: &[u8]) -> Result<usize, CtfsError> {
        let bs = self.block_size as usize;
        self.files[handle.0].buffer.extend_from_slice(data);
        self.files[handle.0].size += data.len() as u64;

        // Flush complete blocks
        loop {
            if self.files[handle.0].buffer.len() < bs {
                break;
            }
            let block_data: Vec<u8> = self.files[handle.0].buffer.drain(..bs).collect();
            self.flush_data_block(handle.0, &block_data)?;
        }

        Ok(data.len())
    }

    /// Append data to an existing file (alias for write, used after open_append).
    pub fn append(&mut self, handle: FileHandle, data: &[u8]) -> Result<usize, CtfsError> {
        self.write(handle, data)
    }

    /// Flush a single data block into the mapping chain for the given file.
    fn flush_data_block(&mut self, file_idx: usize, block_data: &[u8]) -> Result<(), CtfsError> {
        let bs = self.block_size;
        let n = bs as u64 / 8; // entries per block
        let usable = n - 1; // usable entries (last is chain pointer)

        // If sync_entry pre-allocated a pending block for this slot, reuse it.
        // The mapping chain pointer is already set.
        let data_block = if let Some(pending) = self.files[file_idx].pending_block.take() {
            pending
        } else {
            let data_block = self.allocator.alloc();
            let block_index = self.files[file_idx].mapping.data_block_count;
            let root_block = self.files[file_idx].mapping.root_block;
            self.insert_data_block_chain(root_block, block_index, data_block, usable, bs)?;
            data_block
        };

        // Write block data (padded to block_size).
        let offset = data_block * bs as u64;
        self.writer.seek(SeekFrom::Start(offset))?;
        let mut padded = block_data.to_vec();
        padded.resize(bs as usize, 0);
        self.writer.write_all(&padded)?;

        self.files[file_idx].mapping.data_block_count += 1;

        Ok(())
    }

    /// Insert a data block pointer at the given block_index using the bottom-up chain model.
    ///
    /// The chain works as follows:
    /// - Level 1 (root): entries[0..usable-1] hold direct data block pointers (indices 0..usable-1)
    /// - entries[usable] (= entries[N-1]) points to level-2 block
    /// - Level 2: entries[0..usable-1] each point to a level-1 sub-block (each holds usable data ptrs)
    /// - entries[usable] points to level-3, etc.
    fn insert_data_block_chain(
        &mut self,
        root_block: u64,
        block_index: u64,
        data_block: u64,
        usable: u64,
        bs: u32,
    ) -> Result<(), CtfsError> {
        // Determine which level this block_index falls into and the remaining offset.
        // Level 1: indices 0..usable-1 (capacity = usable)
        // Level 2: indices usable..usable+usable^2-1 (capacity = usable^2)
        // Level k: capacity = usable^k, starts at cumulative_capacity(usable, k-1)

        let mut idx = block_index;
        let mut current_level_block = root_block;
        let mut level = 1u32;

        // Walk up through levels until we find which level contains this index
        loop {
            let cap = level_capacity(usable, level);
            if idx < cap {
                // This index belongs at this level
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
            // to the next higher level block
            let block_data = read_block(&mut self.writer, current_level_block, bs)?;
            let chain_ptr = read_ptr(&block_data, usable as usize);
            if chain_ptr == 0 {
                // Allocate the higher-level block
                let new_block = self.allocator.alloc();
                write_zero_block(&mut self.writer, new_block, bs)?;
                write_ptr(&mut self.writer, current_level_block, usable as usize, new_block, bs)?;
                current_level_block = new_block;
            } else {
                current_level_block = chain_ptr;
            }
        }

        // Now we're at `current_level_block` which is a level-`level` block,
        // and `idx` is the offset within this level's address space.
        // Navigate down from this level to place the data block pointer.
        self.navigate_and_insert(current_level_block, level, idx, data_block, usable, bs)
    }

    /// Navigate within a level-k block to insert a data block pointer.
    /// For level 1: just write entries[idx] = data_block.
    /// For level k>1: entries[0..usable-1] each point to level-(k-1) blocks.
    ///   Compute which sub-entry, follow/allocate, recurse.
    fn navigate_and_insert(
        &mut self,
        mapping_block: u64,
        level: u32,
        idx_within_level: u64,
        data_block: u64,
        usable: u64,
        bs: u32,
    ) -> Result<(), CtfsError> {
        if level == 1 {
            // Direct data block pointer
            debug_assert!(idx_within_level < usable,
                "idx {} >= usable {} at level 1", idx_within_level, usable);
            write_ptr(&mut self.writer, mapping_block, idx_within_level as usize, data_block, bs)?;
            return Ok(());
        }

        // Level k > 1: each entry covers level_capacity(usable, level-1) data blocks
        let sub_cap = level_capacity(usable, level - 1);
        let entry_idx = idx_within_level / sub_cap;
        let sub_idx = idx_within_level % sub_cap;

        debug_assert!(entry_idx < usable,
            "entry_idx {} >= usable {} at level {}", entry_idx, usable, level);

        // Read or allocate the sub-block
        let block_data = read_block(&mut self.writer, mapping_block, bs)?;
        let child_block = read_ptr(&block_data, entry_idx as usize);

        let target_block = if child_block == 0 {
            let new_block = self.allocator.alloc();
            write_zero_block(&mut self.writer, new_block, bs)?;
            write_ptr(&mut self.writer, mapping_block, entry_idx as usize, new_block, bs)?;
            new_block
        } else {
            child_block
        };

        self.navigate_and_insert(target_block, level - 1, sub_idx, data_block, usable, bs)
    }

    /// Sync a file's data and metadata to disk so concurrent readers can see
    /// all bytes written so far, including any partial block still in the
    /// write buffer.
    ///
    /// If there is buffered data that does not fill a complete block, a
    /// "pending block" is allocated (or reused from a previous sync), the
    /// buffer content is written to it padded with zeros, and the block
    /// pointer is inserted into the mapping chain. When the buffer later
    /// fills to a complete block, `flush_data_block` reuses this pending
    /// block instead of allocating a new one.
    ///
    /// The file entry's `size` field always reflects the true logical byte
    /// count, so readers only access valid data even though the on-disk
    /// pending block is zero-padded.
    pub fn sync_entry(&mut self, handle: FileHandle) -> Result<(), CtfsError> {
        let bs = self.block_size as usize;
        let file_idx = handle.0;

        if !self.files[file_idx].buffer.is_empty() {
            // Allocate a pending block on the first sync; reuse on subsequent ones.
            if self.files[file_idx].pending_block.is_none() {
                let n = self.block_size as u64 / 8;
                let usable = n - 1;
                let block_index = self.files[file_idx].mapping.data_block_count;
                let root_block = self.files[file_idx].mapping.root_block;

                let data_block = self.allocator.alloc();
                self.insert_data_block_chain(
                    root_block, block_index, data_block, usable, self.block_size,
                )?;
                self.files[file_idx].pending_block = Some(data_block);
            }

            // Write current buffer contents to the pending block (padded).
            let data_block = self.files[file_idx].pending_block.unwrap();
            let offset = data_block as u64 * bs as u64;
            self.writer.seek(SeekFrom::Start(offset))?;
            let mut padded = self.files[file_idx].buffer.clone();
            padded.resize(bs, 0);
            self.writer.write_all(&padded)?;
        }

        // Write the file entry with the full logical size so readers can
        // see all bytes written so far.
        let file = &self.files[file_idx];
        let entry = crate::file_entry::FileEntry {
            size: file.size,
            map_block: file.mapping.root_block,
            name: file.name_encoded,
        };
        let entry_offset =
            self.entries_offset + (file.entry_index as u64) * FILE_ENTRY_SIZE as u64;
        self.writer.seek(SeekFrom::Start(entry_offset))?;
        entry.write_to(&mut self.writer)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Close the container, flushing all buffered data and writing metadata.
    pub fn close(mut self) -> Result<(), CtfsError> {
        // Flush remaining buffered data for each file
        let file_count = self.files.len();
        for i in 0..file_count {
            let buffer = std::mem::take(&mut self.files[i].buffer);
            if !buffer.is_empty() {
                self.flush_data_block(i, &buffer)?;
            }
        }

        // Update file entries in root block
        for file in &self.files {
            let entry = FileEntry {
                size: file.size,
                map_block: file.mapping.root_block,
                name: file.name_encoded,
            };
            let entry_offset = self.entries_offset + (file.entry_index as u64) * FILE_ENTRY_SIZE as u64;
            self.writer.seek(SeekFrom::Start(entry_offset))?;
            entry.write_to(&mut self.writer)?;
        }

        self.writer.flush()?;
        Ok(())
    }
}

/// Read partial data from the last data block of a file using the bottom-up chain model.
/// Used during open_append to restore the write buffer.
fn read_last_data_block_chain(
    file: &mut File,
    root_block: u64,
    block_index: u64,
    block_size: u32,
    partial_bytes: usize,
) -> Result<Vec<u8>, CtfsError> {
    let n = block_size as u64 / 8;
    let usable = n - 1;

    // Navigate the chain to find the data block
    let data_block = resolve_block_chain(file, root_block, block_index, block_size)?;

    // Read the partial data from the data block
    let data_offset = data_block * block_size as u64;
    file.seek(SeekFrom::Start(data_offset))?;
    let mut data = vec![0u8; partial_bytes];
    file.read_exact(&mut data)?;

    let _ = usable; // suppress warning
    Ok(data)
}

/// Resolve a block index to a physical data block number using the bottom-up chain model.
/// This is a standalone function that works on a raw File (used by open_append).
fn resolve_block_chain(
    file: &mut File,
    root_block: u64,
    block_index: u64,
    block_size: u32,
) -> Result<u64, CtfsError> {
    let n = block_size as u64 / 8;
    let usable = n - 1;

    let mut idx = block_index;
    let mut current_level_block = root_block;
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
        // Follow chain pointer at entry[N-1]
        let chain_ptr = read_file_ptr(file, current_level_block, usable as usize, block_size)?;
        if chain_ptr == 0 {
            return Err(CtfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("null chain pointer at block {} following to level {}", current_level_block, level),
            )));
        }
        current_level_block = chain_ptr;
    }

    // Navigate down within this level's block to find the data block
    navigate_to_data_block(file, current_level_block, level, idx, usable, block_size)
}

/// Navigate within a level-k block to find the data block pointer.
fn navigate_to_data_block(
    file: &mut File,
    mapping_block: u64,
    level: u32,
    idx_within_level: u64,
    usable: u64,
    block_size: u32,
) -> Result<u64, CtfsError> {
    if level == 1 {
        let ptr = read_file_ptr(file, mapping_block, idx_within_level as usize, block_size)?;
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

    let child_block = read_file_ptr(file, mapping_block, entry_idx as usize, block_size)?;
    if child_block == 0 {
        return Err(CtfsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("null mapping pointer at block {} index {}", mapping_block, entry_idx),
        )));
    }

    navigate_to_data_block(file, child_block, level - 1, sub_idx, usable, block_size)
}

/// Read a u64 pointer from a block in a raw File.
fn read_file_ptr(file: &mut File, block_num: u64, index: usize, block_size: u32) -> Result<u64, CtfsError> {
    let offset = block_num * block_size as u64 + (index * 8) as u64;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
