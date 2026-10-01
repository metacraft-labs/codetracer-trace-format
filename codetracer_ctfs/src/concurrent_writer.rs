use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::base40::base40_encode;
use crate::block_alloc::AtomicBlockAllocator;
use crate::file_entry::{MemberLayout, FILE_ENTRY_SIZE};
use crate::header::{ExtendedHeader, Header, EXTENDED_HEADER_SIZE, HEADER_SIZE};
use crate::pread_compat::{pread_exact, pwrite_all};
use crate::CtfsError;

/// State for a file entry tracked in the root table.
#[derive(Debug)]
struct FileEntryState {
    name_encoded: u64,
    /// The committed `MapBlock` (see [`MemberLayout`]), updated on flush.
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
///
/// Lays a member out exactly as `CtfsWriter` does (`ctfs-container.md` §2):
/// empty until written, one direct data block while it fits one block, and a
/// mapping — claimed before the new data blocks, with the direct block in slot
/// 0 — once it outgrows it. Blocks are claimed by the append that reaches
/// them.
pub struct FileWriter {
    file_index: usize,
    name_encoded: u64,
    layout: MemberLayout,
    /// Data blocks whose bytes are complete on disk.
    data_block_count: u64,
    /// Total logical bytes written.
    size: u64,
    /// Bytes of the partial last block.
    buffer: Vec<u8>,
    /// The block claimed for logical block `data_block_count`, which `buffer`
    /// fills.
    ///
    /// A flush has to make the partial block visible to readers, but it must
    /// **not** consume a logical block index: the bytes that arrive next belong
    /// to the same logical block, because every reader resolves logical byte
    /// `p` to logical block `p / block_size`. So the block is claimed once,
    /// its pointer placed at the *current* `data_block_count`, and the block
    /// rewritten in place on each further flush until the buffer fills, which
    /// is the point at which the index is consumed.
    ///
    /// This mirrors `CtfsWriter`'s `pending_block` in `writer.rs`; the two
    /// writers must lay out the same blocks for the same byte stream.
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

        // Through `Header::write_to`, not by hand, so the header layout has
        // one definition.
        let header = Header::new();
        let mut header_bytes = Vec::with_capacity(crate::header::HEADER_SIZE);
        header.write_to(&mut header_bytes)?;
        root_block[0..header_bytes.len()].copy_from_slice(&header_bytes);

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

        // Created empty: the name is written and no block is claimed
        // (`ctfs-container.md` §5, "Creating a File").
        entries.push(FileEntryState {
            name_encoded,
            map_block: 0,
            size: 0,
        });
        let entry_offset = self.entries_offset + (file_index as u64) * FILE_ENTRY_SIZE as u64;
        pwrite_all(&self.file, &name_encoded.to_le_bytes(), entry_offset + 16)?;

        Ok(FileWriter {
            file_index,
            name_encoded,
            layout: MemberLayout::Empty,
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

/// What a mapping insert is placing, as opposed to where it is placing it.
///
/// These four are invariant across the descent: the file and allocator being
/// written to, the data block being pointed at, and the block geometry. Only
/// the position — mapping block, level, index within it — changes per level,
/// so keeping them apart makes each recursive call state where it is going
/// rather than restate what it is carrying.
struct MappingInsert<'a> {
    parent: &'a ConcurrentCtfsWriter,
    data_block: u64,
    usable: u64,
    bs: u32,
}

impl FileWriter {
    /// Write data to this file (appends to end), claiming every block the
    /// appended bytes reach.
    pub fn write(&mut self, parent: &ConcurrentCtfsWriter, data: &[u8]) -> Result<usize, CtfsError> {
        let bs = self.block_size as usize;
        self.buffer.extend_from_slice(data);
        self.size += data.len() as u64;

        while self.buffer.len() >= bs {
            let block = match self.pending_block.take() {
                Some(b) => b,
                None => self.claim_data_block(parent)?,
            };
            let block_data: Vec<u8> = self.buffer.drain(..bs).collect();
            write_block_data_at(&parent.file, block, &block_data, self.block_size)?;
            self.data_block_count += 1;
        }
        if !self.buffer.is_empty() && self.pending_block.is_none() {
            self.pending_block = Some(self.claim_data_block(parent)?);
        }

        Ok(data.len())
    }

    /// Publish the bytes written so far: the partial block's bytes, then the
    /// entry's `MapBlock`, then its `Size` (`ctfs-container.md` §6, "Writer
    /// Protocol"), so a reader that sees the new `Size` sees the layout that
    /// holds it.
    pub fn flush(&mut self, parent: &ConcurrentCtfsWriter) -> Result<(), CtfsError> {
        if let Some(block) = self.pending_block {
            // Rewritten whole each time, so stale padding from an earlier
            // flush of the same block cannot survive under later bytes.
            write_block_data_at(&parent.file, block, &self.buffer, self.block_size)?;
        }

        let map_block = self.layout.map_block();
        {
            let mut entries = parent.file_entries.lock().unwrap();
            entries[self.file_index].map_block = map_block;
            entries[self.file_index].size = self.size;
        }

        let entry_offset = parent.entries_offset + (self.file_index as u64) * FILE_ENTRY_SIZE as u64;
        pwrite_all(&parent.file, &map_block.to_le_bytes(), entry_offset + 8)?;
        pwrite_all(&parent.file, &self.size.to_le_bytes(), entry_offset)?;
        pwrite_all(&parent.file, &self.name_encoded.to_le_bytes(), entry_offset + 16)?;

        Ok(())
    }

    /// Claim the data block for logical block `data_block_count`, moving the
    /// member to the layout its size now needs: a direct block while it fits
    /// one block; otherwise a mapping block first (holding the direct block in
    /// slot 0, if there was one), then the data block.
    fn claim_data_block(&mut self, parent: &ConcurrentCtfsWriter) -> Result<u64, CtfsError> {
        let bs = self.block_size;
        let usable = bs as u64 / 8 - 1;
        let block_index = self.data_block_count;
        let mapping = match self.layout {
            MemberLayout::Empty if self.size <= bs as u64 => {
                let b = parent.allocator.allocate();
                self.layout = MemberLayout::Direct(b);
                return Ok(b);
            }
            MemberLayout::Empty | MemberLayout::Direct(_) => {
                let m = parent.allocator.allocate();
                write_zero_block_at(&parent.file, m, bs)?;
                if let MemberLayout::Direct(b) = self.layout {
                    write_ptr_at(&parent.file, m, 0, b, bs)?;
                }
                m
            }
            MemberLayout::Mapped(m) => m,
        };
        self.layout = MemberLayout::Mapped(mapping);
        let data_block = parent.allocator.allocate();
        self.insert_data_block_chain(parent, mapping, block_index, data_block, usable, bs)?;
        Ok(data_block)
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

        let insert = MappingInsert {
            parent,
            data_block,
            usable,
            bs,
        };
        self.navigate_and_insert(&insert, current_level_block, level, idx)
    }

    /// Navigate within a level-k block to insert a data block pointer.
    ///
    /// `insert` carries what is being placed; the three loose parameters carry
    /// where the descent currently is, and are the only things that change per
    /// level.
    fn navigate_and_insert(&self, insert: &MappingInsert<'_>, mapping_block: u64, level: u32, idx_within_level: u64) -> Result<(), CtfsError> {
        let MappingInsert {
            parent,
            data_block,
            usable,
            bs,
        } = *insert;
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

        self.navigate_and_insert(insert, target_block, level - 1, sub_idx)
    }
}
