use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Read as IoRead, Seek, SeekFrom, Write};
use std::path::Path;

use crate::base40::base40_encode;
use crate::block_alloc::BlockAllocator;
use crate::file_entry::{FileEntry, MemberLayout, FILE_ENTRY_SIZE};
use crate::header::{CompressionMethod, ExtendedHeader, Header, EXTENDED_HEADER_SIZE, HEADER_SIZE};
use crate::CtfsError;

/// The random-access byte store a [`CtfsWriter`] lays its container out in.
///
/// CTFS is not a stream format: the writer revisits blocks it has already
/// emitted — it back-patches mapping-chain pointers and rewrites the root
/// block's file-entry table on every `sync_entry`/`close`. So a plain
/// `impl Write` sink is not enough; the store must also seek and read back
/// what it wrote. That is exactly `Write + Seek + Read`.
///
/// Two implementations ship: [`FileStore`] (the host default, unchanged
/// behaviour and unchanged output bytes) and [`MemoryStore`], which keeps the
/// container in a `Vec<u8>`. The in-memory store is what makes the writer
/// usable on `wasm32-unknown-unknown`, where there is no filesystem at all.
/// `Send` is a supertrait so `CtfsWriter` — and every trace writer built on
/// it — stays `Send`, which `create_trace_writer` returns as
/// `Box<dyn TraceWriter + Send>`. Both shipped stores are `Send` already.
pub trait CtfsStore: Write + Seek + IoRead + Send {
    /// Take the finished container bytes, if this store holds them in memory.
    ///
    /// File-backed stores return `None` — their bytes are on disk.
    fn take_bytes(&mut self) -> Option<Vec<u8>> {
        None
    }
}

/// A file-backed [`CtfsStore`] — the host default.
///
/// Wraps `BufWriter<File>` exactly as the writer always did, so the emitted
/// bytes and the I/O pattern are unchanged. `Read` flushes the buffer first,
/// matching the `flush(); seek(); get_mut().read_exact()` sequence the writer
/// used before this abstraction existed.
pub struct FileStore {
    inner: BufWriter<File>,
}

impl FileStore {
    pub fn new(file: File) -> Self {
        FileStore { inner: BufWriter::new(file) }
    }
}

impl Write for FileStore {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for FileStore {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl IoRead for FileStore {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.flush()?;
        self.inner.get_mut().read(buf)
    }
}

impl CtfsStore for FileStore {}

/// A `Vec<u8>`-backed [`CtfsStore`].
///
/// Produces the same container bytes a [`FileStore`] would, without touching
/// a filesystem — the writer's seek-and-back-patch pattern maps directly onto
/// `Cursor<Vec<u8>>`, which zero-fills any gap left by seeking past the end.
pub struct MemoryStore {
    inner: Cursor<Vec<u8>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            inner: Cursor::new(Vec::new()),
        }
    }

    /// Borrow the bytes written so far.
    pub fn as_slice(&self) -> &[u8] {
        self.inner.get_ref()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Write for MemoryStore {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for MemoryStore {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl IoRead for MemoryStore {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl CtfsStore for MemoryStore {
    fn take_bytes(&mut self) -> Option<Vec<u8>> {
        Some(std::mem::take(self.inner.get_mut()))
    }
}

/// Opaque handle to an open file within a CTFS container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHandle(pub(crate) usize);

/// State for an open file being written.
///
/// # Layout (`ctfs-container.md` §2, "`MapBlock` has three forms")
///
/// `layout` is the member's `MapBlock` in decoded form. A member starts
/// [`MemberLayout::Empty`] and owns no block; its first data block makes it
/// [`MemberLayout::Direct`]; the append that takes it past one block claims a
/// level-1 mapping block, puts the direct block in slot 0 and makes it
/// [`MemberLayout::Mapped`] — the mapping block is claimed before any new data
/// block, so two writers given the same appends allocate the same blocks.
///
/// Blocks are claimed when bytes are appended, not when they are flushed: the
/// partial last block is claimed by the append that starts it and held in
/// `pending_block`, its bytes in `buffer` until the block fills, an entry is
/// synced, or the container closes.
#[derive(Debug)]
struct OpenFile {
    entry_index: usize,
    name_encoded: u64,
    layout: MemberLayout,
    /// Data blocks whose bytes are complete on disk.
    data_block_count: u64,
    /// Total bytes written.
    size: u64,
    /// Bytes of the partial last block, not yet a whole block.
    buffer: Vec<u8>,
    /// The block claimed for logical block `data_block_count`, which `buffer`
    /// fills. Its pointer is already in the member's mapping (or is the direct
    /// block), so it is written in place on each sync and when it fills.
    pending_block: Option<u64>,
}

/// Writer for creating CTFS containers.
pub struct CtfsWriter {
    writer: Box<dyn CtfsStore>,
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
fn read_block(writer: &mut dyn CtfsStore, block_num: u64, block_size: u32) -> Result<Vec<u8>, CtfsError> {
    writer.flush()?;
    let offset = block_num * block_size as u64;
    writer.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; block_size as usize];
    writer.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read a u64 pointer at a given index within a block.
fn read_ptr(block_data: &[u8], index: usize) -> u64 {
    let off = index * 8;
    u64::from_le_bytes(block_data[off..off + 8].try_into().unwrap())
}

/// Write a u64 pointer at a given index within a block on disk.
fn write_ptr(writer: &mut dyn CtfsStore, block_num: u64, index: usize, value: u64, block_size: u32) -> Result<(), CtfsError> {
    let offset = block_num * block_size as u64 + (index * 8) as u64;
    writer.seek(SeekFrom::Start(offset))?;
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

/// Write a zero-filled block.
fn write_zero_block(writer: &mut dyn CtfsStore, block_num: u64, block_size: u32) -> Result<(), CtfsError> {
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
    pub fn create_with_compression(path: &Path, block_size: u32, max_root_entries: u32, compression: CompressionMethod) -> Result<Self, CtfsError> {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?;
        Self::create_in_store(Box::new(FileStore::new(file)), block_size, max_root_entries, compression)
    }

    /// Create a new CTFS container held entirely in memory.
    ///
    /// Identical to [`create`](Self::create) apart from where the bytes land:
    /// the container is laid out in a `Vec<u8>`, retrieved at the end with
    /// [`finish_to_bytes`](Self::finish_to_bytes). This is the constructor to
    /// use on `wasm32-unknown-unknown`, which has no filesystem — but it is a
    /// perfectly ordinary host constructor too, and produces the same bytes
    /// the file-backed writer would.
    pub fn create_in_memory(block_size: u32, max_root_entries: u32, compression: CompressionMethod) -> Result<Self, CtfsError> {
        Self::create_in_store(Box::new(MemoryStore::new()), block_size, max_root_entries, compression)
    }

    /// Create a new CTFS container in an arbitrary [`CtfsStore`].
    pub fn create_in_store(
        store: Box<dyn CtfsStore>,
        block_size: u32,
        max_root_entries: u32,
        compression: CompressionMethod,
    ) -> Result<Self, CtfsError> {
        let ext_header = ExtendedHeader::new(block_size, max_root_entries)?;
        let mut writer = store;

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
        let next_block = file_len.div_ceil(ext_header.block_size as u64);

        let mut allocator = BlockAllocator::new();
        // Advance allocator to the next free block
        while allocator.next() < next_block {
            allocator.alloc();
        }

        let bs = ext_header.block_size as u64;

        let mut files = Vec::new();
        for (i, entry) in entries.iter().enumerate() {
            if entry.is_empty() {
                continue;
            }
            let name = crate::base40::base40_decode(entry.name);
            let damaged = |what: String| CtfsError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, what));
            let partial_bytes = (entry.size % bs) as usize;
            let full_blocks = entry.size / bs;
            let (data_block_count, buffer, pending_block) = match entry.layout() {
                MemberLayout::Empty => {
                    if entry.size != 0 {
                        return Err(damaged(format!(
                            "internal file {name} has {} bytes but a null MapBlock; its data cannot be located",
                            entry.size
                        )));
                    }
                    (0, Vec::new(), None)
                }
                MemberLayout::Direct(b) => {
                    if b == 0 || entry.size > bs {
                        return Err(damaged(format!(
                            "internal file {name} is a single data block {b} holding {} bytes, which one {bs}-byte \
                             block cannot be (a null block, or more than one block of data)",
                            entry.size
                        )));
                    }
                    if entry.size == bs {
                        (1, Vec::new(), None)
                    } else {
                        // The block is the member's partial last block.
                        (0, read_block_prefix(&mut file, b, ext_header.block_size, partial_bytes)?, Some(b))
                    }
                }
                MemberLayout::Mapped(m) => {
                    if partial_bytes == 0 {
                        (full_blocks, Vec::new(), None)
                    } else {
                        // Resume the partial last block in place: its pointer
                        // is already in the mapping.
                        let b = resolve_block_chain(&mut file, m, full_blocks, ext_header.block_size)?;
                        (
                            full_blocks,
                            read_block_prefix(&mut file, b, ext_header.block_size, partial_bytes)?,
                            Some(b),
                        )
                    }
                }
            };
            files.push(OpenFile {
                entry_index: i,
                name_encoded: entry.name,
                layout: entry.layout(),
                data_block_count,
                size: entry.size,
                buffer,
                pending_block,
            });
        }

        let writer: Box<dyn CtfsStore> = Box::new(FileStore::new(file));

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

        // A member is created empty and claims no block until it is written
        // (`ctfs-container.md` §5, "Creating a File").
        self.files.push(OpenFile {
            entry_index,
            name_encoded,
            layout: MemberLayout::Empty,
            data_block_count: 0,
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
    ///
    /// Claims every block the appended bytes reach before returning, in file
    /// order — the mapping block first when this append takes the member past
    /// one block (`ctfs-container.md` §5, "Appending Data").
    pub fn write(&mut self, handle: FileHandle, data: &[u8]) -> Result<usize, CtfsError> {
        let bs = self.block_size as usize;
        let fi = handle.0;
        self.files[fi].buffer.extend_from_slice(data);
        self.files[fi].size += data.len() as u64;

        while self.files[fi].buffer.len() >= bs {
            let block = match self.files[fi].pending_block.take() {
                Some(b) => b,
                None => self.claim_data_block(fi)?,
            };
            let block_data: Vec<u8> = self.files[fi].buffer.drain(..bs).collect();
            self.write_block_data(block, &block_data)?;
            self.files[fi].data_block_count += 1;
        }
        if !self.files[fi].buffer.is_empty() && self.files[fi].pending_block.is_none() {
            let block = self.claim_data_block(fi)?;
            self.files[fi].pending_block = Some(block);
        }

        Ok(data.len())
    }

    /// Append data to an existing file (alias for write, used after open_append).
    pub fn append(&mut self, handle: FileHandle, data: &[u8]) -> Result<usize, CtfsError> {
        self.write(handle, data)
    }

    /// Claim the data block for logical block `data_block_count` of file
    /// `fi`, moving the member to the layout its size now needs.
    fn claim_data_block(&mut self, fi: usize) -> Result<u64, CtfsError> {
        let bs = self.block_size;
        let usable = bs as u64 / 8 - 1;
        let block_index = self.files[fi].data_block_count;
        let mapping = match self.files[fi].layout {
            MemberLayout::Empty if self.files[fi].size <= bs as u64 => {
                let b = self.allocator.alloc();
                self.files[fi].layout = MemberLayout::Direct(b);
                return Ok(b);
            }
            // A first write longer than one block: the mapping block first,
            // then the data blocks.
            MemberLayout::Empty => self.claim_mapping_block(None)?,
            // The direct-to-mapped transition: the direct block becomes slot 0.
            MemberLayout::Direct(b) => self.claim_mapping_block(Some(b))?,
            MemberLayout::Mapped(m) => m,
        };
        self.files[fi].layout = MemberLayout::Mapped(mapping);
        let data_block = self.allocator.alloc();
        self.insert_data_block_chain(mapping, block_index, data_block, usable, bs)?;
        Ok(data_block)
    }

    /// Claim and zero a level-1 mapping block, with `slot0` in its first slot.
    fn claim_mapping_block(&mut self, slot0: Option<u64>) -> Result<u64, CtfsError> {
        let m = self.allocator.alloc();
        write_zero_block(&mut *self.writer, m, self.block_size)?;
        if let Some(b) = slot0 {
            write_ptr(&mut *self.writer, m, 0, b, self.block_size)?;
        }
        Ok(m)
    }

    /// Write `data` into `block`, padded to the block size.
    fn write_block_data(&mut self, block: u64, data: &[u8]) -> Result<(), CtfsError> {
        let bs = self.block_size as usize;
        self.writer.seek(SeekFrom::Start(block * bs as u64))?;
        let mut padded = data.to_vec();
        padded.resize(bs, 0);
        self.writer.write_all(&padded)?;
        Ok(())
    }

    /// Store file `fi`'s root entry: `MapBlock` before `Size`
    /// (`ctfs-container.md` §6), then the name.
    fn write_entry(&mut self, fi: usize) -> Result<(), CtfsError> {
        let file = &self.files[fi];
        let entry_offset = self.entries_offset + (file.entry_index as u64) * FILE_ENTRY_SIZE as u64;
        let (size, map_block, name) = (file.size, file.layout.map_block(), file.name_encoded);
        self.writer.seek(SeekFrom::Start(entry_offset + 8))?;
        self.writer.write_all(&map_block.to_le_bytes())?;
        self.writer.seek(SeekFrom::Start(entry_offset))?;
        self.writer.write_all(&size.to_le_bytes())?;
        self.writer.seek(SeekFrom::Start(entry_offset + 16))?;
        self.writer.write_all(&name.to_le_bytes())?;
        Ok(())
    }

    /// Insert a data block pointer at the given block_index using the bottom-up chain model.
    ///
    /// The chain works as follows:
    /// - Level 1 (root): entries[0..usable-1] hold direct data block pointers (indices 0..usable-1)
    /// - entries[usable] (= entries[N-1]) points to level-2 block
    /// - Level 2: entries[0..usable-1] each point to a level-1 sub-block (each holds usable data ptrs)
    /// - entries[usable] points to level-3, etc.
    ///
    /// # A null pointer here is not always "not allocated yet"
    ///
    /// Both null branches below allocate a replacement mapping block, which is
    /// correct only when the slot has genuinely never been used. It is *not*
    /// correct on a container whose mapping was damaged — a crash between two
    /// flushes, say — because the replacement overwrites the only pointer to
    /// the existing subtree and every data block under it becomes unreachable
    /// and unrecoverable, while the append reports success.
    ///
    /// The two cases are distinguishable from the index, because a mapping is
    /// filled in strictly increasing block-index order: a pointer may be null
    /// only when the index being placed is the **first index that pointer
    /// covers** (`idx == 0` after rebasing at a level, `sub_idx == 0` within a
    /// child). Any other index means an earlier insert already went through
    /// this pointer, so a zero is damage. `CTFS-Binary-Format.md` §4, "Null
    /// pointers during allocation", states the rule normatively; it is pinned
    /// by `tests/writer_null_data_block.rs`.
    fn insert_data_block_chain(&mut self, root_block: u64, block_index: u64, data_block: u64, usable: u64, bs: u32) -> Result<(), CtfsError> {
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
                return Err(CtfsError::Io(std::io::Error::other("file too large: exceeds 5-level mapping")));
            }

            // Follow or create the chain pointer from current_level_block[N-1]
            // to the next higher level block
            let block_data = read_block(&mut *self.writer, current_level_block, bs)?;
            let chain_ptr = read_ptr(&block_data, usable as usize);
            if chain_ptr == 0 {
                // Only legitimate for the first index this chain pointer covers.
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
                // Allocate the higher-level block
                let new_block = self.allocator.alloc();
                write_zero_block(&mut *self.writer, new_block, bs)?;
                write_ptr(&mut *self.writer, current_level_block, usable as usize, new_block, bs)?;
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
            debug_assert!(idx_within_level < usable, "idx {} >= usable {} at level 1", idx_within_level, usable);
            write_ptr(&mut *self.writer, mapping_block, idx_within_level as usize, data_block, bs)?;
            return Ok(());
        }

        // Level k > 1: each entry covers level_capacity(usable, level-1) data blocks
        let sub_cap = level_capacity(usable, level - 1);
        let entry_idx = idx_within_level / sub_cap;
        let sub_idx = idx_within_level % sub_cap;

        debug_assert!(entry_idx < usable, "entry_idx {} >= usable {} at level {}", entry_idx, usable, level);

        // Read or allocate the sub-block
        let block_data = read_block(&mut *self.writer, mapping_block, bs)?;
        let child_block = read_ptr(&block_data, entry_idx as usize);

        let target_block = if child_block == 0 {
            // Only legitimate for the first index this child covers; see
            // `insert_data_block_chain`.
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
            let new_block = self.allocator.alloc();
            write_zero_block(&mut *self.writer, new_block, bs)?;
            write_ptr(&mut *self.writer, mapping_block, entry_idx as usize, new_block, bs)?;
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
    /// The partial last block was claimed by the append that started it (its
    /// pointer is already in the mapping, or it is the member's direct block),
    /// so a sync writes the buffered bytes into it, padded with zeros, and the
    /// block is rewritten in place on each later sync and when it fills.
    ///
    /// The file entry's `size` field always reflects the true logical byte
    /// count, so readers only access valid data even though the on-disk
    /// pending block is zero-padded.
    pub fn sync_entry(&mut self, handle: FileHandle) -> Result<(), CtfsError> {
        let file_idx = handle.0;
        if let Some(block) = self.files[file_idx].pending_block {
            let buffer = self.files[file_idx].buffer.clone();
            self.write_block_data(block, &buffer)?;
        }
        self.write_entry(file_idx)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Add a file containing chunked compressed event data.
    ///
    /// This is a convenience method that:
    /// 1. Creates a `ChunkedWriter` to compress events into chunks with inline headers.
    /// 2. Writes the chunked stream as a file in the CTFS container.
    ///
    /// `name`        -- the file name in the container.
    /// `events`      -- concatenated raw serialized event bytes.
    /// `event_sizes` -- byte size of each event.
    /// `first_geids` -- GEID of each event (parallel to `event_sizes`).
    /// `chunk_size`  -- number of events per chunk.
    pub fn add_file_chunked(
        &mut self,
        name: &str,
        events: &[u8],
        event_sizes: &[usize],
        first_geids: &[u64],
        chunk_size: usize,
    ) -> Result<FileHandle, CtfsError> {
        let chunked_writer = crate::chunked::ChunkedWriter::new(self.compression, chunk_size);
        let chunked_data = chunked_writer.write_chunked(events, event_sizes, first_geids)?;

        let handle = self.add_file(name)?;
        self.write(handle, &chunked_data)?;
        Ok(handle)
    }

    /// Close the container and hand back its bytes.
    ///
    /// Equivalent to [`close`](Self::close) for a container created with
    /// [`create_in_memory`](Self::create_in_memory). Errors for a file-backed
    /// container, whose bytes live on disk rather than in the writer.
    pub fn finish_to_bytes(mut self) -> Result<Vec<u8>, CtfsError> {
        self.close_inner()?;
        self.writer.take_bytes().ok_or_else(|| {
            CtfsError::Io(std::io::Error::other(
                "finish_to_bytes: this CTFS container is file-backed; use close() instead",
            ))
        })
    }

    /// Close the container, flushing all buffered data and writing metadata.
    pub fn close(mut self) -> Result<(), CtfsError> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), CtfsError> {
        for i in 0..self.files.len() {
            if let Some(block) = self.files[i].pending_block.take() {
                let buffer = std::mem::take(&mut self.files[i].buffer);
                self.write_block_data(block, &buffer)?;
                self.files[i].data_block_count += 1;
            }
            self.write_entry(i)?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

/// Read the first `len` bytes of `block`. Used by `open_append` to restore a
/// member's partial last block into its write buffer.
fn read_block_prefix(file: &mut File, block: u64, block_size: u32, len: usize) -> Result<Vec<u8>, CtfsError> {
    file.seek(SeekFrom::Start(block * block_size as u64))?;
    let mut data = vec![0u8; len];
    file.read_exact(&mut data)?;
    Ok(data)
}

/// Resolve a block index to a physical data block number using the bottom-up chain model.
/// This is a standalone function that works on a raw File (used by open_append).
fn resolve_block_chain(file: &mut File, root_block: u64, block_index: u64, block_size: u32) -> Result<u64, CtfsError> {
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
