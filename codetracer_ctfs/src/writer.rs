use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use crate::base40::base40_encode;
use crate::block_alloc::BlockAllocator;
use crate::file_entry::{FileEntry, FILE_ENTRY_SIZE};
use crate::header::{ExtendedHeader, Header, EXTENDED_HEADER_SIZE, HEADER_SIZE};
use crate::CtfsError;

/// Opaque handle to an open file within a CTFS container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHandle(pub(crate) usize);

/// State for an open file being written.
#[derive(Debug)]
struct OpenFile {
    entry_index: usize,
    name_encoded: u64,
    map_block: u64,
    /// Block numbers allocated for this file's data.
    data_blocks: Vec<u64>,
    /// Total bytes written.
    size: u64,
    /// Buffered partial block data.
    buffer: Vec<u8>,
}

/// Writer for creating CTFS containers.
pub struct CtfsWriter {
    writer: BufWriter<File>,
    block_size: u32,
    max_root_entries: u32,
    allocator: BlockAllocator,
    files: Vec<OpenFile>,
    entries_offset: u64,
}

impl CtfsWriter {
    /// Create a new CTFS container at the given path.
    pub fn create(path: &Path, block_size: u32, max_root_entries: u32) -> Result<Self, CtfsError> {
        let ext_header = ExtendedHeader::new(block_size, max_root_entries)?;
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write header
        let header = Header::new();
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
        })
    }

    /// Add a new named file to the container. Returns a handle for writing.
    pub fn add_file(&mut self, name: &str) -> Result<FileHandle, CtfsError> {
        if self.files.len() >= self.max_root_entries as usize {
            return Err(CtfsError::TooManyFiles);
        }
        let name_encoded = base40_encode(name)?;
        let entry_index = self.files.len();

        // Allocate a mapping block for this file
        let map_block = self.allocator.alloc();

        self.files.push(OpenFile {
            entry_index,
            name_encoded,
            map_block,
            data_blocks: Vec::new(),
            size: 0,
            buffer: Vec::new(),
        });

        Ok(FileHandle(entry_index))
    }

    /// Write data to an open file.
    pub fn write(&mut self, handle: FileHandle, data: &[u8]) -> Result<usize, CtfsError> {
        let bs = self.block_size as usize;
        let file = &mut self.files[handle.0];
        file.buffer.extend_from_slice(data);
        file.size += data.len() as u64;

        // Flush complete blocks
        while file.buffer.len() >= bs {
            let block_num = self.allocator.alloc();
            file.data_blocks.push(block_num);

            let block_data: Vec<u8> = file.buffer.drain(..bs).collect();
            let offset = block_num * self.block_size as u64;
            self.writer.seek(SeekFrom::Start(offset))?;
            self.writer.write_all(&block_data)?;
        }

        Ok(data.len())
    }

    /// Close the container, flushing all buffered data and writing metadata.
    pub fn close(mut self) -> Result<(), CtfsError> {
        let bs = self.block_size as usize;

        // Flush remaining buffered data for each file
        for file in &mut self.files {
            if !file.buffer.is_empty() {
                let block_num = self.allocator.alloc();
                file.data_blocks.push(block_num);

                // Pad to block size
                let mut block_data = std::mem::take(&mut file.buffer);
                block_data.resize(bs, 0);

                let offset = block_num * self.block_size as u64;
                self.writer.seek(SeekFrom::Start(offset))?;
                self.writer.write_all(&block_data)?;
            }
        }

        // Write mapping blocks for each file
        let ptrs_per_block = bs / 8;
        for file in &self.files {
            let offset = file.map_block * self.block_size as u64;
            self.writer.seek(SeekFrom::Start(offset))?;

            // Write data block pointers
            for (i, &block_num) in file.data_blocks.iter().enumerate() {
                if i >= ptrs_per_block {
                    break; // M0: single mapping block only
                }
                self.writer.write_all(&block_num.to_le_bytes())?;
            }
            // Zero-fill the rest of the mapping block
            let written_ptrs = file.data_blocks.len().min(ptrs_per_block);
            let remaining = (ptrs_per_block - written_ptrs) * 8;
            if remaining > 0 {
                let zeros = vec![0u8; remaining];
                self.writer.write_all(&zeros)?;
            }
        }

        // Update file entries in root block
        for file in &self.files {
            let entry = FileEntry {
                size: file.size,
                map_block: file.map_block,
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
