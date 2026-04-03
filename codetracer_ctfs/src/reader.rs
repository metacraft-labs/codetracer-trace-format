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

        let bs = self.block_size as u64;
        let ptrs_per_block = bs / 8;

        // Read the mapping block
        let map_offset = entry.map_block * bs;
        self.file.seek(SeekFrom::Start(map_offset))?;
        let mut map_data = vec![0u8; bs as usize];
        self.file.read_exact(&mut map_data)?;

        // Parse block pointers from mapping block
        let num_blocks = ((entry.size + bs - 1) / bs) as usize;
        let mut data = Vec::with_capacity(entry.size as usize);

        for i in 0..num_blocks.min(ptrs_per_block as usize) {
            let ptr_offset = i * 8;
            let block_num = u64::from_le_bytes(
                map_data[ptr_offset..ptr_offset + 8].try_into().unwrap()
            );

            let block_offset = block_num * bs;
            self.file.seek(SeekFrom::Start(block_offset))?;

            let to_read = if i == num_blocks - 1 {
                let remaining = entry.size as usize - data.len();
                remaining.min(bs as usize)
            } else {
                bs as usize
            };

            let mut block_buf = vec![0u8; to_read];
            self.file.read_exact(&mut block_buf)?;
            data.extend_from_slice(&block_buf);
        }

        Ok(data)
    }

    fn find_entry(&self, name: &str) -> Option<&FileEntry> {
        let encoded = crate::base40::base40_encode(name).ok()?;
        self.entries.iter().find(|e| e.name == encoded && !e.is_empty())
    }
}
