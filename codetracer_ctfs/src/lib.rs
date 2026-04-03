pub mod base40;
pub mod block_alloc;
pub mod file_entry;
pub mod header;
pub mod reader;
pub mod writer;

pub use base40::{base40_decode, base40_encode};
pub use reader::CtfsReader;
pub use writer::{CtfsWriter, FileHandle};

use std::fmt;

/// Errors that can occur in CTFS operations.
#[derive(Debug)]
pub enum CtfsError {
    Io(std::io::Error),
    InvalidMagic,
    InvalidVersion(u8),
    InvalidBlockSize(u32),
    FileNotFound(String),
    TooManyFiles,
    NameTooLong(String),
    InvalidBase40Char(char),
}

impl fmt::Display for CtfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CtfsError::Io(e) => write!(f, "I/O error: {}", e),
            CtfsError::InvalidMagic => write!(f, "invalid CTFS magic bytes"),
            CtfsError::InvalidVersion(v) => write!(f, "unsupported CTFS version: {}", v),
            CtfsError::InvalidBlockSize(s) => write!(f, "invalid block size: {}", s),
            CtfsError::FileNotFound(n) => write!(f, "file not found: {}", n),
            CtfsError::TooManyFiles => write!(f, "too many files in container"),
            CtfsError::NameTooLong(n) => write!(f, "filename too long: {}", n),
            CtfsError::InvalidBase40Char(c) => write!(f, "invalid base40 character: {}", c),
        }
    }
}

impl std::error::Error for CtfsError {}

impl From<std::io::Error> for CtfsError {
    fn from(e: std::io::Error) -> Self {
        CtfsError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[test]
    fn test_ctfs_create_write_read() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Create and write
        {
            let mut w = CtfsWriter::create(&path, 4096, 31).unwrap();
            let h = w.add_file("meta.json").unwrap();
            let data: Vec<u8> = (0..100).map(|i| (i % 256) as u8).collect();
            w.write(h, &data).unwrap();
            w.close().unwrap();
        }

        // Read back
        {
            let mut r = CtfsReader::open(&path).unwrap();
            let files = r.list_files();
            assert_eq!(files, vec!["meta.json"]);
            assert_eq!(r.file_size("meta.json"), Some(100));

            let data = r.read_file("meta.json").unwrap();
            let expected: Vec<u8> = (0..100).map(|i| (i % 256) as u8).collect();
            assert_eq!(data, expected);
        }
    }

    #[test]
    fn test_ctfs_31_files() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let mut w = CtfsWriter::create(&path, 4096, 31).unwrap();
            for i in 0..31u32 {
                let name = format!("f{:011}", i);
                let h = w.add_file(&name).unwrap();
                let data = vec![i as u8; 64];
                w.write(h, &data).unwrap();
            }
            w.close().unwrap();
        }

        {
            let mut r = CtfsReader::open(&path).unwrap();
            let files = r.list_files();
            assert_eq!(files.len(), 31);

            for i in 0..31u32 {
                let name = format!("f{:011}", i);
                let data = r.read_file(&name).unwrap();
                assert_eq!(data, vec![i as u8; 64]);
            }
        }
    }

    #[test]
    fn test_ctfs_too_many_files() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut w = CtfsWriter::create(&path, 4096, 31).unwrap();
        for i in 0..31u32 {
            let name = format!("f{:011}", i);
            w.add_file(&name).unwrap();
        }
        let result = w.add_file("f00000000031");
        assert!(result.is_err());
        match result.unwrap_err() {
            CtfsError::TooManyFiles => {}
            other => panic!("expected TooManyFiles, got {:?}", other),
        }
    }

    #[test]
    fn test_ctfs_multi_block_file() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let block_size = 4096u32;
        let data_size = 10 * 1024; // 10KB
        let expected_blocks = (data_size + block_size as usize - 1) / block_size as usize;
        assert_eq!(expected_blocks, 3); // 10KB / 4KB = 2.5, rounds up to 3

        let data: Vec<u8> = (0..data_size).map(|i| (i % 251) as u8).collect();

        {
            let mut w = CtfsWriter::create(&path, block_size, 31).unwrap();
            let h = w.add_file("bigfile.dat").unwrap();
            w.write(h, &data).unwrap();
            w.close().unwrap();
        }

        {
            let mut r = CtfsReader::open(&path).unwrap();
            assert_eq!(r.file_size("bigfile.dat"), Some(data_size as u64));
            let read_data = r.read_file("bigfile.dat").unwrap();
            assert_eq!(read_data.len(), data_size);
            assert_eq!(read_data, data);
        }
    }

    #[test]
    fn test_ctfs_large_file_100mb() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let block_size = 4096u32;
        let file_size = 100 * 1024 * 1024usize; // 100 MB

        // Write 100MB file with recognizable pattern
        {
            let mut w = CtfsWriter::create(&path, block_size, 4).unwrap();
            let h = w.add_file("big.dat").unwrap();

            // Write in 1MB chunks to avoid huge allocations
            let chunk_size = 1024 * 1024;
            let mut written = 0usize;
            while written < file_size {
                let remaining = file_size - written;
                let this_chunk = remaining.min(chunk_size);
                let data: Vec<u8> = (0..this_chunk)
                    .map(|i| ((written + i) % 251) as u8)
                    .collect();
                w.write(h, &data).unwrap();
                written += this_chunk;
            }
            w.close().unwrap();
        }

        // Read back: seek to 50MB and read 4KB
        {
            let mut r = CtfsReader::open(&path).unwrap();
            assert_eq!(r.file_size("big.dat"), Some(file_size as u64));

            let seek_offset = 50 * 1024 * 1024u64;
            let mut buf = vec![0u8; 4096];
            let n = r.read_at("big.dat", seek_offset, &mut buf).unwrap();
            assert_eq!(n, 4096);

            // Verify the pattern
            for i in 0..4096 {
                let expected = ((seek_offset as usize + i) % 251) as u8;
                assert_eq!(buf[i], expected, "mismatch at offset {}", seek_offset as usize + i);
            }
        }
    }

    #[test]
    fn test_ctfs_very_large_file_multilevel() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Use 1024-byte blocks: N=128, usable=127
        // Level 1 capacity = 127 blocks = 127 KB
        // We need >127 blocks to exercise level 2
        let block_size = 1024u32;
        let num_blocks = 200u64; // requires level 2
        let file_size = (num_blocks * block_size as u64) as usize;

        {
            let mut w = CtfsWriter::create(&path, block_size, 4).unwrap();
            let h = w.add_file("multi.dat").unwrap();

            // Write block by block with recognizable pattern
            for block_idx in 0..num_blocks {
                let data: Vec<u8> = (0..block_size as usize)
                    .map(|i| ((block_idx as usize * block_size as usize + i) % 251) as u8)
                    .collect();
                w.write(h, &data).unwrap();
            }
            w.close().unwrap();
        }

        {
            let mut r = CtfsReader::open(&path).unwrap();
            assert_eq!(r.file_size("multi.dat"), Some(file_size as u64));

            // Read the last block
            let last_block_offset = (num_blocks - 1) * block_size as u64;
            let mut buf = vec![0u8; block_size as usize];
            let n = r.read_at("multi.dat", last_block_offset, &mut buf).unwrap();
            assert_eq!(n, block_size as usize);

            for i in 0..block_size as usize {
                let expected = ((last_block_offset as usize + i) % 251) as u8;
                assert_eq!(buf[i], expected, "mismatch at offset {}", last_block_offset as usize + i);
            }

            // Also verify full read
            let all_data = r.read_file("multi.dat").unwrap();
            assert_eq!(all_data.len(), file_size);
            for i in 0..file_size {
                assert_eq!(all_data[i], (i % 251) as u8, "full read mismatch at {}", i);
            }
        }
    }

    #[test]
    fn test_ctfs_append_1000_times() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let block_size = 4096u32;
        let append_size = 1024usize; // 1KB per append
        let append_count = 1000;
        let total_size = append_size * append_count; // 1MB

        // Create file with initial data
        {
            let mut w = CtfsWriter::create(&path, block_size, 4).unwrap();
            let h = w.add_file("append.dat").unwrap();
            let data: Vec<u8> = (0..append_size).map(|i| (i % 251) as u8).collect();
            w.write(h, &data).unwrap();
            w.close().unwrap();
        }

        // Append 999 more times
        for round in 1..append_count {
            let mut w = CtfsWriter::open_append(&path).unwrap();
            let h = w.find_file("append.dat").unwrap();
            let offset = round * append_size;
            let data: Vec<u8> = (0..append_size)
                .map(|i| ((offset + i) % 251) as u8)
                .collect();
            w.append(h, &data).unwrap();
            w.close().unwrap();
        }

        // Verify
        {
            let mut r = CtfsReader::open(&path).unwrap();
            assert_eq!(r.file_size("append.dat"), Some(total_size as u64));

            let all_data = r.read_file("append.dat").unwrap();
            assert_eq!(all_data.len(), total_size);
            for i in 0..total_size {
                assert_eq!(all_data[i], (i % 251) as u8, "mismatch at byte {}", i);
            }
        }
    }

    #[test]
    fn test_ctfs_magic_and_version() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let w = CtfsWriter::create(&path, 4096, 31).unwrap();
            w.close().unwrap();
        }

        // Read raw bytes to verify magic and version
        let mut f = std::fs::File::open(&path).unwrap();
        let mut buf = [0u8; 8];
        f.read_exact(&mut buf).unwrap();

        // Magic bytes
        assert_eq!(&buf[0..5], &[0xC0, 0xDE, 0x72, 0xAC, 0xE2]);
        // Version
        assert_eq!(buf[5], 2);
        // Reserved
        assert_eq!(&buf[6..8], &[0, 0]);
    }
}
