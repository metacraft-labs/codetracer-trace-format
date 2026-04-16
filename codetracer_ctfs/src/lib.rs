pub mod base40;
pub mod block_alloc;
pub mod chunked;
pub mod concurrent_reader;
pub mod concurrent_writer;
pub mod file_entry;
pub mod filemap;
pub mod header;
pub mod reader;
pub mod writer;

pub use base40::{base40_decode, base40_encode};
pub use block_alloc::AtomicBlockAllocator;
pub use chunked::{ChunkedReader, ChunkedWriter};
pub use concurrent_reader::ConcurrentCtfsReader;
pub use concurrent_writer::{ConcurrentCtfsWriter, FileWriter};
pub use header::{
    ChunkIndexEntry, CompressionMethod, EncryptionMethod,
    CHUNK_INDEX_ENTRY_SIZE, DEFAULT_CHUNK_SIZE,
};
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
        // Version (v3)
        assert_eq!(buf[5], 3);
        // Compression = None, Encryption = None
        assert_eq!(&buf[6..8], &[0, 0]);
    }

    // ---- M2: Concurrent Access Tests ----

    #[test]
    fn test_ctfs_concurrent_4_writers() {
        use std::sync::Arc;
        use std::thread;
        use std::time::{Duration, Instant};

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let writer = ConcurrentCtfsWriter::create(&path, 4096, 8).unwrap();
        let num_threads = 4;

        // Each thread gets its own FileWriter
        let mut handles = Vec::new();
        let mut file_writers: Vec<Option<crate::concurrent_writer::FileWriter>> = Vec::new();

        for i in 0..num_threads {
            let name = format!("f{:011}", i);
            let fw = writer.add_file(&name).unwrap();
            file_writers.push(Some(fw));
        }

        // Move each FileWriter to its own thread
        for i in 0..num_threads {
            let writer_ref = Arc::clone(&writer);
            let mut fw = file_writers[i].take().unwrap();
            let handle = thread::spawn(move || {
                let start = Instant::now();
                let duration = Duration::from_secs(2);
                let chunk_size = 1024usize;
                let mut total_written = 0usize;

                while start.elapsed() < duration {
                    let data: Vec<u8> = (0..chunk_size)
                        .map(|j| ((total_written + j) % 251) as u8)
                        .collect();
                    fw.write(&writer_ref, &data).unwrap();
                    total_written += chunk_size;
                }

                fw.flush(&writer_ref).unwrap();
                total_written
            });
            handles.push(handle);
        }

        let sizes: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Close the writer (consumes the Arc — we need to unwrap it)
        let writer = Arc::try_unwrap(writer).expect("all threads should have dropped their Arc refs");
        writer.close().unwrap();

        // Verify all 4 files are readable and not corrupt
        let mut reader = CtfsReader::open(&path).unwrap();
        let files = reader.list_files();
        assert_eq!(files.len(), num_threads);

        for i in 0..num_threads {
            let name = format!("f{:011}", i);
            let data = reader.read_file(&name).unwrap();
            assert_eq!(data.len(), sizes[i], "file {} size mismatch", name);

            // Verify pattern
            for j in 0..data.len() {
                assert_eq!(
                    data[j],
                    (j % 251) as u8,
                    "corruption in file {} at byte {}",
                    name,
                    j
                );
            }
        }
    }

    #[test]
    fn test_ctfs_concurrent_read_write() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;
        use std::time::{Duration, Instant};

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let writer = ConcurrentCtfsWriter::create(&path, 4096, 4).unwrap();
        let mut fw = writer.add_file("stream.dat").unwrap();

        // Write an initial chunk and flush so the reader can see the file
        let initial_data: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        fw.write(&writer, &initial_data).unwrap();
        fw.flush(&writer).unwrap();

        let done = Arc::new(AtomicBool::new(false));
        let done_reader = Arc::clone(&done);
        let reader_path = path.clone();

        // Reader thread: periodically refresh and read data
        let reader_handle = thread::spawn(move || {
            let mut reader = ConcurrentCtfsReader::open(&reader_path).unwrap();
            let mut max_size_seen = 0u64;
            let mut reads_done = 0u64;

            while !done_reader.load(Ordering::Relaxed) {
                reader.refresh().unwrap();
                if let Some(size) = reader.file_size("stream.dat") {
                    if size > 0 {
                        assert!(size >= max_size_seen, "size went backwards: {} < {}", size, max_size_seen);
                        max_size_seen = size;

                        // Read the first 4096 bytes and verify pattern
                        let mut buf = [0u8; 4096];
                        let n = reader.read_at("stream.dat", 0, &mut buf).unwrap();
                        if n == 4096 {
                            for j in 0..4096 {
                                assert_eq!(
                                    buf[j],
                                    (j % 251) as u8,
                                    "reader saw corruption at byte {}",
                                    j
                                );
                            }
                        }
                        reads_done += 1;
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }

            (max_size_seen, reads_done)
        });

        // Writer: keep appending for 2 seconds
        let start = Instant::now();
        let duration = Duration::from_secs(2);
        let mut total_written = initial_data.len();

        while start.elapsed() < duration {
            let chunk: Vec<u8> = (0..4096)
                .map(|j| ((total_written + j) % 251) as u8)
                .collect();
            fw.write(&writer, &chunk).unwrap();
            total_written += 4096;

            // Periodically flush so the reader can see progress
            if total_written % (4096 * 16) == 0 {
                fw.flush(&writer).unwrap();
            }
        }

        fw.flush(&writer).unwrap();
        done.store(true, Ordering::Relaxed);

        let (max_size_seen, reads_done) = reader_handle.join().unwrap();

        let writer = Arc::try_unwrap(writer).unwrap();
        writer.close().unwrap();

        // Verify the reader saw increasing sizes and did some reads
        assert!(max_size_seen > 0, "reader never saw any data");
        assert!(reads_done > 0, "reader never completed a read");

        // Full verification via standard reader
        let mut reader = CtfsReader::open(&path).unwrap();
        let data = reader.read_file("stream.dat").unwrap();
        assert_eq!(data.len(), total_written);
        for j in 0..data.len() {
            assert_eq!(data[j], (j % 251) as u8, "corruption at byte {}", j);
        }
    }

    #[test]
    fn test_ctfs_concurrent_8_file_creation() {
        use std::sync::Arc;
        use std::thread;

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let writer = ConcurrentCtfsWriter::create(&path, 4096, 16).unwrap();
        let num_threads = 8;

        let mut handles = Vec::new();
        // Use a barrier to ensure all threads call add_file simultaneously
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));

        for i in 0..num_threads {
            let writer_ref = Arc::clone(&writer);
            let barrier_ref = Arc::clone(&barrier);
            let handle = thread::spawn(move || {
                barrier_ref.wait();
                let name = format!("f{:011}", i);
                let mut fw = writer_ref.add_file(&name).unwrap();

                // Write some data to each file
                let data = vec![i as u8; 1024];
                fw.write(&writer_ref, &data).unwrap();
                fw.flush(&writer_ref).unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        let writer = Arc::try_unwrap(writer).unwrap();
        writer.close().unwrap();

        // Verify all 8 files are present and have correct data
        let mut reader = CtfsReader::open(&path).unwrap();
        let files = reader.list_files();
        assert_eq!(files.len(), num_threads);

        for i in 0..num_threads {
            let name = format!("f{:011}", i);
            let data = reader.read_file(&name).unwrap();
            assert_eq!(data.len(), 1024, "file {} has wrong size", name);
            assert!(
                data.iter().all(|&b| b == i as u8),
                "file {} has wrong data",
                name
            );
        }
    }

    #[test]
    fn test_ctfs_v3_header_with_compression() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let w = CtfsWriter::create_with_compression(
                &path, 4096, 31,
                crate::header::CompressionMethod::Zstd,
            ).unwrap();
            w.close().unwrap();
        }

        // Read raw bytes to verify v3 header
        let mut f = std::fs::File::open(&path).unwrap();
        let mut buf = [0u8; 8];
        f.read_exact(&mut buf).unwrap();

        // Magic bytes
        assert_eq!(&buf[0..5], &[0xC0, 0xDE, 0x72, 0xAC, 0xE2]);
        // Version = 3
        assert_eq!(buf[5], 3);
        // Compression = 1 (Zstd)
        assert_eq!(buf[6], 1);
        // Encryption = 0 (None)
        assert_eq!(buf[7], 0);

        // Verify reader can open it and reports correct compression
        let r = CtfsReader::open(&path).unwrap();
        assert_eq!(r.compression(), crate::header::CompressionMethod::Zstd);
        assert_eq!(r.encryption(), crate::header::EncryptionMethod::None);
    }

    #[test]
    fn test_ctfs_chunked_roundtrip() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let event_count = 100;
        let event_size = 48;
        let chunk_size = 10;

        // Build fake events
        let mut events = Vec::new();
        let mut event_sizes = Vec::new();
        let mut first_geids = Vec::new();
        for i in 0..event_count {
            let geid = 1000u64 + i as u64;
            for j in 0..event_size {
                events.push(((geid as usize + j) % 251) as u8);
            }
            event_sizes.push(event_size);
            first_geids.push(geid);
        }

        // Write chunked file into CTFS container
        {
            let mut w = CtfsWriter::create_with_compression(
                &path, 4096, 31,
                crate::header::CompressionMethod::Zstd,
            ).unwrap();
            w.add_file_chunked("events.bin", &events, &event_sizes, &first_geids, chunk_size)
                .unwrap();
            w.close().unwrap();
        }

        // Read back: decompress all
        {
            let mut r = CtfsReader::open(&path).unwrap();
            let decompressed = r.read_file_chunked("events.bin", None).unwrap();
            assert_eq!(decompressed.len(), events.len());
            assert_eq!(decompressed, events);
        }
    }

    #[test]
    fn test_ctfs_chunked_seek_by_geid() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let event_count = 100;
        let event_size = 32;
        let chunk_size = 10;

        let mut events = Vec::new();
        let mut event_sizes = Vec::new();
        let mut first_geids = Vec::new();
        for i in 0..event_count {
            let geid = i as u64;
            for j in 0..event_size {
                events.push(((geid as usize + j) % 251) as u8);
            }
            event_sizes.push(event_size);
            first_geids.push(geid);
        }

        {
            let mut w = CtfsWriter::create_with_compression(
                &path, 4096, 31,
                crate::header::CompressionMethod::Zstd,
            ).unwrap();
            w.add_file_chunked("events.bin", &events, &event_sizes, &first_geids, chunk_size)
                .unwrap();
            w.close().unwrap();
        }

        // Seek to GEID 55
        {
            let mut r = CtfsReader::open(&path).unwrap();
            let chunk_data = r.read_file_chunked("events.bin", Some(55)).unwrap();

            // Should get events 50..59 (chunk starting at GEID 50)
            let expected_start = 50 * event_size;
            let expected_end = 60 * event_size;
            let expected = &events[expected_start..expected_end];
            assert_eq!(chunk_data.len(), expected.len());
            assert_eq!(chunk_data, expected);
        }
    }

    #[test]
    fn test_ctfs_v3_backward_compat_v2() {
        // Create a v2 file by manually writing the header, then verify
        // that the v3 reader can open it.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&path).unwrap();
            // Magic
            f.write_all(&[0xC0, 0xDE, 0x72, 0xAC, 0xE2]).unwrap();
            // Version = 2
            f.write_all(&[2]).unwrap();
            // Reserved bytes (v2 had these as 0x00)
            f.write_all(&[0, 0]).unwrap();
            // Extended header: block_size=4096, max_root_entries=31
            f.write_all(&4096u32.to_le_bytes()).unwrap();
            f.write_all(&31u32.to_le_bytes()).unwrap();
            // Empty file entries (24 bytes each * 31)
            let empty_entries = vec![0u8; 24 * 31];
            f.write_all(&empty_entries).unwrap();
            // Pad to block_size
            let header_and_entries = 8 + 8 + 24 * 31;
            if header_and_entries < 4096 {
                let padding = vec![0u8; 4096 - header_and_entries];
                f.write_all(&padding).unwrap();
            }
        }

        // v3 reader should accept v2 file
        let r = CtfsReader::open(&path).unwrap();
        assert_eq!(r.compression(), crate::header::CompressionMethod::None);
        assert_eq!(r.encryption(), crate::header::EncryptionMethod::None);
    }

    #[test]
    fn test_ctfs_concurrent_stress() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;
        use std::time::{Duration, Instant};

        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let writer = ConcurrentCtfsWriter::create(&path, 4096, 16).unwrap();
        let num_writers = 8;
        let num_readers = 2;

        let done = Arc::new(AtomicBool::new(false));

        // Create all file writers upfront (needs mutex lock)
        let mut file_writers: Vec<Option<crate::concurrent_writer::FileWriter>> = Vec::new();
        for i in 0..num_writers {
            let name = format!("f{:011}", i);
            let fw = writer.add_file(&name).unwrap();
            file_writers.push(Some(fw));
        }

        // Write initial data and flush so readers can find the files
        for i in 0..num_writers {
            let fw = file_writers[i].as_mut().unwrap();
            let data = vec![0u8; 4096];
            fw.write(&writer, &data).unwrap();
            fw.flush(&writer).unwrap();
        }

        let mut writer_handles = Vec::new();
        let mut reader_handles = Vec::new();

        // Spawn writer threads
        for i in 0..num_writers {
            let writer_ref = Arc::clone(&writer);
            let done_ref = Arc::clone(&done);
            let mut fw = file_writers[i].take().unwrap();
            let handle = thread::spawn(move || {
                let start = Instant::now();
                let duration = Duration::from_secs(5);
                let mut total_written = 4096usize; // account for initial write

                while start.elapsed() < duration && !done_ref.load(Ordering::Relaxed) {
                    let chunk: Vec<u8> = (0..1024)
                        .map(|j| ((total_written + j) % 251) as u8)
                        .collect();
                    fw.write(&writer_ref, &chunk).unwrap();
                    total_written += 1024;

                    // Periodic flush
                    if total_written % (1024 * 32) == 0 {
                        fw.flush(&writer_ref).unwrap();
                    }
                }

                fw.flush(&writer_ref).unwrap();
                total_written
            });
            writer_handles.push(handle);
        }

        // Spawn reader threads
        for _ in 0..num_readers {
            let done_ref = Arc::clone(&done);
            let reader_path = path.clone();
            let handle = thread::spawn(move || {
                let mut reader = ConcurrentCtfsReader::open(&reader_path).unwrap();
                let mut reads = 0u64;

                while !done_ref.load(Ordering::Relaxed) {
                    reader.refresh().unwrap();
                    let files = reader.list_files();

                    for name in &files {
                        if let Some(size) = reader.file_size(name) {
                            if size > 0 {
                                // Read the first block
                                let mut buf = [0u8; 4096];
                                let _ = reader.read_at(name, 0, &mut buf);
                                reads += 1;
                            }
                        }
                    }

                    thread::sleep(Duration::from_millis(5));
                }
                reads
            });
            reader_handles.push(handle);
        }

        // Wait for writer threads to finish
        let mut writer_sizes = Vec::new();
        for handle in writer_handles {
            writer_sizes.push(handle.join().unwrap());
        }

        // Signal readers to stop
        done.store(true, Ordering::Relaxed);

        // Wait for reader threads
        let mut total_reads = 0u64;
        for handle in reader_handles {
            total_reads += handle.join().unwrap();
        }

        let writer = Arc::try_unwrap(writer).unwrap();
        writer.close().unwrap();

        // Verify all files are readable
        let mut reader = CtfsReader::open(&path).unwrap();
        let files = reader.list_files();
        assert_eq!(files.len(), num_writers);

        for i in 0..num_writers {
            let name = format!("f{:011}", i);
            let data = reader.read_file(&name).unwrap();
            assert_eq!(
                data.len(),
                writer_sizes[i],
                "file {} size mismatch: expected {}, got {}",
                name,
                writer_sizes[i],
                data.len()
            );
        }

        assert!(total_reads > 0, "readers should have completed some reads");
    }
}
