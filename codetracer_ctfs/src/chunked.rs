use std::io::Cursor;

use crate::header::{ChunkIndexEntry, CHUNK_INDEX_ENTRY_SIZE, CompressionMethod};
use crate::CtfsError;

/// Default zstd compression level.
const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Write events as independently-compressed chunks with inline headers.
///
/// Each chunk in the output stream has the layout:
///   [Header: 16 bytes][CompressedData: compressed_size bytes]
///
/// Header (little-endian):
///   compressed_size: u32  -- size of compressed data following this header
///   event_count:     u32  -- number of events in this chunk
///   first_geid:      u64  -- GEID of the first event in this chunk
pub struct ChunkedWriter {
    compression: CompressionMethod,
    chunk_size: usize,
    level: i32,
}

impl ChunkedWriter {
    /// Create a new chunked writer.
    ///
    /// `compression` -- the compression method (only Zstd is supported for actual compression).
    /// `chunk_size`  -- number of events per chunk.
    pub fn new(compression: CompressionMethod, chunk_size: usize) -> Self {
        ChunkedWriter {
            compression,
            chunk_size,
            level: DEFAULT_ZSTD_LEVEL,
        }
    }

    /// Set the zstd compression level (1..=22, default 3).
    pub fn with_level(mut self, level: i32) -> Self {
        self.level = level;
        self
    }

    /// Compress event data into a chunked stream with inline headers.
    ///
    /// `events`      -- concatenated raw serialized event bytes.
    /// `event_sizes` -- byte size of each event (so we know where boundaries are).
    /// `first_geids` -- GEID of each event (parallel to `event_sizes`).
    ///
    /// Returns the chunked byte stream ready to be stored in a CTFS file.
    pub fn write_chunked(
        &self,
        events: &[u8],
        event_sizes: &[usize],
        first_geids: &[u64],
    ) -> Result<Vec<u8>, CtfsError> {
        assert_eq!(
            event_sizes.len(),
            first_geids.len(),
            "event_sizes and first_geids must have the same length"
        );

        let total_events = event_sizes.len();
        let mut output = Vec::new();
        let mut event_offset = 0usize;
        let mut event_idx = 0usize;

        while event_idx < total_events {
            let chunk_event_count = self.chunk_size.min(total_events - event_idx);
            let chunk_first_geid = first_geids[event_idx];

            // Gather the raw bytes for this chunk's events
            let mut chunk_raw_size = 0usize;
            for i in 0..chunk_event_count {
                chunk_raw_size += event_sizes[event_idx + i];
            }
            let chunk_raw = &events[event_offset..event_offset + chunk_raw_size];

            // Compress
            let compressed = match self.compression {
                CompressionMethod::Zstd => {
                    zstd::encode_all(Cursor::new(chunk_raw), self.level)
                        .map_err(|e| CtfsError::Io(e))?
                }
                _ => chunk_raw.to_vec(),
            };

            // Write inline header (16 bytes, little-endian)
            let compressed_size = compressed.len() as u32;
            output.extend_from_slice(&compressed_size.to_le_bytes());
            output.extend_from_slice(&(chunk_event_count as u32).to_le_bytes());
            output.extend_from_slice(&chunk_first_geid.to_le_bytes());

            // Write compressed data
            output.extend_from_slice(&compressed);

            event_offset += chunk_raw_size;
            event_idx += chunk_event_count;
        }

        Ok(output)
    }
}

/// Read chunks from a chunked stream.
pub struct ChunkedReader;

impl ChunkedReader {
    /// Decompress all chunks and return the full concatenated event data.
    pub fn decompress_all(data: &[u8]) -> Result<Vec<u8>, CtfsError> {
        let mut output = Vec::new();
        let mut offset = 0usize;

        while offset + CHUNK_INDEX_ENTRY_SIZE <= data.len() {
            let header = Self::read_header_at(data, offset)?;
            offset += CHUNK_INDEX_ENTRY_SIZE;

            let end = offset + header.compressed_size as usize;
            if end > data.len() {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "chunk compressed data extends past end of stream: need {} bytes at offset {}, have {}",
                        header.compressed_size, offset, data.len() - offset
                    ),
                )));
            }

            let compressed = &data[offset..end];
            let decompressed = zstd::decode_all(Cursor::new(compressed))
                .map_err(|e| CtfsError::Io(e))?;
            output.extend_from_slice(&decompressed);

            offset = end;
        }

        Ok(output)
    }

    /// Find and decompress only the chunk containing `target_geid`.
    ///
    /// Returns the decompressed data of the single chunk that contains the
    /// target GEID, along with the chunk's header metadata.
    pub fn seek_to_geid(
        data: &[u8],
        target_geid: u64,
    ) -> Result<(Vec<u8>, ChunkIndexEntry), CtfsError> {
        let mut offset = 0usize;
        let mut best_header: Option<(ChunkIndexEntry, usize)> = None;

        // Scan headers to find the chunk whose first_geid range contains the target.
        // The target is in chunk C if C.first_geid <= target_geid and either
        // there is no next chunk or the next chunk's first_geid > target_geid.
        while offset + CHUNK_INDEX_ENTRY_SIZE <= data.len() {
            let header = Self::read_header_at(data, offset)?;
            let data_offset = offset + CHUNK_INDEX_ENTRY_SIZE;

            if header.first_geid <= target_geid {
                best_header = Some((header, data_offset));
            } else {
                // We've passed the target, the previous chunk is the one
                break;
            }

            offset = data_offset + header.compressed_size as usize;
        }

        let (header, data_offset) = best_header.ok_or_else(|| {
            CtfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no chunk contains GEID {}", target_geid),
            ))
        })?;

        let end = data_offset + header.compressed_size as usize;
        if end > data.len() {
            return Err(CtfsError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "chunk data extends past end of stream",
            )));
        }

        let compressed = &data[data_offset..end];
        let decompressed = zstd::decode_all(Cursor::new(compressed))
            .map_err(|e| CtfsError::Io(e))?;

        Ok((decompressed, header))
    }

    /// Iterate chunk headers without decompressing any data.
    pub fn scan_headers(data: &[u8]) -> Vec<ChunkIndexEntry> {
        let mut headers = Vec::new();
        let mut offset = 0usize;

        while offset + CHUNK_INDEX_ENTRY_SIZE <= data.len() {
            if let Ok(header) = Self::read_header_at(data, offset) {
                let next = offset + CHUNK_INDEX_ENTRY_SIZE + header.compressed_size as usize;
                if next > data.len() {
                    break;
                }
                headers.push(header);
                offset = next;
            } else {
                break;
            }
        }

        headers
    }

    /// Read a 16-byte inline chunk header at the given offset.
    fn read_header_at(data: &[u8], offset: usize) -> Result<ChunkIndexEntry, CtfsError> {
        if offset + CHUNK_INDEX_ENTRY_SIZE > data.len() {
            return Err(CtfsError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "not enough data for chunk header",
            )));
        }

        let compressed_size =
            u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        let event_count =
            u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap());
        let first_geid =
            u64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap());

        Ok(ChunkIndexEntry {
            compressed_size,
            event_count,
            first_geid,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Create fake events: each event is `event_size` bytes with a recognizable pattern.
    fn make_events(
        count: usize,
        event_size: usize,
        start_geid: u64,
    ) -> (Vec<u8>, Vec<usize>, Vec<u64>) {
        let mut data = Vec::with_capacity(count * event_size);
        let mut sizes = Vec::with_capacity(count);
        let mut geids = Vec::with_capacity(count);

        for i in 0..count {
            let geid = start_geid + i as u64;
            // Fill event bytes with a pattern based on GEID
            for j in 0..event_size {
                data.push(((geid as usize + j) % 251) as u8);
            }
            sizes.push(event_size);
            geids.push(geid);
        }

        (data, sizes, geids)
    }

    #[test]
    fn test_chunked_roundtrip() {
        let event_count = 100;
        let event_size = 48; // 24-byte header + 24-byte payload
        let chunk_size = 10;

        let (raw_events, event_sizes, first_geids) =
            make_events(event_count, event_size, 1000);

        let writer = ChunkedWriter::new(CompressionMethod::Zstd, chunk_size);
        let chunked = writer
            .write_chunked(&raw_events, &event_sizes, &first_geids)
            .unwrap();

        // Verify we have the expected number of chunks
        let headers = ChunkedReader::scan_headers(&chunked);
        assert_eq!(headers.len(), 10); // 100 events / 10 per chunk

        // Decompress all and verify byte-for-byte match
        let decompressed = ChunkedReader::decompress_all(&chunked).unwrap();
        assert_eq!(decompressed.len(), raw_events.len());
        assert_eq!(decompressed, raw_events);
    }

    #[test]
    fn test_chunked_seek_by_geid() {
        let event_count = 100;
        let event_size = 32;
        let chunk_size = 10;
        let start_geid = 0u64;

        let (raw_events, event_sizes, first_geids) =
            make_events(event_count, event_size, start_geid);

        let writer = ChunkedWriter::new(CompressionMethod::Zstd, chunk_size);
        let chunked = writer
            .write_chunked(&raw_events, &event_sizes, &first_geids)
            .unwrap();

        // Seek to GEID 55 -- should be in chunk 5 (GEIDs 50..59)
        let target_geid = 55u64;
        let (chunk_data, header) =
            ChunkedReader::seek_to_geid(&chunked, target_geid).unwrap();

        // Verify the header metadata
        assert_eq!(header.first_geid, 50); // chunk 5 starts at GEID 50
        assert_eq!(header.event_count, 10);

        // Verify the decompressed data matches events 50..59
        let expected_start = 50 * event_size;
        let expected_end = 60 * event_size;
        let expected_chunk = &raw_events[expected_start..expected_end];
        assert_eq!(chunk_data.len(), expected_chunk.len());
        assert_eq!(chunk_data, expected_chunk);
    }

    #[test]
    fn test_chunked_seek_first_and_last_geid() {
        let event_count = 30;
        let event_size = 16;
        let chunk_size = 10;
        let start_geid = 100u64;

        let (raw_events, event_sizes, first_geids) =
            make_events(event_count, event_size, start_geid);

        let writer = ChunkedWriter::new(CompressionMethod::Zstd, chunk_size);
        let chunked = writer
            .write_chunked(&raw_events, &event_sizes, &first_geids)
            .unwrap();

        // Seek to the very first GEID
        let (_chunk_data, header) =
            ChunkedReader::seek_to_geid(&chunked, 100).unwrap();
        assert_eq!(header.first_geid, 100);
        assert_eq!(header.event_count, 10);

        // Seek to the very last GEID
        let (_chunk_data, header) =
            ChunkedReader::seek_to_geid(&chunked, 129).unwrap();
        assert_eq!(header.first_geid, 120);
        assert_eq!(header.event_count, 10);
    }

    #[test]
    fn test_chunked_nim_compat_header_format() {
        // Verify the inline header matches the Nim layout:
        // compressed_size:u32 LE, event_count:u32 LE, first_geid:u64 LE
        let event_size = 24;
        let (raw_events, event_sizes, first_geids) =
            make_events(5, event_size, 42);

        let writer = ChunkedWriter::new(CompressionMethod::Zstd, 5);
        let chunked = writer
            .write_chunked(&raw_events, &event_sizes, &first_geids)
            .unwrap();

        // Read header fields directly from bytes
        assert!(chunked.len() >= CHUNK_INDEX_ENTRY_SIZE);

        let compressed_size =
            u32::from_le_bytes(chunked[0..4].try_into().unwrap());
        let event_count =
            u32::from_le_bytes(chunked[4..8].try_into().unwrap());
        let first_geid =
            u64::from_le_bytes(chunked[8..16].try_into().unwrap());

        assert_eq!(event_count, 5);
        assert_eq!(first_geid, 42);
        assert!(compressed_size > 0);

        // Total stream length should be exactly header + compressed data (one chunk)
        assert_eq!(chunked.len(), CHUNK_INDEX_ENTRY_SIZE + compressed_size as usize);

        // Decompress and verify roundtrip
        let decompressed = ChunkedReader::decompress_all(&chunked).unwrap();
        assert_eq!(decompressed, raw_events);
    }

    #[test]
    fn test_chunked_uneven_last_chunk() {
        // 25 events with chunk_size 10 => chunks of 10, 10, 5
        let event_count = 25;
        let event_size = 32;
        let chunk_size = 10;

        let (raw_events, event_sizes, first_geids) =
            make_events(event_count, event_size, 0);

        let writer = ChunkedWriter::new(CompressionMethod::Zstd, chunk_size);
        let chunked = writer
            .write_chunked(&raw_events, &event_sizes, &first_geids)
            .unwrap();

        let headers = ChunkedReader::scan_headers(&chunked);
        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0].event_count, 10);
        assert_eq!(headers[0].first_geid, 0);
        assert_eq!(headers[1].event_count, 10);
        assert_eq!(headers[1].first_geid, 10);
        assert_eq!(headers[2].event_count, 5);
        assert_eq!(headers[2].first_geid, 20);

        let decompressed = ChunkedReader::decompress_all(&chunked).unwrap();
        assert_eq!(decompressed, raw_events);
    }

    #[test]
    fn test_chunked_variable_event_sizes() {
        // Events with different sizes
        let event_count = 20;
        let mut data = Vec::new();
        let mut sizes = Vec::new();
        let mut geids = Vec::new();

        for i in 0..event_count {
            let size = 16 + (i % 5) * 8; // sizes: 16, 24, 32, 40, 48, ...
            for j in 0..size {
                data.push(((i + j) % 251) as u8);
            }
            sizes.push(size);
            geids.push(i as u64);
        }

        let writer = ChunkedWriter::new(CompressionMethod::Zstd, 7);
        let chunked = writer
            .write_chunked(&data, &sizes, &geids)
            .unwrap();

        let headers = ChunkedReader::scan_headers(&chunked);
        // 20 events / 7 per chunk = 3 chunks (7, 7, 6)
        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0].event_count, 7);
        assert_eq!(headers[1].event_count, 7);
        assert_eq!(headers[2].event_count, 6);

        let decompressed = ChunkedReader::decompress_all(&chunked).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_chunked_no_compression() {
        let event_count = 10;
        let event_size = 24;

        let (raw_events, event_sizes, first_geids) =
            make_events(event_count, event_size, 0);

        let writer = ChunkedWriter::new(CompressionMethod::None, 5);
        let chunked = writer
            .write_chunked(&raw_events, &event_sizes, &first_geids)
            .unwrap();

        // With no compression, compressed data == raw data
        let headers = ChunkedReader::scan_headers(&chunked);
        assert_eq!(headers.len(), 2);

        // Each chunk's compressed_size should equal the raw size (5 * 24 = 120)
        assert_eq!(headers[0].compressed_size, 120);
        assert_eq!(headers[1].compressed_size, 120);

        // Note: decompress_all uses zstd, so it won't work for uncompressed data.
        // For uncompressed, the caller would read directly.
        // But we can verify the raw data is there after the header.
        let chunk0_data = &chunked[CHUNK_INDEX_ENTRY_SIZE..CHUNK_INDEX_ENTRY_SIZE + 120];
        assert_eq!(chunk0_data, &raw_events[0..120]);
    }

    #[test]
    fn test_chunked_empty_input() {
        let writer = ChunkedWriter::new(CompressionMethod::Zstd, 10);
        let chunked = writer.write_chunked(&[], &[], &[]).unwrap();
        assert!(chunked.is_empty());

        let headers = ChunkedReader::scan_headers(&chunked);
        assert!(headers.is_empty());

        let decompressed = ChunkedReader::decompress_all(&chunked).unwrap();
        assert!(decompressed.is_empty());
    }
}
