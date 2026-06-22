//! Reader for the dedicated `calls.dat` call stream (M17a).
//!
//! Reads complete call records from a CTFS container's `calls.dat` stream via
//! its companion seekable index `calls.idx`, per
//! `codetracer-trace-format-spec/seekable-zstd.md`. Seeking to a call record by
//! `call_key` decompresses only the one chunk that contains it — no
//! whole-stream decompression — which is the property the M17b db-backend
//! seekable reader will rely on (this M17a reader is the format-level reference
//! the round-trip test drives).
//!
//! The stream is gated by the `has_call_stream` capability flag (bit 8) in
//! `meta.dat`. A reader that does not see the flag, or a container without
//! `calls.dat`, simply has no call stream — the unified `events.log` call tree
//! remains the source of truth.

use codetracer_ctfs::CtfsReader;
use codetracer_trace_writer::call_stream::CallStreamRecord;
use codetracer_trace_writer::meta_dat::meta_dat_has_call_stream;

/// A loaded `calls.idx`: the per-chunk byte offsets into `calls.dat`.
struct CallsIndex {
    chunk_size: usize,
    /// Byte offset of each chunk within `calls.dat`.
    chunk_offsets: Vec<u64>,
}

impl CallsIndex {
    /// Parse `calls.idx`: `[chunk_size: u32 LE][offset_0: u64 LE]...`.
    fn parse(idx: &[u8]) -> Result<CallsIndex, String> {
        if idx.len() < 4 {
            return Err("calls.idx: too short for chunk_size header".to_string());
        }
        let chunk_size = u32::from_le_bytes([idx[0], idx[1], idx[2], idx[3]]) as usize;
        if chunk_size == 0 {
            return Err("calls.idx: chunk_size is zero".to_string());
        }
        let mut chunk_offsets = Vec::new();
        let mut pos = 4usize;
        while pos + 8 <= idx.len() {
            chunk_offsets.push(u64::from_le_bytes([
                idx[pos],
                idx[pos + 1],
                idx[pos + 2],
                idx[pos + 3],
                idx[pos + 4],
                idx[pos + 5],
                idx[pos + 6],
                idx[pos + 7],
            ]));
            pos += 8;
        }
        Ok(CallsIndex { chunk_size, chunk_offsets })
    }
}

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err("calls.dat: truncated varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Decompress one chunk and split it into its length-prefixed records.
///
/// Exposed (`pub`) so the db-backend follow-mode split-stream reader (M1b) can
/// decode an appended `calls.dat` chunk through the EXACT same wire-format path
/// the seekable final-file reader uses, rather than re-implementing the decode —
/// mirroring [`crate::step_stream_reader::decode_chunk_records`]. Each returned
/// element is one record's raw (still-encoded) bytes, ready for
/// [`CallStreamRecord::decode`].
pub fn decode_chunk_records(compressed: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let raw = zstd::decode_all(std::io::Cursor::new(compressed)).map_err(|e| format!("calls.dat: zstd decode failed: {e}"))?;
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < raw.len() {
        let rec_len = decode_varint(&raw, &mut pos)? as usize;
        if pos + rec_len > raw.len() {
            return Err("calls.dat: record extends past end of chunk".to_string());
        }
        records.push(raw[pos..pos + rec_len].to_vec());
        pos += rec_len;
    }
    Ok(records)
}

/// A seekable reader over a container's `calls.dat` stream.
///
/// The index (`calls.idx`) and the raw `calls.dat` bytes are loaded once; each
/// `read(call_key)` decompresses only the single chunk that holds the target
/// record. A simple last-chunk cache avoids re-decompressing when reads are
/// sequential or clustered within a chunk.
pub struct CallStreamReader {
    index: CallsIndex,
    dat: Vec<u8>,
    /// Total number of records (computed by decoding chunks lazily as needed,
    /// but the count is established by walking the last chunk on open).
    record_count: u64,
    /// Cache of the most-recently-decompressed chunk: (chunk_number, records).
    cached_chunk: Option<(usize, Vec<Vec<u8>>)>,
}

impl CallStreamReader {
    /// Open the call stream from already-loaded CTFS internal-file bytes.
    ///
    /// This keeps the format-level reader independent of how the container bytes
    /// were sourced (local file, follow source, HTTP range, overlay) while
    /// preserving the exact same decode/cache path as [`Self::open`].
    pub fn from_files(meta: &[u8], dat: Vec<u8>, idx: Vec<u8>) -> Result<Option<CallStreamReader>, String> {
        if !meta_dat_has_call_stream(meta) {
            return Ok(None);
        }
        let index = CallsIndex::parse(&idx)?;

        // Compute the total record count: all chunks but the last hold
        // chunk_size records; the last holds however many records decode out of
        // it. Empty stream ⇒ zero records.
        let record_count = if index.chunk_offsets.is_empty() {
            0
        } else {
            let last_chunk = index.chunk_offsets.len() - 1;
            let start = index.chunk_offsets[last_chunk] as usize;
            let end = dat.len();
            if start > end {
                return Err("calls.idx: last chunk offset past end of calls.dat".to_string());
            }
            let last_records = decode_chunk_records(&dat[start..end])?.len();
            (last_chunk * index.chunk_size + last_records) as u64
        };

        Ok(Some(CallStreamReader {
            index,
            dat,
            record_count,
            cached_chunk: None,
        }))
    }

    /// Open the call stream from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no dedicated call stream (no `meta.dat`
    /// flag, or no `calls.dat`) — the caller falls back to the unified stream.
    pub fn open(reader: &mut CtfsReader) -> Result<Option<CallStreamReader>, String> {
        // Honor the meta.dat capability flag: only treat calls.dat as
        // authoritative when has_call_stream is set.
        let meta = match reader.read_file("meta.dat") {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };
        if !meta_dat_has_call_stream(&meta) {
            return Ok(None);
        }
        let dat = match reader.read_file("calls.dat") {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let idx = reader
            .read_file("calls.idx")
            .map_err(|e| format!("calls.idx missing despite has_call_stream flag: {e}"))?;
        CallStreamReader::from_files(&meta, dat, idx)
    }

    /// Total number of call records in the stream.
    pub fn count(&self) -> u64 {
        self.record_count
    }

    /// The fixed number of records per chunk (the seek granularity). Exposed so
    /// a downstream seekable reader (the db-backend, M17b) can account for
    /// bounded decompression — e.g. assert that fetching a single call by
    /// `call_key` decompresses at most one chunk.
    pub fn chunk_size(&self) -> usize {
        self.index.chunk_size
    }

    /// The chunk number currently held in the one-chunk decompression cache, or
    /// `None` if nothing has been decompressed yet. Lets a downstream reader
    /// observe exactly which chunks were inflated (bounded-decompression probe).
    pub fn cached_chunk(&self) -> Option<usize> {
        self.cached_chunk.as_ref().map(|(c, _)| *c)
    }

    /// Read the call record at `call_key`, decompressing only its chunk.
    pub fn read(&mut self, call_key: u64) -> Result<CallStreamRecord, String> {
        if call_key >= self.record_count {
            return Err(format!("call_key {call_key} out of range (count {})", self.record_count));
        }
        let chunk_number = (call_key as usize) / self.index.chunk_size;
        let within = (call_key as usize) % self.index.chunk_size;

        // Use the cache when the target chunk is already decompressed.
        let need_decompress = !matches!(&self.cached_chunk, Some((c, _)) if *c == chunk_number);
        if need_decompress {
            let start = self.index.chunk_offsets[chunk_number] as usize;
            let end = if chunk_number + 1 < self.index.chunk_offsets.len() {
                self.index.chunk_offsets[chunk_number + 1] as usize
            } else {
                self.dat.len()
            };
            if start > end || end > self.dat.len() {
                return Err("calls.dat: chunk offsets out of range".to_string());
            }
            let records = decode_chunk_records(&self.dat[start..end])?;
            self.cached_chunk = Some((chunk_number, records));
        }

        let records = &self.cached_chunk.as_ref().unwrap().1;
        if within >= records.len() {
            return Err(format!("call record {within} missing in chunk {chunk_number}"));
        }
        CallStreamRecord::decode(call_key, &records[within])
    }

    /// Read all call records (convenience for tests / small traces). Decodes
    /// each chunk once.
    pub fn read_all(&mut self) -> Result<Vec<CallStreamRecord>, String> {
        let mut out = Vec::with_capacity(self.record_count as usize);
        for key in 0..self.record_count {
            out.push(self.read(key)?);
        }
        Ok(out)
    }
}

/// Open the call stream directly from a `.ct` file path. Returns `Ok(None)`
/// when the container carries no dedicated call stream.
pub fn open_call_stream(path: &std::path::Path) -> Result<Option<CallStreamReader>, String> {
    let mut reader = CtfsReader::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    CallStreamReader::open(&mut reader)
}
