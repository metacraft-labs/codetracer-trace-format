//! Reader for the dedicated `values.dat` parallel value stream (M23b).
//!
//! Reads per-step value records from a CTFS container's `values.dat` stream via
//! its companion seekable index `values.idx`, per
//! `codetracer-trace-format-spec/seekable-zstd.md`. Seeking to a value record by
//! step index decompresses only the one chunk that contains it — no
//! whole-stream decompression — which is the property the M22 db-backend
//! seekable reader will rely on (this M23b reader is the format-level reference
//! the round-trip test drives, mirroring the M23a `StepStreamReader` and the
//! M17a `CallStreamReader`).
//!
//! The stream is gated by the `has_value_stream` capability flag (bit 10) in
//! `meta.dat`. A reader that does not see the flag, or a container without
//! `values.dat`, simply has no value stream — the unified `events.log` value
//! events remain the source of truth.
//!
//! # Parallel-index invariant (record N ↔ step N)
//!
//! The value stream is parallel-indexed to the execution stream: value record
//! `N` holds the variable values visible at step `N`. [`ValueStreamReader::read`]
//! therefore takes a step index and returns that step's value record (an empty
//! record for a step with no variable activity). The reader does not need a
//! cross-reference table — the integer step index IS the value-record index.
//!
//! # File-naming note
//!
//! The value stream lives in its OWN CTFS file pair `values.dat`/`values.idx`,
//! NOT in `steps.dat`. See the module docs of
//! `codetracer_trace_writer::value_stream` for the full rationale (the two
//! streams have different record sizes and Zstd tuning, and a CTFS file is a
//! single seekable byte range with one companion index, so they cannot share
//! one file).

use codetracer_ctfs::CtfsReader;
use codetracer_trace_writer::meta_dat::meta_dat_has_value_stream;
use codetracer_trace_writer::value_stream::ValueRecordEntry;

/// A loaded `values.idx`: the per-chunk byte offsets into `values.dat`.
struct ValuesIndex {
    chunk_size: usize,
    /// Byte offset of each chunk within `values.dat`.
    chunk_offsets: Vec<u64>,
}

impl ValuesIndex {
    /// Parse `values.idx`: `[chunk_size: u32 LE][offset_0: u64 LE]...`.
    fn parse(idx: &[u8]) -> Result<ValuesIndex, String> {
        if idx.len() < 4 {
            return Err("values.idx: too short for chunk_size header".to_string());
        }
        let chunk_size = u32::from_le_bytes([idx[0], idx[1], idx[2], idx[3]]) as usize;
        if chunk_size == 0 {
            return Err("values.idx: chunk_size is zero".to_string());
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
        Ok(ValuesIndex { chunk_size, chunk_offsets })
    }
}

// --- varint helper (unsigned LEB128) for the per-record length prefix ---

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err("values.dat: truncated record-length varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err("values.dat: record-length varint too long".to_string());
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Decompress one chunk and decode all of its length-prefixed value records.
///
/// Exposed (`pub`) so the db-backend follow-mode split-stream reader (M1b) can
/// decode an appended `values.dat` chunk through the EXACT same wire-format path
/// the seekable final-file reader uses, rather than re-implementing the decode —
/// mirroring [`crate::step_stream_reader::decode_chunk_records`].
pub fn decode_chunk_records(compressed: &[u8]) -> Result<Vec<ValueRecordEntry>, String> {
    let raw = zstd::decode_all(std::io::Cursor::new(compressed)).map_err(|e| format!("values.dat: zstd decode failed: {e}"))?;
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < raw.len() {
        let rec_len = decode_varint(&raw, &mut pos)? as usize;
        if pos + rec_len > raw.len() {
            return Err("values.dat: record length extends past chunk".to_string());
        }
        let rec = ValueRecordEntry::decode(&raw[pos..pos + rec_len])?;
        pos += rec_len;
        records.push(rec);
    }
    Ok(records)
}

/// A seekable reader over a container's `values.dat` stream.
///
/// The index (`values.idx`) and the raw `values.dat` bytes are loaded once;
/// each `read(step_index)` decompresses only the single chunk that holds the
/// target record. A simple last-chunk cache avoids re-decompressing when reads
/// are sequential or clustered within a chunk.
pub struct ValueStreamReader {
    index: ValuesIndex,
    dat: Vec<u8>,
    /// Total number of value records (== number of steps).
    record_count: u64,
    /// Cache of the most-recently-decompressed chunk: (chunk_number, records).
    cached_chunk: Option<(usize, Vec<ValueRecordEntry>)>,
}

impl ValueStreamReader {
    /// Open the value stream from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no dedicated value stream (no `meta.dat`
    /// flag, or no `values.dat`) — the caller falls back to the unified stream.
    pub fn open(reader: &mut CtfsReader) -> Result<Option<ValueStreamReader>, String> {
        // Honor the meta.dat capability flag: only treat values.dat as
        // authoritative when has_value_stream is set.
        let has_flag = match reader.read_file("meta.dat") {
            Ok(meta) => meta_dat_has_value_stream(&meta),
            Err(_) => false,
        };
        if !has_flag {
            return Ok(None);
        }
        let dat = match reader.read_file("values.dat") {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let idx = reader.read_file("values.idx").map_err(|e| format!("values.idx missing despite has_value_stream flag: {e}"))?;
        let index = ValuesIndex::parse(&idx)?;

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
                return Err("values.idx: last chunk offset past end of values.dat".to_string());
            }
            let last_records = decode_chunk_records(&dat[start..end])?.len();
            (last_chunk * index.chunk_size + last_records) as u64
        };

        Ok(Some(ValueStreamReader {
            index,
            dat,
            record_count,
            cached_chunk: None,
        }))
    }

    /// Total number of value records in the stream (equals the step count, by
    /// the parallel-index invariant).
    pub fn count(&self) -> u64 {
        self.record_count
    }

    /// The fixed number of records per chunk (the seek granularity). Exposed so
    /// a downstream seekable reader (the db-backend, M22) can account for
    /// bounded decompression — e.g. assert that fetching a single step's values
    /// decompresses at most one chunk.
    pub fn chunk_size(&self) -> usize {
        self.index.chunk_size
    }

    /// The chunk number currently held in the one-chunk decompression cache, or
    /// `None` if nothing has been decompressed yet. Lets a downstream reader
    /// observe exactly which chunks were inflated (bounded-decompression probe).
    pub fn cached_chunk(&self) -> Option<usize> {
        self.cached_chunk.as_ref().map(|(c, _)| *c)
    }

    /// Read the value record for step `step_index`, decompressing only its
    /// chunk. Returns the (possibly empty) value record for that step.
    pub fn read(&mut self, step_index: u64) -> Result<ValueRecordEntry, String> {
        if step_index >= self.record_count {
            return Err(format!("value step index {step_index} out of range (count {})", self.record_count));
        }
        let chunk_number = (step_index as usize) / self.index.chunk_size;
        let within = (step_index as usize) % self.index.chunk_size;

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
                return Err("values.dat: chunk offsets out of range".to_string());
            }
            let records = decode_chunk_records(&self.dat[start..end])?;
            self.cached_chunk = Some((chunk_number, records));
        }

        let records = &self.cached_chunk.as_ref().unwrap().1;
        if within >= records.len() {
            return Err(format!("value record {within} missing in chunk {chunk_number}"));
        }
        Ok(records[within].clone())
    }

    /// Read all value records (convenience for tests / small traces). Decodes
    /// each chunk once.
    pub fn read_all(&mut self) -> Result<Vec<ValueRecordEntry>, String> {
        let mut out = Vec::with_capacity(self.record_count as usize);
        for i in 0..self.record_count {
            out.push(self.read(i)?);
        }
        Ok(out)
    }
}

/// Open the value stream directly from a `.ct` file path. Returns `Ok(None)`
/// when the container carries no dedicated value stream.
pub fn open_value_stream(path: &std::path::Path) -> Result<Option<ValueStreamReader>, String> {
    let mut reader = CtfsReader::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    ValueStreamReader::open(&mut reader)
}
