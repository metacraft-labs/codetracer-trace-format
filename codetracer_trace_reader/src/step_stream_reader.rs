//! Reader for the dedicated `steps.dat` execution stream (M23a).
//!
//! Reads compact step records from a CTFS container's `steps.dat` stream via
//! its companion seekable index `steps.idx`, per
//! `codetracer-trace-format-spec/seekable-zstd.md`. Seeking to a step record by
//! index decompresses only the one chunk that contains it — no whole-stream
//! decompression — which is the property the M22 db-backend seekable reader will
//! rely on (this M23a reader is the format-level reference the round-trip test
//! drives, mirroring the M17a `CallStreamReader`).
//!
//! The stream is gated by the `has_step_stream` capability flag (bit 9) in
//! `meta.dat`. A reader that does not see the flag, or a container without
//! `steps.dat`, simply has no step stream — the unified `events.log` step
//! sequence remains the source of truth.
//!
//! # Independent chunk decode
//!
//! Each chunk is decoded with its own running absolute `global_line_index`
//! (reset to `None` at the chunk start), because the writer guarantees the
//! first `Step` record of every chunk is AbsoluteStep
//! (`step_stream::encode_step_stream`, encoding rule 5). DeltaStep records
//! within a chunk resolve against the running absolute carried forward inside
//! that chunk only — so any chunk decodes correctly without touching its
//! neighbours.

use codetracer_ctfs::CtfsReader;
use codetracer_trace_writer::meta_dat::meta_dat_has_step_stream;
use codetracer_trace_writer::step_stream::{StepStreamRecord, decode_record};

/// A loaded `steps.idx`: the per-chunk byte offsets into `steps.dat`.
struct StepsIndex {
    chunk_size: usize,
    /// Byte offset of each chunk within `steps.dat`.
    chunk_offsets: Vec<u64>,
}

impl StepsIndex {
    /// Parse `steps.idx`: `[chunk_size: u32 LE][offset_0: u64 LE]...`.
    fn parse(idx: &[u8]) -> Result<StepsIndex, String> {
        if idx.len() < 4 {
            return Err("steps.idx: too short for chunk_size header".to_string());
        }
        let chunk_size = u32::from_le_bytes([idx[0], idx[1], idx[2], idx[3]]) as usize;
        if chunk_size == 0 {
            return Err("steps.idx: chunk_size is zero".to_string());
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
        Ok(StepsIndex { chunk_size, chunk_offsets })
    }
}

/// Decompress one chunk and decode all of its records, carrying the running
/// absolute `global_line_index` forward within the chunk (reset at the chunk
/// start, since the first step of a chunk is AbsoluteStep).
///
/// Exposed (`pub`) so the db-backend follow-mode split-stream reader (M1) can
/// decode an appended `steps.dat` chunk through the EXACT same wire-format path
/// the seekable final-file reader uses, rather than re-implementing the decode.
pub fn decode_chunk_records(compressed: &[u8]) -> Result<Vec<StepStreamRecord>, String> {
    let raw = zstd::decode_all(std::io::Cursor::new(compressed)).map_err(|e| format!("steps.dat: zstd decode failed: {e}"))?;
    let mut records = Vec::new();
    let mut pos = 0usize;
    let mut prev_abs: Option<u64> = None;
    while pos < raw.len() {
        let (rec, next) = decode_record(&raw, &mut pos, prev_abs)?;
        prev_abs = next;
        records.push(rec);
    }
    Ok(records)
}

/// A seekable reader over a container's `steps.dat` stream.
///
/// The index (`steps.idx`) and the raw `steps.dat` bytes are loaded once; each
/// `read(index)` decompresses only the single chunk that holds the target
/// record. A simple last-chunk cache avoids re-decompressing when reads are
/// sequential or clustered within a chunk.
pub struct StepStreamReader {
    index: StepsIndex,
    dat: Vec<u8>,
    /// Total number of records.
    record_count: u64,
    /// Cache of the most-recently-decompressed chunk: (chunk_number, records).
    cached_chunk: Option<(usize, Vec<StepStreamRecord>)>,
}

impl StepStreamReader {
    /// Open the step stream from already-loaded CTFS internal-file bytes.
    ///
    /// This keeps the format-level reader independent of how the container bytes
    /// were sourced (local file, follow source, HTTP range, overlay) while
    /// preserving the exact same decode/cache path as [`Self::open`].
    pub fn from_files(meta: &[u8], dat: Vec<u8>, idx: Vec<u8>) -> Result<Option<StepStreamReader>, String> {
        if !meta_dat_has_step_stream(meta) {
            return Ok(None);
        }
        let index = StepsIndex::parse(&idx)?;

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
                return Err("steps.idx: last chunk offset past end of steps.dat".to_string());
            }
            let last_records = decode_chunk_records(&dat[start..end])?.len();
            (last_chunk * index.chunk_size + last_records) as u64
        };

        Ok(Some(StepStreamReader {
            index,
            dat,
            record_count,
            cached_chunk: None,
        }))
    }

    /// Open the step stream from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no dedicated step stream (no `meta.dat`
    /// flag, or no `steps.dat`) — the caller falls back to the unified stream.
    pub fn open(reader: &mut CtfsReader) -> Result<Option<StepStreamReader>, String> {
        // Honor the meta.dat capability flag: only treat steps.dat as
        // authoritative when has_step_stream is set.
        let meta = match reader.read_file("meta.dat") {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };
        if !meta_dat_has_step_stream(&meta) {
            return Ok(None);
        }
        let dat = match reader.read_file("steps.dat") {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let idx = reader
            .read_file("steps.idx")
            .map_err(|e| format!("steps.idx missing despite has_step_stream flag: {e}"))?;
        StepStreamReader::from_files(&meta, dat, idx)
    }

    /// Total number of execution-stream records in the stream.
    pub fn count(&self) -> u64 {
        self.record_count
    }

    /// The fixed number of records per chunk (the seek granularity). Exposed so
    /// a downstream seekable reader (the db-backend, M22) can account for
    /// bounded decompression — e.g. assert that fetching a single step
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

    /// Read the execution-stream record at `index`, decompressing only its
    /// chunk. The returned `StepStreamRecord::Step` carries the absolute
    /// `global_line_index` (deltas already resolved).
    pub fn read(&mut self, index: u64) -> Result<StepStreamRecord, String> {
        if index >= self.record_count {
            return Err(format!("step index {index} out of range (count {})", self.record_count));
        }
        let chunk_number = (index as usize) / self.index.chunk_size;
        let within = (index as usize) % self.index.chunk_size;

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
                return Err("steps.dat: chunk offsets out of range".to_string());
            }
            let records = decode_chunk_records(&self.dat[start..end])?;
            self.cached_chunk = Some((chunk_number, records));
        }

        let records = &self.cached_chunk.as_ref().unwrap().1;
        if within >= records.len() {
            return Err(format!("step record {within} missing in chunk {chunk_number}"));
        }
        Ok(records[within].clone())
    }

    /// Read all execution-stream records (convenience for tests / small traces).
    /// Decodes each chunk once.
    pub fn read_all(&mut self) -> Result<Vec<StepStreamRecord>, String> {
        let mut out = Vec::with_capacity(self.record_count as usize);
        for i in 0..self.record_count {
            out.push(self.read(i)?);
        }
        Ok(out)
    }
}

/// Open the step stream directly from a `.ct` file path. Returns `Ok(None)`
/// when the container carries no dedicated step stream.
pub fn open_step_stream(path: &std::path::Path) -> Result<Option<StepStreamReader>, String> {
    let mut reader = CtfsReader::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    StepStreamReader::open(&mut reader)
}
