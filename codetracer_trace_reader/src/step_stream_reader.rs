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
//! The stream's existence is answered by the STRUCTURAL PRESENCE of `steps.dat`
//! (`findFile` + `FileEntry.Size`), NOT by the `has_step_stream` hint bit (bit
//! 9) in `meta.dat` — that bit may be stamped only at close, so gating on it
//! would refuse a step stream that structurally exists in a still-recording
//! trace (trace-format spec: "Stream-presence flags are a hint, not a gate").
//! A container without `steps.dat` simply has no step stream — the unified
//! `events.log` step sequence remains the source of truth.
//!
//! # Independent chunk decode
//!
//! Each chunk is decoded with its own cursor, starting without one: the first
//! position record of every chunk is an AbsoluteStep (`trace-events.md`
//! §"Encoding Rules"), and deltas resolve against the cursor carried forward
//! inside that chunk only — so any chunk decodes without touching its
//! neighbours. A DeltaStep or DeltaColumn before the chunk's first
//! AbsoluteStep is refused, naming the chunk: resolving it against `0`, or
//! against the previous chunk, would invent positions nobody recorded.

use codetracer_ctfs::CtfsReader;
use codetracer_trace_writer::meta_dat::{FLAG_EXT_HAS_SOURCE_RELOAD, read_meta_dat_ext_flags};
use codetracer_trace_writer::step_stream::{StepStreamRecord, decode_record_declared};

fn decode_zstd_chunk(compressed: &[u8]) -> Result<Vec<u8>, String> {
    crate::chunk_codec::inflate("steps.dat", compressed)
}

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
///
/// Refuses tag 8 (`SourceReload`), which only a container declaring
/// `FLAG_EXT_HAS_SOURCE_RELOAD` may carry; a caller that has read `meta.dat`
/// uses [`decode_chunk_records_declared`].
pub fn decode_chunk_records(compressed: &[u8]) -> Result<Vec<StepStreamRecord>, String> {
    decode_chunk_records_declared(compressed, false)
}

/// [`decode_chunk_records`], accepting tag 8 exactly when
/// `allow_source_reload`. A caller that knows the chunk's number uses
/// [`decode_chunk_records_at`], whose refusals name it.
pub fn decode_chunk_records_declared(compressed: &[u8], allow_source_reload: bool) -> Result<Vec<StepStreamRecord>, String> {
    let raw = decode_zstd_chunk(compressed)?;
    let mut records = Vec::new();
    decode_raw_records(&raw, allow_source_reload, &mut records)?;
    Ok(records)
}

/// Decode an inflated chunk's records into `records`, replacing its contents.
fn decode_raw_records(raw: &[u8], allow_source_reload: bool, records: &mut Vec<StepStreamRecord>) -> Result<(), String> {
    records.clear();
    let mut pos = 0usize;
    let mut prev_abs: Option<u64> = None;
    while pos < raw.len() {
        let (rec, next) = decode_record_declared(raw, &mut pos, prev_abs, allow_source_reload)?;
        prev_abs = next;
        records.push(rec);
    }
    Ok(())
}

/// [`decode_chunk_records_declared`] for chunk number `chunk`, whose refusals
/// name the chunk.
pub fn decode_chunk_records_at(compressed: &[u8], chunk: usize, allow_source_reload: bool) -> Result<Vec<StepStreamRecord>, String> {
    decode_chunk_records_declared(compressed, allow_source_reload).map_err(|e| format!("steps.dat chunk {chunk}: {e}"))
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
    /// The chunk `records` holds, `None` until the first read.
    cached_chunk: Option<usize>,
    /// The records of the most recently inflated chunk.
    records: Vec<StepStreamRecord>,
    /// The inflated bytes of that chunk; kept as a buffer to reuse.
    raw: Vec<u8>,
    decoder: codetracer_ctfs::zstd_compat::Decoder,
    /// Whether `meta.dat` declares `FLAG_EXT_HAS_SOURCE_RELOAD`, which is what
    /// admits tag 8 to this stream.
    allow_source_reload: bool,
}

/// Whether a `meta.dat` declares source reload markers. An absent `meta.dat`
/// (a still-recording trace) declares nothing, so tag 8 is refused there.
pub fn meta_declares_source_reload(meta: &[u8]) -> Result<bool, String> {
    if meta.is_empty() {
        return Ok(false);
    }
    Ok(read_meta_dat_ext_flags(meta)? & FLAG_EXT_HAS_SOURCE_RELOAD != 0)
}

impl StepStreamReader {
    /// Open the step stream from already-loaded CTFS internal-file bytes.
    ///
    /// This keeps the format-level reader independent of how the container bytes
    /// were sourced (local file, follow source, HTTP range, overlay) while
    /// preserving the exact same decode/cache path as [`Self::open`].
    pub fn from_files(meta: &[u8], dat: Vec<u8>, idx: Vec<u8>) -> Result<Option<StepStreamReader>, String> {
        // Existence is answered by STRUCTURAL PRESENCE — the caller resolved
        // `steps.dat` / `steps.idx` by `findFile` + `FileEntry.Size` and handed
        // their bytes here. The `meta.dat` `has_step_stream` hint bit (bit 9) is
        // NOT consulted: a writer may stamp it only at close, so gating on it
        // would refuse a step stream that structurally exists in a still-recording
        // trace (trace-format spec: "Stream-presence flags are a hint, not a
        // gate"). `meta` is read only for its extended flags, which decide
        // whether tag 8 is admitted.
        let allow_source_reload = meta_declares_source_reload(meta)?;
        let index = StepsIndex::parse(&idx)?;
        let decoder = codetracer_ctfs::zstd_compat::Decoder::new().map_err(|e| format!("steps.dat: {e}"))?;
        let mut reader = StepStreamReader {
            index,
            dat,
            record_count: 0,
            cached_chunk: None,
            records: Vec::new(),
            raw: Vec::new(),
            decoder,
            allow_source_reload,
        };

        // Compute the total record count: all chunks but the last hold
        // chunk_size records; the last holds however many records decode out of
        // it. Empty stream ⇒ zero records. Counting is not a read: the cache
        // starts empty, as `cached_chunk` reports.
        if let Some(last_chunk) = reader.index.chunk_offsets.len().checked_sub(1) {
            if reader.index.chunk_offsets[last_chunk] as usize > reader.dat.len() {
                return Err("steps.idx: last chunk offset past end of steps.dat".to_string());
            }
            reader.inflate(last_chunk)?;
            reader.record_count = (last_chunk * reader.index.chunk_size + reader.records.len()) as u64;
            reader.cached_chunk = None;
        }
        Ok(Some(reader))
    }

    /// Inflate and decode chunk `chunk_number` into `records`, unless they
    /// already hold it.
    fn inflate(&mut self, chunk_number: usize) -> Result<(), String> {
        if self.cached_chunk == Some(chunk_number) {
            return Ok(());
        }
        let start = self.index.chunk_offsets[chunk_number] as usize;
        let end = if chunk_number + 1 < self.index.chunk_offsets.len() {
            self.index.chunk_offsets[chunk_number + 1] as usize
        } else {
            self.dat.len()
        };
        if start > end || end > self.dat.len() {
            return Err("steps.dat: chunk offsets out of range".to_string());
        }
        self.cached_chunk = None;
        self.decoder
            .decode_into(&self.dat[start..end], &mut self.raw)
            .map_err(|e| format!("steps.dat chunk {chunk_number}: steps.dat: zstd decode failed: {e}"))?;
        decode_raw_records(&self.raw, self.allow_source_reload, &mut self.records)
            .map_err(|e| format!("steps.dat chunk {chunk_number}: {e}"))?;
        self.cached_chunk = Some(chunk_number);
        Ok(())
    }

    /// Open the step stream from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no `steps.dat` — existence is answered
    /// by STRUCTURAL PRESENCE of the stream file, not by the `has_step_stream`
    /// hint bit (see [`Self::from_files`]).
    pub fn open(reader: &mut CtfsReader) -> Result<Option<StepStreamReader>, String> {
        let dat = match reader.read_file("steps.dat") {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let idx = reader
            .read_file("steps.idx")
            .map_err(|e| format!("steps.idx missing despite steps.dat presence: {e}"))?;
        let meta = reader.read_file("meta.dat").unwrap_or_default();
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
        self.cached_chunk
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

        self.inflate(chunk_number)?;
        match self.records.get(within) {
            Some(rec) => Ok(rec.clone()),
            None => Err(format!("step record {within} missing in chunk {chunk_number}")),
        }
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
