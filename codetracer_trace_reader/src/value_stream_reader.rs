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

use codetracer_ctfs::{CtfsReader, MemberBytes};
use codetracer_trace_writer::value_stream::ValueRecordEntry;

fn decode_zstd_chunk(compressed: &[u8]) -> Result<Vec<u8>, String> {
    crate::chunk_codec::inflate("values.dat", compressed)
}

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
    decode_records(compressed, None)
}

/// [`decode_chunk_records`] for chunk `chunk` of a stream of `chunk_size`
/// records per chunk, whose refusals name each record by its index in the
/// stream.
pub fn decode_chunk_records_at(compressed: &[u8], chunk: usize, chunk_size: usize) -> Result<Vec<ValueRecordEntry>, String> {
    decode_records(compressed, Some((chunk, chunk_size)))
}

/// Every record is framed by its length, and its events must fill the frame
/// exactly (`trace-events.md` §"Call Stream", "Each record is framed by its
/// length"): an event that runs past the frame is refused, naming the record.
fn decode_records(compressed: &[u8], at: Option<(usize, usize)>) -> Result<Vec<ValueRecordEntry>, String> {
    let raw = decode_zstd_chunk(compressed)?;
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < raw.len() {
        let k = records.len();
        let name = || match at {
            Some((c, size)) => format!("values.dat record {} (record {k} of chunk {c})", c * size + k),
            None => format!("values.dat record {k} of its chunk"),
        };
        let rec_len = decode_varint(&raw, &mut pos).map_err(|e| format!("{}: {e}", name()))? as usize;
        if pos + rec_len > raw.len() {
            return Err(format!("{}: its {rec_len}-byte frame extends past the chunk", name()));
        }
        let rec = ValueRecordEntry::decode(&raw[pos..pos + rec_len]).map_err(|e| {
            format!(
                "{}: {} — its events do not fill its {rec_len}-byte frame exactly",
                name(),
                e.trim_start_matches("values.dat: ")
            )
        })?;
        pos += rec_len;
        records.push(rec);
    }
    Ok(records)
}

/// One inflated `values.dat` chunk: its bytes, and where each record's frame
/// lies in them. A record is decoded when it is read, not when its chunk is
/// inflated, and the frames are located only as far as the reads reach, so a
/// point read pays for one record's events and the lengths before it.
struct InflatedChunk {
    number: usize,
    raw: Vec<u8>,
    /// `(start, len)` of each record's events within `raw`, for the records
    /// located so far.
    frames: Vec<(usize, usize)>,
    /// Where the next record's length prefix starts.
    pos: usize,
}

impl InflatedChunk {
    /// Start over on a freshly inflated `raw`.
    fn reset(&mut self, number: usize) {
        self.number = number;
        self.frames.clear();
        self.pos = 0;
    }

    /// Locate records' frames in `raw` until record `target` is located or
    /// the chunk ends, refusing a frame that runs past the chunk with the
    /// message [`decode_chunk_records_at`] gives.
    fn frame_to(&mut self, target: usize, chunk_size: usize) -> Result<(), String> {
        let (raw, c) = (&self.raw, self.number);
        let mut pos = self.pos;
        while self.frames.len() <= target && pos < raw.len() {
            let k = self.frames.len();
            let name = || format!("values.dat record {} (record {k} of chunk {c})", c * chunk_size + k);
            let rec_len = decode_varint(raw, &mut pos).map_err(|e| format!("{}: {e}", name()))? as usize;
            if rec_len > raw.len() - pos {
                return Err(format!("{}: its {rec_len}-byte frame extends past the chunk", name()));
            }
            self.frames.push((pos, rec_len));
            pos += rec_len;
            self.pos = pos;
        }
        Ok(())
    }

    /// Decode record `k` of this chunk.
    fn decode(&self, k: usize, chunk_size: usize) -> Result<ValueRecordEntry, String> {
        let (start, len) = self.frames[k];
        ValueRecordEntry::decode(&self.raw[start..start + len]).map_err(|e| {
            format!(
                "values.dat record {} (record {k} of chunk {}): {} — its events do not fill its {len}-byte frame exactly",
                self.number * chunk_size + k,
                self.number,
                e.trim_start_matches("values.dat: ")
            )
        })
    }
}

/// A seekable reader over a container's `values.dat` stream.
///
/// The index (`values.idx`) and the raw `values.dat` bytes are loaded once;
/// each `read(step_index)` inflates only the single chunk that holds the
/// target record, and decodes only that record. The last inflated chunk is
/// kept, so sequential or clustered reads inflate each chunk once.
pub struct ValueStreamReader {
    index: ValuesIndex,
    /// `values.dat`, shared with the container when it is in memory.
    dat: MemberBytes,
    /// Total number of value records (== number of steps).
    record_count: u64,
    /// The buffers of the most recently inflated chunk.
    chunk: InflatedChunk,
    /// The chunk `chunk` holds, `None` until the first read.
    cached_chunk: Option<usize>,
}

impl ValueStreamReader {
    /// Open the value stream from already-loaded CTFS internal-file bytes.
    ///
    /// This keeps the format-level reader independent of how the container bytes
    /// were sourced (local file, follow source, HTTP range, overlay) while
    /// preserving the exact same decode/cache path as [`Self::open`].
    pub fn from_files(_meta: &[u8], dat: Vec<u8>, idx: Vec<u8>) -> Result<Option<ValueStreamReader>, String> {
        Self::from_member(_meta, MemberBytes::from(dat), &idx)
    }

    /// [`Self::from_files`] over a `values.dat` the reader shares with its
    /// container rather than owns.
    pub fn from_member(_meta: &[u8], dat: MemberBytes, idx: &[u8]) -> Result<Option<ValueStreamReader>, String> {
        // Existence is STRUCTURAL — the caller resolved `values.dat` / `values.idx`
        // by `findFile` + `FileEntry.Size`. The `has_value_stream` hint bit (bit
        // 10) is NOT consulted (trace-format spec: "Stream-presence flags are a
        // hint, not a gate"; a writer may stamp the bit only at close). `_meta`
        // is retained for source compatibility.
        let index = ValuesIndex::parse(idx)?;
        let mut reader = ValueStreamReader {
            index,
            dat,
            record_count: 0,
            chunk: InflatedChunk {
                number: 0,
                raw: Vec::new(),
                frames: Vec::new(),
                pos: 0,
            },
            cached_chunk: None,
        };

        // Compute the total record count: all chunks but the last hold
        // chunk_size records; the last holds however many records are framed
        // in it. Empty stream ⇒ zero records. Counting is not a read: the
        // cache starts empty, as `cached_chunk` reports.
        if let Some(last_chunk) = reader.index.chunk_offsets.len().checked_sub(1) {
            if reader.index.chunk_offsets[last_chunk] as usize > reader.dat.len() {
                return Err("values.idx: last chunk offset past end of values.dat".to_string());
            }
            reader.inflate(last_chunk)?;
            reader.chunk.frame_to(usize::MAX, reader.index.chunk_size)?;
            let last_records = reader.chunk.frames.len();
            reader.record_count = (last_chunk * reader.index.chunk_size + last_records) as u64;
            reader.cached_chunk = None;
        }
        Ok(Some(reader))
    }

    /// Inflate chunk `chunk_number` into the cache, unless it is already
    /// there.
    fn inflate(&mut self, chunk_number: usize) -> Result<(), String> {
        if self.cached_chunk != Some(chunk_number) {
            let start = self.index.chunk_offsets[chunk_number] as usize;
            let end = if chunk_number + 1 < self.index.chunk_offsets.len() {
                self.index.chunk_offsets[chunk_number + 1] as usize
            } else {
                self.dat.len()
            };
            let frame = self
                .dat
                .get(start, end)
                .ok_or_else(|| "values.dat: chunk offsets out of range".to_string())?;
            self.cached_chunk = None;
            codetracer_ctfs::zstd_compat::decode_into(&frame, &mut self.chunk.raw).map_err(|e| format!("values.dat: zstd decode failed: {e}"))?;
            self.chunk.reset(chunk_number);
            self.cached_chunk = Some(chunk_number);
        }
        Ok(())
    }

    /// Open the value stream from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no `values.dat` — existence is answered
    /// by STRUCTURAL PRESENCE of the stream file, not by the `has_value_stream`
    /// hint bit (see [`Self::from_files`]).
    pub fn open(reader: &mut CtfsReader) -> Result<Option<ValueStreamReader>, String> {
        let dat = match reader.read_member("values.dat") {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let idx = reader
            .read_file("values.idx")
            .map_err(|e| format!("values.idx missing despite values.dat presence: {e}"))?;
        let meta = reader.read_file("meta.dat").unwrap_or_default();
        ValueStreamReader::from_member(&meta, dat, &idx)
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
        self.cached_chunk
    }

    /// Read the value record for step `step_index`, decompressing only its
    /// chunk. Returns the (possibly empty) value record for that step.
    pub fn read(&mut self, step_index: u64) -> Result<ValueRecordEntry, String> {
        if step_index >= self.record_count {
            return Err(format!("value step index {step_index} out of range (count {})", self.record_count));
        }
        let chunk_size = self.index.chunk_size;
        let chunk_number = (step_index as usize) / chunk_size;
        let within = (step_index as usize) % chunk_size;
        self.inflate(chunk_number)?;
        self.chunk.frame_to(within, chunk_size)?;
        if within >= self.chunk.frames.len() {
            return Err(format!("value record {within} missing in chunk {chunk_number}"));
        }
        self.chunk.decode(within, chunk_size)
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
