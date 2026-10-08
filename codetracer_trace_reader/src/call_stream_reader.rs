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

use crate::ChunkForm;
use codetracer_ctfs::{CtfsReader, MemberBytes};
use codetracer_trace_writer::call_stream::CallStreamRecord;

fn decode_zstd_chunk(compressed: &[u8]) -> Result<Vec<u8>, String> {
    crate::chunk_codec::inflate("calls.dat", compressed)
}

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

/// A varint. One byte, most fields of a record, is read inline; longer
/// ones, and every refusal, by [`decode_varint_long`].
#[inline]
fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    if let Some(&byte) = data.get(*pos)
        && byte < 0x80
    {
        *pos += 1;
        return Ok(byte as u64);
    }
    decode_varint_long(data, pos)
}

fn decode_varint_long(data: &[u8], pos: &mut usize) -> Result<u64, String> {
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
    let raw = decode_zstd_chunk(compressed)?;
    let mut frames = Vec::new();
    frame_records(&raw, &mut frames)?;
    Ok(frames.into_iter().map(|(s, n)| raw[s..s + n].to_vec()).collect())
}

/// Locate each length-prefixed record of an inflated chunk: `(start, len)`
/// into `raw`, replacing the contents of `frames`.
fn frame_records(raw: &[u8], frames: &mut Vec<(usize, usize)>) -> Result<(), String> {
    frames.clear();
    let mut pos = 0usize;
    while pos < raw.len() {
        let rec_len = decode_varint(raw, &mut pos)? as usize;
        if rec_len > raw.len() - pos {
            return Err("calls.dat: record extends past end of chunk".to_string());
        }
        frames.push((pos, rec_len));
        pos += rec_len;
    }
    Ok(())
}

/// A seekable reader over a container's `calls.dat` stream.
///
/// The index (`calls.idx`) and the raw `calls.dat` bytes are loaded once; each
/// `read(call_key)` decompresses only the single chunk that holds the target
/// record. A simple last-chunk cache avoids re-decompressing when reads are
/// sequential or clustered within a chunk.
pub struct CallStreamReader {
    index: CallsIndex,
    /// `calls.dat`, shared with the container when it is in memory.
    dat: MemberBytes,
    /// Total number of records (computed by decoding chunks lazily as needed,
    /// but the count is established by walking the last chunk on open).
    record_count: u64,
    /// The chunk `raw` holds, `None` until the first read.
    cached_chunk: Option<usize>,
    /// The most recently inflated chunk, and where each record lies in it.
    /// A record is decoded when it is read.
    raw: Vec<u8>,
    frames: Vec<(usize, usize)>,
    /// Whether a chunk is a frame to inflate or its content.
    form: ChunkForm,
}

impl CallStreamReader {
    /// Open the call stream from already-loaded CTFS internal-file bytes.
    ///
    /// This keeps the format-level reader independent of how the container bytes
    /// were sourced (local file, follow source, HTTP range, overlay) while
    /// preserving the exact same decode/cache path as [`Self::open`].
    pub fn from_files(_meta: &[u8], dat: Vec<u8>, idx: Vec<u8>) -> Result<Option<CallStreamReader>, String> {
        Self::from_member(_meta, MemberBytes::from(dat), &idx)
    }

    /// [`Self::from_files`] over a `calls.dat` the reader shares with its
    /// container rather than owns.
    pub fn from_member(meta: &[u8], dat: MemberBytes, idx: &[u8]) -> Result<Option<CallStreamReader>, String> {
        Self::from_member_as(meta, dat, idx, ChunkForm::Framed)
    }

    /// [`Self::from_member`] over chunks stored in `form`: the form of the
    /// container the members came from ([`ChunkForm::of`]).
    pub fn from_member_as(_meta: &[u8], dat: MemberBytes, idx: &[u8], form: ChunkForm) -> Result<Option<CallStreamReader>, String> {
        // Existence is STRUCTURAL — the caller resolved `calls.dat` / `calls.idx`
        // by `findFile` + `FileEntry.Size`. The `has_call_stream` hint bit (bit 8)
        // is NOT consulted (trace-format spec: "Stream-presence flags are a hint,
        // not a gate"; a writer may stamp the bit only at close). `_meta` is
        // retained for source compatibility.
        let index = CallsIndex::parse(idx)?;
        let mut reader = CallStreamReader {
            index,
            dat,
            record_count: 0,
            cached_chunk: None,
            raw: Vec::new(),
            frames: Vec::new(),
            form,
        };

        // Compute the total record count: all chunks but the last hold
        // chunk_size records; the last holds however many records are framed
        // in it. Empty stream ⇒ zero records. Counting is not a read: the
        // cache starts empty, as `cached_chunk` reports.
        if let Some(last_chunk) = reader.index.chunk_offsets.len().checked_sub(1) {
            if reader.index.chunk_offsets[last_chunk] as usize > reader.dat.len() {
                return Err("calls.idx: last chunk offset past end of calls.dat".to_string());
            }
            reader.inflate(last_chunk)?;
            reader.record_count = (last_chunk * reader.index.chunk_size + reader.frames.len()) as u64;
            reader.cached_chunk = None;
        }
        Ok(Some(reader))
    }

    /// Inflate chunk `chunk_number` and locate its records, unless it is the
    /// one already held.
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
        let frame = self.dat.get(start, end).map_err(|e| format!("calls.dat: chunk {chunk_number}: {e}"))?;
        self.cached_chunk = None;
        self.form
            .content_into(&frame, &mut self.raw)
            .map_err(|e| format!("calls.dat: zstd decode failed: {e}"))?;
        frame_records(&self.raw, &mut self.frames)?;
        self.cached_chunk = Some(chunk_number);
        Ok(())
    }

    /// Open the call stream from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no `calls.dat` — existence is answered
    /// by STRUCTURAL PRESENCE of the stream file, not by the `has_call_stream`
    /// hint bit (see [`Self::from_files`]).
    pub fn open(reader: &mut CtfsReader) -> Result<Option<CallStreamReader>, String> {
        crate::retired_streams::refuse_retired_members(reader)?;
        let dat = match reader.read_member("calls.dat") {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let idx = reader
            .read_file("calls.idx")
            .map_err(|e| format!("calls.idx missing despite calls.dat presence: {e}"))?;
        let meta = reader.read_file("meta.dat").unwrap_or_default();
        CallStreamReader::from_member_as(&meta, dat, &idx, ChunkForm::of(reader))
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
        self.cached_chunk
    }

    /// Read the call record at `call_key`, decompressing only its chunk.
    pub fn read(&mut self, call_key: u64) -> Result<CallStreamRecord, String> {
        if call_key >= self.record_count {
            return Err(format!("call_key {call_key} out of range (count {})", self.record_count));
        }
        let chunk_number = (call_key as usize) / self.index.chunk_size;
        let within = (call_key as usize) % self.index.chunk_size;

        self.inflate(chunk_number)?;
        match self.frames.get(within) {
            Some(&(start, len)) => CallStreamRecord::decode(call_key, &self.raw[start..start + len]),
            None => Err(format!("call record {within} missing in chunk {chunk_number}")),
        }
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
