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

use crate::ChunkForm;
use codetracer_ctfs::{CtfsReader, MemberBytes};
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

/// How many records apart [`StepChunk`] notes where decoding can restart.
const CHECKPOINT_EVERY: usize = 64;

/// Where the decode of an inflated chunk stands: the next record, its offset
/// in the chunk and the position deltas resolve against there.
#[derive(Debug, Clone, Copy, Default)]
struct Cursor {
    record: usize,
    pos: usize,
    prev_abs: Option<u64>,
}

/// One inflated `steps.dat` chunk, decoded as far as reads have reached.
///
/// A record's position can depend on every record before it in its chunk
/// (deltas resolve against the last absolute position), so a chunk is decoded
/// from its start. The cursor every [`CHECKPOINT_EVERY`]th record passed is
/// kept, so a read behind the cursor resumes from the last checkpoint at or
/// before it rather than from the start of the chunk.
#[derive(Debug, Default)]
struct StepChunk {
    raw: Vec<u8>,
    cursor: Cursor,
    checkpoints: Vec<Cursor>,
}

impl StepChunk {
    /// Start over on a freshly inflated `raw`.
    fn reset(&mut self) {
        self.cursor = Cursor::default();
        self.checkpoints.clear();
    }

    /// Decode the record under the cursor and move past it, or `None` at the
    /// end of the chunk.
    fn next(&mut self, allow_source_reload: bool) -> Result<Option<StepStreamRecord>, String> {
        let c = &mut self.cursor;
        if c.pos >= self.raw.len() {
            return Ok(None);
        }
        if c.record.is_multiple_of(CHECKPOINT_EVERY) && self.checkpoints.len() == c.record / CHECKPOINT_EVERY {
            self.checkpoints.push(*c);
        }
        let (rec, prev_abs) = decode_record_declared(&self.raw, &mut c.pos, c.prev_abs, allow_source_reload)?;
        c.prev_abs = prev_abs;
        c.record += 1;
        Ok(Some(rec))
    }

    /// Record `within` of the chunk, or `None` when the chunk holds fewer.
    fn record(&mut self, within: usize, allow_source_reload: bool) -> Result<Option<StepStreamRecord>, String> {
        if within < self.cursor.record {
            // Every checkpoint up to the cursor has been passed and noted.
            self.cursor = self.checkpoints[within / CHECKPOINT_EVERY];
        }
        while self.cursor.record < within {
            if self.next(allow_source_reload)?.is_none() {
                return Ok(None);
            }
        }
        self.next(allow_source_reload)
    }
}

/// A seekable reader over a container's `steps.dat` stream.
///
/// The index (`steps.idx`) and the raw `steps.dat` bytes are loaded once; each
/// `read(index)` decompresses only the single chunk that holds the target
/// record, and decodes it only as far as that record. The last chunk is kept,
/// so sequential reads decode each record once and reads clustered within a
/// chunk inflate it once.
pub struct StepStreamReader {
    index: StepsIndex,
    /// `steps.dat`, shared with the container when it is in memory.
    dat: MemberBytes,
    /// Total number of records.
    record_count: u64,
    /// The chunk `chunk` holds, `None` until the first read.
    cached_chunk: Option<usize>,
    /// The most recently inflated chunk; its buffers are reused.
    chunk: StepChunk,
    /// Whether `meta.dat` declares `FLAG_EXT_HAS_SOURCE_RELOAD`, which is what
    /// admits tag 8 to this stream.
    allow_source_reload: bool,
    /// Whether a chunk is a frame to inflate or its content.
    form: ChunkForm,
    /// Chunks inflated so far; see [`StepStreamReader::inflations`].
    inflations: u64,
}

/// Inflate chunk `chunk_number` of the stream `index` locates in `dat` into
/// `chunk`, and start its decode over.
fn inflate_chunk(index: &StepsIndex, dat: &MemberBytes, form: ChunkForm, chunk_number: usize, chunk: &mut StepChunk) -> Result<(), String> {
    let start = index.chunk_offsets[chunk_number] as usize;
    let end = crate::follow::chunk_end("steps.dat", form, dat, &index.chunk_offsets, chunk_number)?;
    let frame = dat.get(start, end).map_err(|e| format!("steps.dat: chunk {chunk_number}: {e}"))?;
    form.content_into(&frame, &mut chunk.raw)
        .map_err(|e| format!("steps.dat chunk {chunk_number}: steps.dat: zstd decode failed: {e}"))?;
    chunk.reset();
    Ok(())
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
        Self::from_member(meta, MemberBytes::from(dat), &idx)
    }

    /// [`Self::from_files`] over a `steps.dat` the reader shares with its
    /// container rather than owns.
    pub fn from_member(meta: &[u8], dat: MemberBytes, idx: &[u8]) -> Result<Option<StepStreamReader>, String> {
        Self::from_member_as(meta, dat, idx, ChunkForm::Framed)
    }

    /// [`Self::from_member`] over chunks stored in `form`: the form of the
    /// container the members came from ([`ChunkForm::of`]).
    pub fn from_member_as(meta: &[u8], dat: MemberBytes, idx: &[u8], form: ChunkForm) -> Result<Option<StepStreamReader>, String> {
        // Existence is answered by STRUCTURAL PRESENCE — the caller resolved
        // `steps.dat` / `steps.idx` by `findFile` + `FileEntry.Size` and handed
        // their bytes here. The `meta.dat` `has_step_stream` hint bit (bit 9) is
        // NOT consulted: a writer may stamp it only at close, so gating on it
        // would refuse a step stream that structurally exists in a still-recording
        // trace (trace-format spec: "Stream-presence flags are a hint, not a
        // gate"). `meta` is read only for its extended flags, which decide
        // whether tag 8 is admitted.
        let allow_source_reload = meta_declares_source_reload(meta)?;
        let index = StepsIndex::parse(idx)?;
        let mut reader = StepStreamReader {
            index,
            dat,
            record_count: 0,
            cached_chunk: None,
            chunk: StepChunk::default(),
            allow_source_reload,
            form,
            inflations: 0,
        };

        // Compute the total record count. Counting is not a read: the cache
        // starts empty, as `cached_chunk` reports.
        let mut scratch = std::mem::take(&mut reader.chunk);
        let index = std::mem::replace(
            &mut reader.index,
            StepsIndex {
                chunk_size: 1,
                chunk_offsets: Vec::new(),
            },
        );
        let counted = reader.count_records(&index, &reader.dat, &mut scratch);
        reader.index = index;
        reader.chunk = scratch;
        reader.record_count = counted?;
        if !reader.index.chunk_offsets.is_empty() {
            reader.inflations += 1;
        }
        Ok(Some(reader))
    }

    /// Inflate chunk `chunk_number` into `chunk`, unless it already holds it.
    fn inflate(&mut self, chunk_number: usize) -> Result<(), String> {
        if self.cached_chunk == Some(chunk_number) {
            return Ok(());
        }
        self.cached_chunk = None;
        inflate_chunk(&self.index, &self.dat, self.form, chunk_number, &mut self.chunk)?;
        self.inflations += 1;
        self.cached_chunk = Some(chunk_number);
        Ok(())
    }

    /// The records the stream holds when its index is `index` and its data
    /// `dat`: every chunk but the last holds `chunk_size`, and the last as many
    /// as decode out of it, decoded into `scratch`.
    fn count_records(&self, index: &StepsIndex, dat: &MemberBytes, scratch: &mut StepChunk) -> Result<u64, String> {
        let Some(last_chunk) = index.chunk_offsets.len().checked_sub(1) else {
            return Ok(0);
        };
        if index.chunk_offsets[last_chunk] as usize > dat.len() {
            return Err("steps.idx: last chunk offset past end of steps.dat".to_string());
        }
        inflate_chunk(index, dat, self.form, last_chunk, scratch)?;
        while scratch
            .next(self.allow_source_reload)
            .map_err(|e| format!("steps.dat chunk {last_chunk}: {e}"))?
            .is_some()
        {}
        Ok((last_chunk * index.chunk_size + scratch.cursor.record) as u64)
    }

    /// Follow a container that is being written: extend this reader by the
    /// chunks `reader`'s container has published since it was opened or last
    /// refreshed (`ctfs-container.md` §6). Only the new last chunk is decoded,
    /// to count its records; chunks already read stay as they were read. A
    /// re-read index that does not extend the one already read is refused.
    pub fn refresh(&mut self, reader: &mut CtfsReader) -> Result<(), String> {
        let (dat, idx) = crate::follow::read_published(reader, "steps")?;
        let index = StepsIndex::parse(&idx)?;
        crate::follow::check_extends(
            "steps.idx",
            self.index.chunk_size,
            &self.index.chunk_offsets,
            index.chunk_size,
            &index.chunk_offsets,
            dat.len(),
        )?;
        if index.chunk_offsets.len() != self.index.chunk_offsets.len() {
            let mut scratch = StepChunk::default();
            self.record_count = self.count_records(&index, &dat, &mut scratch)?;
            self.inflations += 1;
        }
        self.index = index;
        self.dat = dat;
        Ok(())
    }

    /// How many chunks this reader has inflated: reads, counting the last
    /// chunk at open, and counting a new last chunk at a refresh.
    pub fn inflations(&self) -> u64 {
        self.inflations
    }

    /// Open the step stream from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no `steps.dat` — existence is answered
    /// by STRUCTURAL PRESENCE of the stream file, not by the `has_step_stream`
    /// hint bit (see [`Self::from_files`]).
    pub fn open(reader: &mut CtfsReader) -> Result<Option<StepStreamReader>, String> {
        crate::retired_streams::refuse_retired_members(reader)?;
        let dat = match reader.read_member("steps.dat") {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let idx = reader
            .read_file("steps.idx")
            .map_err(|e| format!("steps.idx missing despite steps.dat presence: {e}"))?;
        let meta = reader.read_file("meta.dat").unwrap_or_default();
        StepStreamReader::from_member_as(&meta, dat, &idx, ChunkForm::of(reader))
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
        match self.chunk.record(within, self.allow_source_reload) {
            Ok(Some(rec)) => Ok(rec),
            Ok(None) => Err(format!("step record {within} missing in chunk {chunk_number}")),
            Err(e) => {
                // The cursor stopped inside the record that failed; a later
                // read starts the chunk over and meets the same refusal.
                self.cached_chunk = None;
                Err(format!("steps.dat chunk {chunk_number}: {e}"))
            }
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
