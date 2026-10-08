//! Reader for the dedicated `events.dat` I/O event stream (M23c).
//!
//! Reads I/O event records (stdout/stderr/file/network/error/log) from a CTFS
//! container's `events.dat` stream via its companion seekable index
//! `events.idx`, per `codetracer-trace-format-spec/seekable-zstd.md`. The
//! event-log pane's access pattern is a *paginated scan*: this reader exposes
//! both a single-record [`IoEventStreamReader::read`] (decompressing only the
//! one chunk that holds the target record) and a [`IoEventStreamReader::read_page`]
//! that returns a contiguous range of records, decompressing only the chunk(s)
//! that the page spans. This mirrors the M23a `StepStreamReader`, the M23b
//! `ValueStreamReader`, and the M17a `CallStreamReader`.
//!
//! The stream is gated by the `has_io_event_stream` capability flag (bit 11) in
//! `meta.dat`. A reader that does not see the flag, or a container without
//! `events.dat`, simply has no I/O event stream — the unified `events.log`
//! `Event` records remain the source of truth.
//!
//! # File-naming note: `events.dat` is NOT `events.log`
//!
//! The I/O event stream lives in its OWN CTFS file pair `events.dat`/
//! `events.idx`, DISTINCT from the legacy combined `events.log`. See the module
//! docs of `codetracer_trace_writer::event_stream` for the full rationale.

use crate::ChunkForm;
use codetracer_ctfs::{CtfsReader, MemberBytes};
use codetracer_trace_writer::event_stream::IoEventRecord;

/// A loaded `events.idx`: the per-chunk byte offsets into `events.dat`.
struct EventsIndex {
    chunk_size: usize,
    /// Byte offset of each chunk within `events.dat`.
    chunk_offsets: Vec<u64>,
}

impl EventsIndex {
    /// Parse `events.idx`: `[chunk_size: u32 LE][offset_0: u64 LE]...`.
    fn parse(idx: &[u8]) -> Result<EventsIndex, String> {
        if idx.len() < 4 {
            return Err("events.idx: too short for chunk_size header".to_string());
        }
        let chunk_size = u32::from_le_bytes([idx[0], idx[1], idx[2], idx[3]]) as usize;
        if chunk_size == 0 {
            return Err("events.idx: chunk_size is zero".to_string());
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
        Ok(EventsIndex { chunk_size, chunk_offsets })
    }
}

// --- varint helper (unsigned LEB128) for the per-record length prefix ---

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err("events.dat: truncated record-length varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err("events.dat: record-length varint too long".to_string());
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Decode all of one chunk's length-prefixed I/O event records, inflating the
/// chunk first when it is a frame.
fn decode_chunk_records(stored: &[u8], form: ChunkForm, chunk: usize, chunk_size: usize) -> Result<Vec<IoEventRecord>, String> {
    let raw = form.content("events.dat", stored)?;
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < raw.len() {
        let k = records.len();
        let name = format!("events.dat record {} (record {k} of chunk {chunk})", chunk * chunk_size + k);
        let rec_len = decode_varint(&raw, &mut pos).map_err(|e| format!("{name}: {e}"))? as usize;
        if pos + rec_len > raw.len() {
            return Err(format!("{name}: its {rec_len}-byte frame extends past the chunk"));
        }
        // `decode` refuses a record whose fields do not consume its frame
        // exactly (`trace-events.md`, "Each record is framed by its length").
        let rec = IoEventRecord::decode(&raw[pos..pos + rec_len]).map_err(|e| format!("{name}: {}", e.trim_start_matches("events.dat: ")))?;
        pos += rec_len;
        records.push(rec);
    }
    Ok(records)
}

pub struct IoEventStreamReader {
    index: EventsIndex,
    /// `events.dat`, shared with the container when it is in memory.
    dat: MemberBytes,
    /// Total number of I/O event records.
    record_count: u64,
    /// Cache of the most-recently-decompressed chunk: (chunk_number, records).
    cached_chunk: Option<(usize, Vec<IoEventRecord>)>,
    /// Whether a chunk is a frame to inflate or its content.
    form: ChunkForm,
    /// Chunks inflated so far; see [`IoEventStreamReader::inflations`].
    inflations: u64,
}

/// The records of chunk `k` of the stream `index` locates in `dat`.
fn chunk_records(index: &EventsIndex, dat: &MemberBytes, form: ChunkForm, k: usize) -> Result<Vec<IoEventRecord>, String> {
    if k >= index.chunk_offsets.len() {
        return Err(format!("events.dat: chunk {k} out of range"));
    }
    let start = index.chunk_offsets[k] as usize;
    let end = crate::follow::chunk_end("events.dat", form, dat, &index.chunk_offsets, k)?;
    if start > end || end > dat.len() {
        return Err("events.dat: chunk offsets out of range".to_string());
    }
    let stored = dat.get(start, end).map_err(|e| format!("events.dat: chunk {k}: {e}"))?;
    decode_chunk_records(&stored, form, k, index.chunk_size)
}

/// The records the stream holds when its index is `index` and its data
/// `dat`: every chunk but the last holds `chunk_size`, and the last as many as
/// decode out of it.
fn count_records(index: &EventsIndex, dat: &MemberBytes, form: ChunkForm) -> Result<u64, String> {
    let Some(last_chunk) = index.chunk_offsets.len().checked_sub(1) else {
        return Ok(0);
    };
    if index.chunk_offsets[last_chunk] as usize > dat.len() {
        return Err("events.idx: last chunk offset past end of events.dat".to_string());
    }
    let last_records = chunk_records(index, dat, form, last_chunk)?.len();
    Ok((last_chunk * index.chunk_size + last_records) as u64)
}

impl IoEventStreamReader {
    /// Open the I/O event stream from an already-open CTFS reader. Returns
    /// `Ok(None)` when the container has no `events.dat` — existence is answered
    /// by STRUCTURAL PRESENCE of the stream file, NOT by the `has_io_event_stream`
    /// hint bit (bit 11), which a writer may stamp only at close (trace-format
    /// spec: "Stream-presence flags are a hint, not a gate").
    pub fn open(reader: &mut CtfsReader) -> Result<Option<IoEventStreamReader>, String> {
        crate::retired_streams::refuse_retired_members(reader)?;
        let dat = match reader.read_member("events.dat") {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        let idx = reader
            .read_file("events.idx")
            .map_err(|e| format!("events.idx missing despite events.dat presence: {e}"))?;
        let index = EventsIndex::parse(&idx)?;
        let form = ChunkForm::of(reader);
        let record_count = count_records(&index, &dat, form)?;
        let inflations = u64::from(!index.chunk_offsets.is_empty());
        Ok(Some(IoEventStreamReader {
            index,
            dat,
            record_count,
            cached_chunk: None,
            form,
            inflations,
        }))
    }

    /// Follow a container that is being written: extend this reader by the
    /// chunks `reader`'s container has published since it was opened or last
    /// refreshed (`ctfs-container.md` §6). Only the new last chunk is decoded,
    /// to count its records; the chunk already held stays held. A re-read
    /// index that does not extend the one already read is refused.
    pub fn refresh(&mut self, reader: &mut CtfsReader) -> Result<(), String> {
        let (dat, idx) = crate::follow::read_published(reader, "events")?;
        let index = EventsIndex::parse(&idx)?;
        crate::follow::check_extends(
            "events.idx",
            self.index.chunk_size,
            &self.index.chunk_offsets,
            index.chunk_size,
            &index.chunk_offsets,
            dat.len(),
        )?;
        if index.chunk_offsets.len() != self.index.chunk_offsets.len() {
            self.record_count = count_records(&index, &dat, self.form)?;
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

    /// Total number of I/O event records in the stream.
    pub fn count(&self) -> u64 {
        self.record_count
    }

    /// The fixed number of records per chunk (the page/seek granularity).
    /// Exposed so a downstream reader can account for bounded decompression.
    pub fn chunk_size(&self) -> usize {
        self.index.chunk_size
    }

    /// The chunk number currently held in the one-chunk decompression cache, or
    /// `None` if nothing has been decompressed yet. Lets a downstream reader
    /// observe exactly which chunk was inflated (bounded-decompression probe).
    pub fn cached_chunk(&self) -> Option<usize> {
        self.cached_chunk.as_ref().map(|(c, _)| *c)
    }

    /// Ensure chunk `chunk_number` is decompressed into the cache.
    fn ensure_chunk(&mut self, chunk_number: usize) -> Result<(), String> {
        let need_decompress = !matches!(&self.cached_chunk, Some((c, _)) if *c == chunk_number);
        if need_decompress {
            let records = chunk_records(&self.index, &self.dat, self.form, chunk_number)?;
            self.inflations += 1;
            self.cached_chunk = Some((chunk_number, records));
        }
        Ok(())
    }

    /// Read the I/O event record at index `record_index`, decompressing only its
    /// chunk.
    pub fn read(&mut self, record_index: u64) -> Result<IoEventRecord, String> {
        if record_index >= self.record_count {
            return Err(format!("io event index {record_index} out of range (count {})", self.record_count));
        }
        let chunk_number = (record_index as usize) / self.index.chunk_size;
        let within = (record_index as usize) % self.index.chunk_size;
        self.ensure_chunk(chunk_number)?;
        let records = &self.cached_chunk.as_ref().unwrap().1;
        if within >= records.len() {
            return Err(format!("io event record {within} missing in chunk {chunk_number}"));
        }
        Ok(records[within].clone())
    }

    /// Read a PAGE of I/O event records: the contiguous range
    /// `[start, start + len)` (clamped to the record count), decompressing only
    /// the chunk(s) the page spans. This is the event-log pane's primary access
    /// pattern. Returns the records in order; an empty page (`start >= count` or
    /// `len == 0`) returns an empty vec.
    pub fn read_page(&mut self, start: u64, len: u64) -> Result<Vec<IoEventRecord>, String> {
        if len == 0 || start >= self.record_count {
            return Ok(Vec::new());
        }
        let end = start.saturating_add(len).min(self.record_count);
        let mut out = Vec::with_capacity((end - start) as usize);
        let chunk_size = self.index.chunk_size as u64;
        let mut i = start;
        while i < end {
            let chunk_number = (i / chunk_size) as usize;
            self.ensure_chunk(chunk_number)?;
            let records = &self.cached_chunk.as_ref().unwrap().1;
            // Walk the records of this chunk that fall within the page.
            let chunk_first = chunk_number as u64 * chunk_size;
            let within_start = (i - chunk_first) as usize;
            let chunk_end = (chunk_first + chunk_size).min(end);
            for idx in within_start..(chunk_end - chunk_first) as usize {
                if idx >= records.len() {
                    return Err(format!("events.dat: record {idx} missing in chunk {chunk_number}"));
                }
                out.push(records[idx].clone());
            }
            i = chunk_end;
        }
        Ok(out)
    }

    /// Read all I/O event records (convenience for tests / small traces).
    pub fn read_all(&mut self) -> Result<Vec<IoEventRecord>, String> {
        self.read_page(0, self.record_count)
    }
}

/// Open the I/O event stream directly from a `.ct` file path. Returns `Ok(None)`
/// when the container carries no dedicated I/O event stream.
pub fn open_io_event_stream(path: &std::path::Path) -> Result<Option<IoEventStreamReader>, String> {
    let mut reader = CtfsReader::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    IoEventStreamReader::open(&mut reader)
}
