//! Dedicated I/O event stream (`events.dat`) for materialized CTFS `.ct` traces.
//!
//! This is the M23c deliverable of the Trace-Based-Incremental-Testing
//! campaign (the third sub-milestone of M23 — "finish the trace-events.md
//! Event Stream Redesign"): an *additive*, backward-compatible split of the
//! I/O / log events out of the unified `events.log`. It mirrors the M23a
//! `steps.dat` split, the M23b `values.dat` split, and the M17a `calls.dat`
//! split exactly — recorders that opt in emit, in addition to the unchanged
//! `events.log`, a dedicated I/O event stream plus a companion seekable index,
//! gated by a new `meta.dat` capability flag `has_io_event_stream` (bit 11).
//! Readers that do not know the flag simply ignore the two extra files, so old
//! `.ct`s and old readers keep working byte-for-byte.
//!
//! # File-naming note: `events.dat` is NOT `events.log`
//!
//! The *legacy* combined stream file (every event tag, CBOR/split-binary) is
//! `events.log`. The new I/O event stream introduced here is the distinct file
//! **`events.dat`** (+ its companion **`events.idx`**), holding ONLY the
//! `EventLogKind`-tagged I/O / log events (stdout/stderr/file/network/error/log).
//! The two coexist in one container; M23c does not retire `events.log` (that is
//! a later sub-milestone). Keep the names distinct — they must never collide.
//!
//! # Source events
//!
//! The records derive from the SAME `TraceLowLevelEvent::Event(RecordEvent)`
//! events that feed `events.log` (the legacy "Event" tag 9, the only source of
//! I/O-event records). This guarantees `events.dat` is consistent with the
//! I/O events still present in `events.log`.
//!
//! # `step_id` cross-reference to the execution stream
//!
//! Per `codetracer-trace-format-spec/trace-events.md` §"IO Event Stream
//! Records", each I/O event record carries a `step_id` varint that
//! cross-references the execution stream (`steps.dat`) — the step at which the
//! I/O event occurred. The writer attributes every I/O event to the
//! most-recently-emitted `Step` (the number of `Step` events seen so far minus
//! one). I/O events that appear before the very first `Step` are attributed to
//! step 0. This mirrors how the value stream attributes value events to the
//! current step, and is the same time-coordinate the event-log pane uses.
//!
//! # Per-record wire format
//!
//! IO event records are NOT tagged — each record has a fixed structure
//! (trace-events.md §"IO Event Stream Records"):
//!
//! ```text
//!   kind     : u8 (EventLogKind ordinal)
//!   step_id  : varint (cross-reference to the execution stream)
//!   metadata : varint len + bytes
//!   content  : varint len + bytes
//! ```
//!
//! The `metadata` / `content` bytes are the raw bytes of the legacy
//! `RecordEvent`'s `metadata` / `content` strings (UTF-8), preserved verbatim so
//! the stream is byte-consistent with the `Event` events in `events.log`. M23c
//! does NOT redesign `RecordEvent`; it only routes the I/O events into the
//! parallel stream.
//!
//! # Storage (`events.dat` + `events.idx`)
//!
//! Records are grouped into chunks of `chunk_size` records, each independently
//! Zstd-compressed, concatenated into `events.dat` with **no inline headers**.
//! The companion `events.idx` follows
//! `codetracer-trace-format-spec/seekable-zstd.md`:
//!
//! ```text
//!   events.dat:  [zstd(chunk_0)][zstd(chunk_1)]...
//!   events.idx:  [chunk_size: u32 LE][offset_0: u64 LE][offset_1: u64 LE]...
//! ```
//!
//! `offset_i` is the byte offset of chunk `i` within `events.dat`. The event-log
//! pane loads PAGES of records: to read record `N`, `chunk = N / chunk_size`,
//! decompress that one chunk, and index `N % chunk_size` within it — O(1) chunks,
//! no whole-stream decompression. This mirrors `calls.dat`/`steps.dat`/
//! `values.dat` exactly.

use codetracer_trace_types::{EventLogKind, RecordEvent, TraceLowLevelEvent};

/// Default number of I/O event records per chunk. I/O event records are
/// moderately sized (spec §"Stream Summary": 20-1000 bytes each) and accessed
/// by paginated scan, so a modest chunk size gives a good page granularity
/// without excessive per-page decompression.
pub const DEFAULT_EVENTS_CHUNK_SIZE: usize = 64;

/// One decoded I/O event record. This is the on-disk projection of the `Event`
/// events that, in the legacy unified stream, appear interleaved between `Step`
/// events. M23c keeps the EXISTING `metadata` / `content` payloads (carried as
/// raw bytes here) so the I/O event stream is byte-consistent with `events.log`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoEventRecord {
    /// Event category (`EventLogKind` ordinal: stdout/stderr/file/network/…).
    pub kind: u8,
    /// Cross-reference to the execution stream: the step at which this I/O event
    /// occurred (the index of the most-recently-emitted `Step`).
    pub step_id: u64,
    /// Event metadata (raw bytes; the legacy `RecordEvent.metadata` string).
    pub metadata: Vec<u8>,
    /// Event content (raw bytes; the legacy `RecordEvent.content` string).
    pub content: Vec<u8>,
}

// --- varint helpers (unsigned LEB128) ---

fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err("events.dat: truncated varint".to_string());
        }
        let byte = data[*pos];
        *pos += 1;
        if shift >= 64 {
            return Err("events.dat: varint too long".to_string());
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Read a varint-length-prefixed byte blob from `data` at `*pos`.
fn decode_blob(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, String> {
    let len = decode_varint(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err("events.dat: truncated blob".to_string());
    }
    let blob = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(blob)
}

impl IoEventRecord {
    /// Encode this record (kind / step_id / metadata / content) into `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.kind);
        encode_varint(self.step_id, out);
        encode_varint(self.metadata.len() as u64, out);
        out.extend_from_slice(&self.metadata);
        encode_varint(self.content.len() as u64, out);
        out.extend_from_slice(&self.content);
    }

    /// Decode one record from its raw bytes (the whole record, no length prefix).
    pub fn decode(data: &[u8]) -> Result<IoEventRecord, String> {
        let mut pos = 0usize;
        let rec = IoEventRecord::decode_at(data, &mut pos)?;
        if pos != data.len() {
            return Err(format!("events.dat: trailing bytes in record ({} of {})", pos, data.len()));
        }
        Ok(rec)
    }

    /// Decode one record at `*pos`, advancing `*pos` past it.
    pub fn decode_at(data: &[u8], pos: &mut usize) -> Result<IoEventRecord, String> {
        if *pos >= data.len() {
            return Err("events.dat: truncated record (no kind)".to_string());
        }
        let kind = data[*pos];
        *pos += 1;
        let step_id = decode_varint(data, pos)?;
        let metadata = decode_blob(data, pos)?;
        let content = decode_blob(data, pos)?;
        Ok(IoEventRecord {
            kind,
            step_id,
            metadata,
            content,
        })
    }
}

/// Builds the dedicated I/O event stream from the same event sequence that feeds
/// `events.log`, so the two are guaranteed consistent.
///
/// The builder tracks the current step index (number of `Step` events seen so
/// far). On each `Event`, it appends an [`IoEventRecord`] tagged with the current
/// step id (the index of the most-recently-emitted `Step`, or 0 before the first
/// `Step`). Non-`Event`, non-`Step` events are ignored.
pub struct IoEventStreamBuilder {
    /// Finalized I/O event records, in stream order.
    records: Vec<IoEventRecord>,
    /// The step id to attribute the next I/O event to: index of the
    /// most-recently-emitted `Step`. `None` until the first `Step` is seen
    /// (pre-first-step I/O events attribute to step 0).
    current_step: Option<u64>,
}

impl Default for IoEventStreamBuilder {
    fn default() -> Self {
        IoEventStreamBuilder::new()
    }
}

impl IoEventStreamBuilder {
    pub fn new() -> Self {
        IoEventStreamBuilder {
            records: Vec::new(),
            current_step: None,
        }
    }

    /// Feed one event in stream order.
    pub fn observe(&mut self, event: &TraceLowLevelEvent) {
        match event {
            TraceLowLevelEvent::Step(_) => {
                // Advance the current step id. Step 0 is the first Step.
                self.current_step = Some(match self.current_step {
                    None => 0,
                    Some(s) => s + 1,
                });
            }
            TraceLowLevelEvent::Event(RecordEvent { kind, metadata, content }) => {
                self.records.push(IoEventRecord {
                    kind: event_log_kind_ord(*kind),
                    // I/O events before the first Step attribute to step 0.
                    step_id: self.current_step.unwrap_or(0),
                    metadata: metadata.clone().into_bytes(),
                    content: content.clone().into_bytes(),
                });
            }
            _ => {}
        }
    }

    /// Number of I/O event records built so far.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Finalize: return the I/O event records in stream order.
    pub fn finish(self) -> Vec<IoEventRecord> {
        self.records
    }
}

/// Map an [`EventLogKind`] to its stable on-disk ordinal. The ordinal is the
/// enum's `repr(u8)` discriminant — the exact value the legacy `events.log`
/// already carries — so `events.dat` and `events.log` agree on the kind byte.
fn event_log_kind_ord(kind: EventLogKind) -> u8 {
    kind as u8
}

/// The encoded `events.dat` stream plus its companion `events.idx`.
#[cfg(not(target_arch = "wasm32"))]
pub struct EncodedIoEventStream {
    /// Concatenated Zstd-compressed chunks, no inline headers.
    pub dat: Vec<u8>,
    /// Companion index: `[chunk_size: u32 LE][offset_0: u64 LE]...`.
    pub idx: Vec<u8>,
    /// Number of I/O event records encoded.
    pub record_count: usize,
}

/// Encode I/O event records into `events.dat` (chunked Zstd) + `events.idx`
/// (companion offset index), per seekable-zstd.md.
///
/// Each record is length-prefixed within its chunk so the reader can walk to the
/// `N % chunk_size`-th record without re-deriving sizes (records are variable
/// length). Each chunk is independently Zstd-compressed.
#[cfg(not(target_arch = "wasm32"))]
pub fn encode_io_event_stream(records: &[IoEventRecord], chunk_size: usize, zstd_level: i32) -> Result<EncodedIoEventStream, String> {
    use std::io::Cursor;
    let chunk_size = chunk_size.max(1);
    let mut dat: Vec<u8> = Vec::new();
    let mut idx: Vec<u8> = Vec::new();
    idx.extend_from_slice(&(chunk_size as u32).to_le_bytes());

    let mut i = 0usize;
    while i < records.len() {
        let end = (i + chunk_size).min(records.len());
        // Record the byte offset of this chunk within events.dat.
        idx.extend_from_slice(&(dat.len() as u64).to_le_bytes());

        let mut raw: Vec<u8> = Vec::new();
        for rec in &records[i..end] {
            let mut rec_bytes: Vec<u8> = Vec::new();
            rec.encode(&mut rec_bytes);
            // Length-prefix each record so the reader can index within a chunk.
            encode_varint(rec_bytes.len() as u64, &mut raw);
            raw.extend_from_slice(&rec_bytes);
        }
        let compressed = zstd::encode_all(Cursor::new(&raw[..]), zstd_level).map_err(|e| format!("events.dat: zstd encode failed: {e}"))?;
        dat.extend_from_slice(&compressed);
        i = end;
    }

    Ok(EncodedIoEventStream {
        dat,
        idx,
        record_count: records.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codetracer_trace_types::{Line, PathId, StepRecord};

    fn step(line: i64) -> TraceLowLevelEvent {
        TraceLowLevelEvent::Step(StepRecord {
            path_id: PathId(0),
            line: Line(line),
        })
    }

    fn io_event(kind: EventLogKind, meta: &str, content: &str) -> TraceLowLevelEvent {
        TraceLowLevelEvent::Event(RecordEvent {
            kind,
            metadata: meta.to_string(),
            content: content.to_string(),
        })
    }

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            encode_varint(v, &mut buf);
            let mut pos = 0;
            assert_eq!(decode_varint(&buf, &mut pos).unwrap(), v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn record_roundtrips_including_empty_and_binary() {
        let recs = vec![
            IoEventRecord {
                kind: EventLogKind::Write as u8,
                step_id: 0,
                metadata: b"stdout".to_vec(),
                content: b"hello world\n".to_vec(),
            },
            // Empty metadata + content (still a valid record).
            IoEventRecord {
                kind: EventLogKind::Error as u8,
                step_id: 16384,
                metadata: Vec::new(),
                content: Vec::new(),
            },
            // Embedded NUL / non-UTF-8-shaped bytes survive verbatim.
            IoEventRecord {
                kind: EventLogKind::WriteFile as u8,
                step_id: 7,
                metadata: b"/tmp/f".to_vec(),
                content: vec![0x00, 0xff, 0x10, 0x00],
            },
        ];
        for rec in &recs {
            let mut buf = Vec::new();
            rec.encode(&mut buf);
            assert_eq!(&IoEventRecord::decode(&buf).unwrap(), rec);
        }
    }

    #[test]
    fn builder_attributes_step_ids() {
        let mut b = IoEventStreamBuilder::new();
        // I/O event before the first step → step 0.
        b.observe(&io_event(EventLogKind::Write, "stdout", "pre\n"));
        b.observe(&step(1)); // step 0
        b.observe(&io_event(EventLogKind::Write, "stdout", "a\n"));
        b.observe(&step(2)); // step 1
        b.observe(&step(3)); // step 2
        b.observe(&io_event(EventLogKind::Error, "stderr", "b\n"));
        let recs = b.finish();
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].step_id, 0);
        assert_eq!(recs[1].step_id, 0);
        assert_eq!(recs[2].step_id, 2);
    }

    #[test]
    fn encode_chunks_and_index_shape() {
        let recs: Vec<IoEventRecord> = (0..10)
            .map(|i| IoEventRecord {
                kind: EventLogKind::Write as u8,
                step_id: i,
                metadata: b"m".to_vec(),
                content: format!("line{i}\n").into_bytes(),
            })
            .collect();
        let encoded = encode_io_event_stream(&recs, 4, 3).unwrap();
        assert_eq!(encoded.record_count, 10);
        // chunk_size header (u32) + ceil(10/4)=3 chunk offsets (u64 each).
        assert_eq!(encoded.idx.len(), 4 + 3 * 8);
        let chunk_size = u32::from_le_bytes([encoded.idx[0], encoded.idx[1], encoded.idx[2], encoded.idx[3]]);
        assert_eq!(chunk_size, 4);
    }
}
