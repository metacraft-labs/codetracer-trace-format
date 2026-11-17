//! The span stream: `spans.dat`, `spans.idx` and `spantype.ns`.
//!
//! A span is a bounded, labeled interval of execution — an HTTP request, a
//! process, a test, a native-to-VM crossing — named by the coordinate
//! *(process_ord, thread_id, step range)*.
//!
//! # `spans.dat`
//!
//! A chunked compressed table: each chunk is one zstd frame whose content is
//! the concatenation of length-prefixed records (`[varint rec_len][rec]...`).
//! A chunk seals when it holds [`DEFAULT_SPANS_CHUNK_SIZE`] records, or earlier
//! when the writer is asked to [flush](SpanStreamBuilder::seal), so a chunk may
//! be short anywhere in the stream.
//!
//! # `spans.idx`
//!
//! ```text
//! Header (8 bytes): [chunk_size u32][index_version u16 = 2][reserved u16 = 0]
//! Entries (16 bytes each): [offset u64][cumulative_records u64]
//! ```
//!
//! `offset` is the chunk's first byte in `spans.dat`; `cumulative_records` is
//! the number of records in chunks `0..=i`. Because chunks may be short,
//! `chunk_size` is only the writer's seal-at threshold: a record is located by
//! the cumulative column, never by `index / chunk_size`.
//!
//! # Record (wire version 1)
//!
//! ```text
//! span_id varint (>= 1) | parent_span_id varint | flags u8 | status u8
//! start_wall_ns varint | end_wall_ns varint | process_ord varint
//! thread_id varint | start_step varint | end_step varint
//! [external_recording str | external_path str]   -- only when flags bit 1
//! span_type str | label str | structural u8
//! metadata_count varint | (key str, value str) * metadata_count
//! ```
//!
//! `str` is a varint byte length followed by that many UTF-8 bytes. `flags`
//! bit 0 marks an open record, bit 1 an external binding; `status` is 0
//! (unknown), 1 (ok) or 2 (error); `structural` bits 0..2 are
//! contiguous-on-one-thread, shares-timeline and concurrent-with-siblings. An
//! open record carries `end_wall_ns == 0` and `end_step == 0`. Any other bit,
//! status or trailing byte makes the record malformed.
//!
//! A span may be appended twice under one `span_id` — open, then settled — and
//! readers resolve the pair by last-record-wins.
//!
//! # `spantype.ns`
//!
//! ```text
//! Header (18 bytes): [magic u32 = "SPTY"][version u16 = 1][type_count u32]
//!                    [type_table_offset u64]
//! Type table, 28 bytes per type, by interned id:
//!   [span_type_id u32][name_len u32][name_offset u64][span_count u32][spans_offset u64]
//! Name bytes and span-id lists (u64 LE, ascending, distinct) follow.
//! ```
//!
//! Span types are interned in first-appearance order. The Nim writer's
//! `span_stream.nim` is the reference for every byte here.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::column_aware::{decode_varint, encode_varint};

/// Data member of the span stream.
pub const SPANS_DATA_FILE_NAME: &str = "spans.dat";
/// Index member of the span stream.
pub const SPANS_INDEX_FILE_NAME: &str = "spans.idx";
/// Span-type index member.
pub const SPAN_TYPE_NAMESPACE_FILE_NAME: &str = "spantype.ns";

/// Records per chunk: the seal-at threshold.
pub const DEFAULT_SPANS_CHUNK_SIZE: usize = 64;
/// Zstd level of a span chunk.
pub const SPANS_COMPRESSION_LEVEL: i32 = 3;
/// `spans.idx` layout version.
pub const SPANS_INDEX_VERSION: u16 = 2;
/// `[chunk_size u32][index_version u16][reserved u16]`.
pub const SPANS_INDEX_HEADER_SIZE: usize = 8;
/// `[offset u64][cumulative_records u64]`.
pub const SPANS_INDEX_ENTRY_SIZE: usize = 16;

/// `flags` bit 0: the record is open; its completion is still to come.
pub const SPAN_FLAG_OPEN: u8 = 0x01;
/// `flags` bit 1: the span's execution lives in a different container.
pub const SPAN_FLAG_EXTERNAL: u8 = 0x02;
const SPAN_FLAGS_KNOWN: u8 = SPAN_FLAG_OPEN | SPAN_FLAG_EXTERNAL;

/// `structural` bit 0: an uninterrupted run on one thread.
pub const SPAN_STRUCTURAL_CONTIGUOUS: u8 = 0x01;
/// `structural` bit 1: ordering is comparable with sibling intervals.
pub const SPAN_STRUCTURAL_SHARES_TIMELINE: u8 = 0x02;
/// `structural` bit 2: sibling intervals may overlap in time.
pub const SPAN_STRUCTURAL_CONCURRENT: u8 = 0x04;
const SPAN_STRUCTURAL_KNOWN: u8 = SPAN_STRUCTURAL_CONTIGUOUS | SPAN_STRUCTURAL_SHARES_TIMELINE | SPAN_STRUCTURAL_CONCURRENT;

/// `status`: unknown.
pub const SPAN_STATUS_UNKNOWN: u8 = 0;
/// `status`: ok.
pub const SPAN_STATUS_OK: u8 = 1;
/// `status`: error.
pub const SPAN_STATUS_ERROR: u8 = 2;

/// `spantype.ns` magic, ASCII "SPTY" read as a little-endian u32.
pub const SPAN_TYPE_NS_MAGIC: u32 = 0x5350_5459;
/// `spantype.ns` version.
pub const SPAN_TYPE_NS_VERSION: u16 = 1;
const SPAN_TYPE_NS_HEADER_SIZE: usize = 18;
const SPAN_TYPE_NS_ENTRY_SIZE: usize = 28;

/// One span record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpanRecord {
    /// 1-based; the last-record-wins key.
    pub span_id: u64,
    /// 0 = none.
    pub parent_span_id: u64,
    /// `flags` bit 0: `end_wall_ns` and `end_step` are 0.
    pub is_open: bool,
    /// `flags` bit 1: `external_recording` / `external_path` name the container.
    pub is_external: bool,
    /// [`SPAN_STATUS_UNKNOWN`], [`SPAN_STATUS_OK`] or [`SPAN_STATUS_ERROR`].
    pub status: u8,
    /// UNIX epoch nanoseconds at span start.
    pub start_wall_ns: u64,
    /// 0 when `is_open`.
    pub end_wall_ns: u64,
    /// Ordinal into the process table; 0 = primary.
    pub process_ord: u64,
    pub thread_id: u64,
    /// First step id inside the span.
    pub start_step: u64,
    /// Last step id inside the span; 0 when `is_open`.
    pub end_step: u64,
    /// Recording id of the container holding the span; only when `is_external`.
    pub external_recording: String,
    /// Path of that container relative to this one; only when `is_external`.
    pub external_path: String,
    /// `"web-request"`, `"process"`, `"test"`, ...
    pub span_type: String,
    pub label: String,
    /// `structural` bit 0.
    pub contiguous_on_one_thread: bool,
    /// `structural` bit 1.
    pub shares_timeline: bool,
    /// `structural` bit 2.
    pub concurrent_with_siblings: bool,
    /// Ordered key/value pairs; the order is part of the record.
    pub metadata: Vec<(String, String)>,
}

impl SpanRecord {
    /// The record's `flags` byte.
    pub fn flags_byte(&self) -> u8 {
        (if self.is_open { SPAN_FLAG_OPEN } else { 0 }) | (if self.is_external { SPAN_FLAG_EXTERNAL } else { 0 })
    }

    /// The record's `structural` byte.
    pub fn structural_byte(&self) -> u8 {
        (if self.contiguous_on_one_thread { SPAN_STRUCTURAL_CONTIGUOUS } else { 0 })
            | (if self.shares_timeline { SPAN_STRUCTURAL_SHARES_TIMELINE } else { 0 })
            | (if self.concurrent_with_siblings { SPAN_STRUCTURAL_CONCURRENT } else { 0 })
    }
}

fn put_str(s: &str, out: &mut Vec<u8>) {
    encode_varint(s.len() as u64, out);
    out.extend_from_slice(s.as_bytes());
}

/// Encode `s` as a record (no length prefix). A record the format cannot
/// carry faithfully is refused: a zero `span_id`, an open record with an end,
/// external-binding strings on a span that is not external, or a status
/// outside 0..=2.
pub fn encode_span_record(s: &SpanRecord) -> Result<Vec<u8>, String> {
    if s.span_id == 0 {
        return Err("span record: span_id must be 1-based (got 0)".to_string());
    }
    if s.is_open && (s.end_wall_ns != 0 || s.end_step != 0) {
        return Err(format!("span record: open span {} must have end_wall_ns and end_step == 0", s.span_id));
    }
    if !s.is_external && (!s.external_recording.is_empty() || !s.external_path.is_empty()) {
        return Err(format!(
            "span record: span {} carries external binding fields but flags.external is not set",
            s.span_id
        ));
    }
    if s.status > SPAN_STATUS_ERROR {
        return Err(format!("span record: invalid status value {}", s.status));
    }
    let mut buf = Vec::new();
    encode_varint(s.span_id, &mut buf);
    encode_varint(s.parent_span_id, &mut buf);
    buf.push(s.flags_byte());
    buf.push(s.status);
    encode_varint(s.start_wall_ns, &mut buf);
    encode_varint(s.end_wall_ns, &mut buf);
    encode_varint(s.process_ord, &mut buf);
    encode_varint(s.thread_id, &mut buf);
    encode_varint(s.start_step, &mut buf);
    encode_varint(s.end_step, &mut buf);
    if s.is_external {
        put_str(&s.external_recording, &mut buf);
        put_str(&s.external_path, &mut buf);
    }
    put_str(&s.span_type, &mut buf);
    put_str(&s.label, &mut buf);
    buf.push(s.structural_byte());
    encode_varint(s.metadata.len() as u64, &mut buf);
    for (k, v) in &s.metadata {
        put_str(k, &mut buf);
        put_str(v, &mut buf);
    }
    Ok(buf)
}

fn read_str(data: &[u8], pos: &mut usize, what: &str) -> Result<String, String> {
    let len = decode_varint(data, pos)?;
    let end = usize::try_from(len)
        .ok()
        .and_then(|l| pos.checked_add(l))
        .filter(|&end| end <= data.len())
        .ok_or_else(|| format!("span record: {what} extends past end of record"))?;
    let s = std::str::from_utf8(&data[*pos..end])
        .map_err(|_| format!("span record: {what} is not UTF-8"))?
        .to_string();
    *pos = end;
    Ok(s)
}

/// Decode one record (no length prefix). Every malformation is an error: a
/// truncated field, trailing bytes, an unknown `flags` or `structural` bit, a
/// status outside 0..=2, a zero `span_id`, an open record with an end, a
/// string that is not UTF-8.
pub fn decode_span_record(data: &[u8]) -> Result<SpanRecord, String> {
    let mut pos = 0usize;
    let mut s = SpanRecord {
        span_id: decode_varint(data, &mut pos)?,
        ..SpanRecord::default()
    };
    if s.span_id == 0 {
        return Err("span record: span_id must be 1-based (got 0)".to_string());
    }
    s.parent_span_id = decode_varint(data, &mut pos)?;
    if pos + 2 > data.len() {
        return Err("span record: truncated before flags/status".to_string());
    }
    let flags = data[pos];
    pos += 1;
    if flags & !SPAN_FLAGS_KNOWN != 0 {
        return Err(format!("span record: unknown flags bits set: 0x{flags:02X}"));
    }
    s.is_open = flags & SPAN_FLAG_OPEN != 0;
    s.is_external = flags & SPAN_FLAG_EXTERNAL != 0;
    let status = data[pos];
    pos += 1;
    if status > SPAN_STATUS_ERROR {
        return Err(format!("span record: invalid status value {status}"));
    }
    s.status = status;
    s.start_wall_ns = decode_varint(data, &mut pos)?;
    s.end_wall_ns = decode_varint(data, &mut pos)?;
    s.process_ord = decode_varint(data, &mut pos)?;
    s.thread_id = decode_varint(data, &mut pos)?;
    s.start_step = decode_varint(data, &mut pos)?;
    s.end_step = decode_varint(data, &mut pos)?;
    if s.is_open && (s.end_wall_ns != 0 || s.end_step != 0) {
        return Err(format!("span record: open span {} must have end_wall_ns and end_step == 0", s.span_id));
    }
    if s.is_external {
        s.external_recording = read_str(data, &mut pos, "external_recording")?;
        s.external_path = read_str(data, &mut pos, "external_path")?;
    }
    s.span_type = read_str(data, &mut pos, "span_type")?;
    s.label = read_str(data, &mut pos, "label")?;
    if pos + 1 > data.len() {
        return Err("span record: truncated before structural byte".to_string());
    }
    let structural = data[pos];
    pos += 1;
    if structural & !SPAN_STRUCTURAL_KNOWN != 0 {
        return Err(format!("span record: unknown structural bits set: 0x{structural:02X}"));
    }
    s.contiguous_on_one_thread = structural & SPAN_STRUCTURAL_CONTIGUOUS != 0;
    s.shares_timeline = structural & SPAN_STRUCTURAL_SHARES_TIMELINE != 0;
    s.concurrent_with_siblings = structural & SPAN_STRUCTURAL_CONCURRENT != 0;
    let count = decode_varint(data, &mut pos)?;
    for _ in 0..count {
        let k = read_str(data, &mut pos, "metadata key")?;
        let v = read_str(data, &mut pos, "metadata value")?;
        s.metadata.push((k, v));
    }
    if pos != data.len() {
        return Err(format!("span record: {} trailing bytes after record", data.len() - pos));
    }
    Ok(s)
}

/// One sealed chunk: its bytes for `spans.dat` and its 16-byte `spans.idx`
/// entry. They are written in that order, so an index entry always names a
/// complete chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedSpanChunk {
    pub data: Vec<u8>,
    pub index_entry: [u8; SPANS_INDEX_ENTRY_SIZE],
}

/// Builds the span stream: buffers records into chunks and keeps the
/// span-type index.
#[derive(Debug)]
pub struct SpanStreamBuilder {
    chunk_size: usize,
    buffer: Vec<u8>,
    in_chunk: usize,
    total_records: u64,
    sealed_records: u64,
    data_offset: u64,
    type_ids: HashMap<String, u32>,
    type_order: Vec<String>,
    span_ids_by_type: Vec<BTreeSet<u64>>,
}

impl Default for SpanStreamBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_SPANS_CHUNK_SIZE)
    }
}

impl SpanStreamBuilder {
    /// A builder sealing a chunk every `chunk_size` records (at least 1).
    pub fn new(chunk_size: usize) -> Self {
        SpanStreamBuilder {
            chunk_size: chunk_size.max(1),
            buffer: Vec::new(),
            in_chunk: 0,
            total_records: 0,
            sealed_records: 0,
            data_offset: 0,
            type_ids: HashMap::new(),
            type_order: Vec::new(),
            span_ids_by_type: Vec::new(),
        }
    }

    /// The `spans.idx` header.
    pub fn index_header(&self) -> [u8; SPANS_INDEX_HEADER_SIZE] {
        let mut h = [0u8; SPANS_INDEX_HEADER_SIZE];
        h[..4].copy_from_slice(&(self.chunk_size as u32).to_le_bytes());
        h[4..6].copy_from_slice(&SPANS_INDEX_VERSION.to_le_bytes());
        h
    }

    /// Append one record; returns the chunk it sealed, if it filled one.
    pub fn push(&mut self, span: &SpanRecord) -> Result<Option<SealedSpanChunk>, String> {
        let rec = encode_span_record(span)?;
        encode_varint(rec.len() as u64, &mut self.buffer);
        self.buffer.extend_from_slice(&rec);
        self.in_chunk += 1;
        self.total_records += 1;
        let type_id = match self.type_ids.get(&span.span_type) {
            Some(&id) => id,
            None => {
                let id = self.type_order.len() as u32;
                self.type_ids.insert(span.span_type.clone(), id);
                self.type_order.push(span.span_type.clone());
                self.span_ids_by_type.push(BTreeSet::new());
                id
            }
        };
        self.span_ids_by_type[type_id as usize].insert(span.span_id);
        if self.in_chunk >= self.chunk_size { self.seal() } else { Ok(None) }
    }

    /// Seal whatever is buffered as a chunk, possibly a short one; `None` when
    /// nothing is buffered.
    pub fn seal(&mut self) -> Result<Option<SealedSpanChunk>, String> {
        if self.in_chunk == 0 {
            return Ok(None);
        }
        let frame = codetracer_ctfs::compress_pledged(&self.buffer, SPANS_COMPRESSION_LEVEL, SPANS_DATA_FILE_NAME)?;
        let sealed_through = self.sealed_records + self.in_chunk as u64;
        let mut index_entry = [0u8; SPANS_INDEX_ENTRY_SIZE];
        index_entry[..8].copy_from_slice(&self.data_offset.to_le_bytes());
        index_entry[8..].copy_from_slice(&sealed_through.to_le_bytes());
        self.sealed_records = sealed_through;
        self.data_offset += frame.len() as u64;
        self.buffer.clear();
        self.in_chunk = 0;
        Ok(Some(SealedSpanChunk { data: frame, index_entry }))
    }

    /// Records appended, sealed or not.
    pub fn count(&self) -> u64 {
        self.total_records
    }

    /// The `spantype.ns` image of the types and span ids seen so far.
    pub fn span_type_namespace(&self) -> Vec<u8> {
        let lists: Vec<Vec<u64>> = self.span_ids_by_type.iter().map(|s| s.iter().copied().collect()).collect();
        encode_span_type_namespace(&self.type_order, &lists)
    }
}

/// The `spantype.ns` image for `type_order[i]` interned as id `i`, with
/// `id_lists[i]` its ascending span ids.
pub fn encode_span_type_namespace(type_order: &[String], id_lists: &[Vec<u64>]) -> Vec<u8> {
    assert_eq!(type_order.len(), id_lists.len(), "one span-id list per span type");
    let n = type_order.len();
    let mut cursor = SPAN_TYPE_NS_HEADER_SIZE + n * SPAN_TYPE_NS_ENTRY_SIZE;
    let mut name_offsets = Vec::with_capacity(n);
    for name in type_order {
        name_offsets.push(cursor);
        cursor += name.len();
    }
    let mut list_offsets = Vec::with_capacity(n);
    for list in id_lists {
        list_offsets.push(cursor);
        cursor += list.len() * 8;
    }
    let mut buf = vec![0u8; cursor];
    buf[0..4].copy_from_slice(&SPAN_TYPE_NS_MAGIC.to_le_bytes());
    buf[4..6].copy_from_slice(&SPAN_TYPE_NS_VERSION.to_le_bytes());
    buf[6..10].copy_from_slice(&(n as u32).to_le_bytes());
    buf[10..18].copy_from_slice(&(SPAN_TYPE_NS_HEADER_SIZE as u64).to_le_bytes());
    for id in 0..n {
        let base = SPAN_TYPE_NS_HEADER_SIZE + id * SPAN_TYPE_NS_ENTRY_SIZE;
        let name = type_order[id].as_bytes();
        buf[base..base + 4].copy_from_slice(&(id as u32).to_le_bytes());
        buf[base + 4..base + 8].copy_from_slice(&(name.len() as u32).to_le_bytes());
        buf[base + 8..base + 16].copy_from_slice(&(name_offsets[id] as u64).to_le_bytes());
        buf[base + 16..base + 20].copy_from_slice(&(id_lists[id].len() as u32).to_le_bytes());
        buf[base + 20..base + 28].copy_from_slice(&(list_offsets[id] as u64).to_le_bytes());
        buf[name_offsets[id]..name_offsets[id] + name.len()].copy_from_slice(name);
        for (j, sid) in id_lists[id].iter().enumerate() {
            let at = list_offsets[id] + j * 8;
            buf[at..at + 8].copy_from_slice(&sid.to_le_bytes());
        }
    }
    buf
}

/// One `spantype.ns` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanTypeEntry {
    pub type_id: u32,
    pub name: String,
    pub span_ids: Vec<u64>,
}

fn u16_at(d: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([d[at], d[at + 1]])
}

fn u32_at(d: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(d[at..at + 4].try_into().expect("4 bytes"))
}

fn u64_at(d: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(d[at..at + 8].try_into().expect("8 bytes"))
}

/// Parse a `spantype.ns` image. Refuses a short header, a wrong magic or
/// version, a type name that is not UTF-8, and any table, name or list that
/// lies outside the image.
pub fn parse_span_type_namespace(data: &[u8]) -> Result<Vec<SpanTypeEntry>, String> {
    if data.len() < SPAN_TYPE_NS_HEADER_SIZE {
        return Err(format!("spantype.ns too short: {} bytes", data.len()));
    }
    if u32_at(data, 0) != SPAN_TYPE_NS_MAGIC {
        return Err("spantype.ns: bad magic".to_string());
    }
    let version = u16_at(data, 4);
    if version != SPAN_TYPE_NS_VERSION {
        return Err(format!("spantype.ns: unsupported version {version}"));
    }
    let type_count = u32_at(data, 6) as usize;
    let table = u64_at(data, 10);
    let in_bounds = |off: u64, len: u64| off.checked_add(len).is_some_and(|end| end <= data.len() as u64);
    if table < SPAN_TYPE_NS_HEADER_SIZE as u64 || !in_bounds(table, type_count as u64 * SPAN_TYPE_NS_ENTRY_SIZE as u64) {
        return Err("spantype.ns: type table out of bounds".to_string());
    }
    let mut entries = Vec::with_capacity(type_count);
    for i in 0..type_count {
        let base = table as usize + i * SPAN_TYPE_NS_ENTRY_SIZE;
        let type_id = u32_at(data, base);
        let name_len = u32_at(data, base + 4) as u64;
        let name_off = u64_at(data, base + 8);
        let span_count = u32_at(data, base + 16) as u64;
        let spans_off = u64_at(data, base + 20);
        if !in_bounds(name_off, name_len) {
            return Err(format!("spantype.ns: name out of bounds for type {type_id}"));
        }
        if !in_bounds(spans_off, span_count * 8) {
            return Err(format!("spantype.ns: span id list out of bounds for type {type_id}"));
        }
        let name = std::str::from_utf8(&data[name_off as usize..(name_off + name_len) as usize])
            .map_err(|_| format!("spantype.ns: name of type {type_id} is not UTF-8"))?
            .to_string();
        let span_ids = (0..span_count as usize).map(|j| u64_at(data, spans_off as usize + j * 8)).collect();
        entries.push(SpanTypeEntry { type_id, name, span_ids });
    }
    Ok(entries)
}

/// The span ids recorded under `span_type`; empty when there are none.
pub fn span_ids_of_type(entries: &[SpanTypeEntry], span_type: &str) -> Vec<u64> {
    entries
        .iter()
        .find(|e| e.name == span_type)
        .map(|e| e.span_ids.clone())
        .unwrap_or_default()
}

/// Last-record-wins per `span_id`: the settled spans, ascending by id.
pub fn resolve_spans(records: &[SpanRecord]) -> Vec<SpanRecord> {
    let mut by_id: BTreeMap<u64, &SpanRecord> = BTreeMap::new();
    for r in records {
        by_id.insert(r.span_id, r);
    }
    by_id.into_values().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> SpanRecord {
        SpanRecord {
            span_id: 7,
            parent_span_id: 0,
            status: SPAN_STATUS_OK,
            start_wall_ns: 1_000,
            end_wall_ns: 2_000,
            thread_id: 3,
            start_step: 10,
            end_step: 20,
            span_type: "web-request".into(),
            label: "GET /".into(),
            contiguous_on_one_thread: true,
            metadata: vec![("b".into(), "1".into()), ("a".into(), "2".into())],
            ..SpanRecord::default()
        }
    }

    #[test]
    fn a_record_round_trips_and_keeps_metadata_order() {
        let s = sample();
        assert_eq!(decode_span_record(&encode_span_record(&s).unwrap()).unwrap(), s);
    }

    #[test]
    fn a_trailing_byte_is_refused() {
        let mut bytes = encode_span_record(&sample()).unwrap();
        bytes.push(0);
        assert!(decode_span_record(&bytes).unwrap_err().contains("trailing"));
    }

    #[test]
    fn an_open_record_with_an_end_is_refused_on_both_sides() {
        let s = SpanRecord { is_open: true, ..sample() };
        assert!(encode_span_record(&s).unwrap_err().contains("open span"));
    }

    #[test]
    fn a_short_chunk_publishes_its_true_cumulative_count() {
        let mut b = SpanStreamBuilder::new(4);
        assert!(b.push(&sample()).unwrap().is_none());
        let c = b.seal().unwrap().unwrap();
        assert_eq!(u64::from_le_bytes(c.index_entry[8..].try_into().unwrap()), 1);
        for _ in 0..3 {
            assert!(b.push(&sample()).unwrap().is_none());
        }
        let c = b.push(&sample()).unwrap().expect("the fourth record seals");
        assert_eq!(u64::from_le_bytes(c.index_entry[8..].try_into().unwrap()), 5);
        assert_eq!(b.seal().unwrap(), None);
    }

    #[test]
    fn the_type_index_lists_each_span_once() {
        let mut b = SpanStreamBuilder::new(64);
        let open = SpanRecord {
            is_open: true,
            end_wall_ns: 0,
            end_step: 0,
            ..sample()
        };
        b.push(&open).unwrap();
        b.push(&sample()).unwrap();
        let e = parse_span_type_namespace(&b.span_type_namespace()).unwrap();
        assert_eq!(
            e,
            vec![SpanTypeEntry {
                type_id: 0,
                name: "web-request".into(),
                span_ids: vec![7]
            }]
        );
    }
}
