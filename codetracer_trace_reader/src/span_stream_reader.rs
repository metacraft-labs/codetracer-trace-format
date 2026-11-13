//! Reader of the span stream (`spans.dat`, `spans.idx`) and the span-type
//! index (`spantype.ns`). The layouts are described in
//! `codetracer_trace_writer::span_stream`.
//!
//! Opening decodes no chunk: the record count, each chunk's occupancy and
//! the chunk that holds any record come from the index's cumulative column,
//! which is checked at open — offsets in bounds and non-decreasing, counts
//! non-decreasing — so a damaged index cannot mis-address a record. A chunk
//! is decoded only when one of its records is asked for, and the last one is
//! kept.
//!
//! The reader works on a container still being written: an index entry is
//! appended only after its chunk, and the last indexed chunk's end is found
//! from its zstd frame, since `spans.dat` may already hold the start of the
//! next chunk. A consumer following the stream remembers
//! [`SpanStreamReader::chunk_count`] and asks for
//! [`SpanStreamReader::read_spans_since`] on a reader of the grown container.

use codetracer_ctfs::CtfsReader;
use codetracer_trace_writer::column_aware::decode_varint;
use codetracer_trace_writer::span_stream::{
    SPAN_TYPE_NAMESPACE_FILE_NAME, SPANS_DATA_FILE_NAME, SPANS_INDEX_ENTRY_SIZE, SPANS_INDEX_FILE_NAME, SPANS_INDEX_HEADER_SIZE, SPANS_INDEX_VERSION,
    SpanRecord, SpanTypeEntry, decode_span_record, parse_span_type_namespace, resolve_spans,
};

use crate::chunk_codec::ChunkForm;

fn u64_at(d: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(d[at..at + 8].try_into().expect("8 bytes"))
}

/// A chunk's content split into its length-prefixed records.
fn split_records(raw: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < raw.len() {
        let len = decode_varint(raw, &mut pos)?;
        let end = usize::try_from(len)
            .ok()
            .and_then(|l| pos.checked_add(l))
            .filter(|&e| e <= raw.len())
            .ok_or_else(|| "span record length extends past chunk".to_string())?;
        records.push(raw[pos..end].to_vec());
        pos = end;
    }
    Ok(records)
}

/// The span stream of one container.
#[derive(Debug)]
pub struct SpanStreamReader {
    /// `spans.dat` as stored: frames, or contents in a compact container.
    data: Vec<u8>,
    form: ChunkForm,
    chunk_size: u32,
    offsets: Vec<u64>,
    /// `cumulative[i]`: records in chunks `0..=i`.
    cumulative: Vec<u64>,
    cached: Option<(usize, Vec<Vec<u8>>)>,
    /// Chunks decoded so far; see [`SpanStreamReader::decodes`].
    decodes: std::cell::Cell<u64>,
}

impl SpanStreamReader {
    /// The span stream of the container `reader` holds; `None` when it has no
    /// `spans.dat`.
    pub fn open(reader: &mut CtfsReader) -> Result<Option<SpanStreamReader>, String> {
        if !reader.list_files().iter().any(|f| f == SPANS_DATA_FILE_NAME) {
            return Ok(None);
        }
        let data = reader
            .read_file(SPANS_DATA_FILE_NAME)
            .map_err(|e| format!("failed to read {SPANS_DATA_FILE_NAME}: {e:?}"))?;
        let idx = reader
            .read_file(SPANS_INDEX_FILE_NAME)
            .map_err(|e| format!("failed to read {SPANS_INDEX_FILE_NAME}: {e:?}"))?;
        Self::from_members(data, &idx, ChunkForm::of(reader)).map(Some)
    }

    /// A reader of `spans.dat` (`data`) and `spans.idx` (`idx`) whose chunks
    /// are stored in `form`.
    /// Parse `spans.idx` against a `spans.dat` of `data_len` bytes:
    /// `(chunk_size, offsets, cumulative)`.
    fn parse_index(idx: &[u8], data_len: usize) -> Result<(u32, Vec<u64>, Vec<u64>), String> {
        if idx.len() < SPANS_INDEX_HEADER_SIZE {
            return Err(format!("{SPANS_INDEX_FILE_NAME} too small for its header"));
        }
        let chunk_size = u32::from_le_bytes(idx[0..4].try_into().expect("4 bytes"));
        if chunk_size == 0 {
            return Err(format!("chunkSize in {SPANS_INDEX_FILE_NAME} is 0"));
        }
        let version = u16::from_le_bytes([idx[4], idx[5]]);
        if version != SPANS_INDEX_VERSION {
            return Err(format!(
                "{SPANS_INDEX_FILE_NAME}: unsupported index version {version} (this build reads version {SPANS_INDEX_VERSION})"
            ));
        }
        if idx[6] != 0 || idx[7] != 0 {
            return Err(format!("{SPANS_INDEX_FILE_NAME}: reserved header field is not 0"));
        }
        let entries = &idx[SPANS_INDEX_HEADER_SIZE..];
        if !entries.len().is_multiple_of(SPANS_INDEX_ENTRY_SIZE) {
            return Err(format!("{SPANS_INDEX_FILE_NAME} has trailing bytes in the entry region"));
        }
        let n = entries.len() / SPANS_INDEX_ENTRY_SIZE;
        let mut offsets = Vec::with_capacity(n);
        let mut cumulative = Vec::with_capacity(n);
        for i in 0..n {
            let (o, c) = (
                u64_at(entries, i * SPANS_INDEX_ENTRY_SIZE),
                u64_at(entries, i * SPANS_INDEX_ENTRY_SIZE + 8),
            );
            if o > data_len as u64 {
                return Err(format!(
                    "{SPANS_INDEX_FILE_NAME}: chunk {i} offset is past the end of {SPANS_DATA_FILE_NAME}"
                ));
            }
            if i > 0 {
                if o < offsets[i - 1] {
                    return Err(format!("{SPANS_INDEX_FILE_NAME}: chunk offsets are not monotonic at entry {i}"));
                }
                if c < cumulative[i - 1] {
                    return Err(format!(
                        "{SPANS_INDEX_FILE_NAME}: cumulative record counts are not monotonic at entry {i}"
                    ));
                }
            }
            offsets.push(o);
            cumulative.push(c);
        }
        Ok((chunk_size, offsets, cumulative))
    }

    pub fn from_members(data: Vec<u8>, idx: &[u8], form: ChunkForm) -> Result<SpanStreamReader, String> {
        let (chunk_size, offsets, cumulative) = Self::parse_index(idx, data.len())?;
        Ok(SpanStreamReader {
            data,
            form,
            chunk_size,
            offsets,
            cumulative,
            cached: None,
            decodes: std::cell::Cell::new(0),
        })
    }

    /// Follow a container that is being written (`ctfs-container.md` §6):
    /// extend this reader by the chunks `reader`'s container has published
    /// since it was opened or last refreshed. Nothing is decoded: the record
    /// count and each chunk's records come from the index's cumulative column,
    /// and a chunk may be short anywhere. The chunk held stays held. A re-read
    /// index that does not extend the one already read -- a changed
    /// `chunk_size`, a published entry that changed or disappeared -- is
    /// refused, naming `spans.idx`.
    pub fn refresh(&mut self, reader: &mut CtfsReader) -> Result<(), String> {
        let mut retried = false;
        let (data, idx) = loop {
            let idx = reader
                .read_file(SPANS_INDEX_FILE_NAME)
                .map_err(|e| format!("failed to read {SPANS_INDEX_FILE_NAME}: {e:?}"))?;
            let data = reader
                .read_file(SPANS_DATA_FILE_NAME)
                .map_err(|e| format!("failed to read {SPANS_DATA_FILE_NAME}: {e:?}"))?;
            // An index entry read before its chunk's root entry was published
            // is read again once the root directory is.
            let n = idx.len().saturating_sub(SPANS_INDEX_HEADER_SIZE) / SPANS_INDEX_ENTRY_SIZE;
            let last = (n > 0).then(|| u64_at(&idx[SPANS_INDEX_HEADER_SIZE..], (n - 1) * SPANS_INDEX_ENTRY_SIZE));
            if retried || last.is_none_or(|o| o as usize <= data.len()) {
                break (data, idx);
            }
            reader.refresh().map_err(|e| e.to_string())?;
            retried = true;
        };
        let (chunk_size, offsets, cumulative) = Self::parse_index(&idx, data.len())?;
        if chunk_size != self.chunk_size {
            return Err(format!(
                "{SPANS_INDEX_FILE_NAME}: chunk_size changed from {} to {chunk_size} while the container was followed",
                self.chunk_size
            ));
        }
        if offsets.len() < self.offsets.len() {
            return Err(format!(
                "{SPANS_INDEX_FILE_NAME}: {} published chunk(s) disappeared while the container was followed",
                self.offsets.len() - offsets.len()
            ));
        }
        if let Some(k) = (0..self.offsets.len()).find(|&k| offsets[k] != self.offsets[k] || cumulative[k] != self.cumulative[k]) {
            return Err(format!(
                "{SPANS_INDEX_FILE_NAME}: published chunk {k} changed while the container was followed"
            ));
        }
        self.data = data;
        self.offsets = offsets;
        self.cumulative = cumulative;
        Ok(())
    }

    /// How many chunks this reader has decoded.
    pub fn decodes(&self) -> u64 {
        self.decodes.get()
    }

    /// Records in sealed chunks; an open record and its settled one count as
    /// two.
    pub fn count(&self) -> u64 {
        self.cumulative.last().copied().unwrap_or(0)
    }

    /// Sealed chunks: the cursor a consumer following the stream remembers.
    pub fn chunk_count(&self) -> usize {
        self.offsets.len()
    }

    /// The index header's records-per-chunk: the writer's seal-at threshold,
    /// an upper bound and never a way to locate a record.
    pub fn chunk_size_records(&self) -> u32 {
        self.chunk_size
    }

    /// Append-order index of chunk `chunk`'s first record.
    pub fn first_record_of_chunk(&self, chunk: usize) -> u64 {
        if chunk == 0 {
            0
        } else if chunk > self.cumulative.len() {
            self.count()
        } else {
            self.cumulative[chunk - 1]
        }
    }

    /// Records chunk `chunk` holds; 0 for a chunk that does not exist.
    pub fn records_in_chunk(&self, chunk: usize) -> u64 {
        if chunk >= self.cumulative.len() {
            return 0;
        }
        self.cumulative[chunk] - self.first_record_of_chunk(chunk)
    }

    fn chunk_range(&self, chunk: usize) -> Result<(usize, usize), String> {
        if chunk >= self.offsets.len() {
            return Err(format!("span chunk {chunk} out of range (have {} chunks)", self.offsets.len()));
        }
        let start = self.offsets[chunk] as usize;
        if start > self.data.len() {
            return Err(format!("span chunk offset past end of {SPANS_DATA_FILE_NAME}"));
        }
        if chunk + 1 < self.offsets.len() {
            let end = self.offsets[chunk + 1] as usize;
            if end < start || end > self.data.len() {
                return Err("span chunk offsets out of range".to_string());
            }
            return Ok((start, end));
        }
        if start == self.data.len() || self.form == ChunkForm::Stored {
            return Ok((start, self.data.len()));
        }
        let len = codetracer_ctfs::frame_compressed_size(&self.data[start..]).map_err(|e| format!("cannot determine span chunk frame size: {e}"))?;
        Ok((start, start + len))
    }

    fn decode_chunk(&self, chunk: usize) -> Result<Vec<Vec<u8>>, String> {
        let (start, end) = self.chunk_range(chunk)?;
        self.decodes.set(self.decodes.get() + 1);
        if start == end {
            return Ok(Vec::new());
        }
        let bytes = &self.data[start..end];
        match self.form {
            ChunkForm::Stored => split_records(bytes),
            ChunkForm::Framed => {
                if codetracer_ctfs::declared_content_size(bytes).is_none() {
                    return Err("cannot determine decompressed size for span chunk".to_string());
                }
                if codetracer_ctfs::frame_compressed_size(bytes) != Ok(bytes.len()) {
                    return Err("a span chunk is not exactly one zstd frame".to_string());
                }
                let raw = crate::chunk_codec::inflate(SPANS_DATA_FILE_NAME, bytes)?;
                split_records(&raw)
            }
        }
    }

    /// The record at `index` in append order, not resolved by
    /// last-record-wins. Decodes at most the one chunk that holds it.
    pub fn read_span(&mut self, index: u64) -> Result<SpanRecord, String> {
        let total = self.count();
        if index >= total {
            return Err(format!("span index {index} out of range (count {total})"));
        }
        let chunk = self.cumulative.partition_point(|&c| c <= index);
        if chunk >= self.cumulative.len() {
            return Err(format!("span index {index} has no owning chunk"));
        }
        let within = (index - self.first_record_of_chunk(chunk)) as usize;
        if self.cached.as_ref().is_none_or(|(c, _)| *c != chunk) {
            let records = self.decode_chunk(chunk)?;
            self.cached = Some((chunk, records));
        }
        let records = &self.cached.as_ref().expect("cached above").1;
        let rec = records
            .get(within)
            .ok_or_else(|| format!("span record {within} missing in chunk {chunk}"))?;
        decode_span_record(rec)
    }

    /// Every record in chunks `from..to`, in append order.
    pub fn read_spans_in_chunks(&self, from: usize, to: usize) -> Result<Vec<SpanRecord>, String> {
        if to > self.offsets.len() || from > to {
            return Err(format!(
                "span chunk range [{from}, {to}) out of range (have {} chunks)",
                self.offsets.len()
            ));
        }
        let mut spans = Vec::new();
        for c in from..to {
            for rec in self.decode_chunk(c)? {
                spans.push(decode_span_record(&rec)?);
            }
        }
        Ok(spans)
    }

    /// The records of the chunks sealed since a reader saw `known_chunks`
    /// chunks. Refused when `known_chunks` exceeds the chunk count: the index
    /// does not shrink.
    pub fn read_spans_since(&self, known_chunks: usize) -> Result<Vec<SpanRecord>, String> {
        if known_chunks > self.offsets.len() {
            return Err(format!(
                "knownChunkCount {known_chunks} exceeds current chunk count {} (the index cannot shrink)",
                self.offsets.len()
            ));
        }
        self.read_spans_in_chunks(known_chunks, self.offsets.len())
    }

    /// Every record, in append order.
    pub fn read_all_span_records(&self) -> Result<Vec<SpanRecord>, String> {
        self.read_spans_in_chunks(0, self.offsets.len())
    }

    /// Every span, last record wins per id, ascending by id.
    pub fn settled_spans(&self) -> Result<Vec<SpanRecord>, String> {
        Ok(resolve_spans(&self.read_all_span_records()?))
    }

    /// Up to `limit` settled spans with id at least `from_span_id`; `limit`
    /// 0 means no limit.
    pub fn page_spans(&self, from_span_id: u64, limit: usize) -> Result<Vec<SpanRecord>, String> {
        let page = self.settled_spans()?.into_iter().filter(|s| s.span_id >= from_span_id);
        Ok(if limit == 0 { page.collect() } else { page.take(limit).collect() })
    }
}

/// The span-type index of the container `reader` holds; `None` when it has
/// no `spantype.ns`.
pub fn read_span_type_namespace(reader: &mut CtfsReader) -> Result<Option<Vec<SpanTypeEntry>>, String> {
    if !reader.list_files().iter().any(|f| f == SPAN_TYPE_NAMESPACE_FILE_NAME) {
        return Ok(None);
    }
    let image = reader
        .read_file(SPAN_TYPE_NAMESPACE_FILE_NAME)
        .map_err(|e| format!("failed to read {SPAN_TYPE_NAMESPACE_FILE_NAME}: {e:?}"))?;
    parse_span_type_namespace(&image).map(Some)
}
