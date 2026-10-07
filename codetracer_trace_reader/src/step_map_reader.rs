//! Reader for `step-map.ns` version 2 (`codetracer-trace-format-spec/
//! internal-files.md` §"`step-map.ns`"): the `(path_id, line)` → step-id
//! index of a line-only trace.
//!
//! Two ways in, as the specification describes them:
//!
//! * [`StepMapReader::load_all`] inflates every chunk in order and returns
//!   every list — what a reader that builds `(path, line) -> ids` at open does
//!   — as one [`StepMapIndex`]: the keys in order, and every list's ids in one
//!   buffer, sized once from the header.
//! * [`StepMapReader::lookup`] binary-searches the chunk table for the last
//!   chunk whose first key is not above the target, inflates that chunk alone
//!   and scans it. The scan checks every record of the chunk and notes where
//!   each line's record starts; the chunk and that note are kept, so a later
//!   lookup into the same chunk binary-searches the note and decodes one
//!   record.
//!
//! Every refusal the specification lists is made, by name: decoded counts that
//! disagree with the header, a chunk whose first record's key is not its table
//! key, keys that do not ascend strictly, a `count`, `gap` or `repeat` of `0`,
//! runs whose repeats overshoot `count`, and a frame that does not decode to
//! its declared size. Each is a map that would answer some breakpoint with the
//! wrong steps. Version 1 and every other version are refused.

use crate::ChunkForm;
use codetracer_ctfs::{CtfsReader, MemberBytes};
use codetracer_trace_writer::step_map::{STEP_MAP_CHUNK_ENTRY_SIZE, STEP_MAP_FILE_NAME, STEP_MAP_HEADER_SIZE, STEP_MAP_MAGIC, STEP_MAP_VERSION};

/// One chunk-table entry, resolved to a byte range of the member.
#[derive(Debug, Clone, Copy)]
struct Chunk {
    start: usize,
    end: usize,
    first: (u64, u32),
}

/// A parsed `step-map.ns`: its header and chunk table, with the frames
/// inflated on demand. The chunk the last [`lookup`](Self::lookup) inflated is
/// kept with an index of its lines, so lookups that land in one chunk inflate
/// and check it once.
#[derive(Debug)]
pub struct StepMapReader {
    bytes: MemberBytes,
    path_count: u32,
    line_count: u32,
    step_count: u64,
    chunks: Vec<Chunk>,
    /// The chunk `raw` holds, if any.
    cached_chunk: Option<usize>,
    raw: Vec<u8>,
    /// Each line of the cached chunk, in key order, with the offset in `raw`
    /// of its `count` field.
    lines: Vec<((u64, u32), usize)>,
    /// Whether a chunk is a frame to inflate or its content.
    form: ChunkForm,
}

/// Every line of a `step-map.ns` with its step ids, as
/// [`StepMapReader::load_all`] returns them: the keys in ascending order, and
/// the lists one after the other in a single buffer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepMapIndex {
    keys: Vec<(u64, u32)>,
    /// Line `i`'s ids are `ids[ends[i - 1]..ends[i]]` (from 0 for the first).
    ends: Vec<usize>,
    ids: Vec<u64>,
}

impl StepMapIndex {
    /// Number of `(path_id, line)` keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the map has no line.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Step ids in all lists together.
    pub fn step_count(&self) -> usize {
        self.ids.len()
    }

    /// The step ids of `key`, `None` when no step ran there. The key is
    /// matched as it is stored: a step registered at line 0 is under line 1
    /// (what [`StepMapReader::lookup`] looks line 0 up as).
    pub fn get(&self, key: &(u64, u32)) -> Option<&[u64]> {
        self.keys.binary_search(key).ok().map(|i| self.list(i))
    }

    /// The keys, ascending.
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &(u64, u32)> {
        self.keys.iter()
    }

    /// Every list, in key order.
    pub fn values(&self) -> impl ExactSizeIterator<Item = &[u64]> {
        (0..self.keys.len()).map(|i| self.list(i))
    }

    /// Every key with its list, ascending.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = ((u64, u32), &[u64])> {
        (0..self.keys.len()).map(|i| (self.keys[i], self.list(i)))
    }

    fn list(&self, i: usize) -> &[u64] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] };
        &self.ids[start..self.ends[i]]
    }
}

fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}

fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// A varint of chunk `chunk`; a one-byte varint, the common case, takes the
/// first branch.
#[inline]
fn varint(b: &[u8], pos: &mut usize, chunk: usize) -> Result<u64, String> {
    match b.get(*pos) {
        Some(&byte) if byte < 0x80 => {
            *pos += 1;
            Ok(byte as u64)
        }
        _ => varint_long(b, pos, chunk),
    }
}

/// Append the `repeat` ids `first`, `first + gap`, … of a run whose last id
/// has been checked to fit.
#[inline]
fn push_run(ids: &mut Vec<u64>, first: u64, gap: u64, repeat: u64) {
    let mut next = first;
    ids.extend((0..repeat).map(|_| {
        let id = next;
        // Past the run's last id after it, and unused then.
        next = next.wrapping_add(gap);
        id
    }));
}

/// `a * b`, or `None` past 64 bits. Two factors below 2^32 cannot overflow
/// and are multiplied as they are: a checked 64-bit product is a 128-bit
/// multiply on wasm32, a library call.
#[inline]
fn checked_product(a: u64, b: u64) -> Option<u64> {
    if (a | b) >> 32 == 0 { Some(a * b) } else { a.checked_mul(b) }
}

fn varint_long(b: &[u8], pos: &mut usize, chunk: usize) -> Result<u64, String> {
    // Two bytes, a count or gap below 16,384: no loop.
    if let Some(&[b0, b1, ..]) = b.get(*pos..)
        && b1 < 0x80
    {
        *pos += 2;
        return Ok((b0 & 0x7f) as u64 | (b1 as u64) << 7);
    }
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        let byte = *b
            .get(*pos)
            .ok_or_else(|| format!("{STEP_MAP_FILE_NAME}: chunk {chunk} ends inside a varint"))?;
        *pos += 1;
        if shift == 63 && byte > 1 || shift > 63 {
            return Err(format!("{STEP_MAP_FILE_NAME}: chunk {chunk} holds a varint wider than 64 bits"));
        }
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
    }
}

/// Put chunk `c` of a member in `raw`: its frame inflated, checking it decodes
/// to the size the frame declares, or its content as it is stored.
fn inflate(bytes: &MemberBytes, chunks: &[Chunk], c: usize, form: ChunkForm, raw: &mut Vec<u8>) -> Result<(), String> {
    let name = STEP_MAP_FILE_NAME;
    let chunk = chunks[c];
    let frame = bytes.get(chunk.start, chunk.end).map_err(|e| format!("{name}: chunk {c}'s frame: {e}"))?;
    let frame = frame.as_ref();
    if form == ChunkForm::Stored {
        raw.clear();
        raw.extend_from_slice(frame);
        return Ok(());
    }
    let declared = codetracer_ctfs::zstd_frame::declared_content_size(frame)
        .ok_or_else(|| format!("{name}: chunk {c}'s frame does not declare its content size"))?;
    codetracer_ctfs::zstd_compat::decode_into(frame, raw).map_err(|e| format!("{name}: chunk {c}'s frame does not decode: {e}"))?;
    if raw.len() as u64 != declared {
        return Err(format!(
            "{name}: chunk {c}'s frames decode to {} bytes, not the {declared} its frame header declares",
            raw.len()
        ));
    }
    Ok(())
}

impl StepMapReader {
    /// Open the container's `step-map.ns`. `Ok(None)` when the container has
    /// none — a column-aware trace, or one whose writer stopped before close;
    /// a reader then scans the execution stream.
    pub fn open(reader: &mut CtfsReader) -> Result<Option<StepMapReader>, String> {
        if reader.file_size(STEP_MAP_FILE_NAME).is_none() {
            return Ok(None);
        }
        let bytes = reader
            .read_member(STEP_MAP_FILE_NAME)
            .map_err(|e| format!("{STEP_MAP_FILE_NAME} is present but unreadable: {e}"))?;
        StepMapReader::from_member_as(bytes, ChunkForm::of(reader)).map(Some)
    }

    /// Parse a member's header and chunk table. Frames are not inflated here.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<StepMapReader, String> {
        StepMapReader::from_member(MemberBytes::from(bytes))
    }

    /// [`Self::from_bytes`] over a member the reader shares with its
    /// container rather than owns.
    pub fn from_member(member: MemberBytes) -> Result<StepMapReader, String> {
        StepMapReader::from_member_as(member, ChunkForm::Framed)
    }

    /// [`Self::from_member`] over chunks stored in `form`: the form of the
    /// container the member came from ([`ChunkForm::of`]).
    pub fn from_member_as(member: MemberBytes, form: ChunkForm) -> Result<StepMapReader, String> {
        let name = STEP_MAP_FILE_NAME;
        let len = member.len();
        if len < STEP_MAP_HEADER_SIZE {
            return Err(format!("{name}: {len} bytes is shorter than the {STEP_MAP_HEADER_SIZE}-byte header"));
        }
        let header = member.get(0, STEP_MAP_HEADER_SIZE).map_err(|e| format!("{name}: header: {e}"))?;
        let bytes: &[u8] = &header;
        if u32_at(bytes, 0) != STEP_MAP_MAGIC {
            return Err(format!("{name}: bad magic 0x{:08x}", u32_at(bytes, 0)));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != STEP_MAP_VERSION {
            return Err(format!(
                "{name}: version {version} is not read; this reader reads version {STEP_MAP_VERSION} only \
                 (version 1 stored uncompressed i64 lists and is retired)"
            ));
        }
        let chunk_count = u32_at(bytes, 6) as usize;
        let (path_count, line_count, step_count) = (u32_at(bytes, 10), u32_at(bytes, 14), u64_at(bytes, 18));
        let table_end = chunk_count
            .checked_mul(STEP_MAP_CHUNK_ENTRY_SIZE)
            .and_then(|t| t.checked_add(STEP_MAP_HEADER_SIZE))
            .filter(|end| *end <= len)
            .ok_or_else(|| format!("{name}: the header claims {chunk_count} chunks, more than the member's table can hold"))?;
        if chunk_count == 0 && (path_count, line_count, step_count) != (0, 0, 0) {
            return Err(format!(
                "{name}: the header counts {path_count} paths, {line_count} lines and {step_count} steps but the map has no chunk"
            ));
        }
        let table = member
            .get(STEP_MAP_HEADER_SIZE, table_end)
            .map_err(|e| format!("{name}: chunk table: {e}"))?;
        let mut chunks = Vec::with_capacity(chunk_count);
        for i in 0..chunk_count {
            let e = i * STEP_MAP_CHUNK_ENTRY_SIZE;
            let start = u64_at(&table, e);
            let first = (u64_at(&table, e + 8), u32_at(&table, e + 16));
            let start = usize::try_from(start)
                .ok()
                .and_then(|s| s.checked_add(table_end))
                .filter(|s| *s <= len)
                .ok_or_else(|| format!("{name}: chunk {i}'s frame offset {start} lies past the end of the member"))?;
            if let Some(prev) = chunks.last_mut() {
                let prev: &mut Chunk = prev;
                if start < prev.start {
                    return Err(format!("{name}: chunk {i}'s frame begins before chunk {}'s", i - 1));
                }
                if first <= prev.first {
                    return Err(format!(
                        "{name}: chunk keys do not ascend strictly: chunk {i} starts at {first:?}, chunk {} at {:?}",
                        i - 1,
                        prev.first
                    ));
                }
                prev.end = start;
            }
            chunks.push(Chunk { start, end: len, first });
        }
        drop((header, table));
        Ok(StepMapReader {
            bytes: member,
            path_count,
            line_count,
            step_count,
            chunks,
            cached_chunk: None,
            raw: Vec::new(),
            lines: Vec::new(),
            form,
        })
    }

    /// Distinct path ids with at least one step, as the header states.
    pub fn path_count(&self) -> u32 {
        self.path_count
    }

    /// Distinct `(path_id, line)` keys, as the header states.
    pub fn line_count(&self) -> u32 {
        self.line_count
    }

    /// Step ids in all lists together, as the header states.
    pub fn step_count(&self) -> u64 {
        self.step_count
    }

    /// Number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Every line's step ids, after checking the decoded counts against the
    /// header. The ids are written into one buffer of the header's step
    /// count, the bound a line's own count is held to before its ids are
    /// read.
    pub fn load_all(&self) -> Result<StepMapIndex, String> {
        let mut index = StepMapIndex {
            keys: Vec::with_capacity(self.line_count as usize),
            ends: Vec::with_capacity(self.line_count as usize),
            ids: Vec::with_capacity(self.step_count as usize),
        };
        let mut last: Option<(u64, u32)> = None;
        let mut paths = 0u32;
        let mut raw = Vec::new();
        for c in 0..self.chunks.len() {
            inflate(&self.bytes, &self.chunks, c, self.form, &mut raw)?;
            self.each_record(c, &raw, Some(&mut index), |key, _| {
                if let Some(prev) = last
                    && key <= prev
                {
                    return Err(format!(
                        "{STEP_MAP_FILE_NAME}: keys do not ascend strictly: {key:?} follows {prev:?} at chunk {c}"
                    ));
                }
                if last.is_none_or(|p| p.0 != key.0) {
                    paths += 1;
                }
                last = Some(key);
                Ok(())
            })?;
        }
        for (what, decoded, header) in [
            ("path", paths as u64, self.path_count as u64),
            ("line", index.len() as u64, self.line_count as u64),
            ("step", index.step_count() as u64, self.step_count),
        ] {
            if decoded != header {
                return Err(format!(
                    "{STEP_MAP_FILE_NAME}: the header counts {header} {what}s but the chunks hold {decoded}"
                ));
            }
        }
        Ok(index)
    }

    /// One line's step ids, inflating only the chunk that can hold it.
    /// `Ok(None)` when no step ran on that line.
    ///
    /// The first lookup into a chunk checks every record of it as
    /// [`load_all`](Self::load_all) checks them, without materialising their
    /// ids, and notes where each line's record starts. The chunk and the note
    /// are kept, so a later lookup into the same chunk decodes one record.
    ///
    /// Line 0 is looked up as line 1, the key a writer files a step
    /// registered at line 0 under (`internal-files.md` §"`step-map.ns`",
    /// "Reading").
    pub fn lookup(&mut self, path_id: u64, line: u32) -> Result<Option<Vec<u64>>, String> {
        let target = (path_id, line.max(1));
        let c = self.chunks.partition_point(|ch| ch.first <= target);
        if c == 0 {
            return Ok(None);
        }
        let c = c - 1;
        if self.cached_chunk != Some(c) {
            self.cached_chunk = None;
            inflate(&self.bytes, &self.chunks, c, self.form, &mut self.raw)?;
            let mut lines = std::mem::take(&mut self.lines);
            lines.clear();
            let scanned = self.each_record(c, &self.raw, None, |key, at| {
                lines.push((key, at));
                Ok(())
            });
            self.lines = lines;
            scanned?;
            self.cached_chunk = Some(c);
        }
        let Ok(i) = self.lines.binary_search_by(|(key, _)| key.cmp(&target)) else {
            return Ok(None);
        };
        let (key, mut at) = self.lines[i];
        let count = self.count(c, &self.raw, &mut at, key)?;
        let mut ids = Vec::with_capacity(count as usize);
        self.runs(c, &self.raw, &mut at, key, count, Some(&mut ids))?;
        Ok(Some(ids))
    }

    /// Scan the line records of chunk `c`, inflated in `raw`, checking
    /// everything that can be checked within a chunk. `seen` sees each key,
    /// and the offset of the record's `count` field, once its record has been
    /// checked. With an `index`, each record's key and ids are added to it;
    /// without one, the runs are checked without building any id.
    fn each_record(
        &self,
        c: usize,
        raw: &[u8],
        mut index: Option<&mut StepMapIndex>,
        mut seen: impl FnMut((u64, u32), usize) -> Result<(), String>,
    ) -> Result<(), String> {
        let name = STEP_MAP_FILE_NAME;
        let chunk = self.chunks[c];
        let mut pos = 0usize;
        let (mut path, mut line) = chunk.first;
        let mut first = true;
        while pos < raw.len() {
            let path_delta = varint(raw, &mut pos, c)?;
            let line_field = varint(raw, &mut pos, c)?;
            let key = if first {
                let key = (path.wrapping_add(path_delta), line_field as u32);
                if path_delta != 0 || line_field != chunk.first.1 as u64 {
                    return Err(format!(
                        "{name}: chunk {c}'s first record is ({}, {line_field}), not its table key {:?}",
                        key.0, chunk.first
                    ));
                }
                key
            } else if path_delta > 0 {
                (
                    path.checked_add(path_delta)
                        .ok_or_else(|| format!("{name}: chunk {c}: path id overflows"))?,
                    line_field as u32,
                )
            } else {
                if line_field == 0 {
                    return Err(format!(
                        "{name}: keys do not ascend strictly: chunk {c} repeats ({path}, {line}) with a line delta of 0"
                    ));
                }
                let next = (line as u64) + line_field;
                if next > u32::MAX as u64 {
                    return Err(format!(
                        "{name}: chunk {c}: a line delta takes ({path}, {line}) past the 32-bit line range"
                    ));
                }
                (path, next as u32)
            };
            first = false;
            (path, line) = key;
            let at = pos;
            let count = self.count(c, raw, &mut pos, key)?;
            match index.as_deref_mut() {
                Some(index) => {
                    index.ids.reserve(count as usize);
                    self.runs(c, raw, &mut pos, key, count, Some(&mut index.ids))?;
                    index.keys.push(key);
                    index.ends.push(index.ids.len());
                }
                None => self.runs(c, raw, &mut pos, key, count, None)?,
            }
            seen(key, at)?;
        }
        Ok(())
    }

    /// Read one line record's `count` from `raw` at `pos`, checking it.
    #[inline(always)]
    fn count(&self, c: usize, raw: &[u8], pos: &mut usize, key: (u64, u32)) -> Result<u64, String> {
        let name = STEP_MAP_FILE_NAME;
        let count = varint(raw, pos, c)?;
        if count == 0 {
            return Err(format!("{name}: chunk {c}: line {key:?} has a count of 0"));
        }
        // Bounded by the header before anything is allocated: a list
        // longer than the whole map is a count that disagrees with it.
        if count > self.step_count {
            return Err(format!(
                "{name}: chunk {c}: line {key:?} claims {count} steps, more than the header's step count of {}",
                self.step_count
            ));
        }
        Ok(count)
    }

    /// Read the runs of a line record whose `count` has been read, from `raw`
    /// at `pos`, checking them, and append its step ids to `ids` when given,
    /// which has room for `count` more.
    fn runs(&self, c: usize, raw: &[u8], pos: &mut usize, key: (u64, u32), count: u64, mut ids: Option<&mut Vec<u64>>) -> Result<(), String> {
        let name = STEP_MAP_FILE_NAME;
        // The last id so far; the first run's gaps count from -1.
        let mut prev: Option<u64> = None;
        let mut have = 0u64;
        while have < count {
            let gap = varint(raw, pos, c)?;
            let repeat = varint(raw, pos, c)?;
            if gap == 0 {
                return Err(format!("{name}: chunk {c}: line {key:?} has a gap of 0"));
            }
            if repeat == 0 {
                return Err(format!("{name}: chunk {c}: line {key:?} has a repeat of 0"));
            }
            if repeat > count - have {
                return Err(format!(
                    "{name}: chunk {c}: line {key:?}'s runs overshoot its count of {count} ({} after this run)",
                    have.saturating_add(repeat)
                ));
            }
            // The run's ids are `prev + gap * k` for `k` in `1..=repeat`; its
            // last one is checked once, so every id of the run fits.
            let overflow = || format!("{name}: chunk {c}: line {key:?}'s step ids overflow 64 bits");
            let first = match prev {
                None => gap - 1,
                Some(prev) => prev.checked_add(gap).ok_or_else(overflow)?,
            };
            let last = checked_product(gap, repeat - 1)
                .and_then(|span| first.checked_add(span))
                .ok_or_else(overflow)?;
            if let Some(ids) = ids.as_deref_mut() {
                push_run(ids, first, gap, repeat);
            }
            prev = Some(last);
            have += repeat;
        }
        Ok(())
    }
}
